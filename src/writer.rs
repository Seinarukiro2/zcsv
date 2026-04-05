use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::io::{BufWriter, Write};

const INJECTION_CHARS: &[u8] = b"=+-@\t\r";
const BUF_WRITER_CAP: usize = 256 * 1024;

/// Check if a byte slice starts with an injection char.
#[inline]
fn needs_sanitize(s: &[u8]) -> bool {
    !s.is_empty() && INJECTION_CHARS.contains(&s[0])
}

/// Append a field to output buffer, with quoting + optional sanitization.
/// Zero-copy: reads directly from the source slice.
#[inline]
fn append_field(out: &mut Vec<u8>, field: &[u8], delim: u8, quote: u8, quoting: u32, safe: bool) {
    let s = if safe && needs_sanitize(field) {
        // Prefix with ' — need temp allocation only for injection cases
        let mut tmp = Vec::with_capacity(field.len() + 1);
        tmp.push(b'\'');
        tmp.extend_from_slice(field);
        append_field_inner(out, &tmp, delim, quote, quoting);
        return;
    } else {
        field
    };
    append_field_inner(out, s, delim, quote, quoting);
}

#[inline]
fn append_field_inner(out: &mut Vec<u8>, s: &[u8], delim: u8, quote: u8, quoting: u32) {
    match quoting {
        1 => {
            // QUOTE_ALL
            out.push(quote);
            for &b in s {
                if b == quote { out.push(quote); }
                out.push(b);
            }
            out.push(quote);
        }
        3 => {
            // QUOTE_NONE
            out.extend_from_slice(s);
        }
        _ => {
            // QUOTE_MINIMAL (0) or QUOTE_NONNUMERIC (2)
            let needs_quote = s.contains(&delim) || s.contains(&quote)
                || s.contains(&b'\n') || s.contains(&b'\r');
            let force_quote = quoting == 2 && std::str::from_utf8(s).map_or(true, |v| v.parse::<f64>().is_err());

            if needs_quote || force_quote {
                out.push(quote);
                for &b in s {
                    if b == quote { out.push(quote); }
                    out.push(b);
                }
                out.push(quote);
            } else {
                out.extend_from_slice(s);
            }
        }
    }
}

/// Read a Python string as &[u8] via raw FFI — zero copy.
/// Uses PyUnicode_AsUTF8AndSize to get a pointer to Python's internal buffer.
///
/// # Safety
/// Caller must hold the GIL. `obj` must be a PyUnicode object.
/// The returned slice is valid as long as the Python string is alive.
#[inline]
unsafe fn pystring_as_bytes(obj: *mut ffi::PyObject) -> &'static [u8] {
    unsafe {
        let mut size: ffi::Py_ssize_t = 0;
        let ptr = ffi::PyUnicode_AsUTF8AndSize(obj, &mut size);
        if ptr.is_null() {
            return b"";
        }
        std::slice::from_raw_parts(ptr as *const u8, size as usize)
    }
}

/// Write list[dict] to CSV file (high-level API)
pub fn write_dicts_to_csv(
    path: &str,
    data: &Bound<'_, PyList>,
    delimiter: u8,
    safe: bool,
    _strict: bool,
) -> PyResult<()> {
    if data.is_empty() {
        std::fs::write(path, "")
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        return Ok(());
    }

    let first = data.get_item(0)?;
    let first_dict: &Bound<'_, PyDict> = first.downcast()
        .map_err(|_| pyo3::exceptions::PyTypeError::new_err("Expected list of dicts"))?;

    let fieldnames: Vec<String> = first_dict
        .keys()
        .iter()
        .map(|k| k.extract::<String>())
        .collect::<PyResult<Vec<String>>>()?;

    let file = std::fs::File::create(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let mut wtr = BufWriter::with_capacity(BUF_WRITER_CAP, file);

    // Write header
    for (i, name) in fieldnames.iter().enumerate() {
        if i > 0 { wtr.write_all(&[delimiter]).ok(); }
        wtr.write_all(name.as_bytes()).ok();
    }
    wtr.write_all(b"\n").ok();

    // Write rows
    for item in data.iter() {
        let dict: &Bound<'_, PyDict> = item.downcast()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("Expected dict"))?;

        for (i, key) in fieldnames.iter().enumerate() {
            if i > 0 { wtr.write_all(&[delimiter]).ok(); }
            let val = dict
                .get_item(key)
                .ok()
                .flatten()
                .map(|v| v.str().map(|s| s.to_string()).unwrap_or_default())
                .unwrap_or_default();
            let bytes = val.as_bytes();
            if safe && needs_sanitize(bytes) {
                wtr.write_all(b"'").ok();
            }
            wtr.write_all(bytes).ok();
        }
        wtr.write_all(b"\n").ok();
    }

    wtr.flush().map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    Ok(())
}

// ─── Streaming Writer ───

#[pyclass]
pub struct PyWriter {
    file_obj: PyObject,
    delimiter: u8,
    quotechar: u8,
    quoting: u32,
    lineterminator: Vec<u8>,
    safe: bool,
    buffer: Vec<u8>,
}

#[pymethods]
impl PyWriter {
    #[new]
    #[pyo3(signature = (
        file_obj,
        delimiter = ",",
        quotechar = "\"",
        quoting = 0,
        lineterminator = "\r\n",
        safe = false,
    ))]
    fn new(
        file_obj: PyObject, delimiter: &str, quotechar: &str,
        quoting: u32, lineterminator: &str, safe: bool,
    ) -> PyResult<Self> {
        Ok(PyWriter {
            file_obj,
            delimiter: delimiter.as_bytes().first().copied().unwrap_or(b','),
            quotechar: quotechar.as_bytes().first().copied().unwrap_or(b'"'),
            quoting,
            lineterminator: lineterminator.as_bytes().to_vec(),
            safe,
            buffer: Vec::with_capacity(BUF_WRITER_CAP),
        })
    }

    /// Write a single row — flush immediately for stdlib compat.
    fn writerow(&mut self, py: Python<'_>, row: &Bound<'_, PyAny>) -> PyResult<()> {
        unsafe {
            self.serialize_row_ffi(row.as_ptr())?;
        }
        self.flush_buffer(py)?;
        Ok(())
    }

    /// Write multiple rows — raw FFI extraction, GIL-released serialization, single write.
    fn writerows(&mut self, py: Python<'_>, rows: &Bound<'_, PyAny>) -> PyResult<()> {
        let list_ptr = rows.as_ptr();

        // Phase 1: Extract all string pointers via raw FFI (GIL held, zero-copy)
        // We collect &[u8] slices pointing into Python's string internals.
        // These are valid as long as the Python objects are alive (the list holds refs).
        let (row_data, num_cols_per_row) = unsafe {
            self.extract_rows_ffi(list_ptr)?
        };

        if row_data.is_empty() {
            return Ok(());
        }

        // Phase 2: Serialize with GIL released — pure Rust byte manipulation
        let delim = self.delimiter;
        let quote = self.quotechar;
        let quoting = self.quoting;
        let safe = self.safe;
        let lt = &self.lineterminator;

        let output = py.allow_threads(|| {
            let est = row_data.len() * 12;
            let mut out = Vec::with_capacity(est);
            let mut field_idx = 0;

            for &ncols in &num_cols_per_row {
                for c in 0..ncols {
                    if c > 0 { out.push(delim); }
                    append_field(&mut out, row_data[field_idx], delim, quote, quoting, safe);
                    field_idx += 1;
                }
                out.extend_from_slice(lt);
            }
            out
        });

        // Phase 3: Single write call
        // Convert bytes to Python str for text-mode file objects
        let py_str = unsafe {
            let ptr = ffi::PyUnicode_FromStringAndSize(
                output.as_ptr() as *const i8,
                output.len() as ffi::Py_ssize_t,
            );
            if ptr.is_null() {
                return Err(pyo3::exceptions::PyMemoryError::new_err("alloc"));
            }
            PyObject::from_owned_ptr(py, ptr)
        };
        self.file_obj.call_method1(py, "write", (py_str,))?;

        Ok(())
    }

    fn __del__(&mut self, py: Python<'_>) -> PyResult<()> {
        self.flush_buffer(py)
    }
}

impl PyWriter {
    /// Serialize one row from a raw PyObject pointer into self.buffer.
    /// Uses PyList_GET_ITEM + PyUnicode_AsUTF8AndSize for zero-copy field reads.
    unsafe fn serialize_row_ffi(&mut self, row_ptr: *mut ffi::PyObject) -> PyResult<()> {
        unsafe {
            let is_list = ffi::PyList_Check(row_ptr) != 0;
            let len = if is_list {
                ffi::PyList_Size(row_ptr) as usize
            } else {
                ffi::PyObject_Length(row_ptr) as usize
            };

            for i in 0..len {
                if i > 0 { self.buffer.push(self.delimiter); }

                let item = if is_list {
                    ffi::PyList_GET_ITEM(row_ptr, i as ffi::Py_ssize_t)
                } else {
                    ffi::PySequence_GetItem(row_ptr, i as ffi::Py_ssize_t)
                };

                let bytes = if ffi::PyUnicode_Check(item) != 0 {
                    pystring_as_bytes(item)
                } else {
                    let str_obj = ffi::PyObject_Str(item);
                    let b = pystring_as_bytes(str_obj);
                    ffi::Py_DECREF(str_obj);
                    b
                };

                if !is_list {
                    ffi::Py_DECREF(item);
                }

                append_field(&mut self.buffer, bytes, self.delimiter, self.quotechar, self.quoting, self.safe);
            }
            self.buffer.extend_from_slice(&self.lineterminator);
            Ok(())
        }
    }

    /// Extract all rows from a Python list as &[u8] slices via raw FFI.
    /// Returns (flat vec of field byte slices, vec of column counts per row).
    /// Slices point into Python's internal string buffers — valid while GIL held.
    unsafe fn extract_rows_ffi(
        &self,
        list_ptr: *mut ffi::PyObject,
    ) -> PyResult<(Vec<&'static [u8]>, Vec<usize>)> {
        unsafe {
            if ffi::PyList_Check(list_ptr) == 0 {
                return Err(pyo3::exceptions::PyTypeError::new_err("Expected list"));
            }

            let nrows = ffi::PyList_Size(list_ptr) as usize;
            let mut fields: Vec<&[u8]> = Vec::with_capacity(nrows * 10);
            let mut cols_per_row: Vec<usize> = Vec::with_capacity(nrows);

            for r in 0..nrows {
                let row_ptr = ffi::PyList_GET_ITEM(list_ptr, r as ffi::Py_ssize_t);
                let is_list = ffi::PyList_Check(row_ptr) != 0;
                let ncols = if is_list {
                    ffi::PyList_Size(row_ptr) as usize
                } else {
                    ffi::PyObject_Length(row_ptr) as usize
                };

                for c in 0..ncols {
                    let item = if is_list {
                        ffi::PyList_GET_ITEM(row_ptr, c as ffi::Py_ssize_t)
                    } else {
                        ffi::PySequence_GetItem(row_ptr, c as ffi::Py_ssize_t)
                    };

                    let bytes = if ffi::PyUnicode_Check(item) != 0 {
                        pystring_as_bytes(item)
                    } else {
                        let str_obj = ffi::PyObject_Str(item);
                        let b = pystring_as_bytes(str_obj);
                        ffi::Py_DECREF(str_obj);
                        b
                    };

                    if !is_list { ffi::Py_DECREF(item); }
                    fields.push(bytes);
                }
                cols_per_row.push(ncols);
            }

            Ok((fields, cols_per_row))
        }
    }

    fn flush_buffer(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let py_str = unsafe {
            let ptr = ffi::PyUnicode_FromStringAndSize(
                self.buffer.as_ptr() as *const i8,
                self.buffer.len() as ffi::Py_ssize_t,
            );
            if ptr.is_null() {
                return Err(pyo3::exceptions::PyMemoryError::new_err("alloc"));
            }
            PyObject::from_owned_ptr(py, ptr)
        };
        self.file_obj.call_method1(py, "write", (py_str,))?;
        self.buffer.clear();
        Ok(())
    }
}
