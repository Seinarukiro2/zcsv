use crate::fast_pyobjects;
use crate::row::{Row, SharedData};
use pyo3::exceptions::{PyIndexError, PyKeyError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};
use std::io::Cursor;
use std::sync::Arc;

/// Build SharedData from raw CSV bytes. One allocation for all field data.
fn build_shared_data(
    bytes: &[u8],
    delimiter: u8,
    has_header: bool,
    skip_rows: usize,
    max_rows: Option<usize>,
    columns: Option<&[String]>,
    strict: bool,
) -> Result<(Arc<SharedData>, usize), String> {
    let mut rdr = simd_csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .flexible(!strict)
        .from_reader(Cursor::new(bytes));

    let headers: Vec<String> = if has_header {
        rdr.byte_headers()
            .map_err(|e| format!("CSV header error: {e}"))?
            .iter()
            .map(|h| String::from_utf8_lossy(h).into_owned())
            .collect()
    } else {
        Vec::new()
    };

    let col_indices: Option<Vec<usize>> = columns.map(|cols| {
        cols.iter()
            .filter_map(|name| headers.iter().position(|h| h == name))
            .collect()
    });

    let field_names = match &col_indices {
        Some(indices) => indices.iter().map(|&i| headers[i].clone()).collect(),
        None => headers.clone(),
    };

    let mut data = Vec::with_capacity(bytes.len());
    let mut field_offsets: Vec<(u32, u32)> = Vec::new();
    let mut row_bounds: Vec<(u32, u16)> = Vec::new();
    let mut skipped = 0;

    for result in rdr.byte_records() {
        let record = result.map_err(|e| format!("CSV parse error: {e}"))?;
        if skipped < skip_rows { skipped += 1; continue; }
        if max_rows.is_some_and(|max| row_bounds.len() >= max) { break; }

        let first_field = field_offsets.len() as u32;
        let mut num_fields = 0u16;

        match &col_indices {
            Some(indices) => {
                for &i in indices {
                    let field = record.get(i).unwrap_or(&[]);
                    let start = data.len() as u32;
                    data.extend_from_slice(field);
                    field_offsets.push((start, data.len() as u32));
                    num_fields += 1;
                }
            }
            None => {
                for field in record.iter() {
                    let start = data.len() as u32;
                    data.extend_from_slice(field);
                    field_offsets.push((start, data.len() as u32));
                    num_fields += 1;
                }
            }
        }
        row_bounds.push((first_field, num_fields));
    }

    let num_rows = row_bounds.len();
    Ok((Arc::new(SharedData { data, field_offsets, row_bounds, field_names }), num_rows))
}

/// String-based parse for zcsv.read() type inference path.
pub fn parse_csv_to_strings(
    bytes: &[u8], delimiter: u8, has_header: bool, skip_rows: usize,
    max_rows: Option<usize>, columns: Option<&[String]>, strict: bool,
) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let (shared, num_rows) = build_shared_data(bytes, delimiter, has_header, skip_rows, max_rows, columns, strict)?;
    let headers = shared.field_names.clone();
    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let n = shared.num_fields(row_idx as u32) as usize;
        let row: Vec<String> = (0..n)
            .map(|i| String::from_utf8_lossy(shared.field_bytes(row_idx as u32, i as u16)).into_owned())
            .collect();
        rows.push(row);
    }
    Ok((headers, rows))
}

// ─── Streaming Reader ───
// cursor pattern: __next__ returns &self (INCREF, ~10ns) instead of new Row (Py::new, ~900ns).
// __getitem__ on reader accesses current row's fields.

#[derive(FromPyObject)]
enum FieldIndex {
    #[pyo3(transparent)]
    Int(isize),
    #[pyo3(transparent)]
    Str(String),
}

#[pyclass(sequence)]
pub struct PyReader {
    shared: Arc<SharedData>,
    num_rows: usize,
    /// Current row index (the row returned by last __next__)
    current_row: usize,
    index: usize,
    line_num: usize,
}

#[pymethods]
impl PyReader {
    #[new]
    #[pyo3(signature = (file_obj, delimiter = ",", quotechar = "\"", strict = false, safe = false))]
    fn new(
        py: Python<'_>, file_obj: PyObject, delimiter: &str,
        quotechar: &str, strict: bool, safe: bool,
    ) -> PyResult<Self> {
        let _ = (quotechar, safe);
        let delim = delimiter.as_bytes().first().copied().unwrap_or(b',');
        let content: String = file_obj.call_method0(py, "read")?.extract(py)?;

        let (shared, num_rows) = py.allow_threads(|| {
            build_shared_data(content.as_bytes(), delim, false, 0, None, None, strict)
        }).map_err(pyo3::exceptions::PyValueError::new_err)?;

        Ok(PyReader { shared, num_rows, current_row: 0, index: 0, line_num: 0 })
    }

    fn set_field_names(&mut self, names: Vec<String>) {
        let new = self.shared.clone_with_names(names);
        self.shared = Arc::new(new);
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }

    /// Returns self (cursor) — zero allocation. Just bumps current_row.
    fn __next__(slf: &Bound<'_, Self>) -> PyResult<Option<PyObject>> {
        let mut inner = slf.borrow_mut();
        if inner.index >= inner.num_rows {
            return Ok(None);
        }
        inner.current_row = inner.index;
        inner.index += 1;
        inner.line_num += 1;
        drop(inner); // release borrow before cloning
        // Return ref to self — just Py_INCREF
        Ok(Some(slf.clone().into_any().unbind()))
    }

    /// Access field of current row by index or name.
    fn __getitem__(&self, py: Python<'_>, idx: FieldIndex) -> PyResult<PyObject> {
        let row_idx = self.current_row as u32;
        let nf = self.shared.num_fields(row_idx);
        match idx {
            FieldIndex::Int(i) => {
                let len = nf as isize;
                let r = if i < 0 { len + i } else { i };
                if r < 0 || r >= len {
                    return Err(PyIndexError::new_err("index out of range"));
                }
                self.make_pystring(py, row_idx, r as u16)
            }
            FieldIndex::Str(key) => {
                let idx = self.shared.field_names.iter().position(|n| n == &key)
                    .ok_or_else(|| PyKeyError::new_err(key))?;
                self.make_pystring(py, row_idx, idx as u16)
            }
        }
    }

    fn __len__(&self) -> usize {
        self.shared.num_fields(self.current_row as u32) as usize
    }

    fn __repr__(&self) -> String {
        let row_idx = self.current_row as u32;
        let nf = self.shared.num_fields(row_idx) as usize;
        let fields: Vec<String> = (0..nf)
            .map(|i| format!("'{}'", String::from_utf8_lossy(self.shared.field_bytes(row_idx, i as u16))))
            .collect();
        format!("[{}]", fields.join(", "))
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(list) = other.downcast::<PyList>() {
            let nf = self.shared.num_fields(self.current_row as u32) as usize;
            if list.len() != nf { return Ok(false); }
            for (i, item) in list.iter().enumerate() {
                let s: String = item.extract()?;
                let b = self.shared.field_bytes(self.current_row as u32, i as u16);
                if String::from_utf8_lossy(b).as_ref() != s.as_str() { return Ok(false); }
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn to_list(&self, py: Python<'_>) -> PyResult<PyObject> {
        let row_idx = self.current_row as u32;
        let nf = self.shared.num_fields(row_idx) as usize;
        unsafe {
            let list = ffi::PyList_New(nf as isize);
            for i in 0..nf {
                let s = fast_pyobjects::fast_pystring(self.shared.field_bytes(row_idx, i as u16));
                ffi::PyList_SET_ITEM(list, i as isize, s);
            }
            Ok(PyObject::from_owned_ptr(py, list))
        }
    }

    fn keys(&self, py: Python<'_>) -> PyResult<PyObject> {
        if self.shared.field_names.is_empty() {
            return Err(pyo3::exceptions::PyAttributeError::new_err("no field names"));
        }
        let list = PyList::new(py, self.shared.field_names.iter().map(|n| PyString::new(py, n)))?;
        Ok(list.into_any().unbind())
    }

    fn values(&self, py: Python<'_>) -> PyResult<PyObject> {
        let row_idx = self.current_row as u32;
        let nf = self.shared.num_fields(row_idx) as usize;
        let mut vals = Vec::with_capacity(nf);
        for i in 0..nf { vals.push(self.make_pystring(py, row_idx, i as u16)?); }
        let list = PyList::new(py, vals.iter().map(|v| v.bind(py)))?;
        Ok(list.into_any().unbind())
    }

    fn items(&self, py: Python<'_>) -> PyResult<PyObject> {
        if self.shared.field_names.is_empty() {
            return Err(pyo3::exceptions::PyAttributeError::new_err("no field names"));
        }
        let row_idx = self.current_row as u32;
        let nf = self.shared.num_fields(row_idx) as usize;
        let mut tuples = Vec::with_capacity(nf);
        for i in 0..nf {
            let key = PyString::new(py, &self.shared.field_names[i]);
            let val = self.make_pystring(py, row_idx, i as u16)?;
            let tuple = PyTuple::new(py, [key.as_any(), val.bind(py)])?;
            tuples.push(tuple.into_any().unbind());
        }
        let list = PyList::new(py, tuples.iter().map(|t| t.bind(py)))?;
        Ok(list.into_any().unbind())
    }

    #[pyo3(signature = (key, default = None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<PyObject>) -> PyResult<PyObject> {
        match self.shared.field_names.iter().position(|n| n == key) {
            Some(idx) => self.make_pystring(py, self.current_row as u32, idx as u16),
            None => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    fn __contains__(&self, key: &str) -> bool {
        self.shared.field_names.iter().any(|n| n == key)
    }

    /// Snapshot current row as standalone Row object.
    fn snapshot(&self, py: Python<'_>) -> PyResult<PyObject> {
        let row = Row::new(Arc::clone(&self.shared), self.current_row as u32);
        Ok(Py::new(py, row)?.into_any())
    }

    /// Batch as list[list[str]] via raw FFI — for DictReader fallback.
    #[pyo3(signature = (n = 1000))]
    fn fetch_many_lists(&mut self, py: Python<'_>, n: usize) -> PyResult<PyObject> {
        let end = (self.index + n).min(self.num_rows);
        let count = end - self.index;
        unsafe {
            let outer = ffi::PyList_New(count as isize);
            if outer.is_null() { return Err(pyo3::exceptions::PyMemoryError::new_err("alloc")); }
            for (out_idx, row_idx) in (self.index..end).enumerate() {
                let nf = self.shared.num_fields(row_idx as u32) as usize;
                let inner = ffi::PyList_New(nf as isize);
                for fi in 0..nf {
                    let bytes = self.shared.field_bytes(row_idx as u32, fi as u16);
                    ffi::PyList_SET_ITEM(inner, fi as isize, fast_pyobjects::fast_pystring(bytes));
                }
                ffi::PyList_SET_ITEM(outer, out_idx as isize, inner);
            }
            self.index = end;
            self.line_num += count;
            Ok(PyObject::from_owned_ptr(py, outer))
        }
    }

    #[getter]
    fn total_rows(&self) -> usize { self.num_rows }
    #[getter]
    fn line_num(&self) -> usize { self.line_num }
}

impl PyReader {
    #[inline]
    fn make_pystring(&self, py: Python<'_>, row_idx: u32, field_idx: u16) -> PyResult<PyObject> {
        let bytes = self.shared.field_bytes(row_idx, field_idx);
        let ptr = unsafe { fast_pyobjects::fast_pystring(bytes) };
        if ptr.is_null() { return Err(pyo3::exceptions::PyMemoryError::new_err("alloc")); }
        Ok(unsafe { PyObject::from_owned_ptr(py, ptr) })
    }
}

impl SharedData {
    pub(crate) fn clone_with_names(&self, names: Vec<String>) -> SharedData {
        SharedData {
            data: self.data.clone(),
            field_offsets: self.field_offsets.clone(),
            row_bounds: self.row_bounds.clone(),
            field_names: names,
        }
    }
}
