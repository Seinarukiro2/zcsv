//! Zero-copy lazy Row.
//! All CSV data lives in one SharedData (Arc). Row is just an index into it.
//! No per-row heap allocation except the pyclass shell itself (freelist pooled).

use crate::fast_pyobjects;
use pyo3::exceptions::{PyIndexError, PyKeyError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};
use std::sync::Arc;

/// All parsed CSV data in one contiguous buffer.
pub struct SharedData {
    /// All field bytes concatenated.
    pub data: Vec<u8>,
    /// (start, end) byte offsets into `data` for each field, flattened across all rows.
    pub field_offsets: Vec<(u32, u32)>,
    /// row_bounds[row_idx] = (first_field_idx, num_fields) into field_offsets.
    pub row_bounds: Vec<(u32, u16)>,
    /// Field names for dict-like access (empty if no header).
    pub field_names: Vec<String>,
}

impl SharedData {
    /// Get raw bytes for field `field_idx` of row `row_idx`.
    #[inline]
    pub fn field_bytes(&self, row_idx: u32, field_idx: u16) -> &[u8] {
        let (start_field, num_fields) = self.row_bounds[row_idx as usize];
        debug_assert!((field_idx) < num_fields);
        let (s, e) = self.field_offsets[(start_field as usize) + (field_idx as usize)];
        &self.data[s as usize..e as usize]
    }

    #[inline]
    pub fn num_fields(&self, row_idx: u32) -> u16 {
        self.row_bounds[row_idx as usize].1
    }
}

/// Lightweight CSV row — 16 bytes on stack (Arc + u32 + padding).
/// No per-row heap allocation for field data. Freelist-pooled pyclass.
#[pyclass(sequence, freelist = 4096)]
pub struct Row {
    shared: Arc<SharedData>,
    row_idx: u32,
}

unsafe impl Send for Row {}
unsafe impl Sync for Row {}

impl Row {
    pub fn new(shared: Arc<SharedData>, row_idx: u32) -> Self {
        Row { shared, row_idx }
    }

    #[inline]
    fn num_fields(&self) -> usize {
        self.shared.num_fields(self.row_idx) as usize
    }

    #[inline]
    fn resolve(&self, idx: isize) -> Option<usize> {
        let len = self.num_fields() as isize;
        let r = if idx < 0 { len + idx } else { idx };
        if r >= 0 && r < len { Some(r as usize) } else { None }
    }

    /// Create Python string for field — no caching, just raw FFI.
    #[inline]
    fn field_to_py(&self, py: Python<'_>, field_idx: u16) -> PyResult<PyObject> {
        let bytes = self.shared.field_bytes(self.row_idx, field_idx);
        let ptr = unsafe { fast_pyobjects::fast_pystring(bytes) };
        if ptr.is_null() {
            return Err(pyo3::exceptions::PyMemoryError::new_err("string alloc"));
        }
        Ok(unsafe { PyObject::from_owned_ptr(py, ptr) })
    }
}

#[derive(FromPyObject)]
enum RowIndex {
    #[pyo3(transparent)]
    Int(isize),
    #[pyo3(transparent)]
    Str(String),
}

#[pymethods]
impl Row {
    fn __len__(&self) -> usize { self.num_fields() }

    fn __getitem__(&self, py: Python<'_>, idx: RowIndex) -> PyResult<PyObject> {
        match idx {
            RowIndex::Int(i) => {
                let r = self.resolve(i)
                    .ok_or_else(|| PyIndexError::new_err("index out of range"))?;
                self.field_to_py(py, r as u16)
            }
            RowIndex::Str(key) => {
                let idx = self.shared.field_names.iter().position(|n| n == &key)
                    .ok_or_else(|| PyKeyError::new_err(key))?;
                self.field_to_py(py, idx as u16)
            }
        }
    }

    fn __iter__(slf: PyRef<'_, Self>) -> RowIterator {
        RowIterator { row: slf.into(), index: 0 }
    }

    fn __repr__(&self) -> String {
        let n = self.num_fields();
        let fields: Vec<String> = (0..n)
            .map(|i| {
                let b = self.shared.field_bytes(self.row_idx, i as u16);
                format!("'{}'", String::from_utf8_lossy(b))
            })
            .collect();
        format!("[{}]", fields.join(", "))
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(list) = other.downcast::<PyList>() {
            let n = self.num_fields();
            if list.len() != n { return Ok(false); }
            for (i, item) in list.iter().enumerate() {
                let s: String = item.extract()?;
                let b = self.shared.field_bytes(self.row_idx, i as u16);
                if String::from_utf8_lossy(b).as_ref() != s.as_str() {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn to_list(&self, py: Python<'_>) -> PyResult<PyObject> {
        let n = self.num_fields();
        unsafe {
            let list = ffi::PyList_New(n as isize);
            if list.is_null() {
                return Err(pyo3::exceptions::PyMemoryError::new_err("alloc"));
            }
            for i in 0..n {
                let bytes = self.shared.field_bytes(self.row_idx, i as u16);
                let s = fast_pyobjects::fast_pystring(bytes);
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
        let n = self.num_fields();
        let mut vals = Vec::with_capacity(n);
        for i in 0..n {
            vals.push(self.field_to_py(py, i as u16)?);
        }
        let list = PyList::new(py, vals.iter().map(|v| v.bind(py)))?;
        Ok(list.into_any().unbind())
    }

    fn items(&self, py: Python<'_>) -> PyResult<PyObject> {
        if self.shared.field_names.is_empty() {
            return Err(pyo3::exceptions::PyAttributeError::new_err("no field names"));
        }
        let n = self.num_fields();
        let mut tuples = Vec::with_capacity(n);
        for i in 0..n {
            let key = PyString::new(py, &self.shared.field_names[i]);
            let val = self.field_to_py(py, i as u16)?;
            let tuple = PyTuple::new(py, [key.as_any(), val.bind(py)])?;
            tuples.push(tuple.into_any().unbind());
        }
        let list = PyList::new(py, tuples.iter().map(|t| t.bind(py)))?;
        Ok(list.into_any().unbind())
    }

    #[pyo3(signature = (key, default = None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<PyObject>) -> PyResult<PyObject> {
        match self.shared.field_names.iter().position(|n| n == key) {
            Some(idx) => self.field_to_py(py, idx as u16),
            None => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    fn __contains__(&self, key: &str) -> bool {
        self.shared.field_names.iter().any(|n| n == key)
    }
}

#[pyclass]
pub struct RowIterator {
    row: Py<Row>,
    index: usize,
}

#[pymethods]
impl RowIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let row = self.row.borrow(py);
        if self.index >= row.num_fields() { return Ok(None); }
        let val = row.field_to_py(py, self.index as u16)?;
        self.index += 1;
        Ok(Some(val))
    }
}
