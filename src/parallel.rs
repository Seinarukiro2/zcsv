use crate::fast_pyobjects;
use crate::schema::ColumnType;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};
use rayon::prelude::*;
use std::collections::HashMap;

/// String dedup cache — avoids creating duplicate Python strings.
/// For CSV data with repeating values (categories, countries, booleans).
struct StringCache {
    cache: HashMap<Vec<u8>, *mut ffi::PyObject>,
    hits: usize,
    misses: usize,
    enabled: bool,
}

impl StringCache {
    fn new() -> Self {
        StringCache {
            cache: HashMap::with_capacity(256),
            hits: 0,
            misses: 0,
            enabled: true,
        }
    }

    /// Get cached PyString or create new one. Returns a NEW reference.
    #[inline]
    unsafe fn get_or_create(&mut self, bytes: &[u8]) -> *mut ffi::PyObject {
        unsafe {
            if !self.enabled {
                return fast_pyobjects::fast_pystring(bytes);
            }

            if let Some(&cached) = self.cache.get(bytes) {
                self.hits += 1;
                ffi::Py_INCREF(cached);
                return cached;
            }

            self.misses += 1;
            let ptr = fast_pyobjects::fast_pystring(bytes);
            if !ptr.is_null() {
                ffi::Py_INCREF(ptr);
                self.cache.insert(bytes.to_vec(), ptr);
            }

            let total = self.hits + self.misses;
            if total == 200 && self.hits * 5 < total {
                self.enabled = false;
                for &p in self.cache.values() {
                    ffi::Py_DECREF(p);
                }
                self.cache.clear();
            }

            ptr
        }
    }
}

impl Drop for StringCache {
    fn drop(&mut self) {
        // We must release cache refs when the cache is dropped.
        // This is safe because Drop only runs when GIL is held
        // (StringCache is always created/dropped within a GIL block).
        if !self.cache.is_empty() {
            // Safety: GIL must be held
            unsafe {
                for &ptr in self.cache.values() {
                    ffi::Py_DECREF(ptr);
                }
            }
        }
    }
}

/// Convert raw string rows to list[dict] with typed values.
/// Uses raw FFI for Python object creation + string dedup cache.
pub fn convert_to_dicts(
    py: Python<'_>,
    headers: &[String],
    rows: &[Vec<String>],
    type_map: &HashMap<String, ColumnType>,
    null_values: &[String],
    n_threads: usize,
) -> PyResult<PyObject> {
    if rows.is_empty() {
        let empty_list = PyList::empty(py);
        return Ok(empty_list.into_any().unbind());
    }

    if n_threads > 0 {
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(n_threads)
            .build_global();
    }

    let num_cols = headers.len();
    let num_rows = rows.len();

    // Intern header keys — keep Bound alive to prevent dangling pointers
    let interned_bounds: Vec<Bound<'_, PyString>> = headers
        .iter()
        .map(|h| PyString::intern(py, h))
        .collect();
    let interned_keys: Vec<*mut ffi::PyObject> = interned_bounds
        .iter()
        .map(|s| s.as_ptr())
        .collect();

    let col_types: Vec<&ColumnType> = headers
        .iter()
        .map(|h| type_map.get(h).unwrap_or(&ColumnType::String))
        .collect();

    // Phase 1: Parse all values in parallel (no GIL needed)
    let parsed_columns: Vec<Vec<ParsedValue>> = if num_rows > 500 && num_cols > 1 {
        let columns: Vec<Vec<&str>> = (0..num_cols)
            .map(|col_idx| {
                rows.iter()
                    .map(|row| row.get(col_idx).map(|s| s.as_str()).unwrap_or(""))
                    .collect()
            })
            .collect();

        columns
            .par_iter()
            .enumerate()
            .map(|(col_idx, col_data)| {
                col_data
                    .iter()
                    .map(|val| parse_value(val, col_types[col_idx], null_values))
                    .collect()
            })
            .collect()
    } else {
        (0..num_cols)
            .map(|col_idx| {
                rows.iter()
                    .map(|row| {
                        let val = row.get(col_idx).map(|s| s.as_str()).unwrap_or("");
                        parse_value(val, col_types[col_idx], null_values)
                    })
                    .collect()
            })
            .collect()
    };

    // Phase 2: Build list[dict] via raw FFI with string dedup cache
    unsafe {
        let mut string_cache = StringCache::new();

        let list = ffi::PyList_New(num_rows as isize);
        if list.is_null() {
            return Err(pyo3::exceptions::PyMemoryError::new_err("list alloc failed"));
        }

        for row_idx in 0..num_rows {
            let dict = ffi::PyDict_New();
            if dict.is_null() {
                ffi::Py_DECREF(list);
                return Err(pyo3::exceptions::PyMemoryError::new_err("dict alloc failed"));
            }

            for (col_idx, col) in parsed_columns.iter().enumerate() {
                let val_ptr = col[row_idx].to_ffi_object(&mut string_cache);

                if val_ptr.is_null() {
                    ffi::Py_DECREF(dict);
                    ffi::Py_DECREF(list);
                    return Err(pyo3::exceptions::PyMemoryError::new_err("value alloc failed"));
                }

                ffi::PyDict_SetItem(dict, interned_keys[col_idx], val_ptr);
                ffi::Py_DECREF(val_ptr);
            }

            ffi::PyList_SET_ITEM(list, row_idx as isize, dict);
        }

        Ok(PyObject::from_owned_ptr(py, list))
    }
}

/// Intermediate parsed value — no GIL needed, Send+Sync for rayon.
#[derive(Debug)]
pub(crate) enum ParsedValue {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

impl ParsedValue {
    /// Convert to raw CPython object pointer. Returns a NEW reference.
    #[inline]
    unsafe fn to_ffi_object(&self, cache: &mut StringCache) -> *mut ffi::PyObject {
        unsafe {
            match self {
                ParsedValue::Null => {
                    let p = fast_pyobjects::fast_pynone();
                    ffi::Py_INCREF(p);
                    p
                }
                ParsedValue::Int(v) => fast_pyobjects::fast_pyint(*v),
                ParsedValue::Float(v) => fast_pyobjects::fast_pyfloat(*v),
                ParsedValue::Bool(v) => {
                    let p = fast_pyobjects::fast_pybool(*v);
                    ffi::Py_INCREF(p);
                    p
                }
                ParsedValue::Str(v) => cache.get_or_create(v.as_bytes()),
            }
        }
    }
}

pub(crate) fn parse_value(value: &str, col_type: &ColumnType, null_values: &[String]) -> ParsedValue {
    let trimmed = value.trim();

    if null_values.iter().any(|nv| nv == trimmed) {
        return ParsedValue::Null;
    }

    match col_type {
        ColumnType::Int => trimmed
            .parse::<i64>()
            .map(ParsedValue::Int)
            .unwrap_or_else(|_| ParsedValue::Str(trimmed.to_string())),
        ColumnType::Float => trimmed
            .parse::<f64>()
            .map(ParsedValue::Float)
            .unwrap_or_else(|_| ParsedValue::Str(trimmed.to_string())),
        ColumnType::Bool => match trimmed.to_lowercase().as_str() {
            "true" | "yes" | "1" => ParsedValue::Bool(true),
            "false" | "no" | "0" => ParsedValue::Bool(false),
            _ => ParsedValue::Str(trimmed.to_string()),
        },
        ColumnType::String => ParsedValue::Str(trimmed.to_string()),
    }
}
