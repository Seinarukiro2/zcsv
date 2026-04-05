use crate::schema::ColumnType;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyString};
use rayon::prelude::*;
use std::collections::HashMap;

/// Convert raw string rows to list[dict] with typed values.
/// Uses rayon for parallel column parsing (GIL not needed for parsing).
/// Interned header keys to avoid repeated Python string allocation.
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

    // Configure rayon thread pool if needed
    if n_threads > 0 {
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(n_threads)
            .build_global();
    }

    let num_cols = headers.len();
    let num_rows = rows.len();

    // Intern header strings — create each Python string key ONCE,
    // reuse for all rows. This avoids N*M string allocations.
    let interned_keys: Vec<Py<PyString>> = headers
        .iter()
        .map(|h| PyString::intern(py, h).into())
        .collect();

    // Determine column types
    let col_types: Vec<&ColumnType> = headers
        .iter()
        .map(|h| type_map.get(h).unwrap_or(&ColumnType::String))
        .collect();

    // Phase 1: Parse all values in parallel (GIL released via rayon)
    // Transpose: rows → columns for parallel processing
    let parsed_columns: Vec<Vec<ParsedValue>> = if num_rows > 500 && num_cols > 1 {
        // Parallel column parsing
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
        // Sequential for small datasets
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

    // Phase 2: Build list[dict] with interned keys (GIL held)
    let result = PyList::empty(py);
    for row_idx in 0..num_rows {
        let dict = PyDict::new(py);
        for col_idx in 0..num_cols {
            let py_val = parsed_columns[col_idx][row_idx].to_pyobject(py);
            // Use interned key — no new Python string allocation
            dict.set_item(&interned_keys[col_idx], py_val)?;
        }
        result.append(dict)?;
    }

    Ok(result.into_any().unbind())
}

/// Intermediate parsed value — no GIL needed, Send+Sync for rayon.
#[derive(Debug)]
enum ParsedValue {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

impl ParsedValue {
    fn to_pyobject(&self, py: Python<'_>) -> PyObject {
        match self {
            ParsedValue::Null => py.None(),
            ParsedValue::Int(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
            ParsedValue::Float(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
            ParsedValue::Bool(v) => PyBool::new(py, *v).to_owned().into_any().unbind(),
            ParsedValue::Str(v) => PyString::new(py, v).into_any().unbind(),
        }
    }
}

fn parse_value(value: &str, col_type: &ColumnType, null_values: &[String]) -> ParsedValue {
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
