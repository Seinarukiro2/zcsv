use crate::schema::ColumnType;
use pyo3::prelude::*;
use pyo3::types::PyBool;

/// Convert a string value to a typed Python object based on ColumnType.
#[allow(dead_code)]
pub fn convert_value(py: Python<'_>, value: &str, col_type: &ColumnType, null_values: &[String]) -> PyObject {
    let trimmed = value.trim();

    // Check for null
    if null_values.iter().any(|nv| nv == trimmed) {
        return py.None();
    }

    match col_type {
        ColumnType::Int => {
            if let Ok(v) = trimmed.parse::<i64>() {
                v.into_pyobject(py).unwrap().into_any().unbind()
            } else {
                trimmed.into_pyobject(py).unwrap().into_any().unbind()
            }
        }
        ColumnType::Float => {
            if let Ok(v) = trimmed.parse::<f64>() {
                v.into_pyobject(py).unwrap().into_any().unbind()
            } else {
                trimmed.into_pyobject(py).unwrap().into_any().unbind()
            }
        }
        ColumnType::Bool => {
            match trimmed.to_lowercase().as_str() {
                "true" | "yes" | "1" => PyBool::new(py, true).to_owned().into_any().unbind(),
                "false" | "no" | "0" => PyBool::new(py, false).to_owned().into_any().unbind(),
                _ => trimmed.into_pyobject(py).unwrap().into_any().unbind(),
            }
        }
        ColumnType::String => {
            trimmed.into_pyobject(py).unwrap().into_any().unbind()
        }
    }
}
