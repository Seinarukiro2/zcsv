use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

/// Column types we can infer
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int,
    Float,
    Bool,
    String,
}

impl ColumnType {
    pub fn from_str_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "int" | "integer" => Some(ColumnType::Int),
            "float" | "double" | "number" => Some(ColumnType::Float),
            "bool" | "boolean" => Some(ColumnType::Bool),
            "str" | "string" => Some(ColumnType::String),
            _ => None,
        }
    }
}

/// Parse schema from Python dict: {"col_name": int} or {"col_name": "int"}
pub fn schema_from_pydict(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, ColumnType>> {
    let mut map = HashMap::new();

    for (key, value) in dict.iter() {
        let col_name: String = key.extract()?;

        // Try extracting as Python type object (int, float, bool, str)
        let col_type = if let Ok(type_obj) = value.getattr("__name__") {
            let type_name: String = type_obj.extract()?;
            ColumnType::from_str_name(&type_name)
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(format!("Unknown type: {type_name}"))
                })?
        } else {
            // Try as string
            let type_name: String = value.extract()?;
            ColumnType::from_str_name(&type_name)
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(format!("Unknown type: {type_name}"))
                })?
        };

        map.insert(col_name, col_type);
    }

    Ok(map)
}

/// Infer column types by sampling rows.
/// Strategy: check first min(1000, total) rows per column.
/// If >95% parse as a type, use it. Otherwise fallback to String.
pub fn infer_types(
    headers: &[String],
    rows: &[Vec<String>],
    null_values: &[String],
) -> HashMap<String, ColumnType> {
    let sample_size = rows.len().min(1000);
    let mut result = HashMap::new();

    for (col_idx, header) in headers.iter().enumerate() {
        let mut int_count = 0usize;
        let mut float_count = 0usize;
        let mut bool_count = 0usize;
        let mut null_count = 0usize;
        let mut total = 0usize;

        for row in rows.iter().take(sample_size) {
            let val = match row.get(col_idx) {
                Some(v) => v.trim(),
                None => continue,
            };

            total += 1;

            if null_values.iter().any(|nv| nv == val) {
                null_count += 1;
                continue;
            }

            if val.parse::<i64>().is_ok() {
                int_count += 1;
            } else if val.parse::<f64>().is_ok() {
                float_count += 1;
            } else if matches!(val.to_lowercase().as_str(), "true" | "false" | "yes" | "no" | "1" | "0") {
                bool_count += 1;
            }
        }

        let non_null = total - null_count;
        if non_null == 0 {
            result.insert(header.clone(), ColumnType::String);
            continue;
        }

        let threshold = (non_null as f64 * 0.95) as usize;

        let col_type = if int_count >= threshold && float_count == 0 {
            // Pure int: all numeric values parse as int, none as float-only
            ColumnType::Int
        } else if (int_count + float_count) >= threshold {
            // Mixed int/float or pure float → promote to float
            ColumnType::Float
        } else if bool_count >= threshold {
            ColumnType::Bool
        } else {
            ColumnType::String
        };

        result.insert(header.clone(), col_type);
    }

    result
}
