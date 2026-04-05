use pyo3::exceptions::PyValueError;
use pyo3::PyResult;

/// Pure-Rust validation (no PyResult) — callable with GIL released.
pub fn validate_rfc4180_inner(headers: &[String], rows: &[Vec<String>]) -> Result<(), String> {
    let expected_fields = if headers.is_empty() {
        match rows.first() {
            Some(first) => first.len(),
            None => return Ok(()),
        }
    } else {
        headers.len()
    };

    for (i, row) in rows.iter().enumerate() {
        let line_num = if headers.is_empty() { i + 1 } else { i + 2 };

        if row.len() != expected_fields {
            return Err(format!(
                "RFC 4180 violation at line {line_num}: expected {expected_fields} fields, got {}",
                row.len()
            ));
        }
    }

    Ok(())
}

/// PyResult wrapper for direct Python calls.
#[allow(dead_code)]
pub fn validate_rfc4180(headers: &[String], rows: &[Vec<String>]) -> PyResult<()> {
    validate_rfc4180_inner(headers, rows)
        .map_err(|e| PyValueError::new_err(e))
}
