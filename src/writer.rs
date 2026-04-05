use csv::WriterBuilder;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};

/// Characters that trigger CSV injection (OWASP)
const INJECTION_CHARS: &[char] = &['=', '+', '-', '@', '\t', '\r'];

/// Sanitize a cell value to prevent CSV injection
#[inline]
fn sanitize_cell(value: &str) -> String {
    if value.is_empty() {
        return value.to_string();
    }
    if value.starts_with(INJECTION_CHARS) {
        format!("'{value}")
    } else {
        value.to_string()
    }
}

/// Quoting logic shared between streaming and high-level writer.
#[inline]
fn quote_field(value: &str, delim: &str, quote: char, quoting: u32) -> String {
    match quoting {
        1 => format!("{quote}{}{quote}", value.replace(quote, &format!("{quote}{quote}"))),
        3 => value.to_string(),
        _ => {
            let needs_quote = value.contains(delim)
                || value.contains(quote)
                || value.contains('\n')
                || value.contains('\r');
            let force_quote = quoting == 2 && value.parse::<f64>().is_err();

            if needs_quote || force_quote {
                format!("{quote}{}{quote}", value.replace(quote, &format!("{quote}{quote}")))
            } else {
                value.to_string()
            }
        }
    }
}

/// Format a single row into a CSV line (pure Rust, no GIL needed).
#[inline]
fn format_row_str(values: &[String], delim: &str, quote: char, quoting: u32, safe: bool, lineterminator: &str) -> String {
    let mut line = String::new();
    for (i, val) in values.iter().enumerate() {
        if i > 0 {
            line.push_str(delim);
        }
        let val = if safe { sanitize_cell(val) } else { val.clone() };
        line.push_str(&quote_field(&val, delim, quote, quoting));
    }
    line.push_str(lineterminator);
    line
}

/// Format many rows into a single CSV string (pure Rust, no GIL needed).
fn format_rows_bulk(rows: &[Vec<String>], delim: &str, quote: char, quoting: u32, safe: bool, lineterminator: &str) -> String {
    // Pre-estimate capacity: avg 10 chars per cell
    let est_cap = rows.len() * rows.first().map_or(10, |r| r.len() * 12);
    let mut out = String::with_capacity(est_cap);

    for values in rows {
        for (i, val) in values.iter().enumerate() {
            if i > 0 {
                out.push_str(delim);
            }
            if safe {
                let sanitized = sanitize_cell(val);
                out.push_str(&quote_field(&sanitized, delim, quote, quoting));
            } else {
                out.push_str(&quote_field(val, delim, quote, quoting));
            }
        }
        out.push_str(lineterminator);
    }
    out
}

/// Extract a row as Vec<String>, fast path for list[str].
fn extract_row_strings(row: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(list) = row.downcast::<PyList>() {
        let mut result = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(s) = item.downcast::<PyString>() {
                result.push(s.to_string());
            } else {
                result.push(item.str()?.to_string());
            }
        }
        return Ok(result);
    }
    let iter = row.try_iter()?;
    iter.map(|item| item.and_then(|v| v.str().map(|s| s.to_string())))
        .collect()
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

    let mut wtr = WriterBuilder::new()
        .delimiter(delimiter)
        .from_path(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    wtr.write_record(&fieldnames)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    for item in data.iter() {
        let dict: &Bound<'_, PyDict> = item.downcast()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("Expected dict"))?;

        let row: Vec<String> = fieldnames
            .iter()
            .map(|key| {
                let val = dict
                    .get_item(key)
                    .ok()
                    .flatten()
                    .map(|v| v.str().map(|s| s.to_string()).unwrap_or_default())
                    .unwrap_or_default();
                if safe { sanitize_cell(&val) } else { val }
            })
            .collect();

        wtr.write_record(&row)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    }

    wtr.flush()
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    Ok(())
}

// ─── Streaming Writer (PyO3 class for stdlib compat) ───

#[pyclass]
pub struct PyWriter {
    file_obj: PyObject,
    delimiter: u8,
    quotechar: u8,
    quoting: u32,
    lineterminator: String,
    safe: bool,
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
        file_obj: PyObject,
        delimiter: &str,
        quotechar: &str,
        quoting: u32,
        lineterminator: &str,
        safe: bool,
    ) -> PyResult<Self> {
        Ok(PyWriter {
            file_obj,
            delimiter: delimiter.as_bytes().first().copied().unwrap_or(b','),
            quotechar: quotechar.as_bytes().first().copied().unwrap_or(b'"'),
            quoting,
            lineterminator: lineterminator.to_string(),
            safe,
        })
    }

    /// Write a single row
    fn writerow(&self, py: Python<'_>, row: &Bound<'_, PyAny>) -> PyResult<()> {
        let values = extract_row_strings(row)?;
        let delim_bytes = [self.delimiter];
        let delim = std::str::from_utf8(&delim_bytes).unwrap_or(",");
        let quote = self.quotechar as char;
        let line = format_row_str(&values, delim, quote, self.quoting, self.safe, &self.lineterminator);
        self.file_obj.call_method1(py, "write", (line,))?;
        Ok(())
    }

    /// Write multiple rows — bulk extract via PyO3 FromPyObject, format in Rust, single write().
    fn writerows(&self, py: Python<'_>, rows: &Bound<'_, PyAny>) -> PyResult<()> {
        // Phase 1: Bulk extract — let PyO3 do optimized nested extraction
        // For list[list[str]], PyO3 uses CPython's C API to extract everything at once.
        let rust_rows: Vec<Vec<String>> = rows.extract()?;

        if rust_rows.is_empty() {
            return Ok(());
        }

        // Phase 2: Format entire CSV in Rust with GIL released
        let delim_bytes = [self.delimiter];
        let delim = std::str::from_utf8(&delim_bytes).unwrap_or(",");
        let quote = self.quotechar as char;
        let quoting = self.quoting;
        let safe = self.safe;
        let lt = &self.lineterminator;

        let output = py.allow_threads(|| {
            format_rows_bulk(&rust_rows, delim, quote, quoting, safe, lt)
        });

        // Phase 3: Single write() call
        self.file_obj.call_method1(py, "write", (output,))?;

        Ok(())
    }
}
