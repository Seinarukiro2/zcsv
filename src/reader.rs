use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};
use std::io::Cursor;

/// Parse CSV from a string using SIMD-accelerated parser.
/// Pure Rust, no GIL needed. Returns (headers, rows).
pub fn parse_csv_string(
    content: &str,
    delimiter: u8,
    has_header: bool,
    skip_rows: usize,
    max_rows: Option<usize>,
    columns: Option<&[String]>,
    strict: bool,
) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let mut rdr = simd_csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .flexible(!strict)
        .from_reader(Cursor::new(content.as_bytes()));

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

    let filtered_headers = match &col_indices {
        Some(indices) => indices.iter().map(|&i| headers[i].clone()).collect(),
        None => headers.clone(),
    };

    let mut rows = Vec::new();
    let mut skipped = 0;

    for result in rdr.byte_records() {
        let record = result.map_err(|e| format!("CSV parse error: {e}"))?;

        if skipped < skip_rows {
            skipped += 1;
            continue;
        }

        if let Some(max) = max_rows {
            if rows.len() >= max {
                break;
            }
        }

        let row: Vec<String> = match &col_indices {
            Some(indices) => indices
                .iter()
                .map(|&i| {
                    record.get(i)
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_default()
                })
                .collect(),
            None => record.iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect(),
        };

        rows.push(row);
    }

    Ok((filtered_headers, rows))
}

/// Parse CSV from raw bytes via mmap (no decode to String needed for pure-ASCII).
/// For files already decoded to String, use parse_csv_string.
pub fn parse_csv_bytes(
    bytes: &[u8],
    delimiter: u8,
    has_header: bool,
    skip_rows: usize,
    max_rows: Option<usize>,
    columns: Option<&[String]>,
    strict: bool,
) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
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

    let filtered_headers = match &col_indices {
        Some(indices) => indices.iter().map(|&i| headers[i].clone()).collect(),
        None => headers.clone(),
    };

    let mut rows = Vec::new();
    let mut skipped = 0;

    for result in rdr.byte_records() {
        let record = result.map_err(|e| format!("CSV parse error: {e}"))?;

        if skipped < skip_rows {
            skipped += 1;
            continue;
        }

        if let Some(max) = max_rows {
            if rows.len() >= max {
                break;
            }
        }

        let row: Vec<String> = match &col_indices {
            Some(indices) => indices
                .iter()
                .map(|&i| {
                    record.get(i)
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_default()
                })
                .collect(),
            None => record.iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect(),
        };

        rows.push(row);
    }

    Ok((filtered_headers, rows))
}

// ─── Streaming Reader (PyO3 class for stdlib compat) ───

#[pyclass]
pub struct PyReader {
    rows: Vec<Vec<String>>,
    index: usize,
    line_num: usize,
}

#[pymethods]
impl PyReader {
    #[new]
    #[pyo3(signature = (file_obj, delimiter = ",", quotechar = "\"", strict = false, safe = false))]
    fn new(
        py: Python<'_>,
        file_obj: PyObject,
        delimiter: &str,
        quotechar: &str,
        strict: bool,
        safe: bool,
    ) -> PyResult<Self> {
        let _ = (quotechar, safe);
        let delim = delimiter.as_bytes().first().copied().unwrap_or(b',');

        // Read content with GIL held (Python I/O)
        let content: String = file_obj.call_method0(py, "read")?.extract(py)?;

        // Release GIL for SIMD CSV parsing
        let rows = py.allow_threads(|| -> Result<Vec<Vec<String>>, String> {
            let mut rdr = simd_csv::ReaderBuilder::new()
                .delimiter(delim)
                .has_headers(false)
                .flexible(!strict)
                .from_reader(Cursor::new(content.as_bytes()));

            let mut rows = Vec::new();
            for result in rdr.byte_records() {
                let record = result.map_err(|e| format!("CSV parse error: {e}"))?;
                let row: Vec<String> = record.iter()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .collect();
                rows.push(row);
            }
            Ok(rows)
        }).map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;

        Ok(PyReader {
            rows,
            index: 0,
            line_num: 0,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if self.index >= self.rows.len() {
            return Ok(None);
        }

        let row = &self.rows[self.index];
        self.index += 1;
        self.line_num += 1;

        let py_list = PyList::new(py, row.iter().map(|s| PyString::new(py, s)))?;
        Ok(Some(py_list.into_any().unbind()))
    }

    /// Batch retrieval: get next N rows as list[list[str]] in one call.
    #[pyo3(signature = (n = 1000))]
    fn fetch_many(&mut self, py: Python<'_>, n: usize) -> PyResult<PyObject> {
        let end = (self.index + n).min(self.rows.len());
        let slice = &self.rows[self.index..end];
        let count = slice.len();

        let outer = PyList::empty(py);
        for row in slice {
            let inner = PyList::new(py, row.iter().map(|s| PyString::new(py, s)))?;
            outer.append(inner)?;
        }

        self.index = end;
        self.line_num += count;

        Ok(outer.into_any().unbind())
    }

    #[getter]
    fn total_rows(&self) -> usize {
        self.rows.len()
    }

    #[getter]
    fn line_num(&self) -> usize {
        self.line_num
    }
}
