mod reader;
mod writer;
mod sniffer;
mod validator;
mod schema;
mod types;
mod parallel;
mod fast_pyobjects;
mod row;

use memmap2::Mmap;
use pyo3::prelude::*;
use std::fs::File;

/// Threshold for switching to mmap (bytes). Below this, std::fs::read is faster.
const MMAP_THRESHOLD: u64 = 2 * 1024 * 1024; // 2 MB

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<reader::PyReader>()?;
    m.add_class::<writer::PyWriter>()?;
    m.add_class::<row::Row>()?;
    m.add_class::<row::RowIterator>()?;
    m.add_function(wrap_pyfunction!(read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(write_csv, m)?)?;
    m.add_function(wrap_pyfunction!(sniff_delimiter, m)?)?;
    Ok(())
}

/// Read file bytes — uses mmap for large files, std::fs::read for small ones.
fn read_file_bytes(path: &str) -> Result<FileBytes, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let metadata = file.metadata().map_err(|e| e.to_string())?;
    let size = metadata.len();

    if size >= MMAP_THRESHOLD {
        // mmap: zero-copy, no syscall overhead for reads
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| e.to_string())?;
        Ok(FileBytes::Mmap(mmap))
    } else {
        // Small file: regular read (mmap overhead not worth it)
        let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
        Ok(FileBytes::Vec(bytes))
    }
}

enum FileBytes {
    Mmap(Mmap),
    Vec(Vec<u8>),
}

impl AsRef<[u8]> for FileBytes {
    fn as_ref(&self) -> &[u8] {
        match self {
            FileBytes::Mmap(m) => m.as_ref(),
            FileBytes::Vec(v) => v.as_ref(),
        }
    }
}

/// High-level read: path -> list[dict] with type inference.
/// Uses mmap + SIMD CSV parsing + GIL release.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    path,
    delimiter = None,
    has_header = true,
    schema = None,
    skip_rows = 0,
    max_rows = None,
    columns = None,
    null_values = None,
    encoding = None,
    strict = false,
    n_threads = None,
))]
fn read_csv(
    py: Python<'_>,
    path: &str,
    delimiter: Option<&str>,
    has_header: bool,
    schema: Option<&Bound<'_, pyo3::types::PyDict>>,
    skip_rows: usize,
    max_rows: Option<usize>,
    columns: Option<Vec<String>>,
    null_values: Option<Vec<String>>,
    encoding: Option<&str>,
    strict: bool,
    n_threads: Option<usize>,
) -> PyResult<PyObject> {
    let null_vals = null_values.unwrap_or_else(|| {
        vec!["".into(), "NA".into(), "null".into(), "None".into()]
    });

    let explicit_schema = match schema {
        Some(s) => Some(schema::schema_from_pydict(s)?),
        None => None,
    };

    let path = path.to_string();
    let encoding = encoding.map(|s| s.to_string());
    let delimiter = delimiter.map(|s| s.as_bytes()[0]);
    let null_vals_clone = null_vals.clone();

    // Release GIL for all pure-Rust work: mmap/read, decode, SIMD parse, type inference
    let (headers, raw_rows, type_map) = py.allow_threads(move || -> Result<_, String> {
        let file_bytes = read_file_bytes(&path)?;
        let bytes = file_bytes.as_ref();

        // Check if encoding conversion is needed
        let needs_decode = encoding.is_some()
            || sniffer::needs_encoding_conversion(bytes);

        let (headers, raw_rows) = if needs_decode {
            let content = sniffer::decode_bytes_inner(bytes, encoding.as_deref())?;
            let delim = delimiter.unwrap_or_else(|| sniffer::detect_delimiter(&content));
            reader::parse_csv_to_strings(
                content.as_bytes(), delim, has_header, skip_rows, max_rows, columns.as_deref(), strict,
            )?
        } else {
            let csv_bytes = if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
                &bytes[3..]
            } else {
                bytes
            };
            let delim = delimiter.unwrap_or_else(|| {
                let sample = std::str::from_utf8(&csv_bytes[..csv_bytes.len().min(4096)])
                    .unwrap_or("");
                sniffer::detect_delimiter(sample)
            });
            reader::parse_csv_to_strings(
                csv_bytes, delim, has_header, skip_rows, max_rows, columns.as_deref(), strict,
            )?
        };

        if strict {
            validator::validate_rfc4180_inner(&headers, &raw_rows)?;
        }

        let type_map = match explicit_schema {
            Some(s) => s,
            None => schema::infer_types(&headers, &raw_rows, &null_vals_clone),
        };

        Ok((headers, raw_rows, type_map))
    }).map_err(pyo3::exceptions::PyValueError::new_err)?;

    let n = n_threads.unwrap_or(0);
    let result = parallel::convert_to_dicts(py, &headers, &raw_rows, &type_map, &null_vals, n)?;

    Ok(result)
}

/// High-level write: list[dict] -> CSV file
#[pyfunction]
#[pyo3(signature = (path, data, delimiter = ",", safe = true, strict = false))]
fn write_csv(
    path: &str,
    data: &Bound<'_, pyo3::types::PyList>,
    delimiter: &str,
    safe: bool,
    strict: bool,
) -> PyResult<()> {
    writer::write_dicts_to_csv(path, data, delimiter.as_bytes()[0], safe, strict)
}

/// Sniff delimiter from file content
#[pyfunction]
fn sniff_delimiter(py: Python<'_>, path: &str) -> PyResult<String> {
    let path = path.to_string();
    let delim = py.allow_threads(move || -> Result<u8, String> {
        let file_bytes = read_file_bytes(&path)?;
        let content = sniffer::decode_bytes_inner(file_bytes.as_ref(), None)?;
        Ok(sniffer::detect_delimiter(&content))
    }).map_err(pyo3::exceptions::PyIOError::new_err)?;
    Ok(String::from(delim as char))
}
