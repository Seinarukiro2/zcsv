use encoding_rs::Encoding;
use pyo3::exceptions::PyValueError;
use pyo3::PyResult;
use std::collections::HashMap;

/// Check if bytes need encoding conversion (non-UTF8 or has non-UTF8 BOM).
pub fn needs_encoding_conversion(bytes: &[u8]) -> bool {
    // UTF-16 BOMs
    if bytes.starts_with(&[0xFF, 0xFE]) || bytes.starts_with(&[0xFE, 0xFF]) {
        return true;
    }
    // Check if valid UTF-8
    std::str::from_utf8(bytes).is_err()
}

/// Pure-Rust version (no PyResult) — callable with GIL released.
pub fn decode_bytes_inner(bytes: &[u8], encoding: Option<&str>) -> Result<String, String> {
    // Check BOM first
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        return Ok(String::from_utf8_lossy(&bytes[3..]).into_owned());
    }
    if bytes.starts_with(&[0xFF, 0xFE]) {
        let (decoded, _, had_errors) = encoding_rs::UTF_16LE.decode(bytes);
        if had_errors {
            return Err("Failed to decode UTF-16LE".into());
        }
        return Ok(decoded.into_owned());
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        let (decoded, _, had_errors) = encoding_rs::UTF_16BE.decode(bytes);
        if had_errors {
            return Err("Failed to decode UTF-16BE".into());
        }
        return Ok(decoded.into_owned());
    }

    if let Some(enc_name) = encoding {
        let enc_name_lower = enc_name.to_lowercase();
        if enc_name_lower == "utf-8" || enc_name_lower == "utf8" {
            return Ok(String::from_utf8_lossy(bytes).into_owned());
        }
        let enc = Encoding::for_label(enc_name.as_bytes())
            .ok_or_else(|| format!("Unknown encoding: {enc_name}"))?;
        let (decoded, _, _) = enc.decode(bytes);
        return Ok(decoded.into_owned());
    }

    match std::str::from_utf8(bytes) {
        Ok(s) => Ok(s.to_string()),
        Err(_) => {
            let (decoded, _, _) = encoding_rs::WINDOWS_1252.decode(bytes);
            Ok(decoded.into_owned())
        }
    }
}

/// PyResult wrapper for direct Python calls.
#[allow(dead_code)]
pub fn decode_bytes(bytes: &[u8], encoding: Option<&str>) -> PyResult<String> {
    decode_bytes_inner(bytes, encoding)
        .map_err(|e| PyValueError::new_err(e))
}

/// Detect the most likely delimiter by frequency analysis of first N lines.
pub fn detect_delimiter(content: &str) -> u8 {
    let candidates: &[u8] = &[b',', b';', b'\t', b'|', b':'];
    let sample: String = content.lines().take(20).collect::<Vec<_>>().join("\n");

    if sample.is_empty() {
        return b',';
    }

    let mut scores: HashMap<u8, f64> = HashMap::new();

    for &delim in candidates {
        let lines: Vec<&str> = sample.lines().collect();
        if lines.is_empty() {
            continue;
        }

        let counts: Vec<usize> = lines
            .iter()
            .map(|line| count_unquoted(line, delim))
            .collect();

        let total: usize = counts.iter().sum();
        if total == 0 {
            continue;
        }

        let avg = total as f64 / counts.len() as f64;
        let variance: f64 = counts
            .iter()
            .map(|&c| {
                let diff = c as f64 - avg;
                diff * diff
            })
            .sum::<f64>()
            / counts.len() as f64;
        let std_dev = variance.sqrt();

        let consistency = if std_dev < 0.001 { 1.0 } else { 1.0 / (1.0 + std_dev) };
        scores.insert(delim, avg * consistency);
    }

    scores
        .into_iter()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(delim, _)| delim)
        .unwrap_or(b',')
}

fn count_unquoted(line: &str, delim: u8) -> usize {
    let delim_char = delim as char;
    let mut count = 0;
    let mut in_quotes = false;

    for ch in line.chars() {
        if ch == '"' {
            in_quotes = !in_quotes;
        } else if ch == delim_char && !in_quotes {
            count += 1;
        }
    }

    count
}
