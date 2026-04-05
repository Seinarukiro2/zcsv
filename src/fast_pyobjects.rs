//! Raw CPython FFI helpers for fast Python object creation.
//! Requires full CPython API (no abi3).

use pyo3::ffi;

/// # Safety
/// Caller must hold the GIL. Returns a new reference.
#[inline]
pub unsafe fn fast_pystring(s: &[u8]) -> *mut ffi::PyObject {
    unsafe {
        if s.is_empty() {
            return ffi::PyUnicode_FromStringAndSize(std::ptr::null(), 0);
        }
        if is_ascii(s) {
            let obj = ffi::PyUnicode_New(s.len() as isize, 127);
            if !obj.is_null() {
                let data = ffi::PyUnicode_DATA(obj) as *mut u8;
                std::ptr::copy_nonoverlapping(s.as_ptr(), data, s.len());
            }
            obj
        } else {
            ffi::PyUnicode_DecodeUTF8(s.as_ptr() as *const i8, s.len() as isize, std::ptr::null())
        }
    }
}

#[inline]
fn is_ascii(s: &[u8]) -> bool {
    let (prefix, chunks, suffix) = unsafe { s.align_to::<u64>() };
    for &b in prefix {
        if b >= 128 { return false; }
    }
    const HIGH: u64 = 0x8080_8080_8080_8080;
    for &chunk in chunks {
        if chunk & HIGH != 0 { return false; }
    }
    for &b in suffix {
        if b >= 128 { return false; }
    }
    true
}

/// # Safety
/// Caller must hold the GIL.
#[inline]
pub unsafe fn fast_pyint(v: i64) -> *mut ffi::PyObject {
    unsafe { ffi::PyLong_FromLongLong(v) }
}

/// # Safety
/// Caller must hold the GIL.
#[inline]
pub unsafe fn fast_pyfloat(v: f64) -> *mut ffi::PyObject {
    unsafe { ffi::PyFloat_FromDouble(v) }
}

/// # Safety
/// Caller must hold the GIL. Returns borrowed ref — caller must INCREF if storing.
#[inline]
pub unsafe fn fast_pybool(v: bool) -> *mut ffi::PyObject {
    unsafe { if v { ffi::Py_True() } else { ffi::Py_False() } }
}

/// # Safety
/// Caller must hold the GIL. Returns borrowed ref — caller must INCREF if storing.
#[inline]
pub unsafe fn fast_pynone() -> *mut ffi::PyObject {
    unsafe { ffi::Py_None() }
}
