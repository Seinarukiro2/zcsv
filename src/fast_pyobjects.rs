//! Raw CPython FFI helpers for fast Python object creation.
//! Requires full CPython API (no abi3).

use pyo3::ffi;

/// Create a Python string from bytes using the fastest path.
/// ASCII data (most CSV) → PyUnicode_New + memcpy (no UTF-8 validation).
/// Non-ASCII → PyUnicode_DecodeUTF8.
///
/// # Safety
/// Caller must hold the GIL. Returns a new reference.
#[inline]
pub unsafe fn fast_pystring(s: &[u8]) -> *mut ffi::PyObject {
    if s.is_empty() {
        return ffi::PyUnicode_FromStringAndSize(std::ptr::null(), 0);
    }

    if is_ascii(s) {
        // Fast path: compact ASCII string — skip UTF-8 validation entirely
        let obj = ffi::PyUnicode_New(s.len() as isize, 127);
        if !obj.is_null() {
            let data = ffi::PyUnicode_DATA(obj) as *mut u8;
            std::ptr::copy_nonoverlapping(s.as_ptr(), data, s.len());
        }
        obj
    } else {
        // Non-ASCII: let CPython decode UTF-8
        ffi::PyUnicode_DecodeUTF8(
            s.as_ptr() as *const i8,
            s.len() as isize,
            std::ptr::null(),
        )
    }
}

/// Create a pre-sized Python list and fill with items.
/// Uses PyList_SET_ITEM — steals references, no INCREF.
///
/// # Safety
/// Caller must hold the GIL. Each item must be a new reference (stolen by list).
#[inline]
pub unsafe fn fast_pylist(items: &[*mut ffi::PyObject]) -> *mut ffi::PyObject {
    let list = ffi::PyList_New(items.len() as isize);
    if list.is_null() {
        return std::ptr::null_mut();
    }
    for (i, &item) in items.iter().enumerate() {
        ffi::PyList_SET_ITEM(list, i as isize, item);
    }
    list
}

/// Fast ASCII check — word-at-a-time.
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

#[inline]
pub unsafe fn fast_pyint(v: i64) -> *mut ffi::PyObject {
    ffi::PyLong_FromLongLong(v)
}

#[inline]
pub unsafe fn fast_pyfloat(v: f64) -> *mut ffi::PyObject {
    ffi::PyFloat_FromDouble(v)
}

#[inline]
pub unsafe fn fast_pybool(v: bool) -> *mut ffi::PyObject {
    if v { ffi::Py_True() } else { ffi::Py_False() }
}

#[inline]
pub unsafe fn fast_pynone() -> *mut ffi::PyObject {
    ffi::Py_None()
}
