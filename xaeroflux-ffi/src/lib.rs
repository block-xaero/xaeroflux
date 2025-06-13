// src/ffi.rs

use std::{ffi::CStr, os::raw::c_char};

use xaeroflux::actors::{XaeroEvent, subject::Subject};

/// Opaque pointer to a Subject pipeline.
#[repr(C)]
pub struct FfiSubject {
    _private: [u8; 0],
}
/// Create a new Subject pipeline for the given name.
/// Returns a pointer you must later pass to the other calls.
/// # Safety
/// Document unsafe reference here
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xf_subject_new(
    name: *const c_char,
    workspace_name: *const c_char,
    object_name: *const c_char,
) -> *mut FfiSubject {
    // — parse C string into &str
    let name_rs = unsafe { CStr::from_ptr(name) }
        .to_str()
        .expect("invalid UTF-8");
    let workspace_name_rs = unsafe { CStr::from_ptr(workspace_name) }
        .to_str()
        .expect("invalid UTF-8");
    let object_name_rs = unsafe { CStr::from_ptr(object_name) }
        .to_str()
        .expect("invalid UTF-8");
    let mut hasher = blake3::Hasher::new();
    hasher.update(workspace_name_rs.as_bytes());
    let workspace_name_hash_rs = hasher.finalize();
    let subject = Subject::new_with_workspace(
        String::from(name_rs),
        *workspace_name_hash_rs.as_bytes(),
        String::from(workspace_name_rs),
        String::from(object_name_rs),
    );
    let boxxed_subject = Box::new(subject);
    Box::into_raw(boxxed_subject) as *mut FfiSubject
}

/// Map operator: apply your own callback to every event.
/// `cb` is a C function pointer you supply from Dart.
pub type MapCallback = extern "C" fn(evt: *mut XaeroEvent) -> *mut XaeroEvent;
#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_map(handle: *mut FfiSubject, _cb: MapCallback) -> *mut FfiSubject {
    // — cast handle → &mut Subject
    // — wrap cb in Arc<Fn(XaeroEvent)->XaeroEvent>
    // — subj.pipe = subj.pipe.map(callback)
    // — return the same handle for chaining
    handle
}

/// Filter operator: drop or keep events based on your predicate.
/// Return “true” to keep.
pub type FilterCallback = extern "C" fn(evt: *const XaeroEvent) -> bool;
#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_filter(
    handle: *mut FfiSubject,
    _cb: FilterCallback,
) -> *mut FfiSubject {
    // — same pattern for Filter
    handle
}

/// Drop events lacking a Merkle proof (no callback needed).
#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_filter_merkle_proofs(handle: *mut FfiSubject) -> *mut FfiSubject {
    // subj.pipe = subj.pipe.filter_merkle_proofs()
    // return handle
    handle
}

/// Terminal operator: no more events.
#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_blackhole(handle: *mut FfiSubject) -> *mut FfiSubject {
    // subj.pipe = subj.pipe.blackhole();
    // return handle
    handle
}

/// Run the pipeline to completion (or until it blocks).  
/// After calling this, the Subject is consumed/dropped.
#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_unsafe_run(_handle: *mut FfiSubject) {
    // Box::from_raw(handle as *mut Subject).run();
}
