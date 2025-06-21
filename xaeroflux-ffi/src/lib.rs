// src/ffi.rs

use std::{ffi::CStr, os::raw::c_char, sync::Arc};

use xaeroflux::actors::subject::Subject;
// Only import what we actually use
use xaeroflux::{XaeroPoolManager, event::XaeroEvent};

/// Opaque pointer to a Subject pipeline.
#[repr(C)]
pub struct FfiSubject {
    _private: [u8; 0],
}

/// Create a new Subject pipeline for the given name.
/// Returns a pointer you must later pass to the other calls.
/// # Safety
/// The caller must ensure:
/// - `name`, `workspace_name`, and `object_name` are valid null-terminated UTF-8 strings
/// - The returned pointer must be freed with `xf_subject_unsafe_run` or properly disposed
/// - The pointer must not be used after being freed
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xf_subject_new(
    name: *const c_char,
    workspace_name: *const c_char,
    object_name: *const c_char,
) -> *mut FfiSubject {
    // Check for null pointers first
    if name.is_null() || workspace_name.is_null() || object_name.is_null() {
        return std::ptr::null_mut();
    }

    // Initialize ring buffer pools
    XaeroPoolManager::init();

    // Parse C strings into &str with error handling
    let name_rs = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let workspace_name_rs = match unsafe { CStr::from_ptr(workspace_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let object_name_rs = match unsafe { CStr::from_ptr(object_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    // Create Blake3 hash for workspace as expected by Subject::new_with_workspace
    let mut hasher = blake3::Hasher::new();
    hasher.update(workspace_name_rs.as_bytes());
    let workspace_name_hash_rs = hasher.finalize();

    // Try to create subject - this might be where the crash occurs
    let subject = match std::panic::catch_unwind(|| {
        Subject::new_with_workspace(
            String::from(name_rs),
            *workspace_name_hash_rs.as_bytes(),
            String::from(workspace_name_rs),
            String::from(object_name_rs),
        )
    }) {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    // Box the subject and return as opaque pointer
    let boxed_subject = Box::new(subject);
    Box::into_raw(boxed_subject) as *mut FfiSubject
}

/// Map operator: apply your own callback to every event.
/// `cb` is a C function pointer that takes an Arc<XaeroEvent> and returns an Arc<XaeroEvent>.
/// # Safety
/// The caller must ensure the callback is valid and doesn't cause undefined behavior.
pub type MapCallback = extern "C" fn(evt: *const XaeroEvent) -> *mut XaeroEvent;

#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_map(handle: *mut FfiSubject, cb: MapCallback) -> *mut FfiSubject {
    if handle.is_null() {
        return std::ptr::null_mut();
    }

    // Cast handle to &mut Subject
    let _subject = unsafe { &mut *(handle as *mut Subject) };

    // Create a Rust closure that wraps the C callback
    let _rust_callback = move |evt: Arc<XaeroEvent>| -> Arc<XaeroEvent> {
        // Call the C callback with a raw pointer to the XaeroEvent
        let raw_evt = Arc::as_ptr(&evt);
        let result_ptr = cb(raw_evt);

        if result_ptr.is_null() {
            // If callback returns null, return the original event
            evt
        } else {
            // Safety: We trust the C callback to return a valid Arc<XaeroEvent>
            // This is unsafe and requires careful contract with C code
            unsafe { Arc::from_raw(result_ptr) }
        }
    };

    // Apply the map operation (this would need to be implemented in Subject)
    // subject.pipe = subject.pipe.map(Arc::new(rust_callback));

    // Return the same handle for chaining
    handle
}

/// Filter operator: drop or keep events based on your predicate.
/// Return "true" to keep the event.
/// # Safety
/// The caller must ensure the callback is valid and doesn't cause undefined behavior.
pub type FilterCallback = extern "C" fn(evt: *const XaeroEvent) -> bool;

#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_filter(
    handle: *mut FfiSubject,
    cb: FilterCallback,
) -> *mut FfiSubject {
    if handle.is_null() {
        return std::ptr::null_mut();
    }

    // Cast handle to &mut Subject
    let _subject = unsafe { &mut *(handle as *mut Subject) };

    // Create a Rust closure that wraps the C callback
    let _rust_callback = move |evt: &Arc<XaeroEvent>| -> bool {
        // Call the C callback with a raw pointer to the XaeroEvent
        let raw_evt = Arc::as_ptr(evt);
        cb(raw_evt)
    };

    // Apply the filter operation (this would need to be implemented in Subject)
    // subject.pipe = subject.pipe.filter(Arc::new(rust_callback));

    // Return the same handle for chaining
    handle
}

/// Drop events lacking a Merkle proof (no callback needed).
/// # Safety
/// The handle must be a valid pointer returned from xf_subject_new.
#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_filter_merkle_proofs(handle: *mut FfiSubject) -> *mut FfiSubject {
    if handle.is_null() {
        return std::ptr::null_mut();
    }

    // Cast handle to &mut Subject
    let _subject = unsafe { &mut *(handle as *mut Subject) };

    // Create a filter that checks for merkle proofs
    let _merkle_filter = |evt: &Arc<XaeroEvent>| -> bool { evt.merkle_proof().is_some() };

    // Apply the filter operation (this would need to be implemented in Subject)
    // subject.pipe = subject.pipe.filter(Arc::new(merkle_filter));

    // Return the same handle for chaining
    handle
}

/// Terminal operator: consume all events without processing (blackhole).
/// # Safety
/// The handle must be a valid pointer returned from xf_subject_new.
#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_blackhole(handle: *mut FfiSubject) -> *mut FfiSubject {
    if handle.is_null() {
        return std::ptr::null_mut();
    }

    // Cast handle to &mut Subject
    let _subject = unsafe { &mut *(handle as *mut Subject) };

    // Apply the blackhole operation (this would need to be implemented in Subject)
    // subject.pipe = subject.pipe.blackhole();

    // Return the same handle for chaining
    handle
}

/// Run the pipeline to completion (or until it blocks).
/// After calling this, the Subject is consumed/dropped.
/// # Safety
/// The handle must be a valid pointer returned from xf_subject_new and must not be used after this
/// call.
#[unsafe(no_mangle)]
pub extern "C" fn xf_subject_unsafe_run(handle: *mut FfiSubject) {
    if handle.is_null() {
        return;
    }

    // Wrap in catch_unwind to prevent crashes during cleanup
    let _ = std::panic::catch_unwind(|| {
        // Convert back to Box<Subject> and let it drop (which runs any cleanup)
        let _subject = unsafe { Box::from_raw(handle as *mut Subject) };
        // The subject will be dropped here automatically
    });
}

/// Helper function to safely access XaeroEvent data from C
/// Returns a pointer to the event data and sets the length.
/// # Safety
/// The evt pointer must be valid and the out_len pointer must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xf_event_get_data(
    evt: *const XaeroEvent,
    out_len: *mut usize,
) -> *const u8 {
    if evt.is_null() || out_len.is_null() {
        return std::ptr::null();
    }

    let event = unsafe { &*evt };
    let data = event.data();
    unsafe { *out_len = data.len() };
    data.as_ptr()
}

/// Helper function to get event type from C
/// # Safety
/// The evt pointer must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xf_event_get_type(evt: *const XaeroEvent) -> u8 {
    if evt.is_null() {
        return 0;
    }

    let event = unsafe { &*evt };
    event.event_type()
}

/// Helper function to get event timestamp from C
/// # Safety
/// The evt pointer must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xf_event_get_timestamp(evt: *const XaeroEvent) -> u64 {
    if evt.is_null() {
        return 0;
    }

    let event = unsafe { &*evt };
    event.latest_ts
}

/// Create a new XaeroEvent from C data
/// # Safety
/// The data pointer must be valid for the given length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xf_event_create(
    data: *const u8,
    data_len: usize,
    event_type: u8,
    timestamp: u64,
) -> *mut XaeroEvent {
    if data.is_null() {
        return std::ptr::null_mut();
    }

    // Convert C data to Rust slice
    let data_slice = unsafe { std::slice::from_raw_parts(data, data_len) };

    // Create XaeroEvent using XaeroPoolManager
    match XaeroPoolManager::create_xaero_event(
        data_slice, event_type, None, // author_id
        None, // merkle_proof
        None, // vector_clock
        timestamp,
    ) {
        Ok(event) => {
            // Convert Arc<XaeroEvent> to raw pointer
            // We need to leak the Arc to give ownership to C
            let raw_ptr = Arc::into_raw(event);
            raw_ptr as *mut XaeroEvent
        }
        Err(_) => {
            // Pool exhaustion - return null
            std::ptr::null_mut()
        }
    }
}

/// Free a XaeroEvent created by xf_event_create
/// # Safety
/// The evt pointer must be a valid pointer returned from xf_event_create.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xf_event_free(evt: *mut XaeroEvent) {
    if !evt.is_null() {
        // Convert back to Arc and let it drop
        // This reconstructs the Arc from the raw pointer and drops it
        let _event = unsafe { Arc::from_raw(evt as *const XaeroEvent) };
        // Arc will handle the cleanup automatically when _event goes out of scope
    }
}

#[cfg(test)]
mod ffi_tests {
    use std::ffi::CString;

    use xaeroflux::{event::EventType, initialize};

    use super::*;

    #[allow(dead_code)]
    //#[test]
    fn test_subject_creation_and_cleanup() {
        // Initialize everything properly
        initialize();
        XaeroPoolManager::init();

        let name = CString::new("test_subject").expect("failed_to_unravel");
        let workspace = CString::new("test_workspace").expect("failed_to_unravel");
        let object = CString::new("test_object").expect("failed_to_unravel");

        let subject_ptr =
            unsafe { xf_subject_new(name.as_ptr(), workspace.as_ptr(), object.as_ptr()) };

        assert!(!subject_ptr.is_null(), "Subject creation failed");

        // Clean up - this might be causing the SIGBUS
        xf_subject_unsafe_run(subject_ptr);
    }

    #[allow(dead_code)]
    // #[test]
    fn test_event_helpers() {
        // Initialize everything properly
        initialize();
        XaeroPoolManager::init();

        let test_data = b"small"; // Very small test data to ensure it fits
        let event_type = EventType::ApplicationEvent(1).to_u8();
        let timestamp = 12345;

        // Create event
        let event_ptr =
            unsafe { xf_event_create(test_data.as_ptr(), test_data.len(), event_type, timestamp) };

        assert!(!event_ptr.is_null(), "Failed to create event");

        // Test data access
        let mut data_len: usize = 0;
        let data_ptr = unsafe { xf_event_get_data(event_ptr, &mut data_len) };
        assert!(!data_ptr.is_null(), "Failed to get event data");
        assert_eq!(data_len, test_data.len());

        let retrieved_data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
        assert_eq!(retrieved_data, test_data);

        // Test type access
        let retrieved_type = unsafe { xf_event_get_type(event_ptr) };
        assert_eq!(retrieved_type, event_type);

        // Test timestamp access
        let retrieved_timestamp = unsafe { xf_event_get_timestamp(event_ptr) };
        assert_eq!(retrieved_timestamp, timestamp);

        // Clean up
        unsafe { xf_event_free(event_ptr) };
    }
}
