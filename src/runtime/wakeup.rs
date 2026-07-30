//! Runtime PostgreSQL latch wakeup implementation.

use pgrx::prelude::*;
use std::sync::atomic::{AtomicI32, Ordering};

/// Backend-local pending runtime wake target.
pub(crate) static PENDING_RUNTIME_WAKE_PID: AtomicI32 = AtomicI32::new(0);

pub(crate) unsafe extern "C-unwind" fn runtime_sigterm(signal: i32) {
    unsafe { pg_sys::die(signal) }
}

pub(crate) unsafe fn install_runtime_wakeup_callback() {
    pg_sys::RegisterXactCallback(Some(runtime_wakeup_xact_callback), std::ptr::null_mut());
}

#[cfg_attr(not(test), pg_guard)]
pub(crate) unsafe extern "C-unwind" fn runtime_wakeup_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_COMMIT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => {
            let owner_pid = PENDING_RUNTIME_WAKE_PID.swap(0, Ordering::AcqRel);
            if owner_pid > 0 {
                wake_backend_latch(owner_pid);
            }
        }
        pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PREPARE => {
            PENDING_RUNTIME_WAKE_PID.store(0, Ordering::Release);
        }
        _ => {}
    }
}

#[cfg(not(test))]
unsafe fn wake_backend_latch(owner_pid: i32) {
    const PROC_ARRAY_LWLOCK_INDEX_PG17: usize = 4;
    let proc_array_lock =
        std::ptr::addr_of_mut!((*pg_sys::MainLWLockArray.add(PROC_ARRAY_LWLOCK_INDEX_PG17)).lock);
    pg_sys::LWLockAcquire(proc_array_lock, pg_sys::LWLockMode::LW_SHARED);
    let process = pg_sys::BackendPidGetProcWithLock(owner_pid);
    if !process.is_null() {
        pg_sys::SetLatch(std::ptr::addr_of_mut!((*process).procLatch));
    }
    pg_sys::LWLockRelease(proc_array_lock);
}

#[cfg(test)]
unsafe fn wake_backend_latch(_owner_pid: i32) {}
