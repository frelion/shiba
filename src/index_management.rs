//! Backend identity and transaction guards for user-managed result indexes.
//!
//! SQL `SECURITY DEFINER` functions cannot observe the effective invoker
//! through `current_user`, and DDL executed from them keeps relation locks
//! until the surrounding transaction ends.  These small PostgreSQL-native
//! helpers preserve the outer effective role and reject explicit transaction
//! blocks before the privileged SQL implementation performs any DDL.

use pgrx::prelude::*;

#[pg_extern]
pub fn index_ddl_invoker() -> pg_sys::Oid {
    unsafe { pg_sys::GetOuterUserId() }
}

#[pg_extern]
pub fn require_index_ddl_top_level() {
    const COMMAND_NAME: &[u8] = b"Shiba index management\0";
    unsafe {
        pg_sys::PreventInTransactionBlock(true, COMMAND_NAME.as_ptr().cast::<std::ffi::c_char>());
    }
}

#[pg_extern]
pub fn lock_index_ddl_target(index_oid: pg_sys::Oid) {
    unsafe {
        pg_sys::LockRelationOid(index_oid, pg_sys::AccessExclusiveLock as i32);
    }
}
