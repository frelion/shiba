//! PostgreSQL GUCs for bounding Runtime process resource use.

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

const DEFAULT_RUNTIME_WORK_MEM_KB: i32 = 16 * 1024;
const MIN_RUNTIME_WORK_MEM_KB: i32 = 64;
const MAX_RUNTIME_WORK_MEM_KB: i32 = i32::MAX;

const DEFAULT_RUNTIME_TEMP_FILE_LIMIT_KB: i32 = 1024 * 1024;
const MIN_RUNTIME_TEMP_FILE_LIMIT_KB: i32 = 0;
const MAX_RUNTIME_TEMP_FILE_LIMIT_KB: i32 = i32::MAX;

const DEFAULT_MAX_CACHED_DATAFLOWS: i32 = 128;
const MIN_MAX_CACHED_DATAFLOWS: i32 = 1;
const MAX_MAX_CACHED_DATAFLOWS: i32 = 4_096;

const DEFAULT_BATCH_ROWS: i32 = 16 * 1024;
const MIN_BATCH_ROWS: i32 = 1;
const MAX_BATCH_ROWS: i32 = 1_000_000;

const DEFAULT_BATCH_BYTES: i32 = 16 * 1024 * 1024;
const MIN_BATCH_BYTES: i32 = 1;
const MAX_BATCH_BYTES: i32 = i32::MAX;
const ADMISSION_BYTE_BATCHES: usize = 8;

const DEFAULT_INGRESS_STAGING_LIMIT_KB: i32 = 64 * 1024 * 1024;
const MIN_INGRESS_STAGING_LIMIT_KB: i32 = 1024;
const MAX_INGRESS_STAGING_LIMIT_KB: i32 = i32::MAX;

const DEFAULT_MAX_CACHED_RELATIONS: i32 = 4_096;
const MIN_MAX_CACHED_RELATIONS: i32 = 1;
const MAX_MAX_CACHED_RELATIONS: i32 = 65_536;

const DEFAULT_INGRESS_RETENTION_MS: i32 = 1_000;
const MIN_INGRESS_RETENTION_MS: i32 = 0;
const MAX_INGRESS_RETENTION_MS: i32 = i32::MAX;

static RUNTIME_WORK_MEM_KB: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_RUNTIME_WORK_MEM_KB);
static RUNTIME_TEMP_FILE_LIMIT_KB: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_RUNTIME_TEMP_FILE_LIMIT_KB);
static MAX_CACHED_DATAFLOWS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_MAX_CACHED_DATAFLOWS);
static BATCH_ROWS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_BATCH_ROWS);
static BATCH_BYTES: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_BATCH_BYTES);
static INGRESS_STAGING_LIMIT_KB: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_INGRESS_STAGING_LIMIT_KB);
static MAX_CACHED_RELATIONS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_MAX_CACHED_RELATIONS);
static INGRESS_RETENTION_MS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_INGRESS_RETENTION_MS);
static REPLICATION_CONNINFO: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

pub fn init() {
    GucRegistry::define_int_guc(
        c"shiba.runtime_work_mem",
        c"Work memory available to each Shiba Runtime query operation.",
        c"Sets the PostgreSQL work_mem used by the Shiba Runtime session.",
        &RUNTIME_WORK_MEM_KB,
        MIN_RUNTIME_WORK_MEM_KB,
        MAX_RUNTIME_WORK_MEM_KB,
        GucContext::Sighup,
        GucFlags::UNIT_KB,
    );
    GucRegistry::define_int_guc(
        c"shiba.runtime_temp_file_limit",
        c"Maximum temporary-file space available to the Shiba Runtime.",
        c"Sets the PostgreSQL temp_file_limit used by the Shiba Runtime session.",
        &RUNTIME_TEMP_FILE_LIMIT_KB,
        MIN_RUNTIME_TEMP_FILE_LIMIT_KB,
        MAX_RUNTIME_TEMP_FILE_LIMIT_KB,
        GucContext::Sighup,
        GucFlags::UNIT_KB,
    );
    GucRegistry::define_int_guc(
        c"shiba.max_cached_dataflows",
        c"Maximum number of loaded dataflows cached by the Shiba Runtime.",
        c"Older loaded dataflows are deterministically evicted and rebuilt from durable state when needed.",
        &MAX_CACHED_DATAFLOWS,
        MIN_MAX_CACHED_DATAFLOWS,
        MAX_MAX_CACHED_DATAFLOWS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"shiba.batch_rows",
        c"Target rows processed by one ingress batch or operator step.",
        c"A complete replication message remains indivisible; operator row budgets are hard.",
        &BATCH_ROWS,
        MIN_BATCH_ROWS,
        MAX_BATCH_ROWS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"shiba.batch_bytes",
        c"Target bytes processed by one ingress batch or operator step.",
        c"One indivisible replication message or typed row may exceed this target.",
        &BATCH_BYTES,
        MIN_BATCH_BYTES,
        MAX_BATCH_BYTES,
        GucContext::Sighup,
        GucFlags::UNIT_BYTE,
    );
    GucRegistry::define_int_guc(
        c"shiba.ingress_staging_limit",
        c"Maximum decoded payload bytes retained by open source transactions.",
        c"Shiba stops safely and retains WAL for replay instead of accepting an unbounded transaction.",
        &INGRESS_STAGING_LIMIT_KB,
        MIN_INGRESS_STAGING_LIMIT_KB,
        MAX_INGRESS_STAGING_LIMIT_KB,
        GucContext::Sighup,
        GucFlags::UNIT_KB,
    );
    GucRegistry::define_int_guc(
        c"shiba.max_cached_relations",
        c"Maximum pgoutput relation descriptors retained by one Shiba Runtime.",
        c"The Runtime fails closed at this limit because evicting descriptors can misdecode tuples.",
        &MAX_CACHED_RELATIONS,
        MIN_MAX_CACHED_RELATIONS,
        MAX_MAX_CACHED_RELATIONS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"shiba.ingress_retention",
        c"Minimum retention after ingress transaction finalization.",
        c"Replay-safe transactions remain inspectable for this duration before bounded GC.",
        &INGRESS_RETENTION_MS,
        MIN_INGRESS_RETENTION_MS,
        MAX_INGRESS_RETENTION_MS,
        GucContext::Sighup,
        GucFlags::UNIT_MS,
    );
    GucRegistry::define_string_guc(
        c"shiba.replication_conninfo",
        c"libpq connection parameters for the logical replication connection.",
        c"Use passfile, certificate, or peer authentication; do not place an inline password here.",
        &REPLICATION_CONNINFO,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
}

pub fn runtime_work_mem_kb() -> i32 {
    RUNTIME_WORK_MEM_KB.get()
}

pub fn runtime_temp_file_limit_kb() -> i32 {
    RUNTIME_TEMP_FILE_LIMIT_KB.get()
}

pub fn max_cached_dataflows() -> usize {
    usize::try_from(MAX_CACHED_DATAFLOWS.get())
        .expect("shiba.max_cached_dataflows passed PostgreSQL range validation")
}

pub fn batch_rows() -> usize {
    usize::try_from(BATCH_ROWS.get()).expect("shiba.batch_rows passed PostgreSQL range validation")
}

pub fn batch_bytes() -> usize {
    usize::try_from(BATCH_BYTES.get())
        .expect("shiba.batch_bytes passed PostgreSQL range validation")
}

pub(crate) fn admission_rows() -> usize {
    batch_rows()
}

pub(crate) fn admission_bytes() -> usize {
    admission_bytes_for(batch_bytes())
}

pub(crate) fn admission_row_interval_cap() -> usize {
    usize::try_from(MAX_BATCH_ROWS).expect("batch row cap is positive")
}

pub(crate) fn admission_byte_interval_cap() -> usize {
    usize::try_from(MAX_BATCH_BYTES).expect("batch byte cap is positive")
}

fn admission_bytes_for(batch_bytes: usize) -> usize {
    batch_bytes
        .saturating_mul(ADMISSION_BYTE_BATCHES)
        .min(admission_byte_interval_cap())
}

pub fn max_cached_relations() -> usize {
    usize::try_from(MAX_CACHED_RELATIONS.get())
        .expect("shiba.max_cached_relations passed PostgreSQL range validation")
}

pub fn replication_conninfo() -> Option<CString> {
    REPLICATION_CONNINFO.get()
}

pub fn format_kilobytes(value: i32) -> String {
    debug_assert!(value >= 0);
    format!("{value}kB")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_memory_values_map_to_unambiguous_postgresql_units() {
        assert_eq!(format_kilobytes(0), "0kB");
        assert_eq!(format_kilobytes(16 * 1024), "16384kB");
        assert_eq!(format_kilobytes(i32::MAX), "2147483647kB");
    }

    #[test]
    fn defaults_are_inside_registered_ranges() {
        assert!((MIN_RUNTIME_WORK_MEM_KB..=MAX_RUNTIME_WORK_MEM_KB)
            .contains(&DEFAULT_RUNTIME_WORK_MEM_KB));
        assert!(
            (MIN_RUNTIME_TEMP_FILE_LIMIT_KB..=MAX_RUNTIME_TEMP_FILE_LIMIT_KB)
                .contains(&DEFAULT_RUNTIME_TEMP_FILE_LIMIT_KB)
        );
        assert!((MIN_MAX_CACHED_DATAFLOWS..=MAX_MAX_CACHED_DATAFLOWS)
            .contains(&DEFAULT_MAX_CACHED_DATAFLOWS));
        assert!((MIN_BATCH_ROWS..=MAX_BATCH_ROWS).contains(&DEFAULT_BATCH_ROWS));
        assert!((MIN_BATCH_BYTES..=MAX_BATCH_BYTES).contains(&DEFAULT_BATCH_BYTES));
        assert!(
            (MIN_INGRESS_STAGING_LIMIT_KB..=MAX_INGRESS_STAGING_LIMIT_KB)
                .contains(&DEFAULT_INGRESS_STAGING_LIMIT_KB)
        );
        assert!((MIN_MAX_CACHED_RELATIONS..=MAX_MAX_CACHED_RELATIONS)
            .contains(&DEFAULT_MAX_CACHED_RELATIONS));
        assert!((MIN_INGRESS_RETENTION_MS..=MAX_INGRESS_RETENTION_MS)
            .contains(&DEFAULT_INGRESS_RETENTION_MS));
    }

    #[test]
    fn admission_policy_is_derived_from_the_batch_budget() {
        assert_eq!(
            admission_bytes_for(usize::try_from(DEFAULT_BATCH_BYTES).unwrap()),
            128 * 1024 * 1024
        );
        assert_eq!(
            admission_bytes_for(usize::MAX),
            admission_byte_interval_cap()
        );
    }
}
