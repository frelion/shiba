//! PostgreSQL GUCs for bounding Runtime process resource use.

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

const DEFAULT_RUNTIME_WORK_MEM_KB: i32 = 16 * 1024;
const MIN_RUNTIME_WORK_MEM_KB: i32 = 64;
const MAX_RUNTIME_WORK_MEM_KB: i32 = i32::MAX;

const DEFAULT_RUNTIME_TEMP_FILE_LIMIT_KB: i32 = 1024 * 1024;
const MIN_RUNTIME_TEMP_FILE_LIMIT_KB: i32 = 0;
const MAX_RUNTIME_TEMP_FILE_LIMIT_KB: i32 = i32::MAX;

const DEFAULT_MAX_CACHED_DAGS: i32 = 128;
const MIN_MAX_CACHED_DAGS: i32 = 1;
const MAX_MAX_CACHED_DAGS: i32 = 4_096;

const DEFAULT_STAGE_CHUNK_ROWS: i32 = 2_048;
const MIN_STAGE_CHUNK_ROWS: i32 = 1;
const MAX_STAGE_CHUNK_ROWS: i32 = 1_000_000;

const DEFAULT_MAX_STAGE_ROWS: i32 = 1_000_000;
const MIN_MAX_STAGE_ROWS: i32 = 1;
const MAX_MAX_STAGE_ROWS: i32 = i32::MAX;

const DEFAULT_INGRESS_BATCH_ROWS: i32 = 2_048;
const MIN_INGRESS_BATCH_ROWS: i32 = 1;
const MAX_INGRESS_BATCH_ROWS: i32 = 1_000_000;

const DEFAULT_INGRESS_BATCH_BYTES: i32 = 16 * 1024 * 1024;
const MIN_INGRESS_BATCH_BYTES: i32 = 1;
const MAX_INGRESS_BATCH_BYTES: i32 = i32::MAX;

const DEFAULT_MAX_CACHED_RELATIONS: i32 = 4_096;
const MIN_MAX_CACHED_RELATIONS: i32 = 1;
const MAX_MAX_CACHED_RELATIONS: i32 = 65_536;

const DEFAULT_INGRESS_RETENTION_MS: i32 = 1_000;
const MIN_INGRESS_RETENTION_MS: i32 = 0;
const MAX_INGRESS_RETENTION_MS: i32 = i32::MAX;

static RUNTIME_WORK_MEM_KB: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_RUNTIME_WORK_MEM_KB);
static RUNTIME_TEMP_FILE_LIMIT_KB: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_RUNTIME_TEMP_FILE_LIMIT_KB);
static MAX_CACHED_DAGS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_MAX_CACHED_DAGS);
static STAGE_CHUNK_ROWS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_STAGE_CHUNK_ROWS);
static MAX_STAGE_ROWS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_MAX_STAGE_ROWS);
static INGRESS_BATCH_ROWS: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_INGRESS_BATCH_ROWS);
static INGRESS_BATCH_BYTES: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_INGRESS_BATCH_BYTES);
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
        c"shiba.max_cached_dags",
        c"Maximum number of DAG runtimes cached by one Shiba Runtime.",
        c"Older DAG runtimes are deterministically evicted and their prepared programs released.",
        &MAX_CACHED_DAGS,
        MIN_MAX_CACHED_DAGS,
        MAX_MAX_CACHED_DAGS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"shiba.stage_chunk_rows",
        c"Target number of Stage rows processed by one SQL chunk.",
        c"SQL operators can read this value with current_setting to bound statement work.",
        &STAGE_CHUNK_ROWS,
        MIN_STAGE_CHUNK_ROWS,
        MAX_STAGE_CHUNK_ROWS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"shiba.max_stage_rows",
        c"Maximum allowed rows in a commit-scoped Stage.",
        c"SQL operators can read this value with current_setting to enforce Stage row quotas.",
        &MAX_STAGE_ROWS,
        MIN_MAX_STAGE_ROWS,
        MAX_MAX_STAGE_ROWS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"shiba.ingress_batch_rows",
        c"Target maximum row images in one ingress transaction.",
        c"One individual replication message or tuple remains indivisible.",
        &INGRESS_BATCH_ROWS,
        MIN_INGRESS_BATCH_ROWS,
        MAX_INGRESS_BATCH_ROWS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"shiba.ingress_batch_bytes",
        c"Target maximum pgoutput payload bytes in one ingress transaction.",
        c"One individual replication message or tuple may exceed this target.",
        &INGRESS_BATCH_BYTES,
        MIN_INGRESS_BATCH_BYTES,
        MAX_INGRESS_BATCH_BYTES,
        GucContext::Sighup,
        GucFlags::UNIT_BYTE,
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
        c"libpq connection parameters for the v2 logical replication connection.",
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

pub fn max_cached_dags() -> usize {
    usize::try_from(MAX_CACHED_DAGS.get())
        .expect("shiba.max_cached_dags passed PostgreSQL range validation")
}

pub fn ingress_batch_rows() -> usize {
    usize::try_from(INGRESS_BATCH_ROWS.get())
        .expect("shiba.ingress_batch_rows passed PostgreSQL range validation")
}

pub fn ingress_batch_bytes() -> usize {
    usize::try_from(INGRESS_BATCH_BYTES.get())
        .expect("shiba.ingress_batch_bytes passed PostgreSQL range validation")
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
        assert!((MIN_MAX_CACHED_DAGS..=MAX_MAX_CACHED_DAGS).contains(&DEFAULT_MAX_CACHED_DAGS));
        assert!((MIN_STAGE_CHUNK_ROWS..=MAX_STAGE_CHUNK_ROWS).contains(&DEFAULT_STAGE_CHUNK_ROWS));
        assert!((MIN_MAX_STAGE_ROWS..=MAX_MAX_STAGE_ROWS).contains(&DEFAULT_MAX_STAGE_ROWS));
        assert!(
            (MIN_INGRESS_BATCH_ROWS..=MAX_INGRESS_BATCH_ROWS).contains(&DEFAULT_INGRESS_BATCH_ROWS)
        );
        assert!((MIN_INGRESS_BATCH_BYTES..=MAX_INGRESS_BATCH_BYTES)
            .contains(&DEFAULT_INGRESS_BATCH_BYTES));
        assert!((MIN_MAX_CACHED_RELATIONS..=MAX_MAX_CACHED_RELATIONS)
            .contains(&DEFAULT_MAX_CACHED_RELATIONS));
        assert!((MIN_INGRESS_RETENTION_MS..=MAX_INGRESS_RETENTION_MS)
            .contains(&DEFAULT_INGRESS_RETENTION_MS));
    }
}
