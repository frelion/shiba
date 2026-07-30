//! Runtime implementation boundary.

pub(crate) mod gc;
pub(crate) mod ingress;
pub(crate) mod scheduler;
pub(crate) mod wakeup;

#[cfg(any(test, feature = "pg_test"))]
pub(crate) mod test_failpoints {
    use pgrx::datum::DatumWithOid;
    use pgrx::prelude::*;
    use std::time::Duration;

    pub(crate) fn claim(
        kind: &str,
        result_oid: Option<pg_sys::Oid>,
        stage_id: Option<i32>,
        commit_lsn: Option<&str>,
    ) -> Option<Duration> {
        let available = Spi::get_one::<bool>(
            "SELECT to_regclass('public.shiba_runtime_failpoints') IS NOT NULL",
        )
        .ok()
        .flatten()
        .unwrap_or(false);
        if !available {
            return None;
        }

        let result_oid = result_oid.unwrap_or(pg_sys::InvalidOid);
        let stage_id = stage_id.unwrap_or(-1);
        let has_commit_lsn = commit_lsn.is_some();
        let commit_lsn = commit_lsn.unwrap_or("0/0");
        let arguments = unsafe {
            [
                DatumWithOid::new(kind, pg_sys::TEXTOID),
                DatumWithOid::new(result_oid, pg_sys::OIDOID),
                DatumWithOid::new(stage_id, pg_sys::INT4OID),
                DatumWithOid::new(commit_lsn, pg_sys::TEXTOID),
                DatumWithOid::new(has_commit_lsn, pg_sys::BOOLOID),
            ]
        };
        let pause_ms = Spi::get_one_with_args::<i32>(
            "SELECT max(pause_ms)
             FROM public.shiba_runtime_failpoints
             WHERE kind = $1
               AND NOT fired
               AND (runtime_pid IS NULL OR runtime_pid = pg_backend_pid())
               AND (result_oid IS NULL OR result_oid = $2::oid)
               AND (stage_id IS NULL OR stage_id = $3::integer)
               AND (
                 NOT $5::boolean
                 OR commit_lsn IS NULL
                 OR commit_lsn = $4::pg_lsn
               )",
            &arguments,
        )
        .expect("Shiba could not inspect its test worker failpoint");
        if pause_ms.is_some() {
            Spi::run_with_args(
                "UPDATE public.shiba_runtime_failpoints
                 SET runtime_pid = pg_backend_pid(),
                     stage_id = COALESCE(stage_id, NULLIF($3::integer, -1)),
                     commit_lsn = CASE
                       WHEN $5::boolean
                         THEN COALESCE(commit_lsn, $4::pg_lsn)
                       ELSE commit_lsn
                     END,
                     fired = true
                 WHERE kind = $1
                   AND NOT fired
                   AND (runtime_pid IS NULL OR runtime_pid = pg_backend_pid())
                   AND (result_oid IS NULL OR result_oid = $2::oid)
                   AND (stage_id IS NULL OR stage_id = $3::integer)
                   AND (
                     NOT $5::boolean
                     OR commit_lsn IS NULL
                     OR commit_lsn = $4::pg_lsn
                   )",
                &arguments,
            )
            .expect("Shiba could not claim its test worker failpoint");
        }
        pause_ms.map(|milliseconds| {
            Duration::from_millis(
                u64::try_from(milliseconds).expect("negative Shiba test failpoint pause"),
            )
        })
    }
}
