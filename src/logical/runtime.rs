//! PostgreSQL runtime bridge for validated logical plans.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

use super::physical::PhysicalDagPlan;

/// Loads and validates persisted plan metadata once per database Runtime.
///
/// Operator data remains relational. `DagRuntime` never owns source rows or
/// operator state and applies an inbox transaction by durable LSN.
pub struct DagRuntime {
    result_oid: pg_sys::Oid,
    generation: String,
    physical_plan: PhysicalDagPlan,
}

pub enum LoadOutcome {
    Loaded(DagRuntime),
    Retry,
    Quarantined,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NextApplyOutcome {
    Applied,
    Retry,
    ResourceBlocked,
    Quarantined,
    Inactive,
    Idle,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ApplyResult {
    pub outcome: NextApplyOutcome,
    pub commit_lsn: Option<String>,
}

impl DagRuntime {
    pub fn load(result_oid: pg_sys::Oid) -> Result<LoadOutcome, String> {
        let argument = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
        let loaded = Spi::connect_mut(|client| {
            let table = client
                .update(
                    "SELECT outcome,plan_json,plan_generation,load_error
                     FROM shiba_internal._load_dag_runtime_safely($1::oid)",
                    None,
                    &argument,
                )
                .map_err(|error| error.to_string())?;
            if table.len() != 1 {
                return Err(format!(
                    "DAG runtime load returned {} rows, expected 1",
                    table.len()
                ));
            }
            let row = table.first();
            Ok((
                row.get::<String>(1)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "DAG runtime load returned NULL outcome".to_string())?,
                row.get::<String>(2).map_err(|error| error.to_string())?,
                row.get::<String>(3).map_err(|error| error.to_string())?,
                row.get::<String>(4).map_err(|error| error.to_string())?,
            ))
        })?;
        match loaded.0.as_str() {
            "retry" => return Ok(LoadOutcome::Retry),
            "quarantined" => return Ok(LoadOutcome::Quarantined),
            "loaded" => {}
            unexpected => {
                return Err(format!(
                    "DAG runtime load returned unexpected outcome {unexpected:?}"
                ));
            }
        }
        let serialized = loaded
            .1
            .ok_or_else(|| format!("result OID {result_oid} load returned no physical plan"))?;
        let generation = loaded.2.ok_or_else(|| {
            format!("result OID {result_oid} load returned no physical-plan generation")
        })?;
        let execution: PhysicalDagPlan = serde_json::from_str(&serialized)
            .map_err(|error| format!("invalid physical plan: {error}"))?;
        execution.validate_for_result(result_oid.to_u32())?;
        Ok(LoadOutcome::Loaded(Self {
            result_oid,
            generation,
            physical_plan: execution,
        }))
    }

    pub fn apply_next_transaction(&self) -> Result<ApplyResult, String> {
        let arguments = unsafe {
            [
                DatumWithOid::new(self.result_oid, pg_sys::OIDOID),
                DatumWithOid::new(self.physical_plan.encoded_descriptor(), pg_sys::TEXTOID),
            ]
        };
        Spi::connect_mut(|client| {
            let table = client
                .update(
                    "SELECT outcome,commit_lsn::text
                     FROM shiba._apply_next_dag_change_log($1::oid,$2::jsonb)",
                    None,
                    &arguments,
                )
                .map_err(|error| error.to_string())?;
            if table.len() != 1 {
                return Err(format!(
                    "next DAG apply returned {} rows, expected 1",
                    table.len()
                ));
            }
            let row = table.first();
            let outcome = row
                .get::<String>(1)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "next DAG apply returned NULL outcome".to_string())?;
            let commit_lsn = row.get::<String>(2).map_err(|error| error.to_string())?;
            let outcome = parse_next_apply_outcome(&outcome)?;
            if matches!(
                outcome,
                NextApplyOutcome::Applied
                    | NextApplyOutcome::Retry
                    | NextApplyOutcome::ResourceBlocked
                    | NextApplyOutcome::Quarantined
            ) != commit_lsn.is_some()
            {
                return Err(format!(
                    "next DAG apply returned inconsistent outcome {outcome:?} and commit LSN"
                ));
            }
            Ok(ApplyResult {
                outcome,
                commit_lsn,
            })
        })
    }

    pub fn quarantine(result_oid: pg_sys::Oid, error: &str) -> Result<(), String> {
        quarantine(result_oid, error)
    }

    pub fn matches_generation(&self, generation: &str) -> bool {
        self.generation == generation
    }

    pub fn generation(&self) -> &str {
        &self.generation
    }

    pub fn release_physical_programs(&self) -> Result<(), String> {
        release_physical_programs(self.result_oid, &self.generation)
    }
}

pub fn release_physical_programs(result_oid: pg_sys::Oid, generation: &str) -> Result<(), String> {
    let plan_id = generation
        .parse::<i64>()
        .map_err(|_| format!("invalid physical-plan generation {generation}"))?;
    let arguments = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(plan_id, pg_sys::INT8OID),
        ]
    };
    Spi::run_with_args(
        "SELECT shiba_internal._deallocate_join_physical_plans(
           $1::oid,$2::bigint
         )",
        &arguments,
    )
    .map_err(|error| error.to_string())
}

fn parse_next_apply_outcome(outcome: &str) -> Result<NextApplyOutcome, String> {
    match outcome {
        "applied" => Ok(NextApplyOutcome::Applied),
        "retry" => Ok(NextApplyOutcome::Retry),
        "resource_blocked" => Ok(NextApplyOutcome::ResourceBlocked),
        "quarantined" => Ok(NextApplyOutcome::Quarantined),
        "inactive" => Ok(NextApplyOutcome::Inactive),
        "idle" => Ok(NextApplyOutcome::Idle),
        unexpected => Err(format!(
            "next DAG apply returned unexpected outcome {unexpected:?}"
        )),
    }
}

pub fn quarantine(result_oid: pg_sys::Oid, error: &str) -> Result<(), String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(error, pg_sys::TEXTOID),
        ]
    };
    Spi::run_with_args(
        "UPDATE shiba_internal.dag_runtime_state
         SET active = false,
             last_error = $2,
             failed_at = clock_timestamp()
         WHERE result_oid = $1",
        &arguments,
    )
    .map_err(|spi_error| spi_error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{parse_next_apply_outcome, NextApplyOutcome};

    #[test]
    fn parses_next_apply_outcomes() {
        assert_eq!(
            parse_next_apply_outcome("applied"),
            Ok(NextApplyOutcome::Applied)
        );
        assert_eq!(
            parse_next_apply_outcome("retry"),
            Ok(NextApplyOutcome::Retry)
        );
        assert_eq!(
            parse_next_apply_outcome("resource_blocked"),
            Ok(NextApplyOutcome::ResourceBlocked)
        );
        assert_eq!(
            parse_next_apply_outcome("quarantined"),
            Ok(NextApplyOutcome::Quarantined)
        );
        assert_eq!(
            parse_next_apply_outcome("inactive"),
            Ok(NextApplyOutcome::Inactive)
        );
        assert_eq!(parse_next_apply_outcome("idle"), Ok(NextApplyOutcome::Idle));
        assert_eq!(
            parse_next_apply_outcome("unknown"),
            Err("next DAG apply returned unexpected outcome \"unknown\"".into())
        );
    }
}
