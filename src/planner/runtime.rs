//! PostgreSQL bridge for one bounded durable operator step.
//!
//! PostgreSQL is the sole readiness authority. `LoadedDataflow` is therefore
//! deliberately tiny: it caches validated plan metadata and the last stage
//! selected for fair rotation. Restarting it loses neither input nor work.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

use super::dataflow::{StepOutcome, WorkBudget, WorkQuantum};
use super::model::DataflowPlan;

pub(crate) struct LoadedDataflow {
    plan: DataflowPlan,
    stage_cursor: Option<u32>,
    stage_metadata: Vec<crate::execution::StageMetadataCache>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OperatorStep {
    pub(crate) stage_id: u32,
    pub(crate) outcome: StepOutcome,
    pub(crate) transitions: usize,
}

impl LoadedDataflow {
    pub(crate) fn load(result_oid: pg_sys::Oid) -> Result<Self, String> {
        let argument = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
        let serialized = Spi::get_one_with_args::<String>(
            "SELECT plan::text
             FROM shiba_internal.dataflows
             WHERE result_oid=$1::oid AND active",
            &argument,
        )
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "active dataflow load returned no plan".to_string())?;
        let plan: DataflowPlan = serde_json::from_str(&serialized)
            .map_err(|error| format!("invalid dataflow plan: {error}"))?;
        plan.validate()?;
        Ok(Self {
            stage_metadata: (0..plan.stages.len())
                .map(|_| crate::execution::StageMetadataCache::default())
                .collect(),
            plan,
            stage_cursor: None,
        })
    }

    pub(crate) const fn stage_cursor(&self) -> Option<u32> {
        self.stage_cursor
    }

    /// A durable query selected `stage_id`; this object only remembers it to
    /// rotate the next query fairly. Kernel state is never cached here.
    pub(crate) fn step_quantum(
        &mut self,
        result_oid: pg_sys::Oid,
        stage_id: u32,
        budget: WorkBudget,
        max_transitions: usize,
    ) -> Result<OperatorStep, String> {
        if stage_id as usize >= self.plan.stages.len() {
            return Err("durable readiness selected a stage outside its dataflow plan".into());
        }
        self.stage_cursor = Some(stage_id);
        let metadata_cache = self
            .stage_metadata
            .get(usize::try_from(stage_id).map_err(|_| "operator stage ID exceeds usize")?)
            .cloned()
            .ok_or_else(|| "dataflow metadata cache has no selected stage".to_string())?;
        let mut quantum = WorkQuantum::new(budget, max_transitions);
        let mut outcome = StepOutcome::Idle;
        while let Some(remaining) = quantum.remaining() {
            let execution = crate::execution::execute_step(
                result_oid,
                stage_id,
                &self.plan,
                remaining,
                metadata_cache.clone(),
            )?;
            execution.validate()?;
            outcome = execution.outcome;
            if !matches!(outcome, StepOutcome::Progress | StepOutcome::Yield) {
                break;
            }
            quantum.record(execution.usage)?;
            // A nonempty primitive is already a full bounded page. Only fold
            // adjacent metadata transitions into this transaction.
            if !execution.usage.is_empty() {
                break;
            }
        }
        Ok(OperatorStep {
            stage_id,
            outcome,
            transitions: quantum.transitions(),
        })
    }
}
