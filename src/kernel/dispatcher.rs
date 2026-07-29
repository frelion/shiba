//! One closed Rust dispatcher for every operator step.

use pgrx::prelude::*;

use crate::logical::model::{DataflowPlan, OperatorSpec};
use crate::logical::{StepOutcome, WorkBudget};

use super::{StepStart, StepTxn};

pub(crate) fn execute_step(
    result_oid: pg_sys::Oid,
    stage_id: u32,
    plan: &DataflowPlan,
    budget: WorkBudget,
) -> Result<StepOutcome, String> {
    let stage = plan
        .stages
        .get(usize::try_from(stage_id).map_err(|_| "operator stage ID exceeds usize")?)
        .ok_or_else(|| format!("dataflow has no stage {stage_id}"))?;
    let expected_inputs = if matches!(stage.spec, OperatorSpec::Scan(_)) {
        1
    } else {
        u16::try_from(stage.inputs.len())
            .map_err(|_| format!("operator stage {stage_id} has too many input ports"))?
    };
    let expects_output = !matches!(stage.spec, OperatorSpec::Sink);

    Spi::connect_mut(|client| {
        let transaction = match StepTxn::begin(
            client,
            result_oid,
            stage_id,
            expected_inputs,
            expects_output,
            &plan.execution_settings,
            budget,
        )? {
            StepStart::Blocked => return Ok(StepOutcome::Blocked),
            StepStart::Idle => return Ok(StepOutcome::Idle),
            StepStart::Ready(transaction) => transaction,
        };
        match &stage.spec {
            OperatorSpec::Scan(_) | OperatorSpec::Filter(_) | OperatorSpec::Project(_) => {
                super::linear::execute(transaction, plan, stage_id)
            }
            OperatorSpec::Distinct(_) => super::distinct::execute(transaction, plan, stage_id),
            OperatorSpec::Sink => super::sink::execute(transaction, plan, stage_id),
            OperatorSpec::Join(_) => super::join::execute(transaction, plan, stage_id),
            OperatorSpec::Aggregate(_) => super::aggregate::execute(transaction, plan, stage_id),
            OperatorSpec::Window(_) => super::window::execute(transaction, plan, stage_id),
            OperatorSpec::TopN(_) => super::topn::execute(transaction, plan, stage_id),
        }
    })
}
