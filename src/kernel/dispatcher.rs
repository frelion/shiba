//! One closed Rust dispatcher for every operator step.

use pgrx::prelude::*;

use crate::logical::model::{DataflowPlan, OperatorSpec};
use crate::logical::{StepExecution, WorkBudget};

use super::KernelRunner;

pub(crate) fn execute_step(
    result_oid: pg_sys::Oid,
    stage_id: u32,
    plan: &DataflowPlan,
    budget: WorkBudget,
) -> Result<StepExecution, String> {
    let stage = plan
        .stages
        .get(usize::try_from(stage_id).map_err(|_| "operator stage ID exceeds usize")?)
        .ok_or_else(|| format!("dataflow has no stage {stage_id}"))?;
    let kernel = match &stage.spec {
        OperatorSpec::Scan(_) => &super::linear::SCAN_KERNEL,
        OperatorSpec::Filter(_) | OperatorSpec::Project(_) => &super::linear::TRANSFORM_KERNEL,
        OperatorSpec::Distinct(_) => &super::distinct::KERNEL,
        OperatorSpec::Sink => &super::sink::KERNEL,
        OperatorSpec::Join(_) => &super::join::KERNEL,
        OperatorSpec::Aggregate(_) => &super::aggregate::KERNEL,
        OperatorSpec::Window(_) => &super::window::KERNEL,
        OperatorSpec::TopN(_) => &super::topn::KERNEL,
    };
    Spi::connect_mut(|client| {
        KernelRunner::run(
            client,
            kernel,
            result_oid,
            stage_id,
            &plan.execution_settings,
            budget,
            plan,
        )
    })
}
