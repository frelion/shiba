use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::planner::model::{DataflowPlan, ExecutionSettings};
use crate::planner::{StepExecution, StepOutcome, WorkBudget};

use super::contract::KernelCompletion;
use super::{ProducerKind, StageMetadataCache, StepContext, StepContextStart, WorkUsage};

/// Unforgeable outside this module. `StepContext` requires it for both ends of
/// the lifecycle, so an operator cannot bypass `KernelRunner`.
#[derive(Clone, Copy)]
pub(super) struct LifecyclePermit(());

impl LifecyclePermit {
    const fn new() -> Self {
        Self(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputContract {
    Source,
    Operator,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutputContract {
    EffectStream,
    Sink,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KernelContract {
    inputs: &'static [InputContract],
    output: OutputContract,
}

impl KernelContract {
    pub(crate) const fn new(inputs: &'static [InputContract], output: OutputContract) -> Self {
        Self { inputs, output }
    }

    fn validate(self, context: &StepContext<'_, '_>) -> Result<(), String> {
        if context.inputs().len() != self.inputs.len() {
            return Err("kernel context does not match its input contract".into());
        }
        for (port, expected) in self.inputs.iter().enumerate() {
            let port = u16::try_from(port).map_err(|_| "kernel input port exceeds smallint")?;
            let input = context.input(port)?;
            let producer_matches = matches!(
                (expected, input.producer),
                (InputContract::Source, ProducerKind::Source)
                    | (InputContract::Operator, ProducerKind::Operator)
            );
            if input.port != port || !producer_matches {
                return Err(format!(
                    "kernel input {port} does not match its stream contract"
                ));
            }
        }
        Ok(())
    }
}

/// The complete result of one bounded operator invocation.
///
/// Kernels may mutate typed state and continuation through `StepContext`, but
/// only `KernelRunner` can turn this transition into a durable checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KernelTransition {
    pub(super) completion: KernelCompletion,
    pub(super) usage: WorkUsage,
}

impl KernelTransition {
    pub(crate) const fn new(has_continuation: bool, usage: WorkUsage) -> Self {
        Self::from_completion(
            if has_continuation {
                KernelCompletion::Continue
            } else {
                KernelCompletion::Finished
            },
            usage,
        )
    }

    pub(crate) const fn from_completion(completion: KernelCompletion, usage: WorkUsage) -> Self {
        Self { completion, usage }
    }

    pub(crate) const fn completion(self) -> KernelCompletion {
        self.completion
    }
}

/// One operator algorithm. Transaction setup and checkpoint commit are not
/// part of this contract.
pub(crate) trait Kernel {
    fn contract(&self) -> KernelContract;

    fn step(
        &self,
        context: &mut StepContext<'_, '_>,
        plan: &DataflowPlan,
        stage_id: u32,
    ) -> Result<KernelTransition, String>;
}

pub(crate) type KernelStep = for<'client, 'conn> fn(
    &mut StepContext<'client, 'conn>,
    &DataflowPlan,
    u32,
) -> Result<KernelTransition, String>;

/// Adapts an operator module's single step function to the shared contract.
pub(crate) struct KernelFn {
    contract: KernelContract,
    step: KernelStep,
}

impl KernelFn {
    pub(crate) const fn new(contract: KernelContract, step: KernelStep) -> Self {
        Self { contract, step }
    }
}

impl Kernel for KernelFn {
    fn contract(&self) -> KernelContract {
        self.contract
    }

    fn step(
        &self,
        context: &mut StepContext<'_, '_>,
        plan: &DataflowPlan,
        stage_id: u32,
    ) -> Result<KernelTransition, String> {
        (self.step)(context, plan, stage_id)
    }
}

/// Owns the lifecycle shared by every operator: admission, locking, bounded
/// execution, output publication, and the checkpoint compare-and-set.
pub(crate) struct KernelRunner;

impl KernelRunner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn run(
        client: &mut SpiClient<'_>,
        kernel: &dyn Kernel,
        result_oid: pg_sys::Oid,
        stage_id: u32,
        settings: &ExecutionSettings,
        budget: WorkBudget,
        plan: &DataflowPlan,
        metadata_cache: StageMetadataCache,
    ) -> Result<StepExecution, String> {
        let contract = kernel.contract();
        let permit = LifecyclePermit::new();
        let expected_inputs = u16::try_from(contract.inputs.len())
            .map_err(|_| "kernel input contract exceeds smallint")?;
        let expects_output = contract.output == OutputContract::EffectStream;
        let mut context = match StepContext::begin(
            client,
            result_oid,
            stage_id,
            expected_inputs,
            expects_output,
            settings,
            budget,
            metadata_cache,
            permit,
        )? {
            StepContextStart::Blocked => {
                return Ok(StepExecution::empty(StepOutcome::Blocked));
            }
            StepContextStart::Idle => return Ok(StepExecution::empty(StepOutcome::Idle)),
            StepContextStart::Ready(context) => *context,
        };
        contract.validate(&context)?;
        let transition = kernel.step(&mut context, plan, stage_id)?;
        context.commit(transition, permit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_transition_exposes_completion_without_changing_legacy_constructor() {
        assert_eq!(
            KernelTransition::new(true, WorkUsage::default()).completion(),
            KernelCompletion::Continue
        );
        assert_eq!(
            KernelTransition::new(false, WorkUsage::default()).completion(),
            KernelCompletion::Finished
        );
        assert_eq!(
            KernelTransition::from_completion(KernelCompletion::Continue, WorkUsage::default())
                .completion(),
            KernelCompletion::Continue
        );
    }
}
