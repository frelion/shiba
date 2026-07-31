use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::planner::model::{DataflowPlan, ExecutionSettings};
use crate::planner::{StepExecution, StepOutcome, WorkBudget};

use super::contract::LifecyclePhase;
use super::step::StepReceipt;
use super::{ProducerKind, StageMetadataCache, StepContext, StepContextStart};

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
pub(crate) struct OperatorDescriptor {
    pub(crate) phases: &'static [LifecyclePhase],
    pub(crate) inputs: &'static [InputContract],
    pub(crate) output: OutputContract,
}

impl OperatorDescriptor {
    #[allow(dead_code)]
    pub(crate) fn supports_phase(self, phase: LifecyclePhase) -> bool {
        self.phases.contains(&phase)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KernelContract {
    descriptor: OperatorDescriptor,
}

impl KernelContract {
    pub(crate) const fn with_phases(
        inputs: &'static [InputContract],
        output: OutputContract,
        phases: &'static [LifecyclePhase],
    ) -> Self {
        Self {
            descriptor: OperatorDescriptor {
                phases,
                inputs,
                output,
            },
        }
    }

    pub(crate) const fn descriptor(self) -> OperatorDescriptor {
        self.descriptor
    }

    fn validate(self, context: &StepContext<'_, '_>) -> Result<(), String> {
        if context.inputs().len() != self.descriptor.inputs.len() {
            return Err("kernel context does not match its input contract".into());
        }
        for (port, expected) in self.descriptor.inputs.iter().enumerate() {
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

    fn validate_phase(self, phase: LifecyclePhase) -> Result<(), String> {
        if self.descriptor.supports_phase(phase) {
            Ok(())
        } else {
            Err(format!(
                "kernel does not support the {:?} lifecycle phase",
                phase
            ))
        }
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
    ) -> Result<StepReceipt, String>;
}

pub(crate) type KernelStep = for<'client, 'conn> fn(
    &mut StepContext<'client, 'conn>,
    &DataflowPlan,
    u32,
) -> Result<StepReceipt, String>;

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
    ) -> Result<StepReceipt, String> {
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
        let descriptor = contract.descriptor();
        let expected_inputs = u16::try_from(descriptor.inputs.len())
            .map_err(|_| "kernel input contract exceeds smallint")?;
        let expects_output = descriptor.output == OutputContract::EffectStream;
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
        contract.validate_phase(transition.phase())?;
        context.commit(transition, permit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_contract_exposes_operator_phase_capabilities() {
        let contract = KernelContract::with_phases(
            &[],
            OutputContract::EffectStream,
            &[LifecyclePhase::Process, LifecyclePhase::Frontier],
        );
        assert!(contract
            .descriptor()
            .supports_phase(LifecyclePhase::Process));
        assert!(!contract.descriptor().supports_phase(LifecyclePhase::Admit));
    }

    #[test]
    fn kernel_contract_rejects_an_undeclared_receipt_phase() {
        let contract =
            KernelContract::with_phases(&[], OutputContract::Sink, &[LifecyclePhase::Process]);
        assert!(contract.validate_phase(LifecyclePhase::Frontier).is_err());
        assert!(contract.validate_phase(LifecyclePhase::Process).is_ok());
    }
}
