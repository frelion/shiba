//! Shared execution infrastructure used by every operator.
//!
//! Operator modules should depend on this boundary for lifecycle, storage,
//! continuation, and stream primitives. Keeping these re-exports together
//! makes the dependency direction visible: operators consume the execution
//! protocol, while the protocol does not depend on a concrete operator.

pub(crate) use super::bindings::{
    attribute_matches_slot, compile_named_outputs, compile_stage_bindings,
    validate_output_attributes, BindingInput,
};
pub(crate) use super::continuation::{
    clear_locked as clear_continuation_locked, lock_one as lock_continuation,
    replace_cas as replace_continuation_cas, validate_abi as validate_continuation_abi,
    validate_authority as validate_continuation_authority, Column as ContinuationColumn,
};
pub(crate) use super::contract::{
    AdmissionProgress, InputPosition, KernelCompletion, KernelPhase, OutputFacts, PageFacts,
    PhaseCode, PrimitiveFacts,
};
pub(crate) use super::dispatcher::execute_step;
pub(crate) use super::runner::{
    InputContract, KernelContract, KernelFn, KernelRunner, KernelTransition, OutputContract,
};
pub(crate) use super::step::{
    InputState, OutputAppendTarget, ProducerKind, StageMetadataCache, StepContext, StepContextStart,
};
pub(crate) use super::storage::payload as resolve_payload_storage;
pub(crate) use super::storage::{
    canonical_row_key_sql, scalar_work_bytes_sql, AttributeRef, PayloadStorage, RelationRef,
    TypeRef,
};
pub(crate) use super::stream::{
    advance_input, append_frontier, chunk, next_chunk, payload_facts, ChunkKind, ChunkMeta,
};
pub(crate) use crate::database::{
    database_nonnegative, nonnegative, required as required_table, required_heap as required_row,
};
pub(crate) use crate::planner::WorkUsage;
