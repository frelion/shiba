//! Rust-owned control flow for bounded dataflow stages.
//!
//! PostgreSQL relations remain authoritative for typed rows, arrangements,
//! aggregate state, ordering keys, and pending output. This module owns only
//! scalar control facts: phase, stable row IDs, stream positions, budgets,
//! and transaction outcomes.

mod aggregate;
mod aggregate_capability;
mod bindings;
mod btree;
mod contract;
mod dispatcher;
mod distinct;
mod join;
mod linear;
pub(crate) mod register;
mod sink;
mod step;
mod storage;
mod stream;
mod topn;
mod window;

pub(crate) use crate::logical::WorkUsage;
pub(crate) use bindings::{
    attribute_matches_slot, compile_named_outputs, compile_stage_bindings,
    validate_output_attributes, BindingInput,
};
pub(crate) use contract::{
    AdmissionProgress, InputPosition, OutputFacts, PageFacts, PhaseCode, PrimitiveFacts,
};
pub(crate) use dispatcher::execute_step;
pub(crate) use step::{InputState, OutputAppendTarget, ProducerKind, StepStart, StepTxn};
pub(crate) use storage::{
    canonical_row_key_sql, scalar_work_bytes_sql, AttributeRef, PayloadStorage, RelationRef,
    TypeRef,
};
pub(crate) use stream::{
    advance_input, append_frontier, chunk, next_chunk, payload_facts, ChunkKind, ChunkMeta,
};
