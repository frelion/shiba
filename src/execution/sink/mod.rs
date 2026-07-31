//! Transactional Sink execution.
//!
//! One transaction quantum walks a bounded prefix of effect rows and applies
//! each row's bounded weight page. The arbitrary composite remains inside
//! PostgreSQL; Rust carries only stream positions, signed weights, measured
//! byte sizes, and the shared quantum budget.

use std::collections::BTreeMap;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

use crate::execution::StepReceipt;
use crate::planner::model::{
    BindingId, DataflowPlan, DataflowStage, OperatorSpec, SlotId, SlotType,
};
use crate::planner::{WorkBudget, WorkQuantum};
use crate::postgres::quote_identifier;

use super::{
    advance_input, lock_continuation, next_chunk, nonnegative, replace_continuation_cas,
    required_row, required_table as required,
    validate_continuation_abi as validate_typed_continuation_abi, AttributeRef, ChunkKind,
    ChunkMeta, ContinuationColumn, InputPosition, KernelPhase, RelationRef, StepContext, WorkUsage,
};

const CONTINUATION_COLUMNS: &[ContinuationColumn] = &[
    ContinuationColumn::required("singleton", pg_sys::BOOLOID),
    ContinuationColumn::required("input_stream_id", pg_sys::INT8OID),
    ContinuationColumn::required("input_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::required("row_ordinal", pg_sys::INT8OID),
    ContinuationColumn::nullable("remaining_weight", pg_sys::INT8OID),
];

mod machine;
mod runtime;

use machine::*;
#[cfg(test)]
use runtime::*;

pub(crate) use runtime::KERNEL;

#[cfg(test)]
mod tests;
