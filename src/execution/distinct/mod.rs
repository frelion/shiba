//! Scalar control flow for a durable Distinct.
//!
//! PostgreSQL owns typed keys, representative rows, multiplicities, and the
//! set transition. Rust owns only the immutable input position, bounded
//! counters, and whether the pinned chunk is data or a frontier.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiTupleTable};

use crate::execution::StepReceipt;
use crate::execution::{
    InputPosition, KernelCompletion, KernelPhase, OutputAppendTarget, OutputFacts, PrimitiveFacts,
};
use crate::planner::model::{DataflowPlan, DataflowStage, OperatorSpec};
use crate::planner::scalar_sql::compile_scalar_expression;
use crate::planner::WorkBudget;
use crate::postgres::{format_lsn, parse_lsn, quote_identifier};

use super::btree::{
    resolve_client as resolve_btree_client, resolve_step as resolve_btree_step, BtreeOrder,
};
use super::register::{
    catalog_continuation, catalog_state, column_sql, qualified_internal, resolve_relation_oid,
};
use super::{
    advance_input, append_frontier, attribute_matches_slot, canonical_row_key_sql,
    compile_named_outputs, compile_stage_bindings, lock_continuation, next_chunk, payload_facts,
    replace_continuation_cas, validate_continuation_abi as validate_typed_continuation_abi,
    validate_output_attributes, BindingInput, ChunkKind, ChunkMeta, ContinuationColumn,
    ProducerKind, RelationRef, StepContext, TypeRef, WorkUsage,
};

const CONTINUATION_COLUMNS: &[ContinuationColumn] = &[
    ContinuationColumn::required("singleton", pg_sys::BOOLOID),
    ContinuationColumn::required("phase", pg_sys::INT2OID),
    ContinuationColumn::required("input_stream_id", pg_sys::INT8OID),
    ContinuationColumn::required("input_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::required("next_row_ordinal", pg_sys::INT8OID),
];

mod machine;
mod provision;
mod runtime;

use machine::*;

pub(crate) use provision::provision;
pub(crate) use runtime::KERNEL;

#[cfg(test)]
mod tests;
