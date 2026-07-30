//! Bounded scalar control flow for a durable TopN stage.
//!
//! PostgreSQL owns every typed input, sort key, collation comparison,
//! candidate row, and visible row. Rust owns only fixed plan counts, stable
//! relation IDs, and counters needed to resume a bounded keyset page.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::execution::KernelTransition;
use crate::execution::{
    advance_input, append_frontier, attribute_matches_slot, canonical_row_key_sql, chunk,
    compile_named_outputs, compile_stage_bindings, database_nonnegative as nonnegative, next_chunk,
    payload_facts, replace_continuation_cas, required_table as required,
    validate_continuation_abi as validate_typed_continuation_abi, validate_output_attributes,
    AttributeRef, BindingInput, ChunkKind, ContinuationColumn, InputPosition, KernelCompletion,
    KernelPhase, OutputFacts, PageFacts, PhaseCode, PrimitiveFacts, ProducerKind, RelationRef,
    StepContext, TypeRef, WorkUsage,
};
use crate::planner::model::{DataflowPlan, DataflowStage, OperatorSpec, TopNSpec};
use crate::planner::scalar_sql::compile_scalar_expression;
use crate::planner::WorkBudget;
use crate::postgres::{format_lsn, quote_identifier};

use super::btree::{
    resolve_client as resolve_btree_client, resolve_step as resolve_btree_step, BtreeOrder,
};
use super::register::{
    catalog_continuation, catalog_state, column_sql, qualified_internal, resolve_relation_oid,
};

const ADMIT_PHASE: i16 = 1;
const SELECT_PHASE: i16 = 2;
const DIFF_PHASE: i16 = 3;
const CLEANUP_PHASE: i16 = 4;
const FRONTIER_PHASE: i16 = 5;

const CONTINUATION_COLUMNS: &[ContinuationColumn] = &[
    ContinuationColumn::required("singleton", pg_sys::BOOLOID),
    ContinuationColumn::required("phase", pg_sys::INT2OID),
    ContinuationColumn::required("input_stream_id", pg_sys::INT8OID),
    ContinuationColumn::nullable("input_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::nullable("input_row_ordinal", pg_sys::INT8OID),
    ContinuationColumn::nullable("generation_id", pg_sys::INT8OID),
    ContinuationColumn::nullable("cursor_row_id", pg_sys::INT8OID),
    ContinuationColumn::required("cursor_repeat", pg_sys::BOOLOID),
    ContinuationColumn::nullable_as("offset_remaining", pg_sys::NUMERICOID, "numeric"),
    ContinuationColumn::nullable_as("limit_remaining", pg_sys::NUMERICOID, "numeric"),
    ContinuationColumn::nullable("tie_boundary_row_id", pg_sys::INT8OID),
    ContinuationColumn::nullable("diff_leg", pg_sys::INT2OID),
    ContinuationColumn::nullable("after_kind", pg_sys::INT2OID),
    ContinuationColumn::nullable("after_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::nullable("after_row_ordinal", pg_sys::INT8OID),
];

mod machine;
mod provision;
mod runtime;

use machine::*;
#[cfg(test)]
use runtime::*;

pub(crate) use provision::provision;
pub(crate) use runtime::KERNEL;

#[cfg(test)]
mod tests;
