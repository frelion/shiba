//! Scalar control flow for a durable Aggregate.
//!
//! Group keys, input rows, transition values, ordering keys, distinct sets,
//! and pending output rows never enter this module. They remain in the
//! operator's typed PostgreSQL relations. The Runtime gives this state machine
//! only stable identifiers and the measured result of one database primitive.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiTupleTable};

use crate::execution::StepReceipt;
use crate::execution::{
    InputPosition, KernelCompletion, KernelPhase, OutputFacts, PageFacts, PrimitiveFacts,
};
use crate::planner::model::{
    AggregateExpr, AggregateSpec, DataflowPlan, DataflowStage, OperatorSpec, SortGroupExpr,
};
use crate::planner::scalar_sql::compile_scalar_expression;
use crate::planner::WorkBudget;
use crate::postgres::{format_lsn, parse_lsn, quote_identifier};

use super::aggregate_capability::{
    decode_aggregate_capability, initial_state_sql, AggregateCapability, AGGREGATE_CAPABILITY_SQL,
};
use super::btree::{resolve_client as resolve_btree_client, resolve_step as resolve_btree_step};
use super::register::{
    catalog_continuation, catalog_state, column_sql, qualified_internal, resolve_relation_oid,
};
use super::{
    advance_input, append_frontier, attribute_matches_slot, canonical_row_key_sql, chunk,
    compile_stage_bindings, next_chunk, payload_facts, replace_continuation_cas,
    validate_continuation_abi as validate_typed_continuation_abi, AttributeRef, BindingInput,
    ChunkKind, ContinuationColumn, OutputAppendTarget, ProducerKind, RelationRef, StepContext,
    TypeRef, WorkUsage,
};

const APPLY_PHASE: i16 = 1;
const DRAIN_REBUILD_PHASE: i16 = 2;
const DRAIN_EMIT_PHASE: i16 = 3;
const FRONTIER_PHASE: i16 = 4;

const CONTINUATION_COLUMNS: &[ContinuationColumn] = &[
    ContinuationColumn::required("singleton", pg_sys::BOOLOID),
    ContinuationColumn::required("phase", pg_sys::INT2OID),
    ContinuationColumn::required("input_stream_id", pg_sys::INT8OID),
    ContinuationColumn::nullable("input_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::nullable("input_row_ordinal", pg_sys::INT8OID),
    ContinuationColumn::nullable("group_queue_id", pg_sys::INT8OID),
    ContinuationColumn::nullable("aggregate_ordinal", pg_sys::INT4OID),
    ContinuationColumn::nullable("emit_leg", pg_sys::INT2OID),
    ContinuationColumn::nullable("after_kind", pg_sys::INT2OID),
    ContinuationColumn::nullable("after_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::nullable("after_row_ordinal", pg_sys::INT8OID),
];

mod machine;
mod provision;
mod runtime;

use machine::*;
use provision::*;
use runtime::*;

pub(crate) use provision::provision;
pub(crate) use runtime::KERNEL;

#[cfg(test)]
mod tests;
