//! Bounded scalar control flow for a durable Window stage.
//!
//! PostgreSQL owns partition keys, ordering values, peer comparisons, frame
//! bounds, function arguments, candidate rows, and visible rows. Rust keeps
//! only the phase and stable relation IDs needed to resume one database
//! primitive after a commit or restart.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiTupleTable};

use crate::execution::KernelTransition;
use crate::execution::{
    advance_input, append_frontier, attribute_matches_slot, canonical_row_key_sql, chunk,
    compile_named_outputs, compile_stage_bindings, database_nonnegative as window_nonnegative,
    next_chunk, payload_facts, replace_continuation_cas, required_table as window_required,
    scalar_work_bytes_sql, validate_continuation_abi as validate_typed_continuation_abi,
    validate_output_attributes, AttributeRef, BindingInput, ChunkKind, ContinuationColumn,
    InputPosition, KernelCompletion, KernelPhase, OutputFacts, PageFacts, PhaseCode,
    PrimitiveFacts, ProducerKind, RelationRef, StepContext, TypeRef, WorkUsage,
};
use crate::planner::model::{
    DataflowPlan, DataflowStage, OperatorSpec, OutputSlot, SlotType, WindowExpr, WindowSpec,
};
use crate::planner::scalar_sql::{compile_scalar_expression, SqlBinding};
use crate::planner::WorkBudget;
use crate::postgres::{format_lsn, quote_identifier};

use super::aggregate_capability::{
    decode_aggregate_capability, initial_state_sql, AggregateCapability, AGGREGATE_CAPABILITY_SQL,
};
use super::btree::{
    resolve_client as resolve_btree_client, resolve_step as resolve_btree_step, BtreeOrder,
};
use super::register::{
    catalog_continuation, catalog_state, column_sql, qualified_internal, resolve_relation_oid,
};

const ADMIT_PHASE: i16 = 1;
const ENUMERATE_PHASE: i16 = 2;
const PEERS_PHASE: i16 = 3;
const FRAMES_PHASE: i16 = 4;
const FOLD_AGGREGATE_PHASE: i16 = 5;
const EVALUATE_PHASE: i16 = 6;
const DIFF_PHASE: i16 = 7;
const CLEANUP_PHASE: i16 = 8;
const FRONTIER_PHASE: i16 = 9;

const CONTINUATION_COLUMNS: &[ContinuationColumn] = &[
    ContinuationColumn::required("singleton", pg_sys::BOOLOID),
    ContinuationColumn::required("phase", pg_sys::INT2OID),
    ContinuationColumn::required("input_stream_id", pg_sys::INT8OID),
    ContinuationColumn::nullable("input_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::nullable("input_row_ordinal", pg_sys::INT8OID),
    ContinuationColumn::nullable("partition_queue_id", pg_sys::INT8OID),
    ContinuationColumn::nullable("function_ordinal", pg_sys::INT4OID),
    ContinuationColumn::nullable("output_ordinal", pg_sys::INT8OID),
    ContinuationColumn::nullable("cursor_row_id", pg_sys::INT8OID),
    ContinuationColumn::required("fold_ready", pg_sys::BOOLOID),
    ContinuationColumn::required("cursor_repeat", pg_sys::BOOLOID),
    ContinuationColumn::nullable("diff_leg", pg_sys::INT2OID),
    ContinuationColumn::nullable("cleanup_ordinal", pg_sys::INT4OID),
    ContinuationColumn::nullable("after_kind", pg_sys::INT2OID),
    ContinuationColumn::nullable("after_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::nullable("after_row_ordinal", pg_sys::INT8OID),
];

/// Caps aggregate-frame control work even when every frame is empty.
///
/// Input rows and bytes remain the primary budget. An output ordinal whose
/// frame contains no rows consumes neither, so it also consumes one explicit
/// work item before the step may visit the next ordinal.
const WINDOW_FOLD_WORK_ITEM_CAP: usize = 64;

mod machine;
mod output;
mod primitives;
mod provision;
mod step;

use machine::*;
use output::*;
use primitives::*;
use step::*;

pub(crate) use provision::{provision, KERNEL};

#[cfg(test)]
mod tests;
