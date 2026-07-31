use std::collections::HashSet;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

use crate::execution::StepReceipt;
use crate::planner::model::{DataflowPlan, DataflowStage, OperatorSpec, ScanSpec};
use crate::planner::scalar_sql::compile_scalar_expression;
use crate::postgres::{format_lsn, parse_lsn, quote_identifier};

use super::{
    advance_input, append_frontier, attribute_matches_slot, clear_continuation_locked,
    compile_named_outputs, compile_stage_bindings, lock_continuation, next_chunk, nonnegative,
    payload_facts, replace_continuation_cas, required_table,
    validate_continuation_abi as validate_typed_continuation_abi, validate_output_attributes,
    BindingInput, ChunkKind, ChunkMeta, ContinuationColumn, KernelPhase, OutputAppendTarget,
    OutputFacts, PhaseCode, PrimitiveFacts, ProducerKind, RelationRef, StepContext, TypeRef,
    WorkUsage,
};

const SCAN_COLUMNS: &[ContinuationColumn] = &[
    ContinuationColumn::required("singleton", pg_sys::BOOLOID),
    ContinuationColumn::required("phase", pg_sys::INT2OID),
    ContinuationColumn::required("input_stream_id", pg_sys::INT8OID),
    ContinuationColumn::nullable("input_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::nullable("next_row_ordinal", pg_sys::INT8OID),
    ContinuationColumn::nullable("next_bootstrap_seq", pg_sys::INT8OID),
    ContinuationColumn::nullable_as("pending_frontier_lsn", pg_sys::PG_LSNOID, "pg_lsn"),
];
const TRANSFORM_COLUMNS: &[ContinuationColumn] = &[
    ContinuationColumn::required("singleton", pg_sys::BOOLOID),
    ContinuationColumn::required("input_stream_id", pg_sys::INT8OID),
    ContinuationColumn::required("input_chunk_seq", pg_sys::INT8OID),
    ContinuationColumn::required("next_row_ordinal", pg_sys::INT8OID),
];

mod machine;
mod runtime;
mod storage;

use machine::*;
use storage::*;

pub(crate) use runtime::{SCAN_KERNEL, TRANSFORM_KERNEL};

#[cfg(test)]
mod tests;
