use std::collections::HashSet;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

use crate::logical::model::{DataflowPlan, DataflowStage, OperatorSpec, ScanSpec};
use crate::logical::StepExecution;
use crate::postgres::{format_lsn, parse_lsn, quote_identifier};
use crate::scalar_sql::compile_scalar_expression;

use super::{
    advance_input, append_frontier, attribute_matches_slot, compile_named_outputs,
    compile_stage_bindings, next_chunk, payload_facts, validate_output_attributes, BindingInput,
    ChunkKind, ChunkMeta, OutputFacts, PhaseCode, PrimitiveFacts, ProducerKind, RelationRef,
    StepTxn, TypeRef, WorkUsage,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LinearKind {
    Scan,
    Filter,
    Project,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanPhase {
    Bootstrap = 1,
    SnapshotFrontier = 2,
    Data = 3,
    SourceFrontier = 4,
}

impl ScanPhase {
    fn decode(raw: i16) -> Result<Self, String> {
        match PhaseCode::active(raw)?.value() {
            1 => Ok(Self::Bootstrap),
            2 => Ok(Self::SnapshotFrontier),
            3 => Ok(Self::Data),
            4 => Ok(Self::SourceFrontier),
            phase => Err(format!("Scan continuation has unknown phase {phase}")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScanContinuation {
    phase: ScanPhase,
    input_stream_id: i64,
    input_chunk_seq: Option<i64>,
    next_row_ordinal: Option<i64>,
    next_bootstrap_seq: Option<i64>,
    pending_frontier_lsn: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TransformContinuation {
    input_stream_id: i64,
    input_chunk_seq: i64,
    next_row_ordinal: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TransformFacts {
    usage: WorkUsage,
    first_ordinal: i64,
    last_ordinal: i64,
    output: OutputFacts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BootstrapFacts {
    usage: WorkUsage,
    first_sequence: Option<i64>,
    last_sequence: Option<i64>,
    output: OutputFacts,
}

pub(crate) fn execute(
    mut transaction: StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage_id: u32,
) -> Result<StepExecution, String> {
    let stage = plan
        .stages
        .get(usize::try_from(stage_id).map_err(|_| "stage ID exceeds usize")?)
        .ok_or_else(|| format!("dataflow has no stage {stage_id}"))?;
    let kind = match &stage.spec {
        OperatorSpec::Scan(_) => LinearKind::Scan,
        OperatorSpec::Filter(_) => LinearKind::Filter,
        OperatorSpec::Project(_) => LinearKind::Project,
        _ => return Err("linear kernel received a stateful or Sink stage".into()),
    };
    if transaction.inputs().len() != 1 || transaction.input(0)?.port != 0 {
        return Err("linear stage must have exactly one input".into());
    }
    match (kind, transaction.input(0)?.producer) {
        (LinearKind::Scan, ProducerKind::Source) => {}
        (LinearKind::Filter | LinearKind::Project, ProducerKind::Operator) => {}
        _ => return Err("linear stage input producer does not match its operator".into()),
    }

    let continuation = transaction.continuation_storage()?;
    match kind {
        LinearKind::Scan => execute_scan(transaction, plan, stage, continuation),
        LinearKind::Filter | LinearKind::Project => {
            execute_transform(transaction, plan, stage, kind, continuation)
        }
    }
}

fn execute_scan(
    mut transaction: StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    continuation_relation: RelationRef,
) -> Result<StepExecution, String> {
    let OperatorSpec::Scan(spec) = &stage.spec else {
        return Err("Scan kernel received another operator".into());
    };
    let input = transaction.input(0)?.clone();
    if input.source_oid.map(pg_sys::Oid::to_u32) != Some(spec.source_oid) {
        return Err("Scan source OID does not match its source stream".into());
    }
    validate_continuation_abi(
        &mut transaction,
        &continuation_relation,
        &[
            ("singleton", pg_sys::BOOLOID, true),
            ("phase", pg_sys::INT2OID, true),
            ("input_stream_id", pg_sys::INT8OID, true),
            ("input_chunk_seq", pg_sys::INT8OID, false),
            ("next_row_ordinal", pg_sys::INT8OID, false),
            ("next_bootstrap_seq", pg_sys::INT8OID, false),
            ("pending_frontier_lsn", pg_sys::PG_LSNOID, false),
        ],
    )?;
    let continuation = load_scan_continuation(&mut transaction, &continuation_relation)?;
    validate_authority(
        transaction.checkpoint_had_continuation(),
        continuation.is_some(),
    )?;
    if let Some(continuation) = &continuation {
        if continuation.input_stream_id != input.stream_id {
            return Err("Scan continuation references another input stream".into());
        }
        validate_scan_continuation(continuation)?;
    }

    match continuation {
        Some(continuation) if continuation.phase == ScanPhase::Bootstrap => {
            step_bootstrap(transaction, &continuation_relation, &continuation)
        }
        Some(continuation) if continuation.phase == ScanPhase::SnapshotFrontier => {
            step_snapshot_frontier(transaction, &continuation_relation, &continuation)
        }
        Some(continuation) if continuation.phase == ScanPhase::SourceFrontier => {
            step_scan_frontier(
                transaction,
                &continuation_relation,
                continuation
                    .pending_frontier_lsn
                    .ok_or_else(|| "Scan frontier continuation omitted its LSN".to_string())?,
            )
        }
        continuation => {
            let row_ordinal = continuation
                .as_ref()
                .and_then(|state| state.next_row_ordinal)
                .unwrap_or(0);
            let chunk = next_chunk(&mut transaction, 0)?;
            let Some(chunk) = chunk else {
                if continuation.is_some() {
                    return Err("Scan continuation references a missing input chunk".into());
                }
                return step_available_source_frontier(transaction);
            };
            if chunk.kind != ChunkKind::Data {
                return Err("source stream contains a frontier chunk".into());
            }
            if let Some(continuation) = &continuation {
                if continuation.input_chunk_seq != Some(chunk.sequence) {
                    return Err("Scan continuation is not at its input cursor".into());
                }
            }
            step_data_transform(
                transaction,
                plan,
                stage,
                LinearKind::Scan,
                spec,
                &continuation_relation,
                continuation.is_some(),
                chunk,
                row_ordinal,
            )
        }
    }
}

fn execute_transform(
    mut transaction: StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    kind: LinearKind,
    continuation_relation: RelationRef,
) -> Result<StepExecution, String> {
    validate_continuation_abi(
        &mut transaction,
        &continuation_relation,
        &[
            ("singleton", pg_sys::BOOLOID, true),
            ("input_stream_id", pg_sys::INT8OID, true),
            ("input_chunk_seq", pg_sys::INT8OID, true),
            ("next_row_ordinal", pg_sys::INT8OID, true),
        ],
    )?;
    let continuation = load_transform_continuation(&mut transaction, &continuation_relation)?;
    validate_authority(
        transaction.checkpoint_had_continuation(),
        continuation.is_some(),
    )?;
    let input = transaction.input(0)?.clone();
    if continuation.is_some_and(|state| state.input_stream_id != input.stream_id) {
        return Err("linear continuation references another input stream".into());
    }
    let chunk = next_chunk(&mut transaction, 0)?
        .ok_or_else(|| "runnable linear stage has no input chunk".to_string())?;
    if continuation.is_some_and(|state| state.input_chunk_seq != chunk.sequence) {
        return Err("linear continuation is not at its input cursor".into());
    }
    let row_ordinal = continuation.map_or(0, |state| state.next_row_ordinal);
    if row_ordinal < 0 {
        return Err("linear continuation has a negative row ordinal".into());
    }
    if chunk.kind == ChunkKind::Frontier {
        if row_ordinal != 0 {
            return Err("frontier continuation has a row offset".into());
        }
        append_frontier(&mut transaction, chunk.lsn)?;
        advance_input(
            &mut transaction,
            0,
            chunk.sequence + 1,
            chunk.lsn,
            WorkUsage::default(),
        )?;
        delete_continuation(
            &mut transaction,
            &continuation_relation,
            continuation.is_some(),
        )?;
        return transaction.finish(false, WorkUsage::default());
    }
    let scan_placeholder = ScanSpec {
        source_oid: 0,
        columns: Vec::new(),
    };
    step_data_transform(
        transaction,
        plan,
        stage,
        kind,
        &scan_placeholder,
        &continuation_relation,
        continuation.is_some(),
        chunk,
        row_ordinal,
    )
}

fn step_bootstrap(
    mut transaction: StepTxn<'_, '_>,
    continuation_relation: &RelationRef,
    continuation: &ScanContinuation,
) -> Result<StepExecution, String> {
    let next_sequence = continuation
        .next_bootstrap_seq
        .ok_or_else(|| "Scan bootstrap continuation omitted its cursor".to_string())?;
    let bootstrap = transaction.state_storage(0)?;
    validate_bootstrap_abi(&mut transaction, &bootstrap)?;
    let output = transaction.output()?.clone();
    let output_storage = transaction.payload_storage(output.stream_id)?;
    let bootstrap_attributes = transaction.relation_attributes(bootstrap.oid())?;
    if bootstrap_attributes[1].type_oid != output_storage.row_type.oid() {
        return Err("Scan bootstrap row type changed identity".into());
    }
    let activation_lsn = transaction.input(0)?.activation_lsn;
    let facts = run_bootstrap_primitive(
        &mut transaction,
        &bootstrap,
        &output_storage.relation,
        next_sequence,
        activation_lsn,
    )?;

    let completed = match (facts.first_sequence, facts.last_sequence) {
        (None, None) if facts.usage.input_rows == 0 => true,
        (Some(first), Some(last))
            if first == next_sequence
                && last == next_sequence + i64_from_u64(facts.usage.input_rows)? - 1 =>
        {
            let remaining = first_bootstrap_sequence(&mut transaction, &bootstrap, last + 1)?;
            if let Some(remaining) = remaining {
                if remaining != last + 1 {
                    return Err("Scan bootstrap relation has a sequence gap".into());
                }
                let next = ScanContinuation {
                    next_bootstrap_seq: Some(remaining),
                    ..continuation.clone()
                };
                replace_scan_continuation(
                    &mut transaction,
                    continuation_relation,
                    Some(continuation),
                    &next,
                )?;
                false
            } else {
                true
            }
        }
        _ => return Err("Scan bootstrap primitive returned an invalid cursor".into()),
    };
    if completed {
        replace_scan_continuation(
            &mut transaction,
            continuation_relation,
            Some(continuation),
            &ScanContinuation {
                phase: ScanPhase::SnapshotFrontier,
                input_stream_id: continuation.input_stream_id,
                input_chunk_seq: None,
                next_row_ordinal: None,
                next_bootstrap_seq: None,
                pending_frontier_lsn: Some(activation_lsn),
            },
        )?;
    }
    PrimitiveFacts {
        usage: facts.usage,
        state_rows: facts.usage.input_rows,
        continuation_rows: 1,
        output: facts.output,
    }
    .validate(transaction.budget())?;
    transaction.finish(true, facts.usage)
}

fn step_snapshot_frontier(
    mut transaction: StepTxn<'_, '_>,
    continuation_relation: &RelationRef,
    continuation: &ScanContinuation,
) -> Result<StepExecution, String> {
    let input = transaction.input(0)?.clone();
    let frontier = continuation
        .pending_frontier_lsn
        .ok_or_else(|| "Scan snapshot frontier omitted its LSN".to_string())?;
    if frontier != input.activation_lsn || input.consumed_frontier_lsn != input.activation_lsn {
        return Err("Scan snapshot frontier does not match its activation boundary".into());
    }
    append_frontier(&mut transaction, frontier)?;
    delete_scan_continuation(&mut transaction, continuation_relation, continuation)?;
    transaction.finish(false, WorkUsage::default())
}

fn step_available_source_frontier(
    mut transaction: StepTxn<'_, '_>,
) -> Result<StepExecution, String> {
    let input = transaction.input(0)?.clone();
    let frontier = input
        .available_source_frontier_lsn
        .filter(|frontier| *frontier > input.consumed_frontier_lsn)
        .ok_or_else(|| "runnable Scan has neither a chunk nor a source frontier".to_string())?;
    assert_frontier_skips_no_source_chunk(&mut transaction, frontier)?;
    append_frontier(&mut transaction, frontier)?;
    advance_input(
        &mut transaction,
        0,
        input.next_chunk_seq,
        frontier,
        WorkUsage::default(),
    )?;
    transaction.finish(false, WorkUsage::default())
}

fn step_scan_frontier(
    mut transaction: StepTxn<'_, '_>,
    continuation_relation: &RelationRef,
    frontier: u64,
) -> Result<StepExecution, String> {
    let input = transaction.input(0)?.clone();
    if frontier <= input.consumed_frontier_lsn
        || input
            .available_source_frontier_lsn
            .is_none_or(|available| frontier > available)
    {
        return Err("Scan frontier continuation is outside the published frontier".into());
    }
    assert_frontier_skips_no_source_chunk(&mut transaction, frontier)?;
    append_frontier(&mut transaction, frontier)?;
    advance_input(
        &mut transaction,
        0,
        input.next_chunk_seq,
        frontier,
        WorkUsage::default(),
    )?;
    delete_continuation(&mut transaction, continuation_relation, true)?;
    transaction.finish(false, WorkUsage::default())
}

#[allow(clippy::too_many_arguments)]
fn step_data_transform(
    mut transaction: StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    kind: LinearKind,
    scan_spec: &ScanSpec,
    continuation_relation: &RelationRef,
    had_continuation: bool,
    chunk: ChunkMeta,
    row_ordinal: i64,
) -> Result<StepExecution, String> {
    if row_ordinal < 0 || u64::try_from(row_ordinal).map_or(true, |row| row >= chunk.rows) {
        return Err("linear continuation row is outside its input chunk".into());
    }
    let input = transaction.input(0)?.clone();
    if chunk.sequence != input.next_chunk_seq {
        return Err("linear chunk is not at the consumer cursor".into());
    }
    let input_storage = transaction.payload_storage(input.stream_id)?;
    if row_ordinal == 0 {
        payload_facts(&mut transaction, &input_storage.relation, &chunk)?;
    }
    let output = transaction.output()?.clone();
    let output_storage = transaction.payload_storage(output.stream_id)?;
    let (predicate, expressions) = compile_transform(
        &mut transaction,
        plan,
        stage,
        kind,
        scan_spec,
        &input_storage.row_type,
        &output_storage.row_type,
    )?;
    let emit_rows = kind != LinearKind::Scan || chunk.lsn > input.activation_lsn;
    let facts = run_transform_primitive(
        &mut transaction,
        &input_storage.relation,
        &output_storage.relation,
        output_storage.row_type.sql(),
        &chunk,
        row_ordinal,
        &predicate,
        &expressions,
        emit_rows,
    )?;
    if facts.first_ordinal != row_ordinal {
        return Err("linear primitive did not start at its continuation".into());
    }
    let next_row = facts
        .last_ordinal
        .checked_add(1)
        .ok_or_else(|| "linear row ordinal space exhausted".to_string())?;
    let chunk_rows = i64_from_u64(chunk.rows)?;
    let has_continuation = if next_row < chunk_rows {
        match kind {
            LinearKind::Scan => replace_scan_continuation(
                &mut transaction,
                continuation_relation,
                had_continuation.then_some(&ScanContinuation {
                    phase: ScanPhase::Data,
                    input_stream_id: input.stream_id,
                    input_chunk_seq: Some(chunk.sequence),
                    next_row_ordinal: Some(row_ordinal),
                    next_bootstrap_seq: None,
                    pending_frontier_lsn: None,
                }),
                &ScanContinuation {
                    phase: ScanPhase::Data,
                    input_stream_id: input.stream_id,
                    input_chunk_seq: Some(chunk.sequence),
                    next_row_ordinal: Some(next_row),
                    next_bootstrap_seq: None,
                    pending_frontier_lsn: None,
                },
            )?,
            LinearKind::Filter | LinearKind::Project => replace_transform_continuation(
                &mut transaction,
                continuation_relation,
                had_continuation.then_some(TransformContinuation {
                    input_stream_id: input.stream_id,
                    input_chunk_seq: chunk.sequence,
                    next_row_ordinal: row_ordinal,
                }),
                TransformContinuation {
                    input_stream_id: input.stream_id,
                    input_chunk_seq: chunk.sequence,
                    next_row_ordinal: next_row,
                },
            )?,
        }
        true
    } else if next_row == chunk_rows {
        delete_continuation(&mut transaction, continuation_relation, had_continuation)?;
        let mut has_continuation = false;
        if kind == LinearKind::Scan {
            if let Some(frontier) = source_frontier_after_chunk(&mut transaction, &chunk)? {
                replace_scan_continuation(
                    &mut transaction,
                    continuation_relation,
                    None,
                    &ScanContinuation {
                        phase: ScanPhase::SourceFrontier,
                        input_stream_id: input.stream_id,
                        input_chunk_seq: None,
                        next_row_ordinal: None,
                        next_bootstrap_seq: None,
                        pending_frontier_lsn: Some(frontier),
                    },
                )?;
                has_continuation = true;
            }
        }
        advance_input(
            &mut transaction,
            0,
            chunk.sequence + 1,
            input.consumed_frontier_lsn,
            WorkUsage {
                input_rows: chunk.rows,
                input_bytes: chunk.bytes,
                ..WorkUsage::default()
            },
        )?;
        has_continuation
    } else {
        return Err("linear primitive advanced beyond its input chunk".into());
    };

    PrimitiveFacts {
        usage: facts.usage,
        continuation_rows: u64::from(has_continuation),
        output: facts.output,
        ..PrimitiveFacts::default()
    }
    .validate(transaction.budget())?;
    transaction.finish(has_continuation, facts.usage)
}

#[allow(clippy::too_many_arguments)]
fn run_transform_primitive(
    transaction: &mut StepTxn<'_, '_>,
    input_relation: &RelationRef,
    output_relation: &RelationRef,
    output_type: &str,
    chunk: &ChunkMeta,
    row_ordinal: i64,
    predicate: &str,
    expressions: &str,
    emit_rows: bool,
) -> Result<TransformFacts, String> {
    let output = transaction.output()?.clone();
    let budget = transaction.budget();
    let max_input_rows = i64_from_usize(budget.max_input_rows)?;
    let max_input_bytes = i64_from_usize(budget.max_input_bytes)?;
    let max_output_rows = i64::min(i64_from_usize(budget.max_output_rows)?, output.target_rows);
    let max_output_bytes = i64::min(
        i64_from_usize(budget.max_output_bytes)?,
        output.target_bytes,
    );
    let chunk_rows = i64_from_u64(chunk.rows)?;
    let chunk_lsn = format_lsn(chunk.lsn);
    let query = format!(
        r#"
        WITH input_source AS MATERIALIZED (
          SELECT input_row.row_ordinal,
                 input_row.weight,
                 input_row.row_value,
                 shiba_internal.effect_row_bytes(input_row.row_value)
                   AS input_bytes
          FROM {input_relation} AS input_row
          WHERE input_row.stream_id = $1
            AND input_row.chunk_seq = $2
            AND input_row.row_ordinal >= $3
            AND input_row.row_ordinal < $4
          ORDER BY input_row.row_ordinal
          LIMIT $5
        ),
        input_measured AS MATERIALIZED (
          SELECT input_source.*,
                 row_number() OVER (
                   ORDER BY input_source.row_ordinal
                 ) AS input_count,
                 sum(input_source.input_bytes) OVER (
                   ORDER BY input_source.row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS running_input_bytes
          FROM input_source
        ),
        input_bounded AS MATERIALIZED (
          SELECT *
          FROM input_measured
          WHERE input_count = 1 OR running_input_bytes <= $6
        ),
        predicated AS MATERIALIZED (
          SELECT input_row.*,
                 ($12::boolean AND (({predicate}) IS TRUE)) AS passes
          FROM input_bounded AS input_row
        ),
        evaluated AS MATERIALIZED (
          SELECT input_row.*,
                 CASE WHEN input_row.passes
                   THEN ROW({expressions})::{output_type}
                   ELSE NULL::{output_type}
                 END AS output_value
          FROM predicated AS input_row
        ),
        output_measured AS MATERIALIZED (
          SELECT evaluated.*,
                 sum(evaluated.passes::integer) OVER (
                   ORDER BY evaluated.row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS running_output_rows,
                 sum(
                   CASE WHEN evaluated.passes
                     THEN shiba_internal.effect_row_bytes(
                       evaluated.output_value
                     )
                     ELSE 0
                   END
                 ) OVER (
                   ORDER BY evaluated.row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS running_output_bytes
          FROM evaluated
        ),
        selected AS MATERIALIZED (
          SELECT *
          FROM output_measured
          WHERE running_output_rows = 0
             OR (
               running_output_rows <= $7
               AND running_output_bytes <= $8
             )
             OR (passes AND running_output_rows = 1)
        ),
        stats AS MATERIALIZED (
          SELECT count(*)::bigint AS processed_count,
                 min(row_ordinal)::bigint AS min_ordinal,
                 max(row_ordinal)::bigint AS max_ordinal,
                 coalesce(sum(input_bytes), 0)::bigint AS input_bytes,
                 count(*) FILTER (WHERE passes)::bigint AS output_count,
                 coalesce(
                   sum(shiba_internal.effect_row_bytes(output_value))
                     FILTER (WHERE passes),
                   0
                 )::bigint AS output_bytes
          FROM selected
        ),
        appended AS MATERIALIZED (
          SELECT append.outcome, append.appended_chunk_seq
          FROM stats
          CROSS JOIN LATERAL shiba_internal.append_effect_stream_chunk(
            $9, $10, 'data', stats.output_count, stats.output_bytes,
            $11::pg_lsn
          ) AS append
          WHERE stats.output_count > 0
        ),
        inserted AS (
          INSERT INTO {output_relation}(
            stream_id, chunk_seq, row_ordinal, weight, row_value
          )
          SELECT $9,
                 appended.appended_chunk_seq,
                 row_number() OVER (ORDER BY selected.row_ordinal) - 1,
                 selected.weight,
                 selected.output_value
          FROM selected
          CROSS JOIN appended
          WHERE selected.passes
            AND appended.outcome = 'appended'
          RETURNING shiba_internal.effect_row_bytes(row_value)
            AS stored_bytes
        )
        SELECT stats.processed_count,
               stats.min_ordinal,
               stats.max_ordinal,
               stats.input_bytes,
               stats.output_count,
               stats.output_bytes,
               coalesce(appended.outcome, 'none'),
               appended.appended_chunk_seq,
               (SELECT count(*)::bigint FROM inserted),
               (
                 SELECT coalesce(sum(stored_bytes), 0)::bigint
                 FROM inserted
               )
        FROM stats
        LEFT JOIN appended ON true
        "#,
        input_relation = input_relation.sql(),
        output_relation = output_relation.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
            DatumWithOid::new(row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(chunk_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_input_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_input_bytes, pg_sys::INT8OID),
            DatumWithOid::new(max_output_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_output_bytes, pg_sys::INT8OID),
            DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(output.next_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(chunk_lsn.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(emit_rows, pg_sys::BOOLOID),
        ]
    };
    let table = transaction.write(&query, &arguments)?;
    if table.len() != 1 {
        return Err("linear primitive returned no summary row".into());
    }
    let table = table.first();
    let processed = nonnegative(
        required_table::<i64>(&table, 1, "processed input rows")?,
        "processed input rows",
    )?;
    let first = required_table::<i64>(&table, 2, "first input ordinal")?;
    let last = required_table::<i64>(&table, 3, "last input ordinal")?;
    let input_bytes = nonnegative(
        required_table::<i64>(&table, 4, "processed input bytes")?,
        "processed input bytes",
    )?;
    let emitted = nonnegative(
        required_table::<i64>(&table, 5, "emitted rows")?,
        "emitted rows",
    )?;
    let output_bytes = nonnegative(
        required_table::<i64>(&table, 6, "emitted bytes")?,
        "emitted bytes",
    )?;
    let append_outcome = required_table::<String>(&table, 7, "append outcome")?;
    let appended_sequence = table.get::<i64>(8).map_err(|error| error.to_string())?;
    let inserted = nonnegative(
        required_table::<i64>(&table, 9, "inserted output rows")?,
        "inserted output rows",
    )?;
    let stored_bytes = nonnegative(
        required_table::<i64>(&table, 10, "stored output bytes")?,
        "stored output bytes",
    )?;
    if processed == 0
        || first != row_ordinal
        || last != row_ordinal + i64_from_u64(processed)? - 1
        || inserted != emitted
        || stored_bytes != output_bytes
    {
        return Err("linear primitive returned inconsistent row facts".into());
    }
    let output = match (append_outcome.as_str(), appended_sequence, emitted) {
        ("none", None, 0) => OutputFacts::None,
        ("appended", Some(sequence), rows) if rows > 0 && sequence == output.next_chunk_seq => {
            OutputFacts::Data {
                chunk_seq: sequence,
            }
        }
        _ => return Err("linear primitive returned inconsistent append facts".into()),
    };
    let usage = WorkUsage {
        input_rows: processed,
        input_bytes,
        output_rows: emitted,
        output_bytes,
    };
    usage.validate(budget)?;
    if emitted > u64::try_from(max_output_rows).map_err(|_| "negative output row target")?
        || (emitted > 1
            && output_bytes
                > u64::try_from(max_output_bytes).map_err(|_| "negative output byte target")?)
    {
        return Err("linear primitive exceeded its stream target".into());
    }
    Ok(TransformFacts {
        usage,
        first_ordinal: first,
        last_ordinal: last,
        output,
    })
}

fn run_bootstrap_primitive(
    transaction: &mut StepTxn<'_, '_>,
    bootstrap: &RelationRef,
    output_relation: &RelationRef,
    next_sequence: i64,
    activation_lsn: u64,
) -> Result<BootstrapFacts, String> {
    let output = transaction.output()?.clone();
    let budget = transaction.budget();
    let max_rows = i64::min(
        i64::min(
            i64_from_usize(budget.max_input_rows)?,
            i64_from_usize(budget.max_output_rows)?,
        ),
        output.target_rows,
    );
    let max_bytes = i64::min(
        i64::min(
            i64_from_usize(budget.max_input_bytes)?,
            i64_from_usize(budget.max_output_bytes)?,
        ),
        output.target_bytes,
    );
    let activation_lsn = format_lsn(activation_lsn);
    let query = format!(
        r#"
        WITH candidate AS MATERIALIZED (
          SELECT bootstrap.bootstrap_seq,
                 bootstrap.row_value,
                 shiba_internal.effect_row_bytes(bootstrap.row_value)
                   AS row_bytes
          FROM {bootstrap} AS bootstrap
          WHERE bootstrap.bootstrap_seq >= $1
          ORDER BY bootstrap.bootstrap_seq
          LIMIT $2
        ),
        measured AS MATERIALIZED (
          SELECT candidate.*,
                 row_number() OVER (
                   ORDER BY candidate.bootstrap_seq
                 ) AS running_rows,
                 sum(candidate.row_bytes) OVER (
                   ORDER BY candidate.bootstrap_seq
                   ROWS UNBOUNDED PRECEDING
                 ) AS running_bytes
          FROM candidate
        ),
        selected AS MATERIALIZED (
          SELECT *
          FROM measured
          WHERE running_rows = 1 OR running_bytes <= $3
        ),
        stats AS MATERIALIZED (
          SELECT count(*)::bigint AS row_count,
                 min(bootstrap_seq)::bigint AS first_sequence,
                 max(bootstrap_seq)::bigint AS last_sequence,
                 coalesce(sum(row_bytes), 0)::bigint AS payload_bytes
          FROM selected
        ),
        appended AS MATERIALIZED (
          SELECT append.outcome, append.appended_chunk_seq
          FROM stats
          CROSS JOIN LATERAL shiba_internal.append_effect_stream_chunk(
            $4, $5, 'data', stats.row_count, stats.payload_bytes,
            $6::pg_lsn
          ) AS append
          WHERE stats.row_count > 0
        ),
        inserted AS (
          INSERT INTO {output_relation}(
            stream_id, chunk_seq, row_ordinal, weight, row_value
          )
          SELECT $4,
                 appended.appended_chunk_seq,
                 row_number() OVER (ORDER BY selected.bootstrap_seq) - 1,
                 1,
                 selected.row_value
          FROM selected
          CROSS JOIN appended
          WHERE appended.outcome = 'appended'
          RETURNING shiba_internal.effect_row_bytes(row_value)
            AS stored_bytes
        ),
        deleted AS (
          DELETE FROM {bootstrap} AS bootstrap
          USING selected, appended
          WHERE appended.outcome = 'appended'
            AND bootstrap.bootstrap_seq = selected.bootstrap_seq
          RETURNING bootstrap.bootstrap_seq
        )
        SELECT stats.row_count,
               stats.first_sequence,
               stats.last_sequence,
               stats.payload_bytes,
               coalesce(appended.outcome, 'none'),
               appended.appended_chunk_seq,
               (SELECT count(*)::bigint FROM inserted),
               (
                 SELECT coalesce(sum(stored_bytes), 0)::bigint
                 FROM inserted
               ),
               (SELECT count(*)::bigint FROM deleted)
        FROM stats
        LEFT JOIN appended ON true
        "#,
        bootstrap = bootstrap.sql(),
        output_relation = output_relation.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(next_sequence, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(output.next_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(activation_lsn.as_str(), pg_sys::TEXTOID),
        ]
    };
    let table = transaction.write(&query, &arguments)?;
    if table.len() != 1 {
        return Err("Scan bootstrap primitive returned no summary row".into());
    }
    let table = table.first();
    let rows = nonnegative(
        required_table::<i64>(&table, 1, "bootstrap row count")?,
        "bootstrap row count",
    )?;
    let first_sequence = table.get::<i64>(2).map_err(|error| error.to_string())?;
    let last_sequence = table.get::<i64>(3).map_err(|error| error.to_string())?;
    let bytes = nonnegative(
        required_table::<i64>(&table, 4, "bootstrap bytes")?,
        "bootstrap bytes",
    )?;
    let append_outcome = required_table::<String>(&table, 5, "bootstrap append outcome")?;
    let appended_sequence = table.get::<i64>(6).map_err(|error| error.to_string())?;
    let inserted = nonnegative(
        required_table::<i64>(&table, 7, "bootstrap inserted rows")?,
        "bootstrap inserted rows",
    )?;
    let stored_bytes = nonnegative(
        required_table::<i64>(&table, 8, "bootstrap stored bytes")?,
        "bootstrap stored bytes",
    )?;
    let deleted = nonnegative(
        required_table::<i64>(&table, 9, "bootstrap deleted rows")?,
        "bootstrap deleted rows",
    )?;
    if inserted != rows || deleted != rows || stored_bytes != bytes {
        return Err("Scan bootstrap primitive returned inconsistent row facts".into());
    }
    let output = match (append_outcome.as_str(), appended_sequence, rows) {
        ("none", None, 0) => OutputFacts::None,
        ("appended", Some(sequence), count) if count > 0 && sequence == output.next_chunk_seq => {
            OutputFacts::Data {
                chunk_seq: sequence,
            }
        }
        _ => return Err("Scan bootstrap primitive returned inconsistent append facts".into()),
    };
    let usage = WorkUsage {
        input_rows: rows,
        input_bytes: bytes,
        output_rows: rows,
        output_bytes: bytes,
    };
    usage.validate(budget)?;
    Ok(BootstrapFacts {
        usage,
        first_sequence,
        last_sequence,
        output,
    })
}

fn compile_transform(
    transaction: &mut StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    kind: LinearKind,
    scan_spec: &ScanSpec,
    input_type: &TypeRef,
    output_type: &TypeRef,
) -> Result<(String, String), String> {
    let output_attributes = transaction.composite_attributes(output_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
    match (&stage.spec, kind) {
        (OperatorSpec::Scan(_), LinearKind::Scan) => {
            let source_oid = transaction
                .input(0)?
                .source_oid
                .ok_or_else(|| "Scan input has no source OID".to_string())?;
            let source_attributes = transaction.relation_attributes(source_oid)?;
            let input_attributes = transaction.composite_attributes(input_type)?;
            if source_attributes.len() != input_attributes.len() {
                return Err("source stream row type no longer matches its table".into());
            }
            for (source, input) in source_attributes.iter().zip(&input_attributes) {
                if source.name != input.name
                    || source.type_oid != input.type_oid
                    || source.typmod != input.typmod
                    || source.collation_oid != input.collation_oid
                {
                    return Err("source stream row type no longer matches its table".into());
                }
            }
            let mut expressions = Vec::with_capacity(stage.schema.outputs.len());
            let mut resolved = HashSet::new();
            for output in &stage.schema.outputs {
                let column = scan_spec
                    .columns
                    .iter()
                    .find(|column| column.output == output.slot)
                    .ok_or_else(|| {
                        format!("Scan output slot {} has no source column", output.slot.0)
                    })?;
                if !resolved.insert(column.output) {
                    return Err(format!("Scan output slot {} is duplicated", output.slot.0));
                }
                let source = source_attributes
                    .iter()
                    .find(|attribute| attribute.number == column.attnum)
                    .ok_or_else(|| {
                        format!("Scan source attribute {} no longer exists", column.attnum)
                    })?;
                if !attribute_matches_slot(source, &output.type_) {
                    return Err(format!(
                        "Scan output slot {} changed PostgreSQL type",
                        output.slot.0
                    ));
                }
                expressions.push(format!(
                    "(input_row.row_value).{}",
                    quote_identifier(&source.name)
                ));
            }
            Ok(("TRUE".into(), expressions.join(", ")))
        }
        (OperatorSpec::Filter(spec), LinearKind::Filter) => {
            let bindings = compile_stage_bindings(
                transaction,
                plan,
                stage,
                &[BindingInput {
                    row_type: input_type,
                    alias: "input_row",
                }],
            )?;
            Ok((
                compile_scalar_expression(&spec.predicate, &bindings)?,
                compile_named_outputs(&stage.schema.outputs, &spec.outputs, &bindings, "Filter")?
                    .join(", "),
            ))
        }
        (OperatorSpec::Project(spec), LinearKind::Project) => {
            let bindings = compile_stage_bindings(
                transaction,
                plan,
                stage,
                &[BindingInput {
                    row_type: input_type,
                    alias: "input_row",
                }],
            )?;
            Ok((
                "TRUE".into(),
                compile_named_outputs(
                    &stage.schema.outputs,
                    &spec.expressions,
                    &bindings,
                    "Project",
                )?
                .join(", "),
            ))
        }
        _ => Err("linear operator kind does not match its stage specification".into()),
    }
}

fn load_scan_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
) -> Result<Option<ScanContinuation>, String> {
    let query = format!(
        r#"
        SELECT phase,
               input_stream_id,
               input_chunk_seq,
               next_row_ordinal,
               next_bootstrap_seq,
               pending_frontier_lsn::text
        FROM {}
        WHERE singleton
        FOR UPDATE
        "#,
        relation.sql()
    );
    let table = transaction.lock(&query, &[])?;
    match table.len() {
        0 => Ok(None),
        1 => {
            let table = table.first();
            let frontier = table
                .get::<String>(6)
                .map_err(|error| error.to_string())?
                .map(|value| {
                    parse_lsn(&value)
                        .map_err(|error| format!("invalid Scan continuation LSN: {error}"))
                })
                .transpose()?;
            Ok(Some(ScanContinuation {
                phase: ScanPhase::decode(required_table(&table, 1, "Scan phase")?)?,
                input_stream_id: required_table(&table, 2, "Scan input stream")?,
                input_chunk_seq: table.get::<i64>(3).map_err(|error| error.to_string())?,
                next_row_ordinal: table.get::<i64>(4).map_err(|error| error.to_string())?,
                next_bootstrap_seq: table.get::<i64>(5).map_err(|error| error.to_string())?,
                pending_frontier_lsn: frontier,
            }))
        }
        count => Err(format!("Scan continuation relation contains {count} rows")),
    }
}

fn load_transform_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
) -> Result<Option<TransformContinuation>, String> {
    let query = format!(
        r#"
        SELECT input_stream_id, input_chunk_seq, next_row_ordinal
        FROM {}
        WHERE singleton
        FOR UPDATE
        "#,
        relation.sql()
    );
    let table = transaction.lock(&query, &[])?;
    match table.len() {
        0 => Ok(None),
        1 => {
            let table = table.first();
            Ok(Some(TransformContinuation {
                input_stream_id: required_table(&table, 1, "linear input stream")?,
                input_chunk_seq: required_table(&table, 2, "linear input chunk")?,
                next_row_ordinal: required_table(&table, 3, "linear row cursor")?,
            }))
        }
        count => Err(format!(
            "linear continuation relation contains {count} rows"
        )),
    }
}

fn replace_scan_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    previous: Option<&ScanContinuation>,
    next: &ScanContinuation,
) -> Result<(), String> {
    validate_scan_continuation(next)?;
    if let Some(previous) = previous {
        delete_scan_continuation(transaction, relation, previous)?;
    }
    let frontier = next.pending_frontier_lsn.map(format_lsn);
    let query = format!(
        r#"
        INSERT INTO {}(
          singleton,
          phase,
          input_stream_id,
          input_chunk_seq,
          next_row_ordinal,
          next_bootstrap_seq,
          pending_frontier_lsn
        )
        VALUES(true,$1,$2,$3,$4,$5,$6::pg_lsn)
        RETURNING 1
        "#,
        relation.sql()
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(next.phase as i16, pg_sys::INT2OID),
            DatumWithOid::new(next.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(next.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(next.next_row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(next.next_bootstrap_seq, pg_sys::INT8OID),
            DatumWithOid::new(frontier.as_deref(), pg_sys::TEXTOID),
        ]
    };
    if transaction.write(&query, &arguments)?.len() != 1 {
        return Err("Scan continuation insert did not affect one row".into());
    }
    Ok(())
}

fn delete_scan_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    expected: &ScanContinuation,
) -> Result<(), String> {
    let frontier = expected.pending_frontier_lsn.map(format_lsn);
    let query = format!(
        r#"
        DELETE FROM {}
        WHERE singleton
          AND phase = $1
          AND input_stream_id = $2
          AND input_chunk_seq IS NOT DISTINCT FROM $3
          AND next_row_ordinal IS NOT DISTINCT FROM $4
          AND next_bootstrap_seq IS NOT DISTINCT FROM $5
          AND pending_frontier_lsn IS NOT DISTINCT FROM $6::pg_lsn
        RETURNING 1
        "#,
        relation.sql()
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(expected.phase as i16, pg_sys::INT2OID),
            DatumWithOid::new(expected.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(expected.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(expected.next_row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(expected.next_bootstrap_seq, pg_sys::INT8OID),
            DatumWithOid::new(frontier.as_deref(), pg_sys::TEXTOID),
        ]
    };
    if transaction.write(&query, &arguments)?.len() != 1 {
        return Err("Scan continuation CAS delete did not affect one row".into());
    }
    Ok(())
}

fn replace_transform_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    previous: Option<TransformContinuation>,
    next: TransformContinuation,
) -> Result<(), String> {
    if next.input_stream_id <= 0 || next.input_chunk_seq <= 0 || next.next_row_ordinal < 0 {
        return Err("linear continuation contains an invalid cursor".into());
    }
    if let Some(previous) = previous {
        let query = format!(
            r#"
            DELETE FROM {}
            WHERE singleton
              AND input_stream_id = $1
              AND input_chunk_seq = $2
              AND next_row_ordinal = $3
            RETURNING 1
            "#,
            relation.sql()
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(previous.input_stream_id, pg_sys::INT8OID),
                DatumWithOid::new(previous.input_chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(previous.next_row_ordinal, pg_sys::INT8OID),
            ]
        };
        if transaction.write(&query, &arguments)?.len() != 1 {
            return Err("linear continuation CAS delete did not affect one row".into());
        }
    }
    let query = format!(
        r#"
        INSERT INTO {}(
          singleton,input_stream_id,input_chunk_seq,next_row_ordinal
        )
        VALUES(true,$1,$2,$3)
        RETURNING 1
        "#,
        relation.sql()
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(next.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(next.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(next.next_row_ordinal, pg_sys::INT8OID),
        ]
    };
    if transaction.write(&query, &arguments)?.len() != 1 {
        return Err("linear continuation insert did not affect one row".into());
    }
    Ok(())
}

fn delete_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    expected_exists: bool,
) -> Result<(), String> {
    if !expected_exists {
        return Ok(());
    }
    let query = format!("DELETE FROM {} WHERE singleton RETURNING 1", relation.sql());
    if transaction.write(&query, &[])?.len() != 1 {
        return Err("operator continuation delete did not affect one row".into());
    }
    Ok(())
}

fn validate_scan_continuation(continuation: &ScanContinuation) -> Result<(), String> {
    if continuation.input_stream_id <= 0 {
        return Err("Scan continuation has an invalid input stream".into());
    }
    let valid = match continuation.phase {
        ScanPhase::Bootstrap => {
            continuation.input_chunk_seq.is_none()
                && continuation.next_row_ordinal.is_none()
                && continuation
                    .next_bootstrap_seq
                    .is_some_and(|value| value > 0)
                && continuation.pending_frontier_lsn.is_none()
        }
        ScanPhase::SnapshotFrontier => {
            continuation.input_chunk_seq.is_none()
                && continuation.next_row_ordinal.is_none()
                && continuation.next_bootstrap_seq.is_none()
                && continuation.pending_frontier_lsn.is_some()
        }
        ScanPhase::Data => {
            continuation.input_chunk_seq.is_some_and(|value| value > 0)
                && continuation
                    .next_row_ordinal
                    .is_some_and(|value| value >= 0)
                && continuation.next_bootstrap_seq.is_none()
                && continuation.pending_frontier_lsn.is_none()
        }
        ScanPhase::SourceFrontier => {
            continuation.input_chunk_seq.is_none()
                && continuation.next_row_ordinal.is_none()
                && continuation.next_bootstrap_seq.is_none()
                && continuation.pending_frontier_lsn.is_some()
        }
    };
    if !valid {
        return Err("Scan continuation fields do not match its phase".into());
    }
    Ok(())
}

fn validate_authority(checkpoint: bool, relation: bool) -> Result<(), String> {
    if checkpoint != relation {
        return Err("checkpoint and typed continuation authority disagree".into());
    }
    Ok(())
}

fn validate_continuation_abi(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    expected: &[(&str, pg_sys::Oid, bool)],
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(relation.oid())?;
    if attributes.len() != expected.len() {
        return Err("operator continuation relation changed its ABI".into());
    }
    for (attribute, (name, type_oid, not_null)) in attributes.iter().zip(expected) {
        if attribute.name != *name
            || attribute.type_oid != *type_oid
            || attribute.not_null != *not_null
        {
            return Err("operator continuation relation changed its ABI".into());
        }
    }
    Ok(())
}

fn validate_bootstrap_abi(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(relation.oid())?;
    if attributes.len() != 2
        || attributes[0].name != "bootstrap_seq"
        || attributes[0].type_oid != pg_sys::INT8OID
        || !attributes[0].not_null
        || attributes[1].name != "row_value"
        || !attributes[1].not_null
    {
        return Err("Scan bootstrap relation changed its ABI".into());
    }
    Ok(())
}

fn first_bootstrap_sequence(
    transaction: &mut StepTxn<'_, '_>,
    bootstrap: &RelationRef,
    at_or_after: i64,
) -> Result<Option<i64>, String> {
    let query = format!(
        "SELECT min(bootstrap_seq)::bigint FROM {} WHERE bootstrap_seq >= $1",
        bootstrap.sql()
    );
    let arguments = unsafe { [DatumWithOid::new(at_or_after, pg_sys::INT8OID)] };
    transaction
        .read(&query, &arguments)?
        .first()
        .get_one::<i64>()
        .map_err(|error| error.to_string())
}

fn assert_frontier_skips_no_source_chunk(
    transaction: &mut StepTxn<'_, '_>,
    frontier: u64,
) -> Result<(), String> {
    let input = transaction.input(0)?.clone();
    let frontier = format_lsn(frontier);
    let arguments = unsafe {
        [
            DatumWithOid::new(input.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(input.next_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(frontier.as_str(), pg_sys::TEXTOID),
        ]
    };
    let skipped = transaction
        .read(
            r#"
            SELECT EXISTS (
              SELECT 1
              FROM shiba_internal.effect_stream_chunks AS pending
              WHERE pending.stream_id = $1
                AND pending.chunk_seq >= $2
                AND pending.chunk_lsn <= $3::pg_lsn
            )
            "#,
            &arguments,
        )?
        .first()
        .get_one::<bool>()
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "source frontier check returned NULL".to_string())?;
    if skipped {
        return Err("Scan frontier would skip a published source chunk".into());
    }
    Ok(())
}

fn source_frontier_after_chunk(
    transaction: &mut StepTxn<'_, '_>,
    completed: &ChunkMeta,
) -> Result<Option<u64>, String> {
    let input = transaction.input(0)?.clone();
    let Some(frontier) = input
        .available_source_frontier_lsn
        .filter(|frontier| *frontier > input.consumed_frontier_lsn)
    else {
        return Ok(None);
    };
    let next_input = super::chunk(transaction, &input, completed.sequence + 1)?;
    if next_input.is_none_or(|next| next.lsn > frontier) {
        Ok(Some(frontier))
    } else {
        Ok(None)
    }
}

fn required_table<T: FromDatum + IntoDatum>(
    table: &pgrx::spi::SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    table
        .get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}

fn nonnegative(value: i64, name: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{name} is negative"))
}

fn i64_from_u64(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "resource count exceeds bigint".into())
}

fn i64_from_usize(value: usize) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "work budget exceeds bigint".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn continuation(phase: ScanPhase) -> ScanContinuation {
        match phase {
            ScanPhase::Bootstrap => ScanContinuation {
                phase,
                input_stream_id: 1,
                input_chunk_seq: None,
                next_row_ordinal: None,
                next_bootstrap_seq: Some(1),
                pending_frontier_lsn: None,
            },
            ScanPhase::SnapshotFrontier => ScanContinuation {
                phase,
                input_stream_id: 1,
                input_chunk_seq: None,
                next_row_ordinal: None,
                next_bootstrap_seq: None,
                pending_frontier_lsn: Some(1),
            },
            ScanPhase::Data => ScanContinuation {
                phase,
                input_stream_id: 1,
                input_chunk_seq: Some(2),
                next_row_ordinal: Some(0),
                next_bootstrap_seq: None,
                pending_frontier_lsn: None,
            },
            ScanPhase::SourceFrontier => ScanContinuation {
                phase,
                input_stream_id: 1,
                input_chunk_seq: None,
                next_row_ordinal: None,
                next_bootstrap_seq: None,
                pending_frontier_lsn: Some(3),
            },
        }
    }

    #[test]
    fn scan_phases_have_disjoint_persisted_shapes() {
        for phase in [
            ScanPhase::Bootstrap,
            ScanPhase::SnapshotFrontier,
            ScanPhase::Data,
            ScanPhase::SourceFrontier,
        ] {
            validate_scan_continuation(&continuation(phase)).unwrap();
        }
        let mut invalid = continuation(ScanPhase::SourceFrontier);
        invalid.next_row_ordinal = Some(0);
        assert!(validate_scan_continuation(&invalid).is_err());
    }

    #[test]
    fn phase_decoder_rejects_old_text_or_unknown_codes() {
        assert_eq!(ScanPhase::decode(1), Ok(ScanPhase::Bootstrap));
        assert_eq!(ScanPhase::decode(2), Ok(ScanPhase::SnapshotFrontier));
        assert_eq!(ScanPhase::decode(3), Ok(ScanPhase::Data));
        assert_eq!(ScanPhase::decode(4), Ok(ScanPhase::SourceFrontier));
        assert!(ScanPhase::decode(0).is_err());
        assert!(ScanPhase::decode(5).is_err());
    }

    #[test]
    fn checkpoint_flag_is_only_a_checked_index() {
        assert!(validate_authority(false, false).is_ok());
        assert!(validate_authority(true, true).is_ok());
        assert!(validate_authority(true, false).is_err());
        assert!(validate_authority(false, true).is_err());
    }
}
