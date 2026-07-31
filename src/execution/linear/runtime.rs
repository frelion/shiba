use super::*;

pub(crate) const SCAN_KERNEL: crate::execution::KernelFn = crate::execution::KernelFn::new(
    crate::execution::KernelContract::with_phases(
        &[crate::execution::InputContract::Source],
        crate::execution::OutputContract::EffectStream,
        &[
            crate::execution::LifecyclePhase::Admit,
            crate::execution::LifecyclePhase::Process,
            crate::execution::LifecyclePhase::Frontier,
        ],
    ),
    step,
);
pub(crate) const TRANSFORM_KERNEL: crate::execution::KernelFn = crate::execution::KernelFn::new(
    crate::execution::KernelContract::with_phases(
        &[crate::execution::InputContract::Operator],
        crate::execution::OutputContract::EffectStream,
        &[
            crate::execution::LifecyclePhase::Admit,
            crate::execution::LifecyclePhase::Process,
            crate::execution::LifecyclePhase::Frontier,
        ],
    ),
    step,
);

pub(crate) fn step(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage_id: u32,
) -> Result<StepReceipt, String> {
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
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    continuation_relation: RelationRef,
) -> Result<StepReceipt, String> {
    let OperatorSpec::Scan(spec) = &stage.spec else {
        return Err("Scan kernel received another operator".into());
    };
    let input = transaction.input(0)?.clone();
    if input.source_oid.map(pg_sys::Oid::to_u32) != Some(spec.source_oid) {
        return Err("Scan source OID does not match its source stream".into());
    }
    validate_typed_continuation_abi(transaction, &continuation_relation, SCAN_COLUMNS, "Scan")?;
    let continuation = load_scan_continuation(transaction, &continuation_relation)?;
    crate::execution::validate_continuation_authority(transaction, continuation.is_some())?;
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
            let chunk = next_chunk(transaction, 0)?;
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
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    kind: LinearKind,
    continuation_relation: RelationRef,
) -> Result<StepReceipt, String> {
    validate_typed_continuation_abi(
        transaction,
        &continuation_relation,
        TRANSFORM_COLUMNS,
        "linear",
    )?;
    let continuation = load_transform_continuation(transaction, &continuation_relation)?;
    crate::execution::validate_continuation_authority(transaction, continuation.is_some())?;
    let input = transaction.input(0)?.clone();
    if continuation.is_some_and(|state| state.input_stream_id != input.stream_id) {
        return Err("linear continuation references another input stream".into());
    }
    let chunk = next_chunk(transaction, 0)?
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
        append_frontier(transaction, chunk.lsn)?;
        advance_input(
            transaction,
            0,
            chunk.sequence + 1,
            chunk.lsn,
            WorkUsage::default(),
        )?;
        delete_continuation(transaction, &continuation_relation, continuation.is_some())?;
        return transaction.transition(KernelPhase::Frontier, WorkUsage::default());
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
    transaction: &mut StepContext<'_, '_>,
    continuation_relation: &RelationRef,
    continuation: &ScanContinuation,
) -> Result<StepReceipt, String> {
    let next_sequence = continuation
        .next_bootstrap_seq
        .ok_or_else(|| "Scan bootstrap continuation omitted its cursor".to_string())?;
    let bootstrap = transaction.state_storage(0)?;
    validate_bootstrap_abi(transaction, &bootstrap)?;
    let output = transaction.output()?.clone();
    let output_storage = transaction.payload_storage(output.stream_id)?;
    let bootstrap_attributes = transaction.relation_attributes(bootstrap.oid())?;
    if bootstrap_attributes[1].type_oid != output_storage.row_type.oid() {
        return Err("Scan bootstrap row type changed identity".into());
    }
    let activation_lsn = transaction.input(0)?.activation_lsn;
    let facts = run_bootstrap_primitive(
        transaction,
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
            let remaining = first_bootstrap_sequence(transaction, &bootstrap, last + 1)?;
            if let Some(remaining) = remaining {
                if remaining != last + 1 {
                    return Err("Scan bootstrap relation has a sequence gap".into());
                }
                let next = ScanContinuation {
                    next_bootstrap_seq: Some(remaining),
                    ..continuation.clone()
                };
                replace_scan_continuation(
                    transaction,
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
            transaction,
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
    transaction.transition_facts(
        KernelPhase::Admit,
        PrimitiveFacts {
            usage: facts.usage,
            state_rows: facts.usage.input_rows,
            output: facts.output,
        },
    )
}

fn step_snapshot_frontier(
    transaction: &mut StepContext<'_, '_>,
    continuation_relation: &RelationRef,
    continuation: &ScanContinuation,
) -> Result<StepReceipt, String> {
    let input = transaction.input(0)?.clone();
    let frontier = continuation
        .pending_frontier_lsn
        .ok_or_else(|| "Scan snapshot frontier omitted its LSN".to_string())?;
    if frontier != input.activation_lsn || input.consumed_frontier_lsn != input.activation_lsn {
        return Err("Scan snapshot frontier does not match its activation boundary".into());
    }
    append_frontier(transaction, frontier)?;
    let expected = scan_arguments(continuation);
    replace_continuation_cas(
        transaction,
        continuation_relation,
        SCAN_COLUMNS,
        Some(&expected),
        None,
        "Scan",
    )?;
    transaction.transition(KernelPhase::Frontier, WorkUsage::default())
}

fn step_available_source_frontier(
    transaction: &mut StepContext<'_, '_>,
) -> Result<StepReceipt, String> {
    let input = transaction.input(0)?.clone();
    let frontier = input
        .available_source_frontier_lsn
        .filter(|frontier| *frontier > input.consumed_frontier_lsn)
        .ok_or_else(|| "runnable Scan has neither a chunk nor a source frontier".to_string())?;
    assert_frontier_skips_no_source_chunk(transaction, frontier)?;
    append_frontier(transaction, frontier)?;
    advance_input(
        transaction,
        0,
        input.next_chunk_seq,
        frontier,
        WorkUsage::default(),
    )?;
    transaction.transition(KernelPhase::Frontier, WorkUsage::default())
}

fn step_scan_frontier(
    transaction: &mut StepContext<'_, '_>,
    continuation_relation: &RelationRef,
    frontier: u64,
) -> Result<StepReceipt, String> {
    let input = transaction.input(0)?.clone();
    if frontier <= input.consumed_frontier_lsn
        || input
            .available_source_frontier_lsn
            .is_none_or(|available| frontier > available)
    {
        return Err("Scan frontier continuation is outside the published frontier".into());
    }
    assert_frontier_skips_no_source_chunk(transaction, frontier)?;
    append_frontier(transaction, frontier)?;
    advance_input(
        transaction,
        0,
        input.next_chunk_seq,
        frontier,
        WorkUsage::default(),
    )?;
    delete_continuation(transaction, continuation_relation, true)?;
    transaction.transition(KernelPhase::Frontier, WorkUsage::default())
}

#[allow(clippy::too_many_arguments)]
fn step_data_transform(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    kind: LinearKind,
    scan_spec: &ScanSpec,
    continuation_relation: &RelationRef,
    had_continuation: bool,
    chunk: ChunkMeta,
    row_ordinal: i64,
) -> Result<StepReceipt, String> {
    if row_ordinal < 0 || u64::try_from(row_ordinal).map_or(true, |row| row >= chunk.rows) {
        return Err("linear continuation row is outside its input chunk".into());
    }
    let input = transaction.input(0)?.clone();
    if chunk.sequence != input.next_chunk_seq {
        return Err("linear chunk is not at the consumer cursor".into());
    }
    let input_storage = transaction.payload_storage(input.stream_id)?;
    if row_ordinal == 0 {
        payload_facts(transaction, &input_storage.relation, &chunk)?;
    }
    let output = transaction.output()?.clone();
    let output_storage = transaction.payload_storage(output.stream_id)?;
    let (predicate, expressions) = compile_transform(
        transaction,
        plan,
        stage,
        kind,
        scan_spec,
        &input_storage.row_type,
        &output_storage.row_type,
    )?;
    let emit_rows = kind != LinearKind::Scan || chunk.lsn > input.activation_lsn;
    let facts = run_transform_primitive(
        transaction,
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
    if next_row < chunk_rows {
        match kind {
            LinearKind::Scan => replace_scan_continuation(
                transaction,
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
                transaction,
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
    } else if next_row == chunk_rows {
        delete_continuation(transaction, continuation_relation, had_continuation)?;
        if kind == LinearKind::Scan {
            if let Some(frontier) = source_frontier_after_chunk(transaction, &chunk)? {
                replace_scan_continuation(
                    transaction,
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
            }
        }
        advance_input(
            transaction,
            0,
            chunk.sequence + 1,
            input.consumed_frontier_lsn,
            WorkUsage {
                input_rows: chunk.rows,
                input_bytes: chunk.bytes,
                ..WorkUsage::default()
            },
        )?;
    } else {
        return Err("linear primitive advanced beyond its input chunk".into());
    }

    let facts = PrimitiveFacts {
        usage: facts.usage,
        output: facts.output,
        ..PrimitiveFacts::default()
    };
    transaction.transition_facts(KernelPhase::Process, facts)
}

#[allow(clippy::too_many_arguments)]
// Atomic bounded transform primitive: scan one input prefix, write the typed
// payload, and return only the SQL mutation summary. Output-stream publication
// is owned by StepContext's commit boundary.
fn run_transform_primitive(
    transaction: &mut StepContext<'_, '_>,
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
                 ($11::boolean AND (({predicate}) IS TRUE)) AS passes
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
        inserted AS (
          INSERT INTO {output_relation}(
            stream_id, chunk_seq, row_ordinal, weight, row_value
          )
          SELECT $9,
                 $10,
                 row_number() OVER (ORDER BY selected.row_ordinal) - 1,
                 selected.weight,
                 selected.output_value
          FROM selected
          WHERE selected.passes
          RETURNING shiba_internal.effect_row_bytes(row_value)
            AS stored_bytes
        )
        SELECT stats.processed_count,
               stats.min_ordinal,
               stats.max_ordinal,
               stats.input_bytes,
               stats.output_count,
               stats.output_bytes,
               (SELECT count(*)::bigint FROM inserted),
               (
                 SELECT coalesce(sum(stored_bytes), 0)::bigint
                 FROM inserted
               )
        FROM stats
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
    let inserted = nonnegative(
        required_table::<i64>(&table, 7, "inserted output rows")?,
        "inserted output rows",
    )?;
    let stored_bytes = nonnegative(
        required_table::<i64>(&table, 8, "stored output bytes")?,
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
    let output = if emitted == 0 {
        OutputFacts::None
    } else {
        OutputFacts::Data {
            chunk_seq: output.next_chunk_seq,
        }
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
    if let OutputFacts::Data { chunk_seq } = output {
        transaction.record_output_append(
            OutputAppendTarget::New {
                sequence: chunk_seq,
            },
            emitted,
            output_bytes,
            chunk.lsn,
        )?;
    }
    Ok(TransformFacts {
        usage,
        first_ordinal: first,
        last_ordinal: last,
        output,
    })
}

// Atomic bounded bootstrap primitive: materialize the initial typed payload and
// report its facts; the shared context publishes the resulting data chunk.
fn run_bootstrap_primitive(
    transaction: &mut StepContext<'_, '_>,
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
        inserted AS (
          INSERT INTO {output_relation}(
            stream_id, chunk_seq, row_ordinal, weight, row_value
          )
          SELECT $4,
                 $5,
                 row_number() OVER (ORDER BY selected.bootstrap_seq) - 1,
                 1,
                 selected.row_value
          FROM selected
          RETURNING shiba_internal.effect_row_bytes(row_value)
            AS stored_bytes
        ),
        deleted AS (
          DELETE FROM {bootstrap} AS bootstrap
          USING selected
          WHERE bootstrap.bootstrap_seq = selected.bootstrap_seq
          RETURNING bootstrap.bootstrap_seq
        )
        SELECT stats.row_count,
               stats.first_sequence,
               stats.last_sequence,
               stats.payload_bytes,
               (SELECT count(*)::bigint FROM inserted),
               (
                 SELECT coalesce(sum(stored_bytes), 0)::bigint
                 FROM inserted
               ),
               (SELECT count(*)::bigint FROM deleted)
        FROM stats
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
    let inserted = nonnegative(
        required_table::<i64>(&table, 5, "bootstrap inserted rows")?,
        "bootstrap inserted rows",
    )?;
    let stored_bytes = nonnegative(
        required_table::<i64>(&table, 6, "bootstrap stored bytes")?,
        "bootstrap stored bytes",
    )?;
    let deleted = nonnegative(
        required_table::<i64>(&table, 7, "bootstrap deleted rows")?,
        "bootstrap deleted rows",
    )?;
    if inserted != rows || deleted != rows || stored_bytes != bytes {
        return Err("Scan bootstrap primitive returned inconsistent row facts".into());
    }
    let output = if rows == 0 {
        OutputFacts::None
    } else {
        OutputFacts::Data {
            chunk_seq: output.next_chunk_seq,
        }
    };
    let usage = WorkUsage {
        input_rows: rows,
        input_bytes: bytes,
        output_rows: rows,
        output_bytes: bytes,
    };
    usage.validate(budget)?;
    if let OutputFacts::Data { chunk_seq } = output {
        transaction.record_output_append(
            OutputAppendTarget::New {
                sequence: chunk_seq,
            },
            rows,
            bytes,
            activation_lsn,
        )?;
    }
    Ok(BootstrapFacts {
        usage,
        first_sequence,
        last_sequence,
        output,
    })
}
