use super::*;

pub(crate) const KERNEL: crate::execution::KernelFn = crate::execution::KernelFn::new(
    crate::execution::KernelContract::with_phases(
        &[crate::execution::InputContract::Operator],
        crate::execution::OutputContract::EffectStream,
        &[
            crate::execution::LifecyclePhase::Admit,
            crate::execution::LifecyclePhase::Drain,
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
        .get(usize::try_from(stage_id).map_err(|_| "Aggregate stage ID exceeds usize")?)
        .ok_or_else(|| format!("dataflow has no Aggregate stage {stage_id}"))?;
    let OperatorSpec::Aggregate(spec) = &stage.spec else {
        return Err("Aggregate executor received another operator".into());
    };
    if spec.aggregates.is_empty()
        || transaction.inputs().len() != 1
        || transaction.input(0)?.producer != ProducerKind::Operator
        || stage.inputs.len() != 1
    {
        return Err("Aggregate must have one operator input and one aggregate".into());
    }
    let input = transaction.input(0)?.clone();
    let continuation_relation = transaction.continuation_storage()?;
    validate_execute_continuation_abi(transaction, &continuation_relation)?;
    let stored = match load_execute_continuation(transaction, &continuation_relation)? {
        Some(stored) => stored,
        None => start_execute_continuation(transaction)?,
    };
    crate::execution::validate_continuation_authority(transaction, stored.persisted)?;
    if stored.value.input_stream_id != input.stream_id {
        return Err("Aggregate continuation changed its input stream".into());
    }
    if let Some(position) = stored.value.input {
        if position.stream_id != input.stream_id || position.chunk_seq != input.next_chunk_seq {
            return Err("Aggregate continuation is not at its input cursor".into());
        }
    }
    AggregateMachine::new(spec.aggregates.len() as u32)?.action(stored.value)?;
    if let AggregatePhase::DrainRebuild {
        aggregate_ordinal: 1,
        ..
    } = stored.value.phase
    {
        if let Some(page) = measure_group_page(
            transaction,
            plan,
            stage,
            spec,
            &continuation_relation,
            stored,
        )? {
            return execute_group_page(
                transaction,
                plan,
                stage,
                spec,
                &continuation_relation,
                stored,
                page,
            );
        }
    }
    match stored.value.phase {
        AggregatePhase::Apply => step_apply(
            transaction,
            plan,
            stage,
            spec,
            &continuation_relation,
            stored,
        ),
        AggregatePhase::DrainRebuild { .. } => step_rebuild(
            transaction,
            plan,
            stage,
            spec,
            &continuation_relation,
            stored,
        ),
        AggregatePhase::DrainEmit { .. } => step_emit(
            transaction,
            plan,
            stage,
            spec,
            &continuation_relation,
            stored,
        ),
        AggregatePhase::Frontier => {
            step_frontier(transaction, spec, &continuation_relation, stored)
        }
    }
}

fn start_execute_continuation(
    transaction: &mut StepContext<'_, '_>,
) -> Result<StoredAggregate, String> {
    let input_chunk = next_chunk(transaction, 0)?
        .ok_or_else(|| "runnable Aggregate has no input chunk".to_string())?;
    let input = InputPosition::new(input_chunk.stream_id, input_chunk.sequence, 0)?;
    let (input_cursor, phase) = match input_chunk.kind {
        ChunkKind::Data => (Some(input), AggregatePhase::Apply),
        ChunkKind::Frontier => {
            let dirty = transaction.state_storage(2000)?;
            let queued = transaction.read(
                &format!("SELECT min(queue_id)::bigint FROM {}", dirty.sql()),
                &[],
            )?;
            if queued.len() != 1 {
                return Err("Aggregate dirty queue returned no summary".into());
            }
            match queued
                .first()
                .get::<i64>(1)
                .map_err(|error| error.to_string())?
            {
                Some(group_queue_id) => (
                    None,
                    AggregatePhase::DrainRebuild {
                        group_queue_id,
                        aggregate_ordinal: 1,
                        after: AfterDrain::Frontier(input),
                    },
                ),
                None => (Some(input), AggregatePhase::Frontier),
            }
        }
    };
    Ok(StoredAggregate {
        value: AggregateContinuation {
            input_stream_id: input_chunk.stream_id,
            input: input_cursor,
            phase,
        },
        persisted: false,
    })
}

fn step_frontier(
    transaction: &mut StepContext<'_, '_>,
    spec: &AggregateSpec,
    continuation_relation: &RelationRef,
    stored: StoredAggregate,
) -> Result<StepReceipt, String> {
    let position = stored
        .value
        .input
        .ok_or_else(|| "Aggregate frontier omitted its input cursor".to_string())?;
    let input = transaction.input(0)?.clone();
    let input_chunk = chunk(transaction, &input, position.chunk_seq)?
        .ok_or_else(|| "Aggregate frontier references a missing input chunk".to_string())?;
    if input_chunk.kind != ChunkKind::Frontier
        || input_chunk.stream_id != position.stream_id
        || position.row_ordinal != 0
    {
        return Err("Aggregate frontier continuation is invalid".into());
    }
    let dirty = transaction.state_storage(2000)?;
    let queue_empty = transaction
        .read(
            &format!("SELECT NOT EXISTS(SELECT 1 FROM {})", dirty.sql()),
            &[],
        )?
        .first();
    if !execute_required::<bool>(&queue_empty, 1, "Aggregate dirty queue state")? {
        return Err("Aggregate attempted to forward a frontier before draining".into());
    }
    if spec.groups.is_empty() {
        let groups = transaction.state_storage(1)?;
        let materialized = transaction
            .read(
                &format!("SELECT EXISTS(SELECT 1 FROM {})", groups.sql()),
                &[],
            )?
            .first();
        if !execute_required::<bool>(&materialized, 1, "global Aggregate materialization state")? {
            let causal_lsn = format_lsn(input_chunk.lsn);
            let queued = transaction.write(
                &format!(
                    "WITH global_group AS MATERIALIZED (
                       INSERT INTO {groups}(global_group) VALUES(true)
                       RETURNING group_state_id
                     )
                     INSERT INTO {dirty}(group_state_id,causal_lsn)
                     SELECT group_state_id,$1::pg_lsn FROM global_group
                     RETURNING queue_id",
                    groups = groups.sql(),
                    dirty = dirty.sql(),
                ),
                &unsafe { [DatumWithOid::new(causal_lsn.as_str(), pg_sys::TEXTOID)] },
            )?;
            if queued.len() != 1 {
                return Err("global Aggregate did not queue its empty group".into());
            }
            let queue = execute_required::<i64>(&queued.first(), 1, "global Aggregate queue ID")?;
            let facts = PrimitiveFacts {
                state_rows: 2,
                output: OutputFacts::None,
                ..PrimitiveFacts::default()
            };
            let AggregateTransition::Committed {
                continuation: next, ..
            } = AggregateMachine::new(spec.aggregates.len() as u32)?.apply(
                stored.value,
                AggregateActionResult::Frontier(FrontierResult::GlobalGroupQueued {
                    facts,
                    group_queue_id: queue,
                }),
                transaction.budget(),
            )?;
            replace_execute_continuation(transaction, continuation_relation, stored, next)?;
            return transaction.transition_facts(KernelPhase::Frontier, facts);
        }
    }
    let output = append_frontier(transaction, input_chunk.lsn)?;
    advance_input(
        transaction,
        0,
        input_chunk.sequence + 1,
        input_chunk.lsn,
        WorkUsage::default(),
    )?;
    transaction.reset_admission();
    let transition = AggregateMachine::new(spec.aggregates.len() as u32)?.apply(
        stored.value,
        AggregateActionResult::Frontier(FrontierResult::Forwarded {
            facts: PrimitiveFacts {
                output,
                ..PrimitiveFacts::default()
            },
        }),
        transaction.budget(),
    )?;
    let AggregateTransition::Committed {
        continuation: next, ..
    } = transition;
    replace_execute_continuation(transaction, continuation_relation, stored, next)?;
    transaction.transition_facts(
        KernelPhase::Frontier,
        PrimitiveFacts {
            output,
            ..PrimitiveFacts::default()
        },
    )
}

fn validate_execute_continuation_abi(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
) -> Result<(), String> {
    validate_typed_continuation_abi(transaction, relation, CONTINUATION_COLUMNS, "Aggregate")
}

fn load_execute_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
) -> Result<Option<StoredAggregate>, String> {
    let query = format!(
        "SELECT phase,input_stream_id,input_chunk_seq,input_row_ordinal,
                group_queue_id,aggregate_ordinal,emit_leg,
                after_kind,after_chunk_seq,after_row_ordinal
         FROM {} WHERE singleton FOR UPDATE",
        relation.sql()
    );
    let rows = transaction.lock(&query, &[])?;
    match rows.len() {
        0 => Ok(None),
        1 => {
            let row = rows.first();
            let fields = AggregateFields {
                phase: execute_required(&row, 1, "Aggregate phase")?,
                input_stream_id: execute_required(&row, 2, "Aggregate input stream")?,
                input_chunk_seq: row.get(3).map_err(|error| error.to_string())?,
                input_row_ordinal: row.get(4).map_err(|error| error.to_string())?,
                group_queue_id: row.get(5).map_err(|error| error.to_string())?,
                aggregate_ordinal: row.get(6).map_err(|error| error.to_string())?,
                emit_leg: row.get(7).map_err(|error| error.to_string())?,
                after_kind: row.get(8).map_err(|error| error.to_string())?,
                after_chunk_seq: row.get(9).map_err(|error| error.to_string())?,
                after_row_ordinal: row.get(10).map_err(|error| error.to_string())?,
            };
            Ok(Some(StoredAggregate {
                value: decode_execute_fields(fields)?,
                persisted: true,
            }))
        }
        count => Err(format!(
            "Aggregate continuation relation contains {count} rows"
        )),
    }
}

fn decode_execute_fields(fields: AggregateFields) -> Result<AggregateContinuation, String> {
    let input = match (fields.input_chunk_seq, fields.input_row_ordinal) {
        (Some(chunk_seq), Some(row_ordinal)) => Some(InputPosition::new(
            fields.input_stream_id,
            chunk_seq,
            row_ordinal,
        )?),
        (None, None) => None,
        _ => return Err("Aggregate continuation has a partial input cursor".into()),
    };
    let idle = fields.group_queue_id.is_none()
        && fields.aggregate_ordinal.is_none()
        && fields.emit_leg.is_none()
        && fields.after_kind.is_none()
        && fields.after_chunk_seq.is_none()
        && fields.after_row_ordinal.is_none();
    let phase = match fields.phase {
        APPLY_PHASE if input.is_some() && idle => AggregatePhase::Apply,
        FRONTIER_PHASE if input.is_some() && idle => AggregatePhase::Frontier,
        DRAIN_REBUILD_PHASE
            if input.is_none()
                && fields.group_queue_id.is_some()
                && fields.aggregate_ordinal.is_some()
                && fields.emit_leg.is_none() =>
        {
            AggregatePhase::DrainRebuild {
                group_queue_id: fields.group_queue_id.expect("checked"),
                aggregate_ordinal: u32::try_from(fields.aggregate_ordinal.expect("checked"))
                    .map_err(|_| "Aggregate ordinal is negative")?,
                after: decode_after_drain(fields)?,
            }
        }
        DRAIN_EMIT_PHASE
            if input.is_none()
                && fields.group_queue_id.is_some()
                && fields.aggregate_ordinal.is_none()
                && fields.emit_leg.is_some() =>
        {
            AggregatePhase::DrainEmit {
                group_queue_id: fields.group_queue_id.expect("checked"),
                leg: match fields.emit_leg {
                    Some(1) => EmitLeg::Decide,
                    Some(2) => EmitLeg::InsertPending,
                    _ => return Err("Aggregate continuation has an invalid emit leg".into()),
                },
                after: decode_after_drain(fields)?,
            }
        }
        _ => return Err("Aggregate continuation fields do not encode one phase".into()),
    };
    Ok(AggregateContinuation {
        input_stream_id: fields.input_stream_id,
        input,
        phase,
    })
}

fn decode_after_drain(fields: AggregateFields) -> Result<AfterDrain, String> {
    match (
        fields.after_kind,
        fields.after_chunk_seq,
        fields.after_row_ordinal,
    ) {
        (Some(1), Some(chunk_seq), Some(row_ordinal)) => Ok(AfterDrain::Apply(InputPosition::new(
            fields.input_stream_id,
            chunk_seq,
            row_ordinal,
        )?)),
        (Some(2), None, None) => Ok(AfterDrain::Idle),
        (Some(3), Some(chunk_seq), Some(0)) => Ok(AfterDrain::Frontier(InputPosition::new(
            fields.input_stream_id,
            chunk_seq,
            0,
        )?)),
        _ => Err("Aggregate continuation has an invalid Drain target".into()),
    }
}

fn replace_execute_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    old: StoredAggregate,
    next: Option<AggregateContinuation>,
) -> Result<(), String> {
    let old_fields = old
        .persisted
        .then(|| encode_execute_fields(old.value))
        .transpose()?;
    let next_fields = next.map(encode_execute_fields).transpose()?;
    let old_arguments = old_fields.as_ref().map(aggregate_field_arguments);
    let next_arguments = next_fields.as_ref().map(aggregate_field_arguments);
    replace_continuation_cas(
        transaction,
        relation,
        CONTINUATION_COLUMNS,
        old_arguments.as_ref().map(|arguments| &arguments[..]),
        next_arguments.as_ref().map(|arguments| &arguments[..]),
        "Aggregate",
    )
}

fn encode_execute_fields(continuation: AggregateContinuation) -> Result<AggregateFields, String> {
    let mut fields = AggregateFields {
        phase: match continuation.phase {
            AggregatePhase::Apply => APPLY_PHASE,
            AggregatePhase::DrainRebuild { .. } => DRAIN_REBUILD_PHASE,
            AggregatePhase::DrainEmit { .. } => DRAIN_EMIT_PHASE,
            AggregatePhase::Frontier => FRONTIER_PHASE,
        },
        input_stream_id: continuation.input_stream_id,
        input_chunk_seq: continuation.input.map(|input| input.chunk_seq),
        input_row_ordinal: continuation.input.map(|input| input.row_ordinal),
        group_queue_id: None,
        aggregate_ordinal: None,
        emit_leg: None,
        after_kind: None,
        after_chunk_seq: None,
        after_row_ordinal: None,
    };
    match continuation.phase {
        AggregatePhase::Apply | AggregatePhase::Frontier => {}
        AggregatePhase::DrainRebuild {
            group_queue_id,
            aggregate_ordinal,
            after,
        } => {
            fields.group_queue_id = Some(group_queue_id);
            fields.aggregate_ordinal = Some(
                i32::try_from(aggregate_ordinal)
                    .map_err(|_| "Aggregate ordinal exceeds integer")?,
            );
            encode_after_drain(&mut fields, after);
        }
        AggregatePhase::DrainEmit {
            group_queue_id,
            leg,
            after,
        } => {
            fields.group_queue_id = Some(group_queue_id);
            fields.emit_leg = Some(match leg {
                EmitLeg::Decide => 1,
                EmitLeg::InsertPending => 2,
            });
            encode_after_drain(&mut fields, after);
        }
    }
    Ok(fields)
}

fn encode_after_drain(fields: &mut AggregateFields, after: AfterDrain) {
    match after {
        AfterDrain::Apply(input) => {
            fields.after_kind = Some(1);
            fields.after_chunk_seq = Some(input.chunk_seq);
            fields.after_row_ordinal = Some(input.row_ordinal);
        }
        AfterDrain::Idle => fields.after_kind = Some(2),
        AfterDrain::Frontier(frontier) => {
            fields.after_kind = Some(3);
            fields.after_chunk_seq = Some(frontier.chunk_seq);
            fields.after_row_ordinal = Some(frontier.row_ordinal);
        }
    }
}

fn aggregate_field_arguments(fields: &AggregateFields) -> [DatumWithOid<'_>; 10] {
    unsafe {
        [
            DatumWithOid::new(fields.phase, pg_sys::INT2OID),
            DatumWithOid::new(fields.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(fields.group_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.aggregate_ordinal, pg_sys::INT4OID),
            DatumWithOid::new(fields.emit_leg, pg_sys::INT2OID),
            DatumWithOid::new(fields.after_kind, pg_sys::INT2OID),
            DatumWithOid::new(fields.after_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.after_row_ordinal, pg_sys::INT8OID),
        ]
    }
}

fn execute_required<T: FromDatum + IntoDatum>(
    table: &SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    table
        .get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}

// Atomic bounded Aggregate admission primitive: apply one input prefix to the
// dynamic typed state and queue durable rebuild work.
fn step_apply(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &AggregateSpec,
    continuation_relation: &RelationRef,
    stored: StoredAggregate,
) -> Result<StepReceipt, String> {
    let position = stored
        .value
        .input
        .ok_or_else(|| "Aggregate Apply omitted its input cursor".to_string())?;
    let input = transaction.input(0)?.clone();
    let input_chunk = chunk(transaction, &input, position.chunk_seq)?
        .ok_or_else(|| "Aggregate Apply references a missing input chunk".to_string())?;
    if input_chunk.kind != ChunkKind::Data || input_chunk.stream_id != position.stream_id {
        return Err("Aggregate Apply does not reference data".into());
    }
    let row = position.row_ordinal;
    let row_u64 = u64::try_from(row).map_err(|_| "Aggregate Apply row is negative")?;
    if row_u64 >= input_chunk.rows {
        return Err("Aggregate Apply row is outside its chunk".into());
    }
    let input_storage = transaction.payload_storage(input.stream_id)?;
    if row == 0 {
        payload_facts(transaction, &input_storage.relation, &input_chunk)?;
    }
    let bag = transaction.state_storage(0)?;
    let groups = transaction.state_storage(1)?;
    let dirty = transaction.state_storage(2000)?;
    let bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: &input_storage.row_type,
            alias: "input_row",
        }],
    )?;
    let group_expressions = spec
        .groups
        .iter()
        .map(|group| compile_scalar_expression(&group.key.expr, &bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let mut value_columns = Vec::new();
    let mut value_expressions = Vec::new();
    for (aggregate_index, aggregate) in spec.aggregates.iter().enumerate() {
        for (index, order) in aggregate.order_by.iter().enumerate() {
            value_columns.push(format!("agg_{}_order_{}", aggregate_index + 1, index + 1));
            value_expressions.push(compile_scalar_expression(&order.expr, &bindings)?);
        }
        for (index, distinct) in aggregate.distinct.iter().enumerate() {
            value_columns.push(format!(
                "agg_{}_distinct_{}",
                aggregate_index + 1,
                index + 1
            ));
            value_expressions.push(compile_scalar_expression(&distinct.expr, &bindings)?);
        }
    }
    let group_columns = if group_expressions.is_empty() {
        vec!["global_group".to_string()]
    } else {
        (1..=group_expressions.len())
            .map(|index| format!("group_{index}"))
            .collect()
    };
    let group_values = if group_expressions.is_empty() {
        vec!["true".to_string()]
    } else {
        group_expressions
    };
    let group_evaluations = group_columns
        .iter()
        .cloned()
        .zip(group_values)
        .map(|(column, expression)| format!("{expression} AS {column}"))
        .collect::<Vec<_>>();
    let value_evaluations = value_columns
        .iter()
        .cloned()
        .zip(value_expressions)
        .map(|(column, expression)| format!("{expression} AS {column}"))
        .collect::<Vec<_>>();
    let evaluated_values = group_evaluations
        .iter()
        .cloned()
        .chain(value_evaluations)
        .collect::<Vec<_>>()
        .join(",");
    let representative = value_columns
        .iter()
        .map(|column| format!("(array_agg({column} ORDER BY row_ordinal))[1] AS {column}"))
        .collect::<Vec<_>>()
        .join(",");
    let decision_columns = value_columns
        .iter()
        .map(|column| format!("decision.{column}"))
        .collect::<Vec<_>>()
        .join(",");
    let representative_suffix = if representative.is_empty() {
        String::new()
    } else {
        format!(",{representative}")
    };
    let bag_column_suffix = if value_columns.is_empty() {
        String::new()
    } else {
        format!(",{}", value_columns.join(","))
    };
    let decision_column_suffix = if decision_columns.is_empty() {
        String::new()
    } else {
        format!(",{decision_columns}")
    };
    let group_identity = group_columns.join(",");
    let group_match = aggregate_group_match(transaction, spec, "evaluated_base", "resolved_group")?;
    let row_image = canonical_row_key_sql("evaluated_base.row_value", &input_storage.row_type);
    let budget = transaction.budget();
    let max_input_rows =
        i64::try_from(budget.max_input_rows).map_err(|_| "Aggregate row budget exceeds bigint")?;
    let max_input_bytes = i64::try_from(budget.max_input_bytes)
        .map_err(|_| "Aggregate byte budget exceeds bigint")?;
    let ensured_groups = transaction.write(
        &format!(
            r#"
            WITH candidates AS MATERIALIZED (
              SELECT input_row.row_ordinal,input_row.row_value,
                     shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
              FROM {input} AS input_row
              WHERE input_row.stream_id=$1 AND input_row.chunk_seq=$2
                AND input_row.row_ordinal >= $3
              ORDER BY input_row.row_ordinal LIMIT $4
            ),
            measured AS (
              SELECT candidates.*,
                     row_number() OVER (ORDER BY row_ordinal) AS page_row,
                     sum(row_bytes) OVER (ORDER BY row_ordinal) AS running_bytes
              FROM candidates
            ),
            incoming AS MATERIALIZED (
              SELECT * FROM measured
              WHERE page_row=1 OR running_bytes <= $5
            )
            INSERT INTO {groups}({group_identity})
            SELECT {group_evaluations} FROM incoming AS input_row
            ON CONFLICT ({group_identity}) DO NOTHING
            RETURNING 1
            "#,
            input = input_storage.relation.sql(),
            groups = groups.sql(),
            group_evaluations = group_evaluations.join(","),
        ),
        &unsafe {
            [
                DatumWithOid::new(input_chunk.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(input_chunk.sequence, pg_sys::INT8OID),
                DatumWithOid::new(row, pg_sys::INT8OID),
                DatumWithOid::new(max_input_rows, pg_sys::INT8OID),
                DatumWithOid::new(max_input_bytes, pg_sys::INT8OID),
            ]
        },
    )?;
    let ensured_group_rows = u64::try_from(ensured_groups.len())
        .map_err(|_| "Aggregate group insert count exceeds bigint")?;
    let query = format!(
        r#"
        WITH candidates AS MATERIALIZED (
          SELECT input_row.row_ordinal,input_row.weight,input_row.row_value,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {input} AS input_row
          WHERE input_row.stream_id=$1 AND input_row.chunk_seq=$2
            AND input_row.row_ordinal >= $3
          ORDER BY input_row.row_ordinal LIMIT $4
        ),
        measured AS (
          SELECT candidates.*,
                 row_number() OVER (ORDER BY row_ordinal) AS page_row,
                 sum(row_bytes) OVER (ORDER BY row_ordinal) AS running_bytes
          FROM candidates
        ),
        incoming AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_row=1 OR running_bytes <= $5
        ),
        evaluated_base AS MATERIALIZED (
          SELECT row_ordinal,weight,row_value,row_bytes,
                 {evaluated_values}
          FROM incoming AS input_row
        ),
        evaluated AS MATERIALIZED (
          SELECT evaluated_base.*,
                 {row_image} AS row_image,
                 resolved_group.group_state_id
          FROM evaluated_base
          JOIN {groups} AS resolved_group ON {group_match}
        ),
        prefixes AS (
          SELECT evaluated.*,
                 sum(weight::numeric) OVER (
                   PARTITION BY row_image ORDER BY row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS row_prefix
          FROM evaluated
        ),
        collapsed AS MATERIALIZED (
          SELECT row_image,(array_agg(row_value ORDER BY row_ordinal))[1] AS row_value,
                 (array_agg(group_state_id ORDER BY row_ordinal))[1] AS group_state_id
                 {representative_suffix},
                 sum(weight::numeric) AS net_weight,
                 min(row_prefix) AS min_prefix,max(row_prefix) AS max_prefix
          FROM prefixes GROUP BY row_image
        ),
        existing AS MATERIALIZED (
          SELECT bag.row_id,bag.row_image,bag.multiplicity,bag.group_state_id
          FROM {bag} AS bag JOIN collapsed USING(row_image)
          FOR UPDATE
        ),
        decision AS MATERIALIZED (
          SELECT collapsed.*,existing.row_id,
                 existing.group_state_id AS old_group_state_id,
                 coalesce(existing.multiplicity,0)::numeric AS old_multiplicity,
                 coalesce(existing.multiplicity,0)::numeric+collapsed.net_weight
                   AS new_multiplicity,
                 coalesce(existing.multiplicity,0)::numeric+collapsed.min_prefix
                   AS minimum_multiplicity,
                 coalesce(existing.multiplicity,0)::numeric+collapsed.max_prefix
                   AS maximum_multiplicity
          FROM collapsed LEFT JOIN existing USING(row_image)
        ),
        removed AS (
          DELETE FROM {bag} AS bag USING decision
          WHERE bag.row_id=decision.row_id AND decision.new_multiplicity=0
          RETURNING 1
        ),
        changed AS (
          UPDATE {bag} AS bag SET multiplicity=decision.new_multiplicity::bigint
          FROM decision
          WHERE bag.row_id=decision.row_id AND decision.new_multiplicity>0
            AND decision.new_multiplicity<=9223372036854775807
          RETURNING 1
        ),
        created AS (
          INSERT INTO {bag}(
            row_image,row_value,multiplicity,group_state_id{bag_column_suffix}
          )
          SELECT row_image,row_value,new_multiplicity::bigint,group_state_id
                 {decision_column_suffix}
          FROM decision
          WHERE row_id IS NULL AND new_multiplicity>0
            AND new_multiplicity<=9223372036854775807
          RETURNING 1
        ),
        queued AS (
          INSERT INTO {dirty} AS target(group_state_id,causal_lsn)
          SELECT DISTINCT decision.group_state_id,$6::pg_lsn FROM decision
          ON CONFLICT (group_state_id) DO UPDATE
          SET causal_lsn=greatest(target.causal_lsn,excluded.causal_lsn)
          RETURNING queue_id
        ),
        summary AS (
          SELECT CASE
                      WHEN (SELECT count(*) FROM evaluated)
                           <>(SELECT count(*) FROM incoming)
                        THEN 'missing_group'
                      WHEN bool_or(
                        row_id IS NOT NULL
                        AND old_group_state_id<>group_state_id
                      ) THEN 'group_mismatch'
                      WHEN bool_or(minimum_multiplicity<0) THEN 'negative'
                      WHEN bool_or(maximum_multiplicity>9223372036854775807)
                        THEN 'overflow' ELSE 'ok' END AS status
          FROM decision
        )
        SELECT summary.status,
               (SELECT count(*)::bigint FROM incoming),
               (SELECT coalesce(sum(row_bytes),0)::bigint FROM incoming),
               (SELECT max(row_ordinal)+1 FROM incoming),
               (SELECT count(*)::bigint FROM removed)
                 +(SELECT count(*)::bigint FROM changed)
                 +(SELECT count(*)::bigint FROM created)
                 +(SELECT count(*)::bigint FROM queued)
        FROM summary
        "#,
        input = input_storage.relation.sql(),
        evaluated_values = evaluated_values,
        groups = groups.sql(),
        group_match = group_match,
        row_image = row_image,
        representative_suffix = representative_suffix,
        bag = bag.sql(),
        bag_column_suffix = bag_column_suffix,
        decision_column_suffix = decision_column_suffix,
        dirty = dirty.sql(),
    );
    let causal_lsn = format_lsn(input_chunk.lsn);
    let arguments = unsafe {
        [
            DatumWithOid::new(input_chunk.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(input_chunk.sequence, pg_sys::INT8OID),
            DatumWithOid::new(row, pg_sys::INT8OID),
            DatumWithOid::new(max_input_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_input_bytes, pg_sys::INT8OID),
            DatumWithOid::new(causal_lsn.as_str(), pg_sys::TEXTOID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Aggregate Apply returned no summary".into());
    }
    let result = rows.first();
    let status = execute_required::<String>(&result, 1, "Aggregate Apply status")?;
    if status != "ok" {
        return Err(format!("Aggregate Apply multiplicity is {status}"));
    }
    let applied_rows = aggregate_nonnegative(execute_required::<i64>(
        &result,
        2,
        "Aggregate applied rows",
    )?)?;
    let applied_bytes = aggregate_nonnegative(execute_required::<i64>(
        &result,
        3,
        "Aggregate applied bytes",
    )?)?;
    let next_row = execute_required::<i64>(&result, 4, "Aggregate next input row")?;
    let state_rows =
        aggregate_nonnegative(execute_required::<i64>(&result, 5, "Aggregate state rows")?)?
            .checked_add(ensured_group_rows)
            .ok_or_else(|| "Aggregate Apply state count overflowed".to_string())?;
    if applied_rows == 0 {
        return Err("Aggregate Apply made no bounded input progress".into());
    }
    let expected_next = row
        .checked_add(i64::try_from(applied_rows).map_err(|_| "Aggregate page exceeds bigint")?)
        .ok_or_else(|| "Aggregate input row overflow".to_string())?;
    if next_row != expected_next {
        return Err("Aggregate Apply returned a discontinuous input page".into());
    }
    let dirty_summary = transaction.read(
        &format!("SELECT min(queue_id)::bigint FROM {}", dirty.sql()),
        &[],
    )?;
    if dirty_summary.len() != 1 {
        return Err("Aggregate dirty queue returned no summary".into());
    }
    let first_group_queue_id =
        execute_required::<i64>(&dirty_summary.first(), 1, "Aggregate first dirty group")?;
    let usage = WorkUsage {
        input_rows: applied_rows,
        input_bytes: applied_bytes,
        ..WorkUsage::default()
    };
    let drain_reached = transaction.record_admission(usage)?;
    let chunk_rows =
        i64::try_from(input_chunk.rows).map_err(|_| "Aggregate chunk rows exceed bigint")?;
    let target = if next_row < chunk_rows {
        let next = InputPosition::new(input.stream_id, input_chunk.sequence, next_row)?;
        if drain_reached {
            ApplyTarget::Drain {
                first_group_queue_id,
                after: AfterDrain::Apply(next),
            }
        } else {
            ApplyTarget::Continue(next)
        }
    } else if next_row == chunk_rows {
        advance_input(
            transaction,
            0,
            input_chunk.sequence + 1,
            input.consumed_frontier_lsn,
            WorkUsage {
                input_rows: input_chunk.rows,
                input_bytes: input_chunk.bytes,
                ..WorkUsage::default()
            },
        )?;
        match chunk(transaction, &input, input_chunk.sequence + 1)? {
            Some(next) if next.kind == ChunkKind::Frontier => ApplyTarget::Drain {
                first_group_queue_id,
                after: AfterDrain::Frontier(InputPosition::new(next.stream_id, next.sequence, 0)?),
            },
            _ if drain_reached => ApplyTarget::Drain {
                first_group_queue_id,
                after: AfterDrain::Idle,
            },
            _ => ApplyTarget::Idle,
        }
    } else {
        return Err("Aggregate Apply advanced beyond its input chunk".into());
    };
    let facts = PrimitiveFacts {
        usage,
        state_rows,
        output: OutputFacts::None,
    };
    let transition = AggregateMachine::new(spec.aggregates.len() as u32)?.apply(
        stored.value,
        AggregateActionResult::Applied(AppliedPage { facts, target }),
        transaction.budget(),
    )?;
    let AggregateTransition::Committed {
        continuation: next, ..
    } = transition;
    replace_execute_continuation(transaction, continuation_relation, stored, next)?;
    transaction.transition_facts(KernelPhase::Admit, facts)
}

#[derive(Clone, Debug)]
struct GroupPage {
    last_queue_id: i64,
    causal_lsn: u64,
    input_rows: u64,
    input_bytes: u64,
    output_rows: u64,
    output_bytes: u64,
    desired_ctes: String,
    action_rows: String,
}

#[allow(clippy::too_many_arguments)]
fn measure_group_page(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &AggregateSpec,
    _continuation_relation: &RelationRef,
    stored: StoredAggregate,
) -> Result<Option<GroupPage>, String> {
    let AggregatePhase::DrainRebuild { group_queue_id, .. } = stored.value.phase else {
        return Ok(None);
    };
    for aggregate_index in 0..spec.aggregates.len() {
        let work = transaction.state_storage(
            i32::try_from(2 + aggregate_index)
                .map_err(|_| "Aggregate work slot exceeds integer")?,
        )?;
        let dirty = transaction.state_storage(2000)?;
        let existing = transaction.read(
            &format!(
                "SELECT EXISTS(
                   SELECT 1 FROM {work} AS work
                   JOIN {dirty} AS dirty USING(group_state_id)
                   WHERE dirty.queue_id=$1
                 )",
                work = work.sql(),
                dirty = dirty.sql(),
            ),
            &unsafe { [DatumWithOid::new(group_queue_id, pg_sys::INT8OID)] },
        )?;
        if execute_required::<bool>(&existing.first(), 1, "Aggregate page work state")? {
            return Ok(None);
        }
    }

    let bag = transaction.state_storage(0)?;
    let dirty = transaction.state_storage(2000)?;
    let budget = transaction.budget();
    let max_rows = i64::try_from(budget.max_input_rows)
        .map_err(|_| "Aggregate page row budget exceeds bigint")?;
    let max_bytes = i64::try_from(budget.max_input_bytes)
        .map_err(|_| "Aggregate page byte budget exceeds bigint")?;
    let max_groups = i64::try_from(budget.max_output_rows.max(1))
        .map_err(|_| "Aggregate page output budget exceeds bigint")?;
    let selected = transaction.read(
        &format!(
            r#"
            WITH costs AS MATERIALIZED (
              SELECT dirty.queue_id,dirty.causal_lsn,
                     coalesce(sum(bag.multiplicity),0)::bigint AS input_rows,
                     coalesce(sum(
                       bag.multiplicity
                         * shiba_internal.effect_row_bytes(bag.row_value)
                     ),0)::bigint AS input_bytes
              FROM {dirty} AS dirty
              LEFT JOIN {bag} AS bag USING(group_state_id)
              WHERE dirty.queue_id >= $1
              GROUP BY dirty.queue_id,dirty.causal_lsn
              ORDER BY dirty.queue_id
              LIMIT $4
            ),
            measured AS (
              SELECT costs.*,
                     row_number() OVER (ORDER BY queue_id) AS page_group,
                     sum(input_rows) OVER (ORDER BY queue_id) AS running_rows,
                     sum(input_bytes) OVER (ORDER BY queue_id) AS running_bytes
              FROM costs
            ),
            selected AS (
              SELECT * FROM measured
              WHERE page_group=1
                 OR (running_rows <= $2 AND running_bytes <= $3)
            )
            SELECT max(queue_id)::bigint,
                   coalesce(sum(input_rows),0)::bigint,
                   coalesce(sum(input_bytes),0)::bigint,
                   max(causal_lsn)::text
            FROM selected
            "#,
            dirty = dirty.sql(),
            bag = bag.sql(),
        ),
        &unsafe {
            [
                DatumWithOid::new(group_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(max_rows, pg_sys::INT8OID),
                DatumWithOid::new(max_bytes, pg_sys::INT8OID),
                DatumWithOid::new(max_groups, pg_sys::INT8OID),
            ]
        },
    )?;
    if selected.len() != 1 {
        return Err("Aggregate group page returned no selection summary".into());
    }
    let selected = selected.first();
    let Some(last_queue_id) = selected.get::<i64>(1).map_err(|error| error.to_string())? else {
        return Ok(None);
    };
    let input_rows =
        aggregate_nonnegative(execute_required(&selected, 2, "Aggregate page input rows")?)?;
    let input_bytes = aggregate_nonnegative(execute_required(
        &selected,
        3,
        "Aggregate page input bytes",
    )?)?;
    if input_rows > budget.max_input_rows as u64 || input_bytes > budget.max_input_bytes as u64 {
        return Ok(None);
    }
    let causal_lsn = parse_lsn(
        selected
            .get::<String>(4)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Aggregate page omitted its causal LSN".to_string())?
            .as_str(),
    )?;
    let (desired_ctes, action_rows) = build_group_page_sql(transaction, plan, stage, spec)?;
    let actions = transaction.read(
        &format!(
            "WITH {desired_ctes}, actions AS MATERIALIZED ({action_rows})
             SELECT count(*)::bigint,
                    coalesce(sum(
                      shiba_internal.effect_row_bytes(row_value)
                    ),0)::bigint
             FROM actions"
        ),
        &unsafe {
            [
                DatumWithOid::new(group_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(last_queue_id, pg_sys::INT8OID),
            ]
        },
    )?;
    if actions.len() != 1 {
        return Err("Aggregate page action measurement returned no summary".into());
    }
    let actions = actions.first();
    let output_rows =
        aggregate_nonnegative(execute_required(&actions, 1, "Aggregate page output rows")?)?;
    let output_bytes = aggregate_nonnegative(execute_required(
        &actions,
        2,
        "Aggregate page output bytes",
    )?)?;
    if output_rows > budget.max_output_rows as u64 || output_bytes > budget.max_output_bytes as u64
    {
        return Ok(None);
    }
    Ok(Some(GroupPage {
        last_queue_id,
        causal_lsn,
        input_rows,
        input_bytes,
        output_rows,
        output_bytes,
        desired_ctes,
        action_rows,
    }))
}

fn build_group_page_sql(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &AggregateSpec,
) -> Result<(String, String), String> {
    let input = transaction.input(0)?.clone();
    let input_storage = transaction.payload_storage(input.stream_id)?;
    let output = transaction.output()?.clone();
    let output_storage = transaction.payload_storage(output.stream_id)?;
    let groups = transaction.state_storage(1)?;
    let bag = transaction.state_storage(0)?;
    let dirty = transaction.state_storage(2000)?;
    let bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: &input_storage.row_type,
            alias: "bag",
        }],
    )?;
    let (mut values, representative_cte, representative_join) = if spec.groups.is_empty() {
        (Vec::new(), String::new(), String::new())
    } else {
        let representative_bindings = compile_stage_bindings(
            transaction,
            plan,
            stage,
            &[BindingInput {
                row_type: &input_storage.row_type,
                alias: "representative",
            }],
        )?;
        (
            spec.groups
                .iter()
                .map(|group| compile_scalar_expression(&group.key.expr, &representative_bindings))
                .collect::<Result<Vec<_>, _>>()?,
            format!(
                r#"representatives AS MATERIALIZED (
                  SELECT DISTINCT ON (bag.group_state_id)
                         bag.group_state_id,bag.row_value
                  FROM selected_dirty
                  JOIN {bag} AS bag USING(group_state_id)
                  ORDER BY bag.group_state_id,bag.row_id
                ),"#,
                bag = bag.sql(),
            ),
            " LEFT JOIN representatives AS representative USING(group_state_id)".into(),
        )
    };
    for (aggregate_index, aggregate) in spec.aggregates.iter().enumerate() {
        let function = aggregate_function_sql(transaction, aggregate.function_oid)?;
        let arguments = aggregate
            .args
            .iter()
            .map(|argument| compile_scalar_expression(argument, &bindings))
            .collect::<Result<Vec<_>, _>>()?;
        let call_arguments = if arguments.is_empty() {
            "*".into()
        } else {
            format!(
                "{}{}",
                if aggregate.distinct.is_empty() {
                    ""
                } else {
                    "DISTINCT "
                },
                arguments.join(",")
            )
        };
        let order = if aggregate.order_by.is_empty() {
            String::new()
        } else {
            let mut order = aggregate
                .order_by
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    let capability = resolve_btree_step(transaction, expression, "Aggregate")?;
                    Ok(format!(
                        "bag.{} USING {} NULLS {}",
                        quote_identifier(&format!(
                            "agg_{}_order_{}",
                            aggregate_index + 1,
                            index + 1
                        )),
                        capability.sort_operator,
                        if expression.nulls_first {
                            "FIRST"
                        } else {
                            "LAST"
                        }
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            // DISTINCT aggregate ordering may only name DISTINCT arguments.
            // Equal DISTINCT tuples are the same aggregate input, so no
            // physical row tie-breaker is needed.
            if aggregate.distinct.is_empty() {
                order.push("bag.row_id".into());
            }
            format!(" ORDER BY {}", order.join(","))
        };
        let filter = aggregate
            .filter
            .as_ref()
            .map(|filter| compile_scalar_expression(filter, &bindings))
            .transpose()?
            .map_or_else(String::new, |filter| format!(" FILTER (WHERE {filter})"));
        values.push(format!(
            "(SELECT {function}({call_arguments}{order}){filter}
              FROM {bag_sql} AS bag
              CROSS JOIN LATERAL
                generate_series(1,bag.multiplicity) AS repetition(ordinal)
              WHERE bag.group_state_id=groups.group_state_id)",
            bag_sql = bag.sql(),
        ));
    }
    let present = if spec.groups.is_empty() {
        "true".into()
    } else {
        format!(
            "EXISTS(SELECT 1 FROM {bag_sql} AS present_bag
                    WHERE present_bag.group_state_id=groups.group_state_id)",
            bag_sql = bag.sql(),
        )
    };
    let output_row = format!(
        "ROW({})::{}",
        values.join(","),
        output_storage.row_type.sql()
    );
    let output_key = canonical_row_key_sql("desired_base.row_value", &output_storage.row_type);
    let desired_ctes = format!(
        r#"
        selected_dirty AS MATERIALIZED (
          SELECT * FROM {dirty}
          WHERE queue_id BETWEEN $1 AND $2
        ),
        {representative_cte}
        desired_base AS MATERIALIZED (
          SELECT selected_dirty.queue_id,selected_dirty.group_state_id,
                 groups.published_present,groups.published_key,
                 groups.published_output,
                 {present} AS present,
                 {output_row} AS row_value
          FROM selected_dirty
          JOIN {groups} AS groups USING(group_state_id)
          {representative_join}
        ),
        desired AS MATERIALIZED (
          SELECT desired_base.*,
                 CASE WHEN present THEN {output_key} ELSE NULL::bytea END
                   AS row_key
          FROM desired_base
        ),
        decisions AS MATERIALIZED (
          SELECT desired.*,
                 CASE
                   WHEN published_present=present
                    AND (NOT present OR published_key=row_key) THEN 'unchanged'
                   WHEN NOT published_present AND present THEN 'insert'
                   WHEN published_present AND NOT present THEN 'delete'
                   ELSE 'replace'
                 END AS decision
          FROM desired
        )
        "#,
        dirty = dirty.sql(),
        groups = groups.sql(),
        representative_cte = representative_cte,
        representative_join = representative_join,
    );
    let action_rows = "
        SELECT queue_id,0::smallint AS leg,-1::bigint AS weight,
               published_output AS row_value
        FROM decisions
        WHERE decision IN ('delete','replace')
        UNION ALL
        SELECT queue_id,1::smallint AS leg,1::bigint AS weight,row_value
        FROM decisions
        WHERE decision IN ('insert','replace')
    "
    .into();
    Ok((desired_ctes, action_rows))
}

fn aggregate_function_sql(
    transaction: &mut StepContext<'_, '_>,
    function_oid: u32,
) -> Result<String, String> {
    let rows = transaction.read(
        "SELECT format('%I.%I',namespace.nspname,function.proname)
         FROM pg_catalog.pg_proc AS function
         JOIN pg_catalog.pg_namespace AS namespace
           ON namespace.oid=function.pronamespace
         WHERE function.oid=$1 AND function.prokind='a'",
        &unsafe {
            [DatumWithOid::new(
                pg_sys::Oid::from_u32(function_oid),
                pg_sys::OIDOID,
            )]
        },
    )?;
    if rows.len() != 1 {
        return Err(format!(
            "Aggregate function OID {function_oid} has no unique catalog row"
        ));
    }
    execute_required(&rows.first(), 1, "Aggregate function SQL name")
}

#[allow(clippy::too_many_arguments)]
fn execute_group_page(
    transaction: &mut StepContext<'_, '_>,
    _plan: &DataflowPlan,
    _stage: &DataflowStage,
    spec: &AggregateSpec,
    continuation_relation: &RelationRef,
    stored: StoredAggregate,
    page: GroupPage,
) -> Result<StepReceipt, String> {
    let AggregatePhase::DrainRebuild { after, .. } = stored.value.phase else {
        return Err("Aggregate group page received another phase".into());
    };
    let first_queue_id = match stored.value.phase {
        AggregatePhase::DrainRebuild { group_queue_id, .. } => group_queue_id,
        _ => unreachable!(),
    };
    let output = transaction.output()?.clone();
    let output_storage = transaction.payload_storage(output.stream_id)?;
    if page.output_rows > 0 {
        let append_target =
            transaction.output_append_target(page.output_rows, page.output_bytes)?;
        let (target_sequence, row_offset) = match append_target {
            OutputAppendTarget::New { sequence } => (sequence, 0),
            OutputAppendTarget::Extend {
                sequence,
                row_offset,
                ..
            } => (sequence, row_offset),
        };
        let inserted = transaction.write(
            &format!(
                r#"
                WITH {desired_ctes},
                actions AS MATERIALIZED ({action_rows}),
                numbered AS MATERIALIZED (
                  SELECT row_number() OVER (ORDER BY queue_id,leg)-1
                           AS page_ordinal,
                         weight,row_value
                  FROM actions
                ),
                stored AS (
                  INSERT INTO {output_payload}(
                    stream_id,chunk_seq,row_ordinal,weight,row_value
                  )
                  SELECT $3,$4,$5+page_ordinal,weight,row_value
                  FROM numbered
                  ORDER BY page_ordinal
                  RETURNING shiba_internal.effect_row_bytes(row_value)
                    AS row_bytes
                )
                SELECT count(*)::bigint,
                       coalesce(sum(row_bytes),0)::bigint
                FROM stored
                "#,
                desired_ctes = page.desired_ctes,
                action_rows = page.action_rows,
                output_payload = output_storage.relation.sql(),
            ),
            &unsafe {
                [
                    DatumWithOid::new(first_queue_id, pg_sys::INT8OID),
                    DatumWithOid::new(page.last_queue_id, pg_sys::INT8OID),
                    DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(target_sequence, pg_sys::INT8OID),
                    DatumWithOid::new(
                        i64::try_from(row_offset)
                            .map_err(|_| "Aggregate page row offset exceeds bigint")?,
                        pg_sys::INT8OID,
                    ),
                ]
            },
        )?;
        if inserted.len() != 1 {
            return Err("Aggregate page append returned no summary".into());
        }
        let inserted = inserted.first();
        if aggregate_nonnegative(execute_required(
            &inserted,
            1,
            "Aggregate page inserted rows",
        )?)? != page.output_rows
            || aggregate_nonnegative(execute_required(
                &inserted,
                2,
                "Aggregate page inserted bytes",
            )?)? != page.output_bytes
        {
            return Err("Aggregate page append disagrees with its measurement".into());
        }
        transaction.record_output_append(
            append_target,
            page.output_rows,
            page.output_bytes,
            page.causal_lsn,
        )?;
    }

    let groups = transaction.state_storage(1)?;
    let bag = transaction.state_storage(0)?;
    let dirty = transaction.state_storage(2000)?;
    let updated = transaction.write(
        &format!(
            r#"
            WITH {desired_ctes},
            changed AS (
              UPDATE {groups} AS groups
              SET published_present=desired.present,
                  published_key=desired.row_key,
                  published_output=CASE WHEN desired.present
                                        THEN desired.row_value
                                        ELSE NULL END,
                  pending_present=false,
                  pending_key=NULL,
                  pending_output=NULL
              FROM desired
              WHERE groups.group_state_id=desired.group_state_id
              RETURNING groups.group_state_id
            )
            SELECT count(*)::bigint FROM changed
            "#,
            desired_ctes = page.desired_ctes,
            groups = groups.sql(),
        ),
        &unsafe {
            [
                DatumWithOid::new(first_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(page.last_queue_id, pg_sys::INT8OID),
            ]
        },
    )?;
    let updated = aggregate_nonnegative(execute_required::<i64>(
        &updated.first(),
        1,
        "Aggregate page updated groups",
    )?)?;
    let removed = transaction.write(
        &format!(
            "DELETE FROM {dirty}
             WHERE queue_id BETWEEN $1 AND $2
             RETURNING group_state_id",
            dirty = dirty.sql(),
        ),
        &unsafe {
            [
                DatumWithOid::new(first_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(page.last_queue_id, pg_sys::INT8OID),
            ]
        },
    )?;
    if updated
        != u64::try_from(removed.len()).map_err(|_| "Aggregate page dirty count exceeds bigint")?
    {
        return Err("Aggregate page did not finish every selected group".into());
    }
    let deleted_groups = if !spec.groups.is_empty() {
        transaction
            .write(
                &format!(
                    "DELETE FROM {groups} AS groups
                 WHERE NOT groups.published_present
                   AND NOT groups.pending_present
                   AND NOT EXISTS(
                     SELECT 1 FROM {bag} AS bag
                     WHERE bag.group_state_id=groups.group_state_id
                   )
                 RETURNING 1",
                    groups = groups.sql(),
                    bag = bag.sql(),
                ),
                &[],
            )?
            .len()
    } else {
        0
    };
    let removed_rows =
        u64::try_from(removed.len()).map_err(|_| "Aggregate removed state count exceeds bigint")?;
    let deleted_rows = u64::try_from(deleted_groups)
        .map_err(|_| "Aggregate deleted state count exceeds bigint")?;
    let state_rows = updated
        .checked_add(removed_rows)
        .and_then(|rows| rows.checked_add(deleted_rows))
        .ok_or_else(|| "Aggregate page state count overflowed".to_string())?;
    transaction.record_state_rows(state_rows)?;
    let next_dirty = transaction.read(
        &format!("SELECT min(queue_id)::bigint FROM {}", dirty.sql()),
        &[],
    )?;
    let next_queue = next_dirty
        .first()
        .get::<i64>(1)
        .map_err(|error| error.to_string())?;
    let next = match next_queue {
        Some(group_queue_id) => Some(AggregateContinuation {
            input_stream_id: stored.value.input_stream_id,
            input: None,
            phase: AggregatePhase::DrainRebuild {
                group_queue_id,
                aggregate_ordinal: 1,
                after,
            },
        }),
        None => match after {
            AfterDrain::Apply(input) => Some(AggregateContinuation {
                input_stream_id: stored.value.input_stream_id,
                input: Some(input),
                phase: AggregatePhase::Apply,
            }),
            AfterDrain::Idle => None,
            AfterDrain::Frontier(input) => Some(AggregateContinuation {
                input_stream_id: stored.value.input_stream_id,
                input: Some(input),
                phase: AggregatePhase::Frontier,
            }),
        },
    };
    if let Some(next) = next {
        AggregateMachine::new(spec.aggregates.len() as u32)?.action(next)?;
    }
    replace_execute_continuation(transaction, continuation_relation, stored, next)?;
    transaction.transition(
        KernelPhase::Drain,
        WorkUsage {
            input_rows: page.input_rows,
            input_bytes: page.input_bytes,
            output_rows: page.output_rows,
            output_bytes: page.output_bytes,
        },
    )
}

fn step_rebuild(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &AggregateSpec,
    continuation_relation: &RelationRef,
    stored: StoredAggregate,
) -> Result<StepReceipt, String> {
    let AggregatePhase::DrainRebuild {
        group_queue_id,
        aggregate_ordinal,
        ..
    } = stored.value.phase
    else {
        return Err("Aggregate rebuild received another phase".into());
    };
    let aggregate_index = usize::try_from(aggregate_ordinal - 1)
        .map_err(|_| "Aggregate rebuild ordinal exceeds usize")?;
    let aggregate = spec
        .aggregates
        .get(aggregate_index)
        .ok_or_else(|| "Aggregate rebuild ordinal exceeds its plan".to_string())?;
    let input = transaction.input(0)?.clone();
    let input_storage = transaction.payload_storage(input.stream_id)?;
    let output = transaction.output()?.clone();
    let output_storage = transaction.payload_storage(output.stream_id)?;
    let output_attributes = transaction.composite_attributes(&output_storage.row_type)?;
    let output_attribute = output_attributes
        .get(spec.groups.len() + aggregate_index)
        .ok_or_else(|| "Aggregate output attribute is missing".to_string())?;
    let capability = load_aggregate_capability(
        transaction,
        aggregate.function_oid,
        output_attribute.type_oid,
        aggregate.args.len(),
        aggregate.input_collation_oid,
    )?;
    let bag = transaction.state_storage(0)?;
    let dirty = transaction.state_storage(2000)?;
    let work = transaction.state_storage(
        i32::try_from(2 + aggregate_index).map_err(|_| "Aggregate work slot exceeds integer")?,
    )?;
    let effective_order = aggregate_effective_order(aggregate_index + 1, aggregate)?;
    let order = aggregate_rebuild_order(transaction, &effective_order)?;
    let work_attributes = transaction.relation_attributes(work.oid())?;
    let transition_state = work_attributes
        .get(1)
        .ok_or_else(|| "Aggregate work relation omitted transition state".to_string())?;
    let no_trans_value = work_attributes
        .get(2)
        .ok_or_else(|| "Aggregate work relation omitted no-transition state".to_string())?;
    if work_attributes.first().is_none_or(|attribute| {
        attribute.name != "group_state_id"
            || attribute.type_oid != pg_sys::INT8OID
            || !attribute.not_null
    }) || transition_state.name != "transition_state"
        || transition_state.type_oid != capability.transition_type_oid
        || transition_state.collation_oid != capability.transition_collation_oid
        || no_trans_value.name != "no_trans_value"
        || no_trans_value.type_oid != pg_sys::BOOLOID
        || !no_trans_value.not_null
    {
        return Err("Aggregate rebuild work relation changed its typed ABI".into());
    }
    let work_dirty = "work.group_state_id=dirty.group_state_id";
    let bag_dirty = "bag.group_state_id=dirty.group_state_id";
    let initial_state = initial_state_sql(&capability);
    let initial_no_trans = capability.transition_is_strict && capability.initial_literal.is_none();
    let initialized = transaction.write(
        &format!(
            "INSERT INTO {work}(group_state_id,transition_state,no_trans_value)
             SELECT dirty.group_state_id,{initial_state},$2
             FROM {dirty} AS dirty WHERE dirty.queue_id=$1
             ON CONFLICT (group_state_id) DO UPDATE
             SET transition_state={initial_state},no_trans_value=$2,has_cursor=false,
                 cursor_row_id=NULL,remaining_multiplicity=NULL,complete=false,
                 has_distinct_cursor=false,distinct_transitioned=false
             WHERE {work}.complete
             RETURNING 1",
            work = work.sql(),
            dirty = dirty.sql(),
        ),
        &unsafe {
            [
                DatumWithOid::new(group_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(initial_no_trans, pg_sys::BOOLOID),
            ]
        },
    )?;
    if initialized.len() > 1 {
        return Err("Aggregate rebuild initialized multiple work rows".into());
    }

    let bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: &input_storage.row_type,
            alias: "bag",
        }],
    )?;
    let arguments = aggregate
        .args
        .iter()
        .map(|argument| compile_scalar_expression(argument, &bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let filter = aggregate
        .filter
        .as_ref()
        .map(|filter| {
            compile_scalar_expression(filter, &bindings)
                .map(|filter| format!("coalesce(({filter}),false)"))
        })
        .transpose()?
        .unwrap_or_else(|| "true".into());
    let rebuilt = aggregate_rebuild_page(
        transaction,
        &capability,
        &bag,
        &dirty,
        &work,
        group_queue_id,
        aggregate_ordinal,
        &aggregate.distinct,
        bag_dirty,
        work_dirty,
        &order,
        &arguments,
        &filter,
    )?;
    let state_rows = u64::try_from(initialized.len())
        .map_err(|_| "Aggregate initialization count exceeds bigint")?
        .checked_add(rebuilt.state_rows)
        .ok_or_else(|| "Aggregate rebuild state count overflowed".to_string())?;
    let facts = PrimitiveFacts {
        usage: rebuilt.page.usage,
        state_rows,
        output: OutputFacts::None,
    };
    let transition = AggregateMachine::new(spec.aggregates.len() as u32)?.apply(
        stored.value,
        AggregateActionResult::Rebuilt(RebuiltPage {
            page: rebuilt.page,
            facts,
        }),
        transaction.budget(),
    )?;
    let AggregateTransition::Committed {
        continuation: next, ..
    } = transition;
    replace_execute_continuation(transaction, continuation_relation, stored, next)?;
    transaction.transition_facts(KernelPhase::Drain, facts)
}

fn step_emit(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &AggregateSpec,
    continuation_relation: &RelationRef,
    stored: StoredAggregate,
) -> Result<StepReceipt, String> {
    let AggregatePhase::DrainEmit {
        group_queue_id,
        leg,
        after: _,
    } = stored.value.phase
    else {
        return Err("Aggregate emission received another phase".into());
    };
    let output = transaction.output()?.clone();
    let output_storage = transaction.payload_storage(output.stream_id)?;
    let output_attributes = transaction.composite_attributes(&output_storage.row_type)?;
    if output_attributes.len() != spec.groups.len() + spec.aggregates.len() {
        return Err("Aggregate output payload changed ABI".into());
    }
    let groups = transaction.state_storage(1)?;
    let dirty = transaction.state_storage(2000)?;
    let bag = transaction.state_storage(0)?;
    let arguments = unsafe { [DatumWithOid::new(group_queue_id, pg_sys::INT8OID)] };
    let facts = match leg {
        EmitLeg::Decide => {
            let expression = aggregate_output_expression(
                transaction,
                plan,
                stage,
                spec,
                &output_attributes,
                &output_storage.row_type,
                &dirty,
                &bag,
            )?;
            let decision_rows = transaction.lock(
                &format!(
                    "SELECT CASE
                       WHEN groups.published_present=desired.present
                        AND (
                          NOT desired.present
                          OR groups.published_key=keyed.output_key
                        ) THEN 'unchanged'
                       WHEN NOT groups.published_present AND desired.present THEN 'insert'
                       WHEN groups.published_present AND NOT desired.present THEN 'delete'
                       ELSE 'replace'
                     END
                     FROM {groups} AS groups
                     JOIN {dirty} AS dirty ON {predicate}
                     CROSS JOIN LATERAL (
                       SELECT {present} AS present,
                              {row} AS output_row
                     ) AS desired
                     CROSS JOIN LATERAL (
                       SELECT CASE WHEN desired.present
                                   THEN {key} ELSE NULL::bytea END AS output_key
                     ) AS keyed
                     WHERE dirty.queue_id=$1
                     FOR UPDATE OF groups",
                    groups = groups.sql(),
                    dirty = dirty.sql(),
                    predicate = "groups.group_state_id=dirty.group_state_id",
                    present = expression.present,
                    row = expression.row,
                    key = expression.key,
                ),
                &arguments,
            )?;
            if decision_rows.len() != 1 {
                return Err("Aggregate emission has no unique group state".into());
            }
            let decision =
                execute_required::<String>(&decision_rows.first(), 1, "Aggregate output decision")?;
            let (facts, prepared, completed) = match decision.as_str() {
                "unchanged" => {
                    let changed = transaction.write(
                        &format!(
                            "UPDATE {groups} AS groups
                             SET pending_present=false,pending_key=NULL,pending_output=NULL
                             FROM {dirty} AS dirty
                             WHERE dirty.queue_id=$1 AND {predicate}
                             RETURNING 1",
                            groups = groups.sql(),
                            dirty = dirty.sql(),
                            predicate = "groups.group_state_id=dirty.group_state_id",
                        ),
                        &arguments,
                    )?;
                    require_aggregate_one(changed, "clear unchanged pending output")?;
                    (
                        PrimitiveFacts {
                            state_rows: 1,
                            output: OutputFacts::None,
                            ..PrimitiveFacts::default()
                        },
                        "unchanged",
                        true,
                    )
                }
                "insert" => {
                    let facts = aggregate_emit_row(
                        transaction,
                        &groups,
                        &dirty,
                        &output_storage.relation,
                        &expression,
                        group_queue_id,
                        1,
                        EmitSource::Desired,
                    )?;
                    (facts, "insert", true)
                }
                "delete" => {
                    let facts = aggregate_emit_row(
                        transaction,
                        &groups,
                        &dirty,
                        &output_storage.relation,
                        &expression,
                        group_queue_id,
                        -1,
                        EmitSource::Published,
                    )?;
                    (facts, "delete", true)
                }
                "replace" => {
                    let facts = aggregate_emit_row(
                        transaction,
                        &groups,
                        &dirty,
                        &output_storage.relation,
                        &expression,
                        group_queue_id,
                        -1,
                        EmitSource::ReplacementRetraction,
                    )?;
                    (facts, "replace", false)
                }
                other => {
                    return Err(format!(
                        "Aggregate returned unknown output decision {other:?}"
                    ));
                }
            };
            let discarded = aggregate_discard_work(transaction, spec, &dirty, group_queue_id)?;
            let (next_group, finished_rows) = if completed {
                aggregate_finish_dirty_group(
                    transaction,
                    spec,
                    &groups,
                    &bag,
                    &dirty,
                    group_queue_id,
                )?
            } else {
                (None, 0)
            };
            let mut facts = facts;
            facts.state_rows = facts
                .state_rows
                .checked_add(discarded)
                .and_then(|rows| rows.checked_add(finished_rows))
                .ok_or_else(|| "Aggregate emission state count overflowed".to_string())?;
            let prepared = match prepared {
                "unchanged" => PreparedOutput::Unchanged {
                    facts,
                    next_group_queue_id: next_group,
                },
                "insert" => PreparedOutput::Inserted {
                    facts,
                    next_group_queue_id: next_group,
                },
                "delete" => PreparedOutput::Deleted {
                    facts,
                    next_group_queue_id: next_group,
                },
                "replace" => PreparedOutput::ReplacementRetracted { facts },
                _ => unreachable!(),
            };
            let transition = AggregateMachine::new(spec.aggregates.len() as u32)?.apply(
                stored.value,
                AggregateActionResult::OutputPrepared(prepared),
                transaction.budget(),
            )?;
            let AggregateTransition::Committed {
                continuation: next,
                facts,
            } = transition;
            replace_execute_continuation(transaction, continuation_relation, stored, next)?;
            facts
        }
        EmitLeg::InsertPending => {
            let facts = aggregate_emit_pending_row(
                transaction,
                &groups,
                &dirty,
                &output_storage.relation,
                group_queue_id,
            )?;
            let (next_group, finished_rows) = aggregate_finish_dirty_group(
                transaction,
                spec,
                &groups,
                &bag,
                &dirty,
                group_queue_id,
            )?;
            let mut facts = facts;
            facts.state_rows = facts
                .state_rows
                .checked_add(finished_rows)
                .ok_or_else(|| "Aggregate emission state count overflowed".to_string())?;
            let transition = AggregateMachine::new(spec.aggregates.len() as u32)?.apply(
                stored.value,
                AggregateActionResult::PendingEmitted(PendingOutput {
                    facts,
                    next_group_queue_id: next_group,
                }),
                transaction.budget(),
            )?;
            let AggregateTransition::Committed {
                continuation: next,
                facts,
            } = transition;
            replace_execute_continuation(transaction, continuation_relation, stored, next)?;
            facts
        }
    };
    transaction.transition_facts(KernelPhase::Drain, facts)
}

#[derive(Clone, Debug)]
struct AggregateRebuildOrder {
    columns: Vec<String>,
    order_sql: String,
    range_predicates: Vec<String>,
}

#[derive(Clone, Debug)]
pub(super) struct AggregateOrderKey<'a> {
    pub(super) expression: &'a SortGroupExpr,
    pub(super) column: String,
}

pub(super) fn aggregate_effective_order<'a>(
    aggregate_ordinal: usize,
    aggregate: &'a AggregateExpr,
) -> Result<Vec<AggregateOrderKey<'a>>, String> {
    if !aggregate.distinct.is_empty()
        && aggregate.order_by.iter().any(|order| {
            !aggregate
                .distinct
                .iter()
                .any(|distinct| aggregate_same_value(order, distinct))
        })
    {
        return Err("Aggregate DISTINCT ordering is not covered by its DISTINCT tuple".into());
    }
    let mut effective = aggregate
        .order_by
        .iter()
        .enumerate()
        .map(|(index, expression)| AggregateOrderKey {
            expression,
            column: format!("agg_{aggregate_ordinal}_order_{}", index + 1),
        })
        .collect::<Vec<_>>();
    effective.extend(
        aggregate
            .distinct
            .iter()
            .enumerate()
            .filter(|(_, distinct)| {
                !aggregate
                    .order_by
                    .iter()
                    .any(|order| aggregate_same_value(order, distinct))
            })
            .map(|(index, expression)| AggregateOrderKey {
                expression,
                column: format!("agg_{aggregate_ordinal}_distinct_{}", index + 1),
            }),
    );
    Ok(effective)
}

fn aggregate_same_value(left: &SortGroupExpr, right: &SortGroupExpr) -> bool {
    left.expr == right.expr
        && left.type_ == right.type_
        && left.equality_operator_oid == right.equality_operator_oid
}

#[derive(Clone, Copy, Debug)]
struct AggregateRebuildPrimitive {
    page: PageFacts,
    state_rows: u64,
}

#[derive(Clone, Debug)]
struct AggregateOutputExpression {
    present: String,
    row: String,
    key: String,
}

#[derive(Clone, Copy, Debug)]
enum EmitSource {
    Desired,
    Published,
    ReplacementRetraction,
}

fn load_aggregate_capability(
    transaction: &mut StepContext<'_, '_>,
    function_oid: u32,
    output_type_oid: pg_sys::Oid,
    argument_count: usize,
    input_collation_oid: u32,
) -> Result<AggregateCapability, String> {
    let function_oid = pg_sys::Oid::from_u32(function_oid);
    if function_oid == pg_sys::InvalidOid {
        return Err("Aggregate function OID is invalid".into());
    }
    let rows = transaction.read(AGGREGATE_CAPABILITY_SQL, &unsafe {
        [
            DatumWithOid::new(function_oid, pg_sys::OIDOID),
            DatumWithOid::new(output_type_oid, pg_sys::OIDOID),
        ]
    })?;
    decode_aggregate_capability(
        rows,
        function_oid.to_u32(),
        argument_count,
        input_collation_oid,
    )
}

fn aggregate_group_match(
    transaction: &mut StepContext<'_, '_>,
    spec: &AggregateSpec,
    left: &str,
    right: &str,
) -> Result<String, String> {
    if spec.groups.is_empty() {
        return Ok(format!("{left}.global_group={right}.global_group"));
    }
    spec.groups
        .iter()
        .enumerate()
        .map(|(index, group)| {
            let column = quote_identifier(&format!("group_{}", index + 1));
            let equality = resolve_btree_step(transaction, &group.key, "Aggregate GROUP BY")?
                .equality_operator;
            Ok(aggregate_null_safe_equality(
                &format!("{left}.{column}"),
                &format!("{right}.{column}"),
                &equality,
            ))
        })
        .collect::<Result<Vec<_>, String>>()
        .map(|predicates| predicates.join(" AND "))
}

pub(super) fn aggregate_null_safe_equality(left: &str, right: &str, equality: &str) -> String {
    format!(
        "(
           ({left} IS NULL AND {right} IS NULL)
           OR
           ({left} IS NOT NULL
            AND {right} IS NOT NULL
            AND {left} {equality} {right})
         )"
    )
}

fn aggregate_rebuild_order(
    transaction: &mut StepContext<'_, '_>,
    keys: &[AggregateOrderKey<'_>],
) -> Result<AggregateRebuildOrder, String> {
    if keys.is_empty() {
        return Ok(AggregateRebuildOrder {
            columns: Vec::new(),
            order_sql: "bag.row_id".into(),
            range_predicates: vec!["bag.row_id > work.cursor_row_id".into()],
        });
    }
    let mut columns = Vec::with_capacity(keys.len());
    let mut order = Vec::with_capacity(keys.len() + 1);
    let mut ranges = Vec::with_capacity(keys.len().saturating_mul(2) + 1);
    let mut equal = Vec::with_capacity(keys.len());
    for (index, key) in keys.iter().enumerate() {
        let capability = resolve_btree_step(transaction, key.expression, "Aggregate")?;
        let operator = capability.sort_operator;
        let equality_operator = capability.equality_operator;
        let column = &key.column;
        let column_sql = quote_identifier(column);
        order.push(format!(
            "bag.{column_sql} USING {operator} NULLS {}",
            if key.expression.nulls_first {
                "FIRST"
            } else {
                "LAST"
            }
        ));
        let prefix = if equal.is_empty() {
            String::new()
        } else {
            format!("{} AND ", equal.join(" AND "))
        };
        let cursor = quote_identifier(&format!("cursor_order_{}", index + 1));
        ranges.push(format!(
            "({prefix}
              work.{cursor} IS NOT NULL
              AND bag.{column_sql} IS NOT NULL
              AND work.{cursor} {operator} bag.{column_sql})"
        ));
        ranges.push(if key.expression.nulls_first {
            format!(
                "({prefix}
                  work.{cursor} IS NULL
                  AND bag.{column_sql} IS NOT NULL)"
            )
        } else {
            format!(
                "({prefix}
                  work.{cursor} IS NOT NULL
                  AND bag.{column_sql} IS NULL)"
            )
        });
        equal.push(aggregate_null_safe_equality(
            &format!("bag.{column_sql}"),
            &format!("work.{cursor}"),
            &equality_operator,
        ));
        columns.push(column.clone());
    }
    order.push("bag.row_id".into());
    ranges.push(format!(
        "({} AND bag.row_id > work.cursor_row_id)",
        equal.join(" AND ")
    ));
    Ok(AggregateRebuildOrder {
        columns,
        order_sql: order.join(","),
        range_predicates: ranges,
    })
}

#[allow(clippy::too_many_arguments)]
// Atomic bounded Aggregate rebuild primitive: advance one ordered group page;
// dynamic composite SQL stays local because its row type and ordering are
// operator-specific.
fn aggregate_rebuild_page(
    transaction: &mut StepContext<'_, '_>,
    capability: &AggregateCapability,
    bag: &RelationRef,
    dirty: &RelationRef,
    work: &RelationRef,
    group_queue_id: i64,
    aggregate_ordinal: u32,
    distinct: &[SortGroupExpr],
    bag_dirty: &str,
    work_dirty: &str,
    order: &AggregateRebuildOrder,
    arguments: &[String],
    filter: &str,
) -> Result<AggregateRebuildPrimitive, String> {
    let distinct_count = distinct.len();
    let arguments_nonnull = if arguments.is_empty() {
        "true".into()
    } else {
        arguments
            .iter()
            .map(|argument| format!("({argument}) IS NOT NULL"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let transition_call = format!(
        "{}(fold.state_value{})",
        capability.transition_function,
        if arguments.is_empty() {
            String::new()
        } else {
            format!(",{}", arguments.join(","))
        }
    );
    let (next_state, next_no_trans) = if capability.transition_is_strict {
        let advance = if capability.initial_literal.is_none() {
            let first = arguments.first().ok_or_else(|| {
                "strict Aggregate with NULL initial state has no argument".to_string()
            })?;
            format!(
                "CASE WHEN fold.no_trans_value
                      THEN ({first})::{}
                      WHEN fold.state_value IS NULL
                      THEN fold.state_value
                      ELSE {transition_call} END",
                capability.transition_type
            )
        } else {
            format!(
                "CASE WHEN fold.state_value IS NULL
                      THEN fold.state_value ELSE {transition_call} END"
            )
        };
        (
            format!(
                "CASE WHEN bag.apply_transition
                      THEN CASE WHEN {arguments_nonnull}
                                THEN {advance} ELSE fold.state_value END
                      ELSE fold.state_value END"
            ),
            format!(
                "CASE WHEN bag.apply_transition AND ({arguments_nonnull})
                      THEN false ELSE fold.no_trans_value END"
            ),
        )
    } else {
        (
            format!(
                "CASE WHEN bag.apply_transition
                      THEN {transition_call} ELSE fold.state_value END"
            ),
            "false".into(),
        )
    };

    let (distinct_ctes, distinct_assignments) = if distinct_count == 0 {
        (
            format!(
                r#"
            prepared AS MATERIALIZED (
              SELECT bag.*,({filter}) IS TRUE AS apply_transition
              FROM selected AS bag
            )
            "#
            ),
            String::new(),
        )
    } else {
        let distinct_columns = (1..=distinct_count)
            .map(|index| quote_identifier(&format!("agg_{aggregate_ordinal}_distinct_{index}")))
            .collect::<Vec<_>>();
        let distinct_equalities = distinct
            .iter()
            .map(|expression| {
                resolve_btree_step(transaction, expression, "Aggregate DISTINCT")
                    .map(|capability| capability.equality_operator)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let lag_columns = distinct_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "pg_catalog.lag(bag.{column}) OVER (
                       ORDER BY bag.page_ordinal
                     ) AS previous_distinct_{}",
                    index + 1
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let persisted_equal = distinct_columns
            .iter()
            .zip(&distinct_equalities)
            .enumerate()
            .map(|(index, (column, equality))| {
                aggregate_null_safe_equality(
                    &format!("evaluated.{column}"),
                    &format!("work.cursor_distinct_{}", index + 1),
                    equality,
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let previous_equal = distinct_columns
            .iter()
            .zip(&distinct_equalities)
            .enumerate()
            .map(|(index, (column, equality))| {
                aggregate_null_safe_equality(
                    &format!("evaluated.{column}"),
                    &format!("evaluated.previous_distinct_{}", index + 1),
                    equality,
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let cursor_assignments = distinct_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "cursor_distinct_{}=CASE
                       WHEN bag.page_ordinal IS NULL THEN work.cursor_distinct_{}
                       ELSE bag.{column} END",
                    index + 1,
                    index + 1
                )
            })
            .collect::<Vec<_>>();
        let sql = format!(
            r#"
            evaluated AS MATERIALIZED (
              SELECT bag.*,({filter}) IS TRUE AS passes_filter,{lag_columns}
              FROM selected AS bag
            ),
            boundaries AS MATERIALIZED (
              SELECT evaluated.*,
                     CASE
                       WHEN evaluated.page_ordinal=1
                         THEN NOT (
                           work.has_distinct_cursor AND ({persisted_equal})
                         )
                       ELSE NOT ({previous_equal})
                     END AS starts_tuple,
                     work.distinct_transitioned AS persisted_transitioned
              FROM evaluated
              JOIN {work} AS work ON true
              JOIN {dirty} AS dirty
                ON dirty.queue_id=$1
               AND work.group_state_id=dirty.group_state_id
            ),
            segmented AS MATERIALIZED (
              SELECT boundaries.*,
                     sum(starts_tuple::integer) OVER (
                       ORDER BY page_ordinal
                     ) AS tuple_ordinal
              FROM boundaries
            ),
            prepared_base AS MATERIALIZED (
              SELECT segmented.*,
                     tuple_ordinal=0 AND persisted_transitioned
                       AS previously_transitioned,
                     count(*) FILTER (WHERE passes_filter) OVER (
                       PARTITION BY tuple_ordinal
                       ORDER BY page_ordinal
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                     ) AS passing_before
              FROM segmented
            ),
            prepared AS MATERIALIZED (
              SELECT prepared_base.*,
                     passes_filter
                       AND NOT previously_transitioned
                       AND passing_before=0 AS apply_transition,
                     previously_transitioned OR bool_or(passes_filter) OVER (
                       PARTITION BY tuple_ordinal
                       ORDER BY page_ordinal
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     ) AS tuple_transitioned
              FROM prepared_base
            )
            "#,
            work = work.sql(),
            dirty = dirty.sql(),
        );
        let assignments = std::iter::once(
            "has_distinct_cursor=CASE WHEN bag.page_ordinal IS NULL
                                      THEN work.has_distinct_cursor ELSE true END"
                .to_string(),
        )
        .chain(std::iter::once(
            "distinct_transitioned=CASE WHEN bag.page_ordinal IS NULL
                                        THEN work.distinct_transitioned
                                        ELSE bag.tuple_transitioned END"
                .to_string(),
        ))
        .chain(cursor_assignments)
        .collect::<Vec<_>>()
        .join(",");
        (sql, format!(",{assignments}"))
    };

    let cursor_assignments = order
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let cursor = quote_identifier(&format!("cursor_order_{}", index + 1));
            let column = quote_identifier(column);
            format!(
                "{cursor}=CASE WHEN bag.page_ordinal IS NULL
                         THEN work.{cursor} ELSE bag.{column} END"
            )
        })
        .collect::<Vec<_>>();
    let cursor_assignments = if cursor_assignments.is_empty() {
        String::new()
    } else {
        format!(",{}", cursor_assignments.join(","))
    };
    let effective_multiplicity: String = if distinct_count != 0 {
        "1::bigint".into()
    } else {
        "CASE WHEN work.remaining_multiplicity IS NOT NULL
                    AND bag.row_id=work.cursor_row_id
              THEN work.remaining_multiplicity ELSE bag.multiplicity END"
            .into()
    };
    let remaining_multiplicity = if distinct_count != 0 {
        "NULL::bigint"
    } else {
        "NULLIF(bag.remaining_after,0)"
    };
    let candidate_range = |predicate: &str| {
        format!(
            r#"
            (
              SELECT bag.*,{effective_multiplicity} AS effective_multiplicity
              FROM {bag} AS bag
              JOIN {dirty} AS dirty ON {bag_dirty}
              JOIN {work} AS work ON {work_dirty}
              WHERE dirty.queue_id=$1 AND NOT work.complete
                AND ({predicate})
              ORDER BY {order}
              LIMIT $4
            )
            "#,
            bag = bag.sql(),
            dirty = dirty.sql(),
            work = work.sql(),
            order = order.order_sql,
        )
    };
    let mut candidate_ranges = vec![
        candidate_range("NOT work.has_cursor"),
        candidate_range(
            "work.remaining_multiplicity IS NOT NULL
             AND bag.row_id=work.cursor_row_id",
        ),
    ];
    candidate_ranges.extend(
        order
            .range_predicates
            .iter()
            .map(|predicate| candidate_range(&format!("work.has_cursor AND ({predicate})"))),
    );
    let candidate_ranges = candidate_ranges.join(" UNION ALL ");
    let budget = transaction.budget();
    let max_rows = i64::try_from(budget.max_input_rows)
        .map_err(|_| "Aggregate rebuild row budget exceeds bigint")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Aggregate rebuild row budget overflow".to_string())?;
    let max_bytes = i64::try_from(budget.max_input_bytes)
        .map_err(|_| "Aggregate rebuild byte budget exceeds bigint")?;
    let source_order = format!("{},occurrence.value", order.order_sql);
    let page_order = format!("{},bag.occurrence", order.order_sql);
    let query = format!(
        r#"
        WITH RECURSIVE bag_candidates AS MATERIALIZED (
          SELECT bag.*
          FROM ({candidate_ranges}) AS bag
          ORDER BY {order}
          LIMIT $4
        ),
        ranked AS MATERIALIZED (
          SELECT bag.*,
                 coalesce(
                   sum(bag.effective_multiplicity::numeric) OVER (
                     ORDER BY {order}
                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ),
                   0::numeric
                 ) AS preceding_multiplicity
          FROM bag_candidates AS bag
        ),
        source AS MATERIALIZED (
          SELECT bag.*,occurrence.value AS occurrence,
                 bag.effective_multiplicity-occurrence.value AS remaining_after,
                 shiba_internal.effect_row_bytes(bag.row_value)::bigint AS row_bytes
          FROM ranked AS bag
          CROSS JOIN LATERAL pg_catalog.generate_series(
            1::bigint,
            least(
              bag.effective_multiplicity,
              greatest($4::numeric-bag.preceding_multiplicity,0::numeric)::bigint
            )
          ) AS occurrence(value)
          WHERE bag.preceding_multiplicity<$4::numeric
          ORDER BY {source_order}
        ),
        measured AS MATERIALIZED (
          SELECT bag.*,
                 row_number() OVER (ORDER BY {page_order}) AS page_ordinal,
                 sum(bag.row_bytes::numeric) OVER (
                   ORDER BY {page_order}
                 ) AS running_bytes
          FROM source AS bag
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal<=$2
            AND (page_ordinal=1 OR running_bytes<=$3::numeric)
        ),
        {distinct_ctes},
        fold(step,state_value,no_trans_value) AS (
          SELECT 0::bigint,work.transition_state,work.no_trans_value
          FROM {work} AS work
          JOIN {dirty} AS dirty ON {work_dirty}
          WHERE dirty.queue_id=$1
          UNION ALL
          SELECT bag.page_ordinal,{next_state},{next_no_trans}
          FROM fold
          JOIN prepared AS bag ON bag.page_ordinal=fold.step+1
        ),
        final_fold AS MATERIALIZED (
          SELECT * FROM fold ORDER BY step DESC LIMIT 1
        ),
        final_selected AS MATERIALIZED (
          SELECT * FROM prepared ORDER BY page_ordinal DESC LIMIT 1
        ),
        updated AS (
          UPDATE {work} AS work
          SET transition_state=final_fold.state_value,
              no_trans_value=final_fold.no_trans_value,
              has_cursor=CASE WHEN bag.page_ordinal IS NULL
                              THEN work.has_cursor ELSE true END,
              cursor_row_id=CASE WHEN bag.page_ordinal IS NULL
                                 THEN work.cursor_row_id ELSE bag.row_id END,
              remaining_multiplicity=CASE WHEN bag.page_ordinal IS NULL
                                          THEN work.remaining_multiplicity
                                          ELSE {remaining_multiplicity} END,
              complete=(SELECT count(*) FROM source)
                         =(SELECT count(*) FROM selected)
              {cursor_assignments}
              {distinct_assignments}
          FROM final_fold
          CROSS JOIN {dirty} AS dirty
          LEFT JOIN final_selected AS bag ON true
          WHERE dirty.queue_id=$1 AND {work_dirty}
          RETURNING 1
        )
        SELECT CASE
                 WHEN (
                   SELECT count(*) FROM {work} AS work
                   JOIN {dirty} AS dirty ON {work_dirty}
                   WHERE dirty.queue_id=$1
                 )<>1 THEN 'missing_work'
                 WHEN (SELECT count(*) FROM updated)<>1 THEN 'missing_update'
                 ELSE 'ok'
               END,
               count(*)::bigint,
               coalesce(sum(row_bytes),0)::bigint,
               (array_agg(row_id ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM updated)
        FROM selected
        "#,
        dirty = dirty.sql(),
        work = work.sql(),
        order = order.order_sql,
        source_order = source_order,
    );
    let rows = transaction.write(&query, &unsafe {
        [
            DatumWithOid::new(group_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    })?;
    if rows.len() != 1 {
        return Err("Aggregate rebuild returned no page summary".into());
    }
    let row = rows.first();
    let status: String = execute_required(&row, 1, "Aggregate rebuild status")?;
    if status != "ok" {
        return Err(format!("Aggregate rebuild returned {status}"));
    }
    let processed_rows =
        aggregate_nonnegative(execute_required::<i64>(&row, 2, "Aggregate rebuild rows")?)?;
    let processed_bytes =
        aggregate_nonnegative(execute_required::<i64>(&row, 3, "Aggregate rebuild bytes")?)?;
    let last_row_id = row.get::<i64>(4).map_err(|error| error.to_string())?;
    let complete = execute_required(&row, 5, "Aggregate rebuild completion")?;
    let state_rows = aggregate_nonnegative(execute_required::<i64>(
        &row,
        6,
        "Aggregate rebuild mutations",
    )?)?;
    if last_row_id.is_some() != (processed_rows > 0) {
        return Err("Aggregate rebuild row cursor is inconsistent".into());
    }
    if state_rows == 0 {
        return Err("Aggregate rebuild did not mutate its work state".into());
    }
    Ok(AggregateRebuildPrimitive {
        page: PageFacts {
            usage: WorkUsage {
                input_rows: processed_rows,
                input_bytes: processed_bytes,
                ..WorkUsage::default()
            },
            last_row_id,
            complete,
        },
        state_rows,
    })
}

#[allow(clippy::too_many_arguments)]
fn aggregate_output_expression(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &AggregateSpec,
    output_attributes: &[AttributeRef],
    output_type: &TypeRef,
    dirty: &RelationRef,
    bag: &RelationRef,
) -> Result<AggregateOutputExpression, String> {
    let input = transaction.input(0)?.clone();
    let input_storage = transaction.payload_storage(input.stream_id)?;
    let mut values = Vec::with_capacity(spec.groups.len() + spec.aggregates.len());
    let mut from = format!("{} AS dirty", dirty.sql());
    if !spec.groups.is_empty() {
        let bindings = compile_stage_bindings(
            transaction,
            plan,
            stage,
            &[BindingInput {
                row_type: &input_storage.row_type,
                alias: "representative",
            }],
        )?;
        values.extend(
            spec.groups
                .iter()
                .map(|group| compile_scalar_expression(&group.key.expr, &bindings))
                .collect::<Result<Vec<_>, _>>()?,
        );
        from.push_str(&format!(
            " JOIN LATERAL (
                SELECT bag.row_value
                FROM {bag} AS bag
                WHERE bag.group_state_id=dirty.group_state_id
                ORDER BY bag.row_id
                LIMIT 1
              ) AS representative ON true",
            bag = bag.sql(),
        ));
    }
    for (index, aggregate) in spec.aggregates.iter().enumerate() {
        let alias = format!("work_{}", index + 1);
        let work = transaction.state_storage(
            i32::try_from(2 + index).map_err(|_| "Aggregate work slot exceeds integer")?,
        )?;
        from.push_str(&format!(
            " JOIN {} AS {alias}
                ON {alias}.group_state_id=dirty.group_state_id",
            work.sql()
        ));
        let capability = load_aggregate_capability(
            transaction,
            aggregate.function_oid,
            output_attributes[spec.groups.len() + index].type_oid,
            aggregate.args.len(),
            aggregate.input_collation_oid,
        )?;
        let value = capability.final_function.map_or_else(
            || format!("{alias}.transition_state"),
            |function| format!("{function}({alias}.transition_state)"),
        );
        values.push(value);
    }
    let row = format!(
        "(SELECT ROW({values})::{} FROM {from}
          WHERE dirty.queue_id=$1)",
        output_type.sql(),
        values = values.join(","),
    );
    let key = canonical_row_key_sql("desired.output_row", output_type);
    let present = if spec.groups.is_empty() {
        "true".into()
    } else {
        format!(
            "EXISTS(
               SELECT 1 FROM {bag} AS bag
               JOIN {dirty} AS dirty
                 ON bag.group_state_id=dirty.group_state_id
               WHERE dirty.queue_id=$1
             )",
            bag = bag.sql(),
            dirty = dirty.sql(),
        )
    };
    Ok(AggregateOutputExpression { present, row, key })
}

#[allow(clippy::too_many_arguments)]
fn aggregate_emit_row(
    transaction: &mut StepContext<'_, '_>,
    groups: &RelationRef,
    dirty: &RelationRef,
    output_payload: &RelationRef,
    expression: &AggregateOutputExpression,
    group_queue_id: i64,
    weight: i64,
    source: EmitSource,
) -> Result<PrimitiveFacts, String> {
    let (row, assignment, guard, desired) = match source {
        EmitSource::Desired => (
            "desired.row_value".into(),
            "published_present=true,published_key=desired.row_key,
             published_output=emitted.row_value,
             pending_present=false,pending_key=NULL,pending_output=NULL",
            format!("({})", expression.present),
            Some(expression),
        ),
        EmitSource::Published => (
            "groups.published_output".into(),
            "published_present=false,published_key=NULL,published_output=NULL,
             pending_present=false,pending_key=NULL,pending_output=NULL",
            "groups.published_present".into(),
            None,
        ),
        EmitSource::ReplacementRetraction => (
            "groups.published_output".into(),
            "published_present=false,published_key=NULL,published_output=NULL,
             pending_present=true,pending_key=desired.row_key,
             pending_output=desired.row_value",
            "groups.published_present".into(),
            Some(expression),
        ),
    };
    aggregate_append_output(
        transaction,
        groups,
        dirty,
        output_payload,
        group_queue_id,
        weight,
        row,
        assignment,
        guard,
        desired,
    )
}

fn aggregate_emit_pending_row(
    transaction: &mut StepContext<'_, '_>,
    groups: &RelationRef,
    dirty: &RelationRef,
    output_payload: &RelationRef,
    group_queue_id: i64,
) -> Result<PrimitiveFacts, String> {
    aggregate_append_output(
        transaction,
        groups,
        dirty,
        output_payload,
        group_queue_id,
        1,
        "groups.pending_output".into(),
        "published_present=true,published_key=groups.pending_key,
         published_output=emitted.row_value,
         pending_present=false,pending_key=NULL,pending_output=NULL",
        "groups.pending_present".into(),
        None,
    )
}

#[allow(clippy::too_many_arguments)]
// Atomic bounded Aggregate output primitive: reconcile one pending aggregate
// row with typed payload/state. Effect publication is deferred to StepContext.
fn aggregate_append_output(
    transaction: &mut StepContext<'_, '_>,
    groups: &RelationRef,
    dirty: &RelationRef,
    output_payload: &RelationRef,
    group_queue_id: i64,
    weight: i64,
    row: String,
    assignment: &str,
    guard: String,
    desired_output: Option<&AggregateOutputExpression>,
) -> Result<PrimitiveFacts, String> {
    let output = transaction.output()?.clone();
    let desired = desired_output
        .map(|expression| {
            format!(
                "desired AS MATERIALIZED (
                   SELECT desired.output_row AS row_value,{key} AS row_key
                   FROM (SELECT {row} AS output_row) AS desired
                 ),",
                key = expression.key,
                row = expression.row,
            )
        })
        .unwrap_or_default();
    let rows = transaction.write(
        &format!(
            "WITH {desired}
             emitted AS MATERIALIZED (
               SELECT {row} AS row_value,dirty.causal_lsn
               FROM {groups} AS groups
               JOIN {dirty} AS dirty
                 ON groups.group_state_id=dirty.group_state_id
               {desired_from}
               WHERE dirty.queue_id=$1 AND {guard}
             ),
             summary AS MATERIALIZED (
               SELECT shiba_internal.effect_row_bytes(row_value)::bigint AS bytes,
                      causal_lsn
               FROM emitted
             ),
             payload_insert AS (
               INSERT INTO {payload}(
                 stream_id,chunk_seq,row_ordinal,weight,row_value
               )
               SELECT $2,$3,0,$4,emitted.row_value
               FROM emitted
               RETURNING 1
             ),
             state_update AS (
               UPDATE {groups} AS groups SET {assignment}
               FROM emitted{desired_from},{dirty} AS dirty
               WHERE dirty.queue_id=$1
                 AND groups.group_state_id=dirty.group_state_id
               RETURNING 1
             )
             SELECT summary.bytes,summary.causal_lsn::text,
                    (SELECT count(*)::bigint FROM payload_insert),
                    (SELECT count(*)::bigint FROM state_update)
             FROM summary",
            groups = groups.sql(),
            dirty = dirty.sql(),
            payload = output_payload.sql(),
            desired_from = if desired_output.is_some() {
                ",desired"
            } else {
                ""
            },
        ),
        &unsafe {
            [
                DatumWithOid::new(group_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(output.next_chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(weight, pg_sys::INT8OID),
            ]
        },
    )?;
    if rows.len() != 1 {
        return Err("Aggregate emission did not produce one summary".into());
    }
    let row = rows.first();
    let bytes = aggregate_nonnegative(execute_required::<i64>(&row, 1, "Aggregate output bytes")?)?;
    let causal_lsn = parse_lsn(&execute_required::<String>(
        &row,
        2,
        "Aggregate output LSN",
    )?)?;
    let inserted = aggregate_nonnegative(execute_required::<i64>(
        &row,
        3,
        "Aggregate payload inserts",
    )?)?;
    let updated =
        aggregate_nonnegative(execute_required::<i64>(&row, 4, "Aggregate group updates")?)?;
    if bytes == 0 || inserted != 1 || updated != 1 {
        return Err("Aggregate typed output append is inconsistent".into());
    }
    let sequence = output.next_chunk_seq;
    transaction.record_output_append(OutputAppendTarget::New { sequence }, 1, bytes, causal_lsn)?;
    Ok(PrimitiveFacts {
        usage: WorkUsage {
            output_rows: 1,
            output_bytes: bytes,
            ..WorkUsage::default()
        },
        state_rows: 1,
        output: OutputFacts::Data {
            chunk_seq: sequence,
        },
    })
}

fn aggregate_discard_work(
    transaction: &mut StepContext<'_, '_>,
    spec: &AggregateSpec,
    dirty: &RelationRef,
    group_queue_id: i64,
) -> Result<u64, String> {
    let mut removed = 0_u64;
    for aggregate_index in 0..spec.aggregates.len() {
        let work = transaction.state_storage(
            i32::try_from(2 + aggregate_index)
                .map_err(|_| "Aggregate work slot exceeds integer")?,
        )?;
        let rows = transaction.write(
            &format!(
                "DELETE FROM {work} AS work USING {dirty} AS dirty
                 WHERE dirty.queue_id=$1
                   AND work.group_state_id=dirty.group_state_id
                 RETURNING 1",
                work = work.sql(),
                dirty = dirty.sql(),
            ),
            &unsafe { [DatumWithOid::new(group_queue_id, pg_sys::INT8OID)] },
        )?;
        require_aggregate_one(rows, "discard completed work row")?;
        removed = removed
            .checked_add(1)
            .ok_or_else(|| "Aggregate work deletion count overflowed".to_string())?;
    }
    Ok(removed)
}

fn aggregate_finish_dirty_group(
    transaction: &mut StepContext<'_, '_>,
    spec: &AggregateSpec,
    groups: &RelationRef,
    bag: &RelationRef,
    dirty: &RelationRef,
    group_queue_id: i64,
) -> Result<(Option<i64>, u64), String> {
    let removed = transaction.write(
        &format!(
            "DELETE FROM {} WHERE queue_id=$1
             RETURNING queue_id,group_state_id",
            dirty.sql()
        ),
        &unsafe { [DatumWithOid::new(group_queue_id, pg_sys::INT8OID)] },
    )?;
    if removed.len() != 1 {
        return Err("Aggregate did not remove one completed dirty group".into());
    }
    let removed = removed.first();
    if execute_required::<i64>(&removed, 1, "Aggregate dirty deletion")? != group_queue_id {
        return Err("Aggregate did not remove one completed dirty group".into());
    }
    let group_state_id = execute_required::<i64>(&removed, 2, "Aggregate completed group state")?;
    let mut state_rows = 1_u64;
    if !spec.groups.is_empty() {
        let tombstone = transaction.write(
            &format!(
                "DELETE FROM {groups} AS groups
                 WHERE groups.group_state_id=$1
                   AND NOT groups.published_present
                   AND NOT groups.pending_present
                   AND NOT EXISTS (
                     SELECT 1 FROM {bag} AS bag
                     WHERE bag.group_state_id=groups.group_state_id
                   )
                 RETURNING 1",
                groups = groups.sql(),
                bag = bag.sql(),
            ),
            &unsafe { [DatumWithOid::new(group_state_id, pg_sys::INT8OID)] },
        )?;
        if tombstone.len() > 1 {
            return Err("Aggregate deleted multiple group tombstones".into());
        }
        state_rows = state_rows
            .checked_add(
                u64::try_from(tombstone.len())
                    .map_err(|_| "Aggregate tombstone count exceeds bigint")?,
            )
            .ok_or_else(|| "Aggregate state deletion count overflowed".to_string())?;
    }
    // A new SPI statement observes the deletion. A sibling data-modifying CTE
    // would read the old base-table snapshot here.
    let queued = transaction.read(
        &format!("SELECT min(queue_id)::bigint FROM {}", dirty.sql()),
        &[],
    )?;
    if queued.len() != 1 {
        return Err("Aggregate dirty queue returned no summary".into());
    }
    let next = queued
        .first()
        .get::<i64>(1)
        .map_err(|error| error.to_string())?;
    if let Some(next) = next {
        if next <= group_queue_id {
            return Err("Aggregate dirty queue did not advance".into());
        }
    }
    Ok((next, state_rows))
}

fn require_aggregate_one(rows: SpiTupleTable<'_>, operation: &str) -> Result<(), String> {
    if rows.len() != 1 {
        return Err(format!("Aggregate failed to {operation}"));
    }
    Ok(())
}
