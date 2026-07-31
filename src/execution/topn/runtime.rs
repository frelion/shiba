use super::*;

/// Execute one TopN checkpoint. Every action performs one bounded set
/// primitive; typed rows and ordering values remain in PostgreSQL.
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
        .get(usize::try_from(stage_id).map_err(|_| "TopN stage ID exceeds usize")?)
        .ok_or_else(|| format!("dataflow has no TopN stage {stage_id}"))?;
    let OperatorSpec::TopN(spec) = &stage.spec else {
        return Err("TopN kernel received another operator".into());
    };
    if stage.inputs.len() != 1
        || transaction.inputs().len() != 1
        || transaction.input(0)?.port != 0
        || transaction.input(0)?.producer != ProducerKind::Operator
    {
        return Err("TopN must have one operator input".into());
    }

    let machine = TopNMachine::new(spec.limit, spec.offset, spec.with_ties);
    let storage = load_topn_storage(transaction, stage, spec)?;
    validate_topn_control_state(transaction, &storage)?;
    let expressions = compile_topn_expressions(
        transaction,
        plan,
        stage,
        spec,
        &storage.input_type,
        &storage.output_type,
    )?;
    let durable = load_topn_continuation(transaction, &storage.continuation)?;
    crate::execution::validate_continuation_authority(transaction, durable.is_some())?;
    let current = match durable {
        Some(durable) => durable,
        None => start_topn_continuation(transaction, &storage, machine)?,
    };
    if current.continuation.input_stream_id != transaction.input(0)?.stream_id {
        return Err("TopN continuation changed its input stream".into());
    }
    if let Some(input) = current.continuation.input {
        if input.stream_id != transaction.input(0)?.stream_id
            || input.chunk_seq != transaction.input(0)?.next_chunk_seq
        {
            return Err("TopN continuation is not at its input cursor".into());
        }
    }

    let action = machine.action(current.continuation)?;
    let result = match action {
        TopNAction::Admit { input } => TopNActionResult::Admitted(run_topn_admission(
            transaction,
            &storage,
            &expressions,
            input,
        )?),
        TopNAction::SelectCandidates {
            generation_id,
            progress,
        } => TopNActionResult::Selected(run_topn_selection(
            transaction,
            &storage,
            &expressions,
            spec,
            generation_id,
            progress,
        )?),
        TopNAction::Diff {
            generation_id,
            leg,
            cursor,
        } => TopNActionResult::Diffed(run_topn_diff(
            transaction,
            &storage,
            generation_id,
            leg,
            cursor,
        )?),
        TopNAction::Cleanup {
            generation_id,
            cursor,
        } => {
            let after_drain = phase_after_drain(current.continuation.phase)?;
            let mut page =
                run_topn_cleanup(transaction, &storage, generation_id, cursor, after_drain)?;
            if page.complete {
                let finalized = finish_topn_drain(transaction, &storage)?;
                page.facts.state_rows = page
                    .facts
                    .state_rows
                    .checked_add(finalized)
                    .ok_or_else(|| "TopN cleanup state count overflow".to_string())?;
            }
            TopNActionResult::Cleaned(page)
        }
        TopNAction::ForwardFrontier { input } => {
            TopNActionResult::FrontierForwarded(run_topn_frontier(transaction, input)?)
        }
    };
    let transition = machine.apply(current.continuation, result, transaction.budget())?;
    let TopNTransition::Committed {
        continuation: next,
        facts,
    } = transition;
    replace_topn_continuation(
        transaction,
        &storage.continuation,
        current.persisted.then_some(current.continuation),
        next,
    )?;
    let phase = match current.continuation.phase {
        TopNPhase::Admit => KernelPhase::Admit,
        TopNPhase::Frontier => KernelPhase::Frontier,
        TopNPhase::Select { .. } | TopNPhase::Diff { .. } | TopNPhase::Cleanup { .. } => {
            KernelPhase::Drain
        }
    };
    transaction.transition_facts(phase, facts)
}

fn start_topn_continuation(
    transaction: &mut StepContext<'_, '_>,
    storage: &TopNStorage,
    machine: TopNMachine,
) -> Result<DurableTopN, String> {
    let chunk = next_chunk(transaction, 0)?
        .ok_or_else(|| "runnable TopN has no input chunk".to_string())?;
    let input = InputPosition::new(chunk.stream_id, chunk.sequence, 0)?;
    let (input, phase) = match chunk.kind {
        ChunkKind::Data => (Some(input), TopNPhase::Admit),
        ChunkKind::Frontier if topn_is_dirty(transaction, storage)? => (
            None,
            initial_topn_drain_phase(transaction, machine, AfterDrain::Frontier(input))?,
        ),
        ChunkKind::Frontier => (Some(input), TopNPhase::Frontier),
    };
    Ok(DurableTopN {
        continuation: TopNContinuation {
            input_stream_id: chunk.stream_id,
            input,
            phase,
        },
        persisted: false,
    })
}

fn initial_topn_drain_phase(
    transaction: &mut StepContext<'_, '_>,
    machine: TopNMachine,
    after_drain: AfterDrain,
) -> Result<TopNPhase, String> {
    let generation_id = next_generation_id(transaction)?;
    Ok(if machine.limit == 0 {
        TopNPhase::Diff {
            generation_id,
            leg: DiffLeg::Remove,
            cursor: TopNDiffCursor::default(),
            after_drain,
        }
    } else {
        TopNPhase::Select {
            generation_id,
            progress: SelectionProgress::initial(machine.offset, machine.limit),
            after_drain,
        }
    })
}

fn load_topn_storage(
    transaction: &mut StepContext<'_, '_>,
    stage: &DataflowStage,
    spec: &TopNSpec,
) -> Result<TopNStorage, String> {
    let input_stream = transaction.input(0)?.stream_id;
    let output_stream = transaction.output()?.stream_id;
    let input_payload = transaction.payload_storage(input_stream)?;
    let output_payload = transaction.payload_storage(output_stream)?;
    let storage = TopNStorage {
        input: transaction.state_storage(0)?,
        candidate: transaction.state_storage(1)?,
        visible: transaction.state_storage(2)?,
        control: transaction.state_storage(3)?,
        continuation: transaction.continuation_storage()?,
        input_payload: input_payload.relation,
        output_payload: output_payload.relation,
        input_type: input_payload.row_type,
        output_type: output_payload.row_type,
    };
    validate_topn_storage(transaction, &storage, stage, spec)?;
    Ok(storage)
}

fn validate_topn_storage(
    transaction: &mut StepContext<'_, '_>,
    storage: &TopNStorage,
    stage: &DataflowStage,
    spec: &TopNSpec,
) -> Result<(), String> {
    let input = transaction.relation_attributes(storage.input.oid())?;
    let expected_input_len = 4usize
        .checked_add(spec.order_by.len())
        .ok_or_else(|| "TopN input ABI is too wide".to_string())?;
    if input.len() != expected_input_len
        || !attribute_is(&input[0], "entry_id", pg_sys::INT8OID, true)
        || !attribute_is(&input[1], "row_key", pg_sys::BYTEAOID, true)
        || input[2].name != "row_value"
        || input[2].type_oid != storage.input_type.oid()
        || !input[2].not_null
        || !attribute_is(&input[3], "multiplicity", pg_sys::NUMERICOID, true)
    {
        return Err("TopN input relation has an invalid ABI".into());
    }
    for (ordinal, (attribute, order)) in input[4..].iter().zip(&spec.order_by).enumerate() {
        if attribute.name != format!("key_{}", ordinal + 1)
            || !attribute_matches_slot(attribute, &order.type_)
        {
            return Err("TopN ordering column changed its typed ABI".into());
        }
    }

    let candidate = transaction.relation_attributes(storage.candidate.oid())?;
    if candidate.len() != 5
        || !attribute_is(&candidate[0], "candidate_id", pg_sys::INT8OID, true)
        || !attribute_is(&candidate[1], "generation_id", pg_sys::INT8OID, true)
        || !attribute_is(&candidate[2], "output_key", pg_sys::BYTEAOID, true)
        || candidate[3].name != "output_row"
        || candidate[3].type_oid != storage.output_type.oid()
        || !candidate[3].not_null
        || !attribute_is(&candidate[4], "multiplicity", pg_sys::NUMERICOID, true)
    {
        return Err("TopN candidate relation has an invalid ABI".into());
    }
    let visible = transaction.relation_attributes(storage.visible.oid())?;
    if visible.len() != 4
        || !attribute_is(&visible[0], "visible_id", pg_sys::INT8OID, true)
        || !attribute_is(&visible[1], "output_key", pg_sys::BYTEAOID, true)
        || visible[2].name != "output_row"
        || visible[2].type_oid != storage.output_type.oid()
        || !visible[2].not_null
        || !attribute_is(&visible[3], "multiplicity", pg_sys::NUMERICOID, true)
    {
        return Err("TopN visible relation has an invalid ABI".into());
    }
    let control = transaction.relation_attributes(storage.control.oid())?;
    if control.len() != 3
        || !attribute_is(&control[0], "singleton", pg_sys::BOOLOID, true)
        || !attribute_is(&control[1], "dirty", pg_sys::BOOLOID, true)
        || !attribute_is(&control[2], "causal_lsn", pg_sys::PG_LSNOID, false)
    {
        return Err("TopN control relation has an invalid ABI".into());
    }
    validate_topn_continuation_abi(transaction, &storage.continuation)?;

    let output = transaction.composite_attributes(&storage.output_type)?;
    validate_output_attributes(&output, &stage.schema.outputs)?;
    Ok(())
}

fn validate_topn_control_state(
    transaction: &mut StepContext<'_, '_>,
    storage: &TopNStorage,
) -> Result<(), String> {
    let rows = transaction.read(
        &format!(
            "SELECT dirty,causal_lsn IS NOT NULL FROM {} WHERE singleton",
            storage.control.sql()
        ),
        &[],
    )?;
    if rows.len() != 1 {
        return Err("TopN control state has no singleton row".into());
    }
    let row = rows.first();
    let dirty: bool = required(&row, 1, "TopN dirty state")?;
    let has_lsn: bool = required(&row, 2, "TopN causal LSN presence")?;
    if dirty != has_lsn || (dirty && transaction.admission_progress().is_empty()) {
        return Err("TopN dirty state disagrees with its admission checkpoint".into());
    }
    Ok(())
}

fn topn_is_dirty(
    transaction: &mut StepContext<'_, '_>,
    storage: &TopNStorage,
) -> Result<bool, String> {
    let rows = transaction.read(
        &format!(
            "SELECT dirty FROM {} WHERE singleton",
            storage.control.sql()
        ),
        &[],
    )?;
    if rows.len() != 1 {
        return Err("TopN control state has no singleton row".into());
    }
    required(&rows.first(), 1, "TopN dirty state")
}

fn validate_topn_continuation_abi(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
) -> Result<(), String> {
    validate_typed_continuation_abi(transaction, relation, CONTINUATION_COLUMNS, "TopN")
}

fn compile_topn_expressions(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &TopNSpec,
    input_type: &TypeRef,
    output_type: &TypeRef,
) -> Result<TopNExpressions, String> {
    let bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: input_type,
            alias: "input_row",
        }],
    )?;
    let key_expressions = spec
        .order_by
        .iter()
        .map(|key| compile_scalar_expression(&key.expr, &bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let key_columns = (1..=spec.order_by.len())
        .map(|ordinal| format!("key_{ordinal}"))
        .collect::<Vec<_>>();
    let output_expressions =
        compile_named_outputs(&stage.schema.outputs, &spec.outputs, &bindings, "TopN")?.join(", ");
    let output_attributes = transaction.composite_attributes(output_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;

    let mut resolved = Vec::with_capacity(spec.order_by.len());
    for key in &spec.order_by {
        resolved.push(resolve_btree_step(transaction, key, "TopN")?);
    }
    let order_by = resolved
        .iter()
        .enumerate()
        .map(|(index, order)| {
            format!(
                "input_row.key_{} USING {} NULLS {}",
                index + 1,
                order.sort_operator,
                if order.nulls_first { "FIRST" } else { "LAST" }
            )
        })
        .chain(std::iter::once("input_row.entry_id ASC".into()))
        .collect::<Vec<_>>()
        .join(", ");
    let keyset_after = keyset_after_sql(&resolved);
    let keys_equal = keys_equal_sql(&resolved, "input_row", "tie_boundary");
    Ok(TopNExpressions {
        key_expressions,
        key_columns,
        output_expressions,
        order_by,
        keyset_after,
        keys_equal,
    })
}

fn keyset_after_sql(orders: &[BtreeOrder]) -> String {
    let mut alternatives = Vec::with_capacity(orders.len() + 1);
    let mut equal_prefix = Vec::new();
    for (index, order) in orders.iter().enumerate() {
        let column = format!("key_{}", index + 1);
        let before = format!("boundary.{column}");
        let current = format!("input_row.{column}");
        let after = if order.nulls_first {
            format!(
                "(CASE WHEN {before} IS NULL THEN {current} IS NOT NULL \
                 WHEN {current} IS NULL THEN FALSE \
                 ELSE {before} {} {current} END)",
                order.sort_operator
            )
        } else {
            format!(
                "(CASE WHEN {before} IS NULL THEN FALSE \
                 WHEN {current} IS NULL THEN TRUE \
                 ELSE {before} {} {current} END)",
                order.sort_operator
            )
        };
        alternatives.push(if equal_prefix.is_empty() {
            after
        } else {
            format!("({} AND {after})", equal_prefix.join(" AND "))
        });
        equal_prefix.push(format!(
            "(({before} IS NULL AND {current} IS NULL) OR \
             ({before} IS NOT NULL AND {current} IS NOT NULL \
              AND {before} {} {current}))",
            order.equality_operator
        ));
    }
    let id_after = "input_row.entry_id > boundary.entry_id";
    alternatives.push(if equal_prefix.is_empty() {
        id_after.into()
    } else {
        format!("({} AND {id_after})", equal_prefix.join(" AND "))
    });
    alternatives.join(" OR ")
}

fn keys_equal_sql(orders: &[BtreeOrder], left: &str, right: &str) -> String {
    if orders.is_empty() {
        return "TRUE".into();
    }
    orders
        .iter()
        .enumerate()
        .map(|(index, order)| {
            let column = format!("key_{}", index + 1);
            format!(
                "(({left}.{column} IS NULL AND {right}.{column} IS NULL) OR \
                 ({left}.{column} IS NOT NULL AND {right}.{column} IS NOT NULL \
                  AND {left}.{column} {} {right}.{column}))",
                order.equality_operator
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[derive(Clone, Debug)]
pub(super) struct TopNFields {
    pub(super) phase: i16,
    pub(super) input_stream_id: i64,
    pub(super) input_chunk_seq: Option<i64>,
    pub(super) input_row_ordinal: Option<i64>,
    pub(super) generation_id: Option<i64>,
    pub(super) cursor_row_id: Option<i64>,
    pub(super) cursor_repeat: bool,
    pub(super) offset_remaining: Option<String>,
    pub(super) limit_remaining: Option<String>,
    pub(super) tie_boundary_row_id: Option<i64>,
    pub(super) diff_leg: Option<i16>,
    after_kind: Option<i16>,
    after_chunk_seq: Option<i64>,
    after_row_ordinal: Option<i64>,
}

fn load_topn_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
) -> Result<Option<DurableTopN>, String> {
    let query = format!(
        r#"
        SELECT phase,input_stream_id,input_chunk_seq,input_row_ordinal,
               generation_id,cursor_row_id,cursor_repeat,offset_remaining::text,
               limit_remaining::text,tie_boundary_row_id,diff_leg,
               after_kind,after_chunk_seq,after_row_ordinal
        FROM {}
        WHERE singleton
        FOR UPDATE
        "#,
        relation.sql()
    );
    let rows = transaction.lock(&query, &[])?;
    match rows.len() {
        0 => Ok(None),
        1 => {
            let row = rows.first();
            let fields = TopNFields {
                phase: required(&row, 1, "TopN phase")?,
                input_stream_id: required(&row, 2, "TopN input stream")?,
                input_chunk_seq: row.get(3).map_err(|error| error.to_string())?,
                input_row_ordinal: row.get(4).map_err(|error| error.to_string())?,
                generation_id: row.get(5).map_err(|error| error.to_string())?,
                cursor_row_id: row.get(6).map_err(|error| error.to_string())?,
                cursor_repeat: required(&row, 7, "TopN cursor repeat")?,
                offset_remaining: row.get(8).map_err(|error| error.to_string())?,
                limit_remaining: row.get(9).map_err(|error| error.to_string())?,
                tie_boundary_row_id: row.get(10).map_err(|error| error.to_string())?,
                diff_leg: row.get(11).map_err(|error| error.to_string())?,
                after_kind: row.get(12).map_err(|error| error.to_string())?,
                after_chunk_seq: row.get(13).map_err(|error| error.to_string())?,
                after_row_ordinal: row.get(14).map_err(|error| error.to_string())?,
            };
            Ok(Some(DurableTopN {
                continuation: decode_topn_fields(fields)?,
                persisted: true,
            }))
        }
        count => Err(format!("TopN continuation relation contains {count} rows")),
    }
}

pub(super) fn decode_topn_fields(fields: TopNFields) -> Result<TopNContinuation, String> {
    let kind = TopNPhaseKind::from_code(PhaseCode::active(fields.phase)?)?;
    let after = || {
        decode_after_drain(
            fields.input_stream_id,
            fields.after_kind,
            fields.after_chunk_seq,
            fields.after_row_ordinal,
        )
    };
    let input = match (kind, fields.input_chunk_seq, fields.input_row_ordinal) {
        (TopNPhaseKind::Admit | TopNPhaseKind::Frontier, Some(chunk), Some(row)) => {
            Some(InputPosition::new(fields.input_stream_id, chunk, row)?)
        }
        (TopNPhaseKind::Select | TopNPhaseKind::Diff | TopNPhaseKind::Cleanup, None, None) => None,
        _ => return Err("TopN continuation has an invalid input cursor shape".into()),
    };
    let cursor = TopNCursor {
        row_id: fields.cursor_row_id,
    };
    let phase = match kind {
        TopNPhaseKind::Admit => {
            require_topn_nulls(&fields, &[4, 5, 6, 7, 8, 9, 10, 11, 12, 13])?;
            TopNPhase::Admit
        }
        TopNPhaseKind::Select => {
            if fields.diff_leg.is_some() || fields.cursor_repeat {
                return Err("TopN Select continuation contains Diff state".into());
            }
            TopNPhase::Select {
                generation_id: fields
                    .generation_id
                    .ok_or_else(|| "TopN Select omitted its generation".to_string())?,
                progress: SelectionProgress {
                    cursor,
                    offset_remaining: parse_u64_numeric(
                        fields.offset_remaining.as_deref(),
                        "TopN OFFSET",
                    )?,
                    limit_remaining: parse_u64_numeric(
                        fields.limit_remaining.as_deref(),
                        "TopN LIMIT",
                    )?,
                    tie_boundary_row_id: fields.tie_boundary_row_id,
                },
                after_drain: after()?,
            }
        }
        TopNPhaseKind::Diff => {
            if fields.offset_remaining.is_some()
                || fields.limit_remaining.is_some()
                || fields.tie_boundary_row_id.is_some()
            {
                return Err("TopN Diff continuation contains selection state".into());
            }
            TopNPhase::Diff {
                generation_id: fields
                    .generation_id
                    .ok_or_else(|| "TopN Diff omitted its generation".to_string())?,
                leg: match fields.diff_leg {
                    Some(1) => DiffLeg::Remove,
                    Some(2) => DiffLeg::Add,
                    _ => return Err("TopN Diff continuation has an invalid leg".into()),
                },
                cursor: TopNDiffCursor {
                    row_id: fields.cursor_row_id,
                    repeat: fields.cursor_repeat,
                },
                after_drain: after()?,
            }
        }
        TopNPhaseKind::Cleanup => {
            if fields.offset_remaining.is_some()
                || fields.limit_remaining.is_some()
                || fields.tie_boundary_row_id.is_some()
                || fields.diff_leg.is_some()
                || fields.cursor_repeat
            {
                return Err("TopN Cleanup continuation contains another phase's state".into());
            }
            TopNPhase::Cleanup {
                generation_id: fields
                    .generation_id
                    .ok_or_else(|| "TopN Cleanup omitted its generation".to_string())?,
                cursor,
                after_drain: after()?,
            }
        }
        TopNPhaseKind::Frontier => {
            require_topn_nulls(&fields, &[4, 5, 6, 7, 8, 9, 10, 11, 12, 13])?;
            TopNPhase::Frontier
        }
    };
    Ok(TopNContinuation {
        input_stream_id: fields.input_stream_id,
        input,
        phase,
    })
}

fn require_topn_nulls(fields: &TopNFields, ordinals: &[usize]) -> Result<(), String> {
    let present = |ordinal| match ordinal {
        4 => fields.generation_id.is_some(),
        5 => fields.cursor_row_id.is_some(),
        6 => fields.cursor_repeat,
        7 => fields.offset_remaining.is_some(),
        8 => fields.limit_remaining.is_some(),
        9 => fields.tie_boundary_row_id.is_some(),
        10 => fields.diff_leg.is_some(),
        11 => fields.after_kind.is_some(),
        12 => fields.after_chunk_seq.is_some(),
        13 => fields.after_row_ordinal.is_some(),
        _ => true,
    };
    if ordinals.iter().copied().any(present) {
        return Err("TopN continuation contains fields from another phase".into());
    }
    Ok(())
}

fn decode_after_drain(
    input_stream_id: i64,
    kind: Option<i16>,
    chunk_seq: Option<i64>,
    row_ordinal: Option<i64>,
) -> Result<AfterDrain, String> {
    match (kind, chunk_seq, row_ordinal) {
        (Some(1), Some(chunk), Some(row)) => Ok(AfterDrain::Admit(InputPosition::new(
            input_stream_id,
            chunk,
            row,
        )?)),
        (Some(2), None, None) => Ok(AfterDrain::FinishInput),
        (Some(3), Some(chunk), Some(row)) => Ok(AfterDrain::Frontier(InputPosition::new(
            input_stream_id,
            chunk,
            row,
        )?)),
        _ => Err("TopN continuation has an invalid Drain target".into()),
    }
}

pub(super) fn encode_topn_fields(continuation: TopNContinuation) -> TopNFields {
    let mut fields = TopNFields {
        phase: continuation.phase.code().value(),
        input_stream_id: continuation.input_stream_id,
        input_chunk_seq: continuation.input.map(|input| input.chunk_seq),
        input_row_ordinal: continuation.input.map(|input| input.row_ordinal),
        generation_id: None,
        cursor_row_id: None,
        cursor_repeat: false,
        offset_remaining: None,
        limit_remaining: None,
        tie_boundary_row_id: None,
        diff_leg: None,
        after_kind: None,
        after_chunk_seq: None,
        after_row_ordinal: None,
    };
    match continuation.phase {
        TopNPhase::Admit | TopNPhase::Frontier => {}
        TopNPhase::Select {
            generation_id,
            progress,
            after_drain,
        } => {
            fields.generation_id = Some(generation_id);
            fields.cursor_row_id = progress.cursor.row_id;
            fields.offset_remaining = Some(progress.offset_remaining.to_string());
            fields.limit_remaining = Some(progress.limit_remaining.to_string());
            fields.tie_boundary_row_id = progress.tie_boundary_row_id;
            encode_after(&mut fields, after_drain);
        }
        TopNPhase::Diff {
            generation_id,
            leg,
            cursor,
            after_drain,
        } => {
            fields.generation_id = Some(generation_id);
            fields.cursor_row_id = cursor.row_id;
            fields.cursor_repeat = cursor.repeat;
            fields.diff_leg = Some(match leg {
                DiffLeg::Remove => 1,
                DiffLeg::Add => 2,
            });
            encode_after(&mut fields, after_drain);
        }
        TopNPhase::Cleanup {
            generation_id,
            cursor,
            after_drain,
        } => {
            fields.generation_id = Some(generation_id);
            fields.cursor_row_id = cursor.row_id;
            encode_after(&mut fields, after_drain);
        }
    }
    fields
}

fn encode_after(fields: &mut TopNFields, after: AfterDrain) {
    match after {
        AfterDrain::Admit(input) => {
            fields.after_kind = Some(1);
            fields.after_chunk_seq = Some(input.chunk_seq);
            fields.after_row_ordinal = Some(input.row_ordinal);
        }
        AfterDrain::FinishInput => fields.after_kind = Some(2),
        AfterDrain::Frontier(input) => {
            fields.after_kind = Some(3);
            fields.after_chunk_seq = Some(input.chunk_seq);
            fields.after_row_ordinal = Some(input.row_ordinal);
        }
    }
}

fn replace_topn_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    old: Option<TopNContinuation>,
    next: Option<TopNContinuation>,
) -> Result<(), String> {
    let old_fields = old.map(encode_topn_fields);
    let next_fields = next.map(encode_topn_fields);
    let old_arguments = old_fields.as_ref().map(topn_field_arguments);
    let next_arguments = next_fields.as_ref().map(topn_field_arguments);
    replace_continuation_cas(
        transaction,
        relation,
        CONTINUATION_COLUMNS,
        old_arguments.as_ref().map(|arguments| &arguments[..]),
        next_arguments.as_ref().map(|arguments| &arguments[..]),
        "TopN",
    )
}

fn topn_field_arguments<'a>(fields: &'a TopNFields) -> [DatumWithOid<'a>; 14] {
    unsafe {
        [
            DatumWithOid::new(fields.phase, pg_sys::INT2OID),
            DatumWithOid::new(fields.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(fields.generation_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.cursor_row_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.cursor_repeat, pg_sys::BOOLOID),
            DatumWithOid::new(fields.offset_remaining.as_deref(), pg_sys::TEXTOID),
            DatumWithOid::new(fields.limit_remaining.as_deref(), pg_sys::TEXTOID),
            DatumWithOid::new(fields.tie_boundary_row_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.diff_leg, pg_sys::INT2OID),
            DatumWithOid::new(fields.after_kind, pg_sys::INT2OID),
            DatumWithOid::new(fields.after_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.after_row_ordinal, pg_sys::INT8OID),
        ]
    }
}

fn parse_u64_numeric(value: Option<&str>, label: &str) -> Result<u64, String> {
    value
        .ok_or_else(|| format!("{label} continuation value is NULL"))?
        .parse()
        .map_err(|_| format!("{label} continuation value is not an unsigned integer"))
}

fn i64_from_usize(value: usize, name: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{name} exceeds bigint"))
}

fn attribute_is(
    attribute: &AttributeRef,
    name: &str,
    type_oid: pg_sys::Oid,
    not_null: bool,
) -> bool {
    attribute.name == name && attribute.type_oid == type_oid && attribute.not_null == not_null
}

// Atomic bounded TopN admission primitive: apply one input prefix to the
// dynamic ordered state and return its durable mutation summary.
fn run_topn_admission(
    transaction: &mut StepContext<'_, '_>,
    storage: &TopNStorage,
    expressions: &TopNExpressions,
    input: InputPosition,
) -> Result<TopNAdmission, String> {
    let input_state = transaction.input(0)?.clone();
    let input_chunk = chunk(transaction, &input_state, input.chunk_seq)?
        .ok_or_else(|| "TopN admission references a missing input chunk".to_string())?;
    if input_chunk.kind != ChunkKind::Data || input_chunk.stream_id != input.stream_id {
        return Err("TopN admission does not reference a data chunk".into());
    }
    if input.row_ordinal == 0 {
        payload_facts(transaction, &storage.input_payload, &input_chunk)?;
    }
    let chunk_rows =
        i64::try_from(input_chunk.rows).map_err(|_| "TopN chunk row count exceeds bigint")?;
    if input.row_ordinal >= chunk_rows {
        return Err("TopN admission cursor is outside its data chunk".into());
    }
    let budget = transaction.budget();
    let max_rows = i64_from_usize(budget.max_input_rows, "TopN input row budget")?;
    let max_bytes = i64_from_usize(budget.max_input_bytes, "TopN input byte budget")?;
    let key_select = expressions
        .key_expressions
        .iter()
        .zip(&expressions.key_columns)
        .map(|(expression, column)| format!("{expression} AS {}", quote_identifier(column)))
        .collect::<Vec<_>>()
        .join(",");
    let key_select = if key_select.is_empty() {
        String::new()
    } else {
        format!(",{key_select}")
    };
    let key_columns = expressions
        .key_columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>();
    let insert_keys = if key_columns.is_empty() {
        String::new()
    } else {
        format!(",{}", key_columns.join(","))
    };
    let representative_keys = if key_columns.is_empty() {
        String::new()
    } else {
        format!(
            ",{}",
            key_columns
                .iter()
                .map(|column| format!("representative.{column}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let decision_keys = if key_columns.is_empty() {
        String::new()
    } else {
        format!(
            ",{}",
            key_columns
                .iter()
                .map(|column| format!("decision.{column}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let update_keys = key_columns
        .iter()
        .map(|column| format!("{column}=EXCLUDED.{column}"))
        .collect::<Vec<_>>();
    let update_keys = if update_keys.is_empty() {
        String::new()
    } else {
        format!(",{}", update_keys.join(","))
    };
    let row_key = canonical_row_key_sql("input_row.row_value", &storage.input_type);
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          SELECT input_row.row_ordinal,input_row.weight,input_row.row_value,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {payload} AS input_row
          WHERE input_row.stream_id=$1 AND input_row.chunk_seq=$2
            AND input_row.row_ordinal >= $3
          ORDER BY input_row.row_ordinal
          LIMIT $4
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY row_ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_ordinal) AS running_bytes
          FROM source
        ),
        bounded AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal=1 OR running_bytes <= $5
        ),
        evaluated AS MATERIALIZED (
          SELECT input_row.*,
                 {row_key} AS row_key
                 {key_select}
          FROM bounded AS input_row
        ),
        prefixes AS MATERIALIZED (
          SELECT evaluated.*,
                 sum(weight::numeric) OVER (
                   PARTITION BY row_key ORDER BY row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS key_prefix
          FROM evaluated
        ),
        collapsed AS MATERIALIZED (
          SELECT row_key,min(row_ordinal) AS representative_ordinal,
                 sum(weight::numeric) AS net_weight,
                 min(key_prefix) AS min_prefix
          FROM prefixes
          GROUP BY row_key
        ),
        representative AS MATERIALIZED (
          SELECT evaluated.*
          FROM evaluated
          JOIN collapsed
            ON collapsed.row_key=evaluated.row_key
           AND collapsed.representative_ordinal=evaluated.row_ordinal
        ),
        existing AS MATERIALIZED (
          SELECT state.entry_id,state.row_key,state.multiplicity
          FROM {state} AS state
          JOIN collapsed USING(row_key)
          FOR UPDATE OF state
        ),
        decision AS MATERIALIZED (
          SELECT collapsed.*,representative.row_value
                 {representative_keys},
                 existing.entry_id,
                 coalesce(existing.multiplicity,0::numeric) AS old_multiplicity,
                 coalesce(existing.multiplicity,0::numeric)+collapsed.net_weight
                   AS new_multiplicity,
                 coalesce(existing.multiplicity,0::numeric)+collapsed.min_prefix
                   AS minimum_multiplicity
          FROM collapsed
          JOIN representative USING(row_key)
          LEFT JOIN existing USING(row_key)
        ),
        status AS MATERIALIZED (
          SELECT CASE
                   WHEN EXISTS(
                     SELECT 1 FROM decision WHERE minimum_multiplicity < 0
                   ) THEN 'negative'
                   WHEN EXISTS(SELECT 1 FROM {candidate}) THEN 'dirty_candidate'
                   ELSE 'ok'
                 END AS value
        ),
        removed AS (
          DELETE FROM {state} AS state
          USING decision,status
          WHERE status.value='ok'
            AND decision.new_multiplicity=0
            AND state.entry_id=decision.entry_id
          RETURNING 1
        ),
        changed AS (
          INSERT INTO {state}(row_key,row_value,multiplicity{insert_keys})
          SELECT decision.row_key,decision.row_value,decision.new_multiplicity
                 {decision_keys}
          FROM decision,status
          WHERE status.value='ok' AND decision.new_multiplicity > 0
          ON CONFLICT(row_key) DO UPDATE
          SET row_value=EXCLUDED.row_value,
              multiplicity=EXCLUDED.multiplicity
              {update_keys}
          RETURNING 1
        ),
        control_changed AS (
          UPDATE {control} AS control
          SET dirty=true,
              causal_lsn=CASE
                WHEN control.causal_lsn IS NULL THEN $6::pg_lsn
                ELSE greatest(control.causal_lsn,$6::pg_lsn)
              END
          FROM status
          WHERE control.singleton AND status.value='ok'
          RETURNING 1
        )
        SELECT (SELECT value FROM status),
               count(*)::bigint,
               min(row_ordinal)::bigint,
               max(row_ordinal)::bigint,
               coalesce(sum(row_bytes),0)::bigint,
               (SELECT count(*)::bigint FROM removed)
                 +(SELECT count(*)::bigint FROM changed)
                 +(SELECT count(*)::bigint FROM control_changed)
        FROM bounded
        "#,
        payload = storage.input_payload.sql(),
        state = storage.input.sql(),
        candidate = storage.candidate.sql(),
        control = storage.control.sql(),
        row_key = row_key,
    );
    let causal_lsn = format_lsn(input_chunk.lsn);
    let arguments = unsafe {
        [
            DatumWithOid::new(input.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(input.chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(input.row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(causal_lsn.as_str(), pg_sys::TEXTOID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("TopN admission returned no summary".into());
    }
    let row = rows.first();
    let status = required::<String>(&row, 1, "TopN admission status")?;
    if status != "ok" {
        return Err(format!("TopN admission returned {status}"));
    }
    let processed = nonnegative(
        required(&row, 2, "TopN admitted rows")?,
        "TopN admitted rows",
    )?;
    let first = required::<i64>(&row, 3, "TopN first admitted row")?;
    let last = required::<i64>(&row, 4, "TopN last admitted row")?;
    let input_bytes = nonnegative(
        required(&row, 5, "TopN admitted bytes")?,
        "TopN admitted bytes",
    )?;
    let touched = nonnegative(
        required(&row, 6, "TopN touched state rows")?,
        "TopN touched state rows",
    )?;
    if processed == 0
        || first != input.row_ordinal
        || last
            != input
                .row_ordinal
                .checked_add(i64::try_from(processed).map_err(|_| "TopN page exceeds bigint")?)
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| "TopN input ordinal overflow".to_string())?
    {
        return Err("TopN admission returned inconsistent row facts".into());
    }
    let next_row = last
        .checked_add(1)
        .ok_or_else(|| "TopN input ordinal exhausted".to_string())?;
    let usage = WorkUsage {
        input_rows: processed,
        input_bytes,
        ..WorkUsage::default()
    };
    let drain_reached = transaction.record_admission(usage)?;
    let target = if next_row < chunk_rows {
        let next = InputPosition::new(input.stream_id, input.chunk_seq, next_row)?;
        if drain_reached {
            TopNAdmissionTarget::Drain {
                generation_id: next_generation_id(transaction)?,
                after_drain: AfterDrain::Admit(next),
            }
        } else {
            TopNAdmissionTarget::Continue(next)
        }
    } else if next_row == chunk_rows {
        advance_input(
            transaction,
            0,
            input_chunk.sequence + 1,
            input_state.consumed_frontier_lsn,
            WorkUsage {
                input_rows: input_chunk.rows,
                input_bytes: input_chunk.bytes,
                ..WorkUsage::default()
            },
        )?;
        match chunk(transaction, &input_state, input_chunk.sequence + 1)? {
            Some(next) if next.kind == ChunkKind::Frontier => TopNAdmissionTarget::Drain {
                generation_id: next_generation_id(transaction)?,
                after_drain: AfterDrain::Frontier(InputPosition::new(
                    next.stream_id,
                    next.sequence,
                    0,
                )?),
            },
            _ if drain_reached => TopNAdmissionTarget::Drain {
                generation_id: next_generation_id(transaction)?,
                after_drain: AfterDrain::FinishInput,
            },
            _ => TopNAdmissionTarget::Idle,
        }
    } else {
        return Err("TopN admission advanced beyond its input chunk".into());
    };
    Ok(TopNAdmission {
        facts: PrimitiveFacts {
            usage,
            state_rows: touched,
            output: OutputFacts::None,
        },
        target,
    })
}

fn next_generation_id(transaction: &mut StepContext<'_, '_>) -> Result<i64, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(transaction.result_oid(), pg_sys::OIDOID),
            DatumWithOid::new(transaction.stage_id(), pg_sys::INT4OID),
        ]
    };
    let rows = transaction.read(
        r#"
        SELECT checkpoint.revision + 1
        FROM shiba_internal.operator_checkpoints AS checkpoint
        WHERE checkpoint.result_oid=$1 AND checkpoint.stage_id=$2
        "#,
        &arguments,
    )?;
    if rows.len() != 1 {
        return Err("TopN checkpoint generation is missing".into());
    }
    let generation = required(&rows.first(), 1, "TopN generation")?;
    validate_generation_id(generation)?;
    Ok(generation)
}

// Atomic bounded TopN selection primitive: advance one deterministic ranked
// page, keeping sort/tie semantics local to the operator.
fn run_topn_selection(
    transaction: &mut StepContext<'_, '_>,
    storage: &TopNStorage,
    expressions: &TopNExpressions,
    spec: &TopNSpec,
    generation_id: i64,
    progress: SelectionProgress,
) -> Result<TopNSelection, String> {
    let budget = transaction.budget();
    let max_rows = i64_from_usize(budget.max_input_rows, "TopN selection row budget")?;
    let max_bytes = i64_from_usize(budget.max_input_bytes, "TopN selection byte budget")?;
    let offset = progress.offset_remaining.to_string();
    let limit = progress.limit_remaining.to_string();
    let output_key = canonical_row_key_sql("selected_rows.output_row", &storage.output_type);
    let query = format!(
        r#"
        WITH cursor_boundary AS MATERIALIZED (
          SELECT * FROM {state} WHERE entry_id=$2
        ),
        source AS MATERIALIZED (
          SELECT input_row.*,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {state} AS input_row
          WHERE input_row.multiplicity > 0
            AND (
              $2 IS NULL OR EXISTS(
                SELECT 1
                FROM cursor_boundary AS boundary
                WHERE {keyset_after}
              )
            )
          ORDER BY {order_by}
          LIMIT $7
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY {source_order}) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY {source_order}) AS running_bytes
          FROM source
        ),
        bounded AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal=1 OR running_bytes <= $8
        ),
        offset_prefix AS MATERIALIZED (
          SELECT bounded.*,
                 coalesce(
                   sum(multiplicity) OVER (
                     ORDER BY page_ordinal ROWS BETWEEN UNBOUNDED PRECEDING
                       AND 1 PRECEDING
                   ),
                   0::numeric
                 ) AS multiplicity_before
          FROM bounded
        ),
        offsetted AS MATERIALIZED (
          SELECT offset_prefix.*,
                 greatest(
                   multiplicity
                     - least(
                         multiplicity,
                         greatest($3::numeric-multiplicity_before,0::numeric)
                       ),
                   0::numeric
                 ) AS available
          FROM offset_prefix
        ),
        limit_prefix AS MATERIALIZED (
          SELECT offsetted.*,
                 coalesce(
                   sum(available) OVER (
                     ORDER BY page_ordinal ROWS BETWEEN UNBOUNDED PRECEDING
                       AND 1 PRECEDING
                   ),
                   0::numeric
                 ) AS available_before
          FROM offsetted
        ),
        limited AS MATERIALIZED (
          SELECT limit_prefix.*,
                 greatest(
                   least(
                     available,
                     greatest($4::numeric-available_before,0::numeric)
                   ),
                   0::numeric
                 ) AS base_take
          FROM limit_prefix
        ),
        new_boundary AS MATERIALIZED (
          SELECT entry_id,page_ordinal
          FROM limited
          WHERE $6::boolean
            AND $4::numeric > 0
            AND available > 0
            AND available_before < $4::numeric
            AND available_before + available >= $4::numeric
          ORDER BY page_ordinal
          LIMIT 1
        ),
        boundary_choice AS MATERIALIZED (
          SELECT $5::bigint AS entry_id,0::bigint AS page_ordinal,true AS persisted
          WHERE $5 IS NOT NULL
          UNION ALL
          SELECT new_boundary.entry_id,new_boundary.page_ordinal,false
          FROM new_boundary
          WHERE $5 IS NULL
        ),
        tie_boundary AS MATERIALIZED (
          SELECT state.*,boundary_choice.page_ordinal,boundary_choice.persisted
          FROM boundary_choice
          JOIN {state} AS state USING(entry_id)
        ),
        classified AS MATERIALIZED (
          SELECT input_row.*,
                 tie_boundary.entry_id AS boundary_entry_id,
                 tie_boundary.page_ordinal AS boundary_page_ordinal,
                 tie_boundary.persisted AS boundary_persisted,
                 CASE
                   WHEN tie_boundary.entry_id IS NULL THEN input_row.base_take
                   WHEN NOT $6::boolean THEN input_row.base_take
                   WHEN input_row.page_ordinal < tie_boundary.page_ordinal
                     THEN input_row.available
                   WHEN ({keys_equal}) THEN input_row.available
                   ELSE 0::numeric
                 END AS take_weight,
                 CASE
                   WHEN tie_boundary.entry_id IS NULL THEN false
                   ELSE ({keys_equal})
                 END AS tied
          FROM limited AS input_row
          LEFT JOIN tie_boundary ON true
        ),
        selected_rows AS MATERIALIZED (
          SELECT input_row.*,
                 ROW({outputs})::{output_type} AS output_row
          FROM classified AS input_row
          WHERE input_row.take_weight > 0
        ),
        keyed AS MATERIALIZED (
          SELECT selected_rows.*,
                 {output_key} AS output_key
          FROM selected_rows
        ),
        collapsed AS MATERIALIZED (
          SELECT output_key,min(page_ordinal) AS representative_ordinal,
                 sum(take_weight) AS multiplicity
          FROM keyed
          GROUP BY output_key
        ),
        candidate_rows AS MATERIALIZED (
          SELECT collapsed.output_key,keyed.output_row,collapsed.multiplicity
          FROM collapsed
          JOIN keyed
            ON keyed.output_key=collapsed.output_key
           AND keyed.page_ordinal=collapsed.representative_ordinal
        ),
        inserted AS (
          INSERT INTO {candidate} AS target(
            generation_id,output_key,output_row,multiplicity
          )
          SELECT $1,output_key,output_row,multiplicity
          FROM candidate_rows
          ON CONFLICT(generation_id,output_key) DO UPDATE
          SET output_row=EXCLUDED.output_row,
              multiplicity=target.multiplicity+EXCLUDED.multiplicity
          RETURNING 1
        ),
        last_processed AS MATERIALIZED (
          SELECT page.*
          FROM bounded AS page
          ORDER BY page.page_ordinal DESC
          LIMIT 1
        ),
        has_more AS MATERIALIZED (
          SELECT EXISTS(
            SELECT 1
            FROM {state} AS input_row
            JOIN last_processed AS boundary ON true
            WHERE input_row.multiplicity > 0 AND ({keyset_after})
          ) AS value
        ),
        summary AS MATERIALIZED (
          SELECT count(*)::bigint AS processed,
                 coalesce(sum(row_bytes),0)::bigint AS input_bytes,
                 (array_agg(entry_id ORDER BY page_ordinal DESC))[1] AS last_id,
                 greatest(
                   $3::numeric-coalesce(sum(multiplicity),0::numeric),
                   0::numeric
                 ) AS offset_remaining,
                 greatest(
                   $4::numeric-coalesce(sum(available),0::numeric),
                   0::numeric
                 ) AS limit_remaining,
                 (SELECT entry_id FROM boundary_choice) AS tie_boundary_row_id,
                 coalesce(
                   bool_or(
                     boundary_entry_id IS NOT NULL
                     AND page_ordinal > boundary_page_ordinal
                     AND NOT tied
                   ),
                   false
                 ) AS crossed_tie_boundary
          FROM classified
        )
        SELECT summary.processed,summary.input_bytes,summary.last_id,
               summary.offset_remaining::text,
               summary.limit_remaining::text,
               summary.tie_boundary_row_id,
               CASE
                 WHEN summary.processed=0 THEN true
                 WHEN NOT $6::boolean AND summary.limit_remaining=0 THEN true
                 WHEN $6::boolean
                      AND summary.tie_boundary_row_id IS NOT NULL
                      AND summary.crossed_tie_boundary THEN true
                 WHEN NOT coalesce((SELECT value FROM has_more),false) THEN true
                 ELSE false
               END,
               (SELECT count(*)::bigint FROM inserted)
        FROM summary
        "#,
        state = storage.input.sql(),
        candidate = storage.candidate.sql(),
        keyset_after = expressions.keyset_after,
        order_by = expressions.order_by,
        source_order = expressions.order_by.replace("input_row.", "source."),
        keys_equal = expressions.keys_equal,
        outputs = expressions.output_expressions,
        output_type = storage.output_type.sql(),
        output_key = output_key,
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(generation_id, pg_sys::INT8OID),
            DatumWithOid::new(progress.cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(offset.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(limit.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(progress.tie_boundary_row_id, pg_sys::INT8OID),
            DatumWithOid::new(spec.with_ties, pg_sys::BOOLOID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("TopN selection returned no summary".into());
    }
    let row = rows.first();
    let processed = nonnegative(
        required(&row, 1, "TopN selected input rows")?,
        "TopN selected input rows",
    )?;
    let input_bytes = nonnegative(
        required(&row, 2, "TopN selected input bytes")?,
        "TopN selected input bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let offset_remaining = parse_u64_numeric(
        Some(&required::<String>(&row, 4, "TopN remaining OFFSET")?),
        "TopN OFFSET",
    )?;
    let limit_remaining = parse_u64_numeric(
        Some(&required::<String>(&row, 5, "TopN remaining LIMIT")?),
        "TopN LIMIT",
    )?;
    let tie_boundary_row_id = row.get(6).map_err(|error| error.to_string())?;
    let complete: bool = required(&row, 7, "TopN selection completion")?;
    let changed = nonnegative(
        required(&row, 8, "TopN candidate rows")?,
        "TopN candidate rows",
    )?;
    if processed == 0 && (!complete || last_row_id.is_some()) {
        return Err("TopN empty selection page is not complete".into());
    }
    Ok(TopNSelection {
        page: TopNPage {
            facts: PrimitiveFacts {
                usage: WorkUsage {
                    input_rows: processed,
                    input_bytes,
                    ..WorkUsage::default()
                },
                state_rows: changed,
                output: OutputFacts::None,
            },
            last_row_id,
            complete,
        },
        progress: SelectionProgress {
            cursor: TopNCursor {
                row_id: last_row_id,
            },
            offset_remaining,
            limit_remaining,
            tie_boundary_row_id,
        },
    })
}

// Atomic bounded TopN diff primitive: reconcile one visible ranked page and
// write its payload/state delta; shared context publishes the data chunk.
fn run_topn_diff(
    transaction: &mut StepContext<'_, '_>,
    storage: &TopNStorage,
    generation_id: i64,
    leg: DiffLeg,
    cursor: TopNDiffCursor,
) -> Result<TopNDiffPage, String> {
    cursor.validate()?;
    let causal_rows = transaction.read(
        &format!(
            "SELECT causal_lsn::text FROM {} \
             WHERE singleton AND dirty AND causal_lsn IS NOT NULL",
            storage.control.sql()
        ),
        &[],
    )?;
    if causal_rows.len() != 1 {
        return Err("TopN dirty state has no unique causal LSN".into());
    }
    let lsn: String = required(&causal_rows.first(), 1, "TopN causal LSN")?;
    let output = transaction.output()?.clone();
    let budget = transaction.budget();
    let max_rows = i64::min(
        i64::min(
            i64_from_usize(budget.max_input_rows, "TopN diff input row budget")?,
            i64_from_usize(budget.max_output_rows, "TopN diff output row budget")?,
        ),
        output.target_rows,
    );
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "TopN diff row budget overflow".to_string())?;
    let max_bytes = i64::min(
        i64::min(
            i64_from_usize(budget.max_input_bytes, "TopN diff input byte budget")?,
            i64_from_usize(budget.max_output_bytes, "TopN diff output byte budget")?,
        ),
        output.target_bytes,
    );
    let cursor_predicate = |identity: &str| {
        if cursor.repeat {
            format!("{identity}>=$2")
        } else if cursor.row_id.is_some() {
            format!("{identity}>$2")
        } else {
            "$2 IS NULL".into()
        }
    };
    let (source, compared, mutation, weight) = match leg {
        DiffLeg::Remove => (
            format!(
                r#"
                SELECT visible.visible_id AS row_id,visible.output_key,
                       visible.output_row,visible.multiplicity,
                       shiba_internal.effect_row_bytes(visible.output_row) AS row_bytes
                FROM {visible} AS visible
                WHERE {cursor_predicate}
                ORDER BY visible.visible_id
                LIMIT $5
                "#,
                visible = storage.visible.sql(),
                cursor_predicate = cursor_predicate("visible.visible_id"),
            ),
            format!(
                r#"
                SELECT bounded_prefix.*,
                       bounded_prefix.multiplicity
                         -coalesce(candidate.multiplicity,0::numeric) AS delta
                FROM bounded_prefix
                LEFT JOIN {candidate} AS candidate
                  ON candidate.generation_id=$1
                 AND candidate.output_key=bounded_prefix.output_key
                "#,
                candidate = storage.candidate.sql(),
            ),
            format!(
                r#"
                deleted AS (
                  DELETE FROM {visible} AS visible
                  USING differences
                  WHERE visible.visible_id=differences.row_id
                    AND visible.multiplicity=differences.slice
                  RETURNING 1
                ),
                changed AS (
                  UPDATE {visible} AS visible
                  SET multiplicity=visible.multiplicity-differences.slice
                  FROM differences
                  WHERE visible.visible_id=differences.row_id
                    AND visible.multiplicity>differences.slice
                  RETURNING 1
                )
                "#,
                visible = storage.visible.sql(),
            ),
            "-differences.slice",
        ),
        DiffLeg::Add => (
            format!(
                r#"
                SELECT candidate.candidate_id AS row_id,candidate.output_key,
                       candidate.output_row,candidate.multiplicity,
                       shiba_internal.effect_row_bytes(candidate.output_row) AS row_bytes
                FROM {candidate} AS candidate
                WHERE candidate.generation_id=$1
                  AND {cursor_predicate}
                ORDER BY candidate.candidate_id
                LIMIT $5
                "#,
                candidate = storage.candidate.sql(),
                cursor_predicate = cursor_predicate("candidate.candidate_id"),
            ),
            format!(
                r#"
                SELECT bounded_prefix.*,
                       bounded_prefix.multiplicity
                         -coalesce(visible.multiplicity,0::numeric) AS delta
                FROM bounded_prefix
                LEFT JOIN {visible} AS visible
                  ON visible.output_key=bounded_prefix.output_key
                "#,
                visible = storage.visible.sql(),
            ),
            format!(
                r#"
                changed AS (
                  INSERT INTO {visible} AS target(
                    output_key,output_row,multiplicity
                  )
                  SELECT output_key,output_row,slice::numeric
                  FROM differences
                  ON CONFLICT(output_key) DO UPDATE
                  SET output_row=EXCLUDED.output_row,
                      multiplicity=target.multiplicity+EXCLUDED.multiplicity
                  RETURNING 1
                ),
                deleted AS (
                  SELECT 1 WHERE false
                )
                "#,
                visible = storage.visible.sql(),
            ),
            "differences.slice",
        ),
    };
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          {source}
        ),
        numbered AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY row_id) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_id) AS running_bytes
          FROM source
        ),
        bounded_prefix AS MATERIALIZED (
          SELECT numbered.*
          FROM numbered
          WHERE page_ordinal<=$3
            AND (page_ordinal=1 OR running_bytes<=$4)
        ),
        joined AS MATERIALIZED (
          {compared}
        ),
        marked AS MATERIALIZED (
          SELECT joined.*,
                 min(
                   CASE WHEN delta > 9223372036854775807::numeric
                        THEN page_ordinal
                   END
                 ) OVER () AS first_huge_ordinal
          FROM joined
        ),
        compared AS MATERIALIZED (
          SELECT marked.*
          FROM marked
          WHERE first_huge_ordinal IS NULL
             OR page_ordinal<=first_huge_ordinal
        ),
        differences AS MATERIALIZED (
          SELECT compared.*,
                 least(delta,9223372036854775807::numeric)::bigint AS slice
          FROM compared
          WHERE delta>0
        ),
        stats AS MATERIALIZED (
          SELECT count(*)::bigint AS compared_rows,
                 coalesce(sum(row_bytes),0)::bigint AS compared_bytes,
                 (array_agg(row_id ORDER BY page_ordinal DESC))[1] AS last_id,
                 coalesce(bool_or(delta>9223372036854775807::numeric),false)
                   AS repeat_cursor,
                 (SELECT count(*)::bigint FROM differences) AS emitted_rows,
                 (SELECT coalesce(sum(row_bytes),0)::bigint FROM differences)
                   AS emitted_bytes
          FROM compared
        ),
        payload_insert AS (
          INSERT INTO {output_payload}(
            stream_id,chunk_seq,row_ordinal,weight,row_value
          )
          SELECT $7,$8,
                 row_number() OVER (ORDER BY differences.page_ordinal)-1,
                 {weight},differences.output_row
          FROM differences
          RETURNING 1
        ),
        {mutation}
        SELECT stats.compared_rows,stats.compared_bytes,stats.last_id,
               (SELECT count(*) FROM source)
                 =(SELECT count(*) FROM bounded_prefix)
                 AND (SELECT count(*) FROM bounded_prefix)=stats.compared_rows
                 AND NOT stats.repeat_cursor AS complete,
               stats.repeat_cursor,stats.emitted_rows,stats.emitted_bytes,
               (SELECT count(*)::bigint FROM payload_insert),
               (SELECT count(*)::bigint FROM changed)
                 +(SELECT count(*)::bigint FROM deleted)
        FROM stats
        "#,
        output_payload = storage.output_payload.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(generation_id, pg_sys::INT8OID),
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
            DatumWithOid::new(lsn.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(output.next_chunk_seq, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("TopN diff returned no summary".into());
    }
    let row = rows.first();
    let compared_rows = nonnegative(
        required(&row, 1, "TopN compared rows")?,
        "TopN compared rows",
    )?;
    let compared_bytes = nonnegative(
        required(&row, 2, "TopN compared bytes")?,
        "TopN compared bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let complete = required(&row, 4, "TopN diff completion")?;
    let repeat_cursor = required(&row, 5, "TopN residual cursor")?;
    let emitted = nonnegative(required(&row, 6, "TopN diff rows")?, "TopN diff rows")?;
    let emitted_bytes = nonnegative(required(&row, 7, "TopN diff bytes")?, "TopN diff bytes")?;
    let inserted = nonnegative(required(&row, 8, "TopN payload rows")?, "TopN payload rows")?;
    let mutated = nonnegative(
        required(&row, 9, "TopN visible mutations")?,
        "TopN visible mutations",
    )?;
    let output_facts = if emitted == 0 {
        if inserted != 0 || mutated != 0 {
            return Err("TopN appended or mutated an empty diff".into());
        }
        OutputFacts::None
    } else {
        if inserted != emitted || mutated != emitted {
            return Err("TopN diff append is inconsistent".into());
        }
        OutputFacts::Data {
            chunk_seq: output.next_chunk_seq,
        }
    };
    if let OutputFacts::Data { chunk_seq } = output_facts {
        transaction.record_output_append(
            OutputAppendTarget::New {
                sequence: chunk_seq,
            },
            emitted,
            emitted_bytes,
            parse_lsn(&lsn)?,
        )?;
    }
    Ok(TopNDiffPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: compared_rows,
                input_bytes: compared_bytes,
                output_rows: emitted,
                output_bytes: emitted_bytes,
            },
            state_rows: mutated,
            output: output_facts,
        },
        last_row_id,
        complete,
        repeat_cursor,
    })
}

fn run_topn_cleanup(
    transaction: &mut StepContext<'_, '_>,
    storage: &TopNStorage,
    generation_id: i64,
    cursor: TopNCursor,
    _after_drain: AfterDrain,
) -> Result<TopNPage, String> {
    let budget = transaction.budget();
    let max_rows = i64_from_usize(budget.max_input_rows, "TopN cleanup row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "TopN cleanup row budget overflow".to_string())?;
    let max_bytes = i64_from_usize(budget.max_input_bytes, "TopN cleanup byte budget")?;
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          SELECT candidate_id,
                 shiba_internal.effect_row_bytes(output_row) AS row_bytes
          FROM {candidate}
          WHERE generation_id=$1
            AND ($2 IS NULL OR candidate_id >= $2)
          ORDER BY candidate_id
          LIMIT $5
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY candidate_id) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY candidate_id) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal <= $3
            AND (page_ordinal=1 OR running_bytes <= $4)
        ),
        deleted AS (
          DELETE FROM {candidate} AS candidate
          USING selected
          WHERE candidate.candidate_id=selected.candidate_id
          RETURNING candidate.candidate_id
        )
        SELECT count(*)::bigint,
               coalesce(sum(row_bytes),0)::bigint,
               (array_agg(candidate_id ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM deleted)
        FROM selected
        "#,
        candidate = storage.candidate.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(generation_id, pg_sys::INT8OID),
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("TopN cleanup returned no summary".into());
    }
    let row = rows.first();
    let deleted = nonnegative(required(&row, 1, "TopN cleanup rows")?, "TopN cleanup rows")?;
    let bytes = nonnegative(
        required(&row, 2, "TopN cleanup bytes")?,
        "TopN cleanup bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let complete: bool = required(&row, 4, "TopN cleanup completion")?;
    let mutation_count = nonnegative(
        required(&row, 5, "TopN candidate deletes")?,
        "TopN candidate deletes",
    )?;
    if mutation_count != deleted {
        return Err("TopN cleanup delete count is inconsistent".into());
    }
    Ok(TopNPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: deleted,
                input_bytes: bytes,
                ..WorkUsage::default()
            },
            state_rows: mutation_count,
            output: OutputFacts::None,
        },
        last_row_id,
        complete,
    })
}

fn finish_topn_drain(
    transaction: &mut StepContext<'_, '_>,
    storage: &TopNStorage,
) -> Result<u64, String> {
    let candidate_rows = transaction.read(
        &format!("SELECT count(*)::bigint FROM {}", storage.candidate.sql()),
        &[],
    )?;
    if required::<i64>(
        &candidate_rows.first(),
        1,
        "TopN candidate rows after cleanup",
    )? != 0
    {
        return Err("TopN cleanup left candidate rows behind".into());
    }
    let reset = transaction.write(
        &format!(
            "UPDATE {} SET dirty=false,causal_lsn=NULL \
             WHERE singleton AND dirty AND causal_lsn IS NOT NULL \
             RETURNING singleton",
            storage.control.sql()
        ),
        &[],
    )?;
    if reset.len() != 1 {
        return Err("TopN Drain did not reset its dirty control state".into());
    }
    Ok(1)
}

fn run_topn_frontier(
    transaction: &mut StepContext<'_, '_>,
    input: InputPosition,
) -> Result<PrimitiveFacts, String> {
    if input.row_ordinal != 0 {
        return Err("TopN frontier has a row cursor".into());
    }
    let input_state = transaction.input(0)?.clone();
    let frontier = chunk(transaction, &input_state, input.chunk_seq)?
        .ok_or_else(|| "TopN frontier chunk is missing".to_string())?;
    if frontier.kind != ChunkKind::Frontier || frontier.stream_id != input.stream_id {
        return Err("TopN frontier continuation references data".into());
    }
    let output = append_frontier(transaction, frontier.lsn)?;
    advance_input(
        transaction,
        0,
        frontier.sequence + 1,
        frontier.lsn,
        WorkUsage::default(),
    )?;
    transaction.reset_admission();
    Ok(PrimitiveFacts {
        output,
        ..PrimitiveFacts::default()
    })
}
