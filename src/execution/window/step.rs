use super::*;

pub(crate) fn step(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage_id: u32,
) -> Result<KernelTransition, String> {
    let stage = plan
        .stages
        .get(usize::try_from(stage_id).map_err(|_| "Window stage ID exceeds usize")?)
        .ok_or_else(|| format!("dataflow has no Window stage {stage_id}"))?;
    let OperatorSpec::Window(spec) = &stage.spec else {
        return Err("Window kernel received another operator".into());
    };
    if stage.inputs.len() != 1
        || transaction.inputs().len() != 1
        || transaction.input(0)?.port != 0
        || transaction.input(0)?.producer != ProducerKind::Operator
    {
        return Err("Window must have one operator input".into());
    }
    let capabilities = spec
        .functions
        .iter()
        .map(|function| resolve_window_function(transaction, function))
        .collect::<Result<Vec<_>, _>>()?;
    let storage = load_window_storage(transaction, stage, spec, &capabilities)?;
    let expressions = compile_window_expressions(
        transaction,
        plan,
        stage,
        spec,
        &storage.input_type,
        &storage.output_type,
        capabilities,
    )?;
    let machine = WindowMachine::new(
        expressions
            .functions
            .iter()
            .map(|function| match function.capability {
                WindowFunctionCapability::Native(_) => WindowFunctionKind::Native,
                WindowFunctionCapability::Aggregate(_) => WindowFunctionKind::Aggregate,
            })
            .collect(),
    )?;
    let durable = load_window_continuation(transaction, &storage.continuation)?;
    crate::execution::validate_continuation_authority(transaction, durable.is_some())?;
    let current = match durable {
        Some(durable) => durable,
        None => start_window_continuation(transaction, &storage)?,
    };
    if current.continuation.input_stream_id != transaction.input(0)?.stream_id {
        return Err("Window continuation changed its input stream".into());
    }
    if let Some(input) = current.continuation.input {
        if input.stream_id != transaction.input(0)?.stream_id
            || input.chunk_seq != transaction.input(0)?.next_chunk_seq
        {
            return Err("Window continuation is not at its input cursor".into());
        }
    }
    let action = machine.action(current.continuation)?;
    let result = match action {
        WindowAction::Admit { input } => WindowActionResult::Admitted(run_window_admission(
            transaction,
            &storage,
            &expressions,
            input,
        )?),
        WindowAction::Enumerate {
            partition_queue_id,
            cursor,
        } => WindowActionResult::Enumerated(run_window_enumeration(
            transaction,
            &storage,
            &expressions,
            partition_queue_id,
            cursor,
        )?),
        WindowAction::BuildPeers {
            partition_queue_id,
            cursor,
        } => WindowActionResult::PeersBuilt(run_window_peers(
            transaction,
            &storage,
            &expressions,
            partition_queue_id,
            cursor,
        )?),
        WindowAction::BuildFrames {
            partition_queue_id,
            cursor,
        } => WindowActionResult::FramesBuilt(run_window_frames(
            transaction,
            &storage,
            &expressions,
            spec,
            partition_queue_id,
            cursor,
        )?),
        WindowAction::FoldAggregate {
            partition_queue_id,
            function_ordinal,
            cursor,
        } => WindowActionResult::AggregateFolded(run_window_aggregate_fold(
            transaction,
            &storage,
            &expressions,
            partition_queue_id,
            function_ordinal,
            cursor,
        )?),
        WindowAction::Evaluate {
            partition_queue_id,
            function_ordinal,
            cursor,
        } => WindowActionResult::Evaluated(run_window_evaluate(
            transaction,
            &storage,
            &expressions,
            partition_queue_id,
            function_ordinal,
            cursor,
        )?),
        WindowAction::Diff {
            partition_queue_id,
            leg,
            cursor,
        } => WindowActionResult::Diffed(run_window_diff(
            transaction,
            &storage,
            partition_queue_id,
            leg,
            cursor,
        )?),
        WindowAction::Cleanup {
            partition_queue_id,
            cursor,
        } => {
            let after = phase_after_partitions(current.continuation.phase)?;
            let cleanup = run_window_cleanup(
                transaction,
                &storage,
                &expressions,
                partition_queue_id,
                cursor,
                after,
            )?;
            WindowActionResult::Cleaned(cleanup)
        }
        WindowAction::ForwardFrontier { input } => {
            WindowActionResult::FrontierForwarded(run_window_frontier(transaction, input)?)
        }
    };
    let transition = machine.apply(current.continuation, result, transaction.budget())?;
    let WindowTransition::Committed {
        continuation: next,
        facts,
    } = transition;
    let has_continuation = next.is_some();
    if facts.continuation_rows != u64::from(has_continuation) {
        return Err("Window continuation mutation disagrees with primitive facts".into());
    }
    replace_window_continuation(
        transaction,
        &storage.continuation,
        current.persisted.then_some(current.continuation),
        next,
    )?;
    transaction.transition(has_continuation, facts.usage)
}

pub(super) fn start_window_continuation(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
) -> Result<DurableWindow, String> {
    let chunk = next_chunk(transaction, 0)?
        .ok_or_else(|| "runnable Window has no input chunk".to_string())?;
    let input = InputPosition::new(chunk.stream_id, chunk.sequence, 0)?;
    let (input, phase) = match chunk.kind {
        ChunkKind::Data => (Some(input), WindowPhase::Admit),
        ChunkKind::Frontier => {
            let arguments: [DatumWithOid<'_>; 0] = [];
            let rows = transaction.read(
                &format!(
                    "SELECT min(partition_id)::bigint FROM {} WHERE dirty",
                    storage.partitions.sql()
                ),
                &arguments,
            )?;
            if rows.len() != 1 {
                return Err("Window dirty queue returned no summary".into());
            }
            match rows
                .first()
                .get::<i64>(1)
                .map_err(|error| error.to_string())?
            {
                Some(first_partition_queue_id) => (
                    None,
                    WindowPhase::Enumerate {
                        partition_queue_id: first_partition_queue_id,
                        cursor: WindowCursor::default(),
                        after_partitions: AfterPartitions::Frontier(input),
                    },
                ),
                None => (Some(input), WindowPhase::Frontier),
            }
        }
    };
    Ok(DurableWindow {
        continuation: WindowContinuation {
            input_stream_id: chunk.stream_id,
            input,
            phase,
        },
        persisted: false,
    })
}

pub(super) fn load_window_storage(
    transaction: &mut StepContext<'_, '_>,
    stage: &DataflowStage,
    spec: &WindowSpec,
    capabilities: &[WindowFunctionCapability],
) -> Result<WindowStorage, String> {
    if capabilities.len() != spec.functions.len() {
        return Err("Window capability count changed".into());
    }
    let input_stream = transaction.input(0)?.stream_id;
    let output_stream = transaction.output()?.stream_id;
    let input_payload = transaction.payload_storage(input_stream)?;
    let output_payload = transaction.payload_storage(output_stream)?;
    let mut accumulators = Vec::with_capacity(spec.functions.len());
    let mut ntile_states = Vec::with_capacity(spec.functions.len());
    for (index, capability) in capabilities.iter().enumerate() {
        accumulators.push(
            if matches!(capability, WindowFunctionCapability::Aggregate(_)) {
                Some(
                    transaction.state_storage(
                        i32::try_from(1001 + index)
                            .map_err(|_| "Window accumulator slot exceeds integer")?,
                    )?,
                )
            } else {
                None
            },
        );
        ntile_states.push(
            if matches!(
                capability,
                WindowFunctionCapability::Native(NativeWindow::Ntile)
            ) {
                Some(
                    transaction.state_storage(
                        i32::try_from(2001 + index)
                            .map_err(|_| "Window ntile state slot exceeds integer")?,
                    )?,
                )
            } else {
                None
            },
        );
    }
    let storage = WindowStorage {
        partitions: transaction.state_storage(0)?,
        input: transaction.state_storage(1)?,
        ordered: transaction.state_storage(2)?,
        peers: transaction.state_storage(3)?,
        frames: transaction.state_storage(4)?,
        candidate: transaction.state_storage(5)?,
        visible: transaction.state_storage(6)?,
        continuation: transaction.continuation_storage()?,
        accumulators,
        ntile_states,
        input_payload: input_payload.relation,
        output_payload: output_payload.relation,
        input_type: input_payload.row_type,
        output_type: output_payload.row_type,
    };
    validate_window_storage(transaction, &storage, stage, spec, capabilities)?;
    Ok(storage)
}

pub(super) fn validate_window_storage(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    stage: &DataflowStage,
    spec: &WindowSpec,
    capabilities: &[WindowFunctionCapability],
) -> Result<(), String> {
    let partitions = transaction.relation_attributes(storage.partitions.oid())?;
    if partitions.len() != 4 + spec.partition_by.len()
        || !window_attribute_is(&partitions[0], "partition_id", pg_sys::INT8OID, true)
        || !window_attribute_is(&partitions[1], "dirty", pg_sys::BOOLOID, true)
        || !window_attribute_is(&partitions[2], "causal_lsn", pg_sys::PG_LSNOID, false)
        || !window_attribute_is(&partitions[3], "row_count", pg_sys::NUMERICOID, true)
    {
        return Err("Window partition relation has an invalid ABI".into());
    }
    for (index, (attribute, key)) in partitions[4..].iter().zip(&spec.partition_by).enumerate() {
        if attribute.name != format!("partition_{}", index + 1)
            || !attribute_matches_slot(attribute, &key.type_)
        {
            return Err("Window partition key changed its typed ABI".into());
        }
    }

    let input = transaction.relation_attributes(storage.input.oid())?;
    if input.len() != 5 + spec.order_by.len()
        || !window_attribute_is(&input[0], "entry_id", pg_sys::INT8OID, true)
        || !window_attribute_is(&input[1], "row_key", pg_sys::BYTEAOID, true)
        || input[2].name != "row_value"
        || input[2].type_oid != storage.input_type.oid()
        || !input[2].not_null
        || !window_attribute_is(&input[3], "multiplicity", pg_sys::NUMERICOID, true)
        || !window_attribute_is(&input[4], "partition_id", pg_sys::INT8OID, true)
    {
        return Err("Window input relation has an invalid ABI".into());
    }
    for (index, (attribute, key)) in input[5..].iter().zip(&spec.order_by).enumerate() {
        if attribute.name != format!("order_{}", index + 1)
            || !attribute_matches_slot(attribute, &key.type_)
        {
            return Err("Window order key changed its typed ABI".into());
        }
    }

    let ordered = transaction.relation_attributes(storage.ordered.oid())?;
    if ordered.len() != 4 + spec.functions.len()
        || !window_attribute_is(&ordered[0], "ordinal", pg_sys::INT8OID, true)
        || !window_attribute_is(&ordered[1], "entry_id", pg_sys::INT8OID, true)
        || !window_attribute_is(&ordered[2], "copy_ordinal", pg_sys::INT8OID, true)
        || !window_attribute_is(&ordered[3], "peer_id", pg_sys::INT8OID, false)
    {
        return Err("Window ordered relation has an invalid ABI".into());
    }
    for (index, (attribute, function)) in ordered[4..].iter().zip(&spec.functions).enumerate() {
        if attribute.name != format!("function_{}", index + 1)
            || !attribute_matches_slot(attribute, &function.type_)
            || attribute.not_null
        {
            return Err("Window function result changed its typed ABI".into());
        }
    }
    validate_exact_window_attributes(
        transaction,
        &storage.peers,
        &[
            ("peer_id", pg_sys::INT8OID, true),
            ("first_ordinal", pg_sys::INT8OID, true),
            ("last_ordinal", pg_sys::INT8OID, true),
        ],
        "peer",
    )?;
    validate_exact_window_attributes(
        transaction,
        &storage.frames,
        &[
            ("ordinal", pg_sys::INT8OID, true),
            ("start_1", pg_sys::INT8OID, false),
            ("end_1", pg_sys::INT8OID, false),
            ("start_2", pg_sys::INT8OID, false),
            ("end_2", pg_sys::INT8OID, false),
            ("start_3", pg_sys::INT8OID, false),
            ("end_3", pg_sys::INT8OID, false),
            ("frame_count", pg_sys::INT8OID, true),
        ],
        "frame",
    )?;
    validate_window_output_state(
        transaction,
        &storage.candidate,
        "candidate_id",
        &storage.output_type,
        "candidate",
    )?;
    validate_window_output_state(
        transaction,
        &storage.visible,
        "visible_id",
        &storage.output_type,
        "visible",
    )?;
    validate_window_continuation_abi(transaction, &storage.continuation)?;
    if storage.accumulators.len() != capabilities.len()
        || storage.ntile_states.len() != capabilities.len()
    {
        return Err("Window function state count changed".into());
    }
    for (index, capability) in capabilities.iter().enumerate() {
        match capability {
            WindowFunctionCapability::Aggregate(capability) => {
                let accumulator = storage.accumulators[index]
                    .as_ref()
                    .ok_or_else(|| "Window aggregate omitted its accumulator".to_string())?;
                if storage.ntile_states[index].is_some() {
                    return Err("Window aggregate has native state".into());
                }
                let attributes = transaction.relation_attributes(accumulator.oid())?;
                if attributes.len() != 5
                    || !window_attribute_is(&attributes[0], "singleton", pg_sys::BOOLOID, true)
                    || !window_attribute_is(&attributes[1], "partition_id", pg_sys::INT8OID, true)
                    || !window_attribute_is(&attributes[2], "output_ordinal", pg_sys::INT8OID, true)
                    || attributes[3].name != "state_value"
                    || attributes[3].type_oid != capability.transition_type_oid
                    || attributes[3].collation_oid != capability.transition_collation_oid
                    || !window_attribute_is(&attributes[4], "no_trans_value", pg_sys::BOOLOID, true)
                {
                    return Err("Window aggregate accumulator has an invalid ABI".into());
                }
            }
            WindowFunctionCapability::Native(NativeWindow::Ntile) => {
                if storage.accumulators[index].is_some() {
                    return Err("Window ntile has aggregate state".into());
                }
                let state = storage.ntile_states[index]
                    .as_ref()
                    .ok_or_else(|| "Window ntile omitted its state".to_string())?;
                validate_exact_window_attributes(
                    transaction,
                    state,
                    &[
                        ("singleton", pg_sys::BOOLOID, true),
                        ("partition_id", pg_sys::INT8OID, true),
                        ("bucket_count", pg_sys::INT8OID, false),
                        ("first_ordinal", pg_sys::INT8OID, false),
                    ],
                    "ntile state",
                )?;
            }
            WindowFunctionCapability::Native(_) => {
                if storage.accumulators[index].is_some() || storage.ntile_states[index].is_some() {
                    return Err("stateless Window function has durable state".into());
                }
            }
        }
    }
    let output = transaction.composite_attributes(&storage.output_type)?;
    validate_output_attributes(&output, &stage.schema.outputs)?;
    Ok(())
}

pub(super) fn validate_window_output_state(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    identity: &str,
    output_type: &TypeRef,
    label: &str,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(relation.oid())?;
    if attributes.len() != 5
        || !window_attribute_is(&attributes[0], identity, pg_sys::INT8OID, true)
        || !window_attribute_is(&attributes[1], "partition_id", pg_sys::INT8OID, true)
        || !window_attribute_is(&attributes[2], "output_key", pg_sys::BYTEAOID, true)
        || attributes[3].name != "output_row"
        || attributes[3].type_oid != output_type.oid()
        || !attributes[3].not_null
        || !window_attribute_is(&attributes[4], "multiplicity", pg_sys::NUMERICOID, true)
    {
        return Err(format!("Window {label} relation has an invalid ABI"));
    }
    Ok(())
}

pub(super) fn validate_window_continuation_abi(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
) -> Result<(), String> {
    validate_typed_continuation_abi(transaction, relation, CONTINUATION_COLUMNS, "Window")
}

pub(super) fn validate_exact_window_attributes(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    expected: &[(&str, pg_sys::Oid, bool)],
    label: &str,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(relation.oid())?;
    if attributes.len() != expected.len()
        || attributes
            .iter()
            .zip(expected)
            .any(|(actual, (name, type_oid, not_null))| {
                !window_attribute_is(actual, name, *type_oid, *not_null)
            })
    {
        return Err(format!("Window {label} relation has an invalid ABI"));
    }
    Ok(())
}

pub(super) fn compile_window_expressions(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &WindowSpec,
    input_type: &TypeRef,
    output_type: &TypeRef,
    capabilities: Vec<WindowFunctionCapability>,
) -> Result<WindowExpressions, String> {
    validate_window_frame(spec)?;
    if capabilities.len() != spec.functions.len() {
        return Err("Window capability count changed".into());
    }
    let input_bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: input_type,
            alias: "input_row",
        }],
    )?;
    let current_bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: input_type,
            alias: "current_input",
        }],
    )?;
    let target_bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: input_type,
            alias: "target_input",
        }],
    )?;
    let partition_expressions = spec
        .partition_by
        .iter()
        .map(|key| compile_scalar_expression(&key.expr, &input_bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let partition_columns = (1..=partition_expressions.len())
        .map(|index| format!("partition_{index}"))
        .collect();
    let order_expressions = spec
        .order_by
        .iter()
        .map(|key| compile_scalar_expression(&key.expr, &input_bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let order_columns = (1..=order_expressions.len())
        .map(|index| format!("order_{index}"))
        .collect::<Vec<_>>();
    let resolved = spec
        .order_by
        .iter()
        .map(|key| resolve_btree_step(transaction, key, "Window"))
        .collect::<Result<Vec<_>, _>>()?;
    let order_by = resolved
        .iter()
        .enumerate()
        .map(|(index, order)| {
            format!(
                "input_row.order_{} USING {} NULLS {}",
                index + 1,
                order.sort_operator,
                if order.nulls_first { "FIRST" } else { "LAST" }
            )
        })
        .chain(std::iter::once("input_row.entry_id ASC".into()))
        .collect::<Vec<_>>()
        .join(",");
    let keyset_after = window_keyset_sql(&resolved, "input_row", "boundary");
    let peer_equal = window_keys_equal_sql(&resolved, "next_row", "boundary_row");
    let outputs = compile_window_outputs(&stage.schema.outputs, spec, &input_bindings)?;
    let mut functions = Vec::with_capacity(spec.functions.len());
    for (function, capability) in spec.functions.iter().zip(capabilities) {
        functions.push(WindowFunctionPlan {
            current_arguments: function
                .args
                .iter()
                .map(|argument| compile_scalar_expression(argument, &current_bindings))
                .collect::<Result<_, _>>()?,
            target_arguments: function
                .args
                .iter()
                .map(|argument| compile_scalar_expression(argument, &target_bindings))
                .collect::<Result<_, _>>()?,
            filter: function
                .filter
                .as_ref()
                .map(|filter| compile_scalar_expression(filter, &current_bindings))
                .transpose()?
                .unwrap_or_else(|| "true".into()),
            result_type: resolve_window_type_sql(transaction, &function.type_)?,
            capability,
        });
    }
    let frame_start_offset = spec
        .frame
        .start_offset
        .as_ref()
        .map(|offset| compile_scalar_expression(offset, &current_bindings))
        .transpose()?;
    let frame_end_offset = spec
        .frame
        .end_offset
        .as_ref()
        .map(|offset| compile_scalar_expression(offset, &current_bindings))
        .transpose()?;
    let output_attributes = transaction.composite_attributes(output_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
    Ok(WindowExpressions {
        partition_expressions,
        partition_columns,
        order_expressions,
        order_columns,
        order_by,
        keyset_after,
        peer_equal,
        outputs,
        functions,
        frame_start_offset,
        frame_end_offset,
    })
}

pub(super) fn validate_window_frame(spec: &WindowSpec) -> Result<(), String> {
    let options = spec.frame.options;
    let modes = [
        pg_sys::FRAMEOPTION_ROWS,
        pg_sys::FRAMEOPTION_RANGE,
        pg_sys::FRAMEOPTION_GROUPS,
    ]
    .into_iter()
    .filter(|flag| options & flag != 0)
    .count();
    if modes != 1 {
        return Err("Window frame has no unique ROWS, RANGE, or GROUPS mode".into());
    }
    let starts = [
        pg_sys::FRAMEOPTION_START_UNBOUNDED_PRECEDING,
        pg_sys::FRAMEOPTION_START_CURRENT_ROW,
        pg_sys::FRAMEOPTION_START_OFFSET_PRECEDING,
        pg_sys::FRAMEOPTION_START_OFFSET_FOLLOWING,
    ]
    .into_iter()
    .filter(|flag| options & flag != 0)
    .count();
    let ends = [
        pg_sys::FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
        pg_sys::FRAMEOPTION_END_CURRENT_ROW,
        pg_sys::FRAMEOPTION_END_OFFSET_PRECEDING,
        pg_sys::FRAMEOPTION_END_OFFSET_FOLLOWING,
    ]
    .into_iter()
    .filter(|flag| options & flag != 0)
    .count();
    if starts != 1 || ends != 1 {
        return Err("Window frame has invalid start or end bounds".into());
    }
    let start_offset = options & pg_sys::FRAMEOPTION_START_OFFSET != 0;
    let end_offset = options & pg_sys::FRAMEOPTION_END_OFFSET != 0;
    if start_offset != spec.frame.start_offset.is_some()
        || end_offset != spec.frame.end_offset.is_some()
    {
        return Err("Window frame offset expression does not match its options".into());
    }
    if options & pg_sys::FRAMEOPTION_RANGE != 0 && (start_offset || end_offset) {
        return Err(
            "resumable Window RANGE offsets are not supported by the bounded frame ABI".into(),
        );
    }
    Ok(())
}

pub(super) fn compile_window_outputs(
    outputs: &[OutputSlot],
    spec: &WindowSpec,
    bindings: &[SqlBinding],
) -> Result<String, String> {
    if outputs.len() != spec.outputs.len() + spec.functions.len() {
        return Err("Window outputs do not match its stage schema".into());
    }
    let mut sql = compile_named_outputs(
        &outputs[..spec.outputs.len()],
        &spec.outputs,
        bindings,
        "Window passthrough",
    )?;
    for (index, (output, function)) in outputs[spec.outputs.len()..]
        .iter()
        .zip(&spec.functions)
        .enumerate()
    {
        if output.slot != function.output {
            return Err("Window function output order changed".into());
        }
        sql.push(format!("updated.function_{}", index + 1));
    }
    Ok(sql.join(","))
}

pub(super) fn resolve_window_type_sql(
    transaction: &mut StepContext<'_, '_>,
    type_: &SlotType,
) -> Result<String, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(pg_sys::Oid::from(type_.type_oid), pg_sys::OIDOID),
            DatumWithOid::new(type_.typmod, pg_sys::INT4OID),
        ]
    };
    let rows = transaction.read(
        r#"
        SELECT pg_catalog.format_type(type_catalog.oid,$2)
        FROM pg_catalog.pg_type AS type_catalog
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid=type_catalog.typnamespace
        WHERE type_catalog.oid=$1 AND type_catalog.typtype<>'p'
          AND namespace.nspname='pg_catalog'
        "#,
        &arguments,
    )?;
    if rows.len() != 1 {
        return Err("Window function result type is not a trusted pg_catalog type".into());
    }
    window_required(&rows.first(), 1, "Window function result type")
}

pub(super) fn window_keyset_sql(
    orders: &[BtreeOrder],
    current_alias: &str,
    boundary_alias: &str,
) -> String {
    let mut alternatives = Vec::with_capacity(orders.len() + 1);
    let mut prefix = Vec::new();
    for (index, order) in orders.iter().enumerate() {
        let column = format!("order_{}", index + 1);
        let before = format!("{boundary_alias}.{column}");
        let current = format!("{current_alias}.{column}");
        let after = if order.nulls_first {
            format!(
                "(CASE WHEN {before} IS NULL THEN {current} IS NOT NULL \
                 WHEN {current} IS NULL THEN false \
                 ELSE {before} {} {current} END)",
                order.sort_operator
            )
        } else {
            format!(
                "(CASE WHEN {before} IS NULL THEN false \
                 WHEN {current} IS NULL THEN true \
                 ELSE {before} {} {current} END)",
                order.sort_operator
            )
        };
        alternatives.push(if prefix.is_empty() {
            after
        } else {
            format!("({} AND {after})", prefix.join(" AND "))
        });
        prefix.push(format!(
            "(({before} IS NULL AND {current} IS NULL) OR \
             ({before} IS NOT NULL AND {current} IS NOT NULL \
              AND {before} {} {current}))",
            order.equality_operator
        ));
    }
    let id = format!("{current_alias}.entry_id>{boundary_alias}.entry_id");
    alternatives.push(if prefix.is_empty() {
        id
    } else {
        format!("({} AND {id})", prefix.join(" AND "))
    });
    alternatives.join(" OR ")
}

pub(super) fn window_keys_equal_sql(orders: &[BtreeOrder], left: &str, right: &str) -> String {
    if orders.is_empty() {
        return "true".into();
    }
    orders
        .iter()
        .enumerate()
        .map(|(index, order)| {
            let column = format!("order_{}", index + 1);
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

pub(super) fn resolve_window_function(
    transaction: &mut StepContext<'_, '_>,
    function: &WindowExpr,
) -> Result<WindowFunctionCapability, String> {
    if function.aggregate {
        if function.star && !function.args.is_empty() {
            return Err("Window aggregate star has explicit arguments".into());
        }
        return load_window_aggregate(transaction, function)
            .map(WindowFunctionCapability::Aggregate);
    }
    if function.filter.is_some() || function.star {
        return Err("native Window function cannot use FILTER or star".into());
    }
    let arguments = unsafe {
        [DatumWithOid::new(
            pg_sys::Oid::from(function.function_oid),
            pg_sys::OIDOID,
        )]
    };
    let rows = transaction.read(
        r#"
        SELECT procedure.proname::text,procedure.pronargs::integer
        FROM pg_catalog.pg_proc AS procedure
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid=procedure.pronamespace
        WHERE procedure.oid=$1 AND procedure.prokind='w'
          AND procedure.provolatile='i' AND namespace.nspname='pg_catalog'
        "#,
        &arguments,
    )?;
    if rows.len() != 1 {
        return Err(format!(
            "Window function OID {} is not a trusted native capability",
            function.function_oid
        ));
    }
    let row = rows.first();
    let name: String = window_required(&row, 1, "Window function name")?;
    let arity: i32 = window_required(&row, 2, "Window function arity")?;
    if usize::try_from(arity).ok() != Some(function.args.len()) {
        return Err("Window function arity changed".into());
    }
    let capability = match (name.as_str(), function.args.len()) {
        ("row_number", 0) => NativeWindow::RowNumber,
        ("rank", 0) => NativeWindow::Rank,
        ("dense_rank", 0) => NativeWindow::DenseRank,
        ("percent_rank", 0) => NativeWindow::PercentRank,
        ("cume_dist", 0) => NativeWindow::CumeDist,
        ("ntile", 1) => NativeWindow::Ntile,
        ("lag", 1..=3) => NativeWindow::Lag,
        ("lead", 1..=3) => NativeWindow::Lead,
        ("first_value", 1) => NativeWindow::FirstValue,
        ("last_value", 1) => NativeWindow::LastValue,
        ("nth_value", 2) => NativeWindow::NthValue,
        _ => return Err(format!("Window function {name} has no bounded capability")),
    };
    Ok(WindowFunctionCapability::Native(capability))
}

pub(super) fn load_window_aggregate(
    transaction: &mut StepContext<'_, '_>,
    function: &WindowExpr,
) -> Result<AggregateCapability, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(pg_sys::Oid::from(function.function_oid), pg_sys::OIDOID),
            DatumWithOid::new(pg_sys::Oid::from(function.type_.type_oid), pg_sys::OIDOID),
        ]
    };
    let rows = transaction.read(AGGREGATE_CAPABILITY_SQL, &arguments)?;
    decode_aggregate_capability(
        rows,
        function.function_oid,
        function.args.len(),
        function.input_collation_oid,
    )
}

pub(super) fn window_attribute_is(
    attribute: &AttributeRef,
    name: &str,
    type_oid: pg_sys::Oid,
    not_null: bool,
) -> bool {
    attribute.name == name && attribute.type_oid == type_oid && attribute.not_null == not_null
}

pub(super) fn window_i64_budget(value: usize, name: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{name} exceeds bigint"))
}

#[derive(Clone, Copy, Debug)]
pub(super) struct WindowFields {
    pub(super) phase: i16,
    pub(super) input_stream_id: i64,
    pub(super) input_chunk_seq: Option<i64>,
    pub(super) input_row_ordinal: Option<i64>,
    pub(super) partition_queue_id: Option<i64>,
    pub(super) function_ordinal: Option<i32>,
    pub(super) output_ordinal: Option<i64>,
    pub(super) cursor_row_id: Option<i64>,
    pub(super) fold_ready: bool,
    pub(super) cursor_repeat: bool,
    pub(super) diff_leg: Option<i16>,
    pub(super) cleanup_ordinal: Option<i32>,
    pub(super) after_kind: Option<i16>,
    pub(super) after_chunk_seq: Option<i64>,
    pub(super) after_row_ordinal: Option<i64>,
}

pub(super) fn load_window_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
) -> Result<Option<DurableWindow>, String> {
    let query = format!(
        r#"
        SELECT phase,input_stream_id,input_chunk_seq,input_row_ordinal,
               partition_queue_id,function_ordinal,output_ordinal,
               cursor_row_id,fold_ready,cursor_repeat,diff_leg,
               cleanup_ordinal,after_kind,after_chunk_seq,after_row_ordinal
        FROM {} WHERE singleton FOR UPDATE
        "#,
        relation.sql()
    );
    let rows = transaction.lock(&query, &[])?;
    match rows.len() {
        0 => Ok(None),
        1 => {
            let row = rows.first();
            let fields = WindowFields {
                phase: window_required(&row, 1, "Window phase")?,
                input_stream_id: window_required(&row, 2, "Window input stream")?,
                input_chunk_seq: row.get(3).map_err(|error| error.to_string())?,
                input_row_ordinal: row.get(4).map_err(|error| error.to_string())?,
                partition_queue_id: row.get(5).map_err(|error| error.to_string())?,
                function_ordinal: row.get(6).map_err(|error| error.to_string())?,
                output_ordinal: row.get(7).map_err(|error| error.to_string())?,
                cursor_row_id: row.get(8).map_err(|error| error.to_string())?,
                fold_ready: window_required(&row, 9, "Window Fold ready state")?,
                cursor_repeat: window_required(&row, 10, "Window cursor repeat")?,
                diff_leg: row.get(11).map_err(|error| error.to_string())?,
                cleanup_ordinal: row.get(12).map_err(|error| error.to_string())?,
                after_kind: row.get(13).map_err(|error| error.to_string())?,
                after_chunk_seq: row.get(14).map_err(|error| error.to_string())?,
                after_row_ordinal: row.get(15).map_err(|error| error.to_string())?,
            };
            Ok(Some(DurableWindow {
                continuation: decode_window_fields(fields)?,
                persisted: true,
            }))
        }
        count => Err(format!(
            "Window continuation relation contains {count} rows"
        )),
    }
}

pub(super) fn decode_window_fields(fields: WindowFields) -> Result<WindowContinuation, String> {
    let input = match (fields.input_chunk_seq, fields.input_row_ordinal) {
        (Some(chunk_seq), Some(row_ordinal)) => Some(InputPosition::new(
            fields.input_stream_id,
            chunk_seq,
            row_ordinal,
        )?),
        (None, None) => None,
        _ => return Err("Window continuation has a partial input cursor".into()),
    };
    let kind = WindowPhaseKind::from_code(PhaseCode::active(fields.phase)?)?;
    if kind != WindowPhaseKind::FoldAggregate && fields.fold_ready {
        return Err("non-Fold Window continuation contains ready state".into());
    }
    let queue = || {
        fields
            .partition_queue_id
            .ok_or_else(|| "Window continuation omitted its partition".to_string())
    };
    let cursor = WindowCursor {
        row_id: fields.cursor_row_id,
    };
    let after = || {
        decode_after_partitions(
            fields.input_stream_id,
            fields.after_kind,
            fields.after_chunk_seq,
            fields.after_row_ordinal,
        )
    };
    let plain = || -> Result<(), String> {
        if fields.function_ordinal.is_some()
            || fields.output_ordinal.is_some()
            || fields.cursor_repeat
            || fields.diff_leg.is_some()
            || fields.cleanup_ordinal.is_some()
        {
            Err("Window continuation contains another phase's fields".into())
        } else {
            Ok(())
        }
    };
    let phase = match kind {
        WindowPhaseKind::Admit | WindowPhaseKind::Frontier => {
            if fields.partition_queue_id.is_some()
                || fields.function_ordinal.is_some()
                || fields.output_ordinal.is_some()
                || fields.cursor_row_id.is_some()
                || fields.cursor_repeat
                || fields.diff_leg.is_some()
                || fields.cleanup_ordinal.is_some()
                || fields.after_kind.is_some()
                || fields.after_chunk_seq.is_some()
                || fields.after_row_ordinal.is_some()
            {
                return Err("Window idle phase contains work fields".into());
            }
            if kind == WindowPhaseKind::Admit {
                WindowPhase::Admit
            } else {
                WindowPhase::Frontier
            }
        }
        WindowPhaseKind::Enumerate => {
            plain()?;
            WindowPhase::Enumerate {
                partition_queue_id: queue()?,
                cursor,
                after_partitions: after()?,
            }
        }
        WindowPhaseKind::Peers => {
            plain()?;
            WindowPhase::Peers {
                partition_queue_id: queue()?,
                cursor,
                after_partitions: after()?,
            }
        }
        WindowPhaseKind::Frames => {
            plain()?;
            WindowPhase::Frames {
                partition_queue_id: queue()?,
                cursor,
                after_partitions: after()?,
            }
        }
        WindowPhaseKind::FoldAggregate | WindowPhaseKind::Evaluate => {
            if fields.cursor_repeat || fields.diff_leg.is_some() || fields.cleanup_ordinal.is_some()
            {
                return Err("Window function continuation contains another phase's fields".into());
            }
            let ordinal =
                u32::try_from(fields.function_ordinal.ok_or_else(|| {
                    "Window function continuation omitted its ordinal".to_string()
                })?)
                .map_err(|_| "Window function ordinal is negative")?;
            if kind == WindowPhaseKind::FoldAggregate {
                WindowPhase::FoldAggregate {
                    partition_queue_id: queue()?,
                    function_ordinal: ordinal,
                    cursor: WindowFoldCursor {
                        output_ordinal: fields.output_ordinal.ok_or_else(|| {
                            "Window aggregate fold omitted its output ordinal".to_string()
                        })?,
                        last_frame_ordinal: fields.cursor_row_id,
                        ready_to_finalize: fields.fold_ready,
                    },
                    after_partitions: after()?,
                }
            } else {
                if fields.output_ordinal.is_some() {
                    return Err("Window native evaluation contains an output ordinal".into());
                }
                WindowPhase::Evaluate {
                    partition_queue_id: queue()?,
                    function_ordinal: ordinal,
                    cursor,
                    after_partitions: after()?,
                }
            }
        }
        WindowPhaseKind::Diff => {
            if fields.function_ordinal.is_some()
                || fields.output_ordinal.is_some()
                || fields.cleanup_ordinal.is_some()
            {
                return Err("Window Diff continuation contains another phase's fields".into());
            }
            WindowPhase::Diff {
                partition_queue_id: queue()?,
                leg: match fields.diff_leg {
                    Some(1) => DiffLeg::Remove,
                    Some(2) => DiffLeg::Add,
                    _ => return Err("Window Diff continuation has an invalid leg".into()),
                },
                cursor: WindowDiffCursor {
                    row_id: fields.cursor_row_id,
                    repeat: fields.cursor_repeat,
                },
                after_partitions: after()?,
            }
        }
        WindowPhaseKind::Cleanup => {
            if fields.function_ordinal.is_some()
                || fields.output_ordinal.is_some()
                || fields.cursor_repeat
                || fields.diff_leg.is_some()
            {
                return Err("Window Cleanup continuation contains another phase's fields".into());
            }
            WindowPhase::Cleanup {
                partition_queue_id: queue()?,
                cursor: WindowCleanupCursor {
                    relation_ordinal: u32::try_from(fields.cleanup_ordinal.ok_or_else(|| {
                        "Window Cleanup continuation omitted its relation".to_string()
                    })?)
                    .map_err(|_| "Window cleanup ordinal is negative")?,
                    row: cursor,
                },
                after_partitions: after()?,
            }
        }
    };
    Ok(WindowContinuation {
        input_stream_id: fields.input_stream_id,
        input,
        phase,
    })
}

pub(super) fn decode_after_partitions(
    input_stream_id: i64,
    kind: Option<i16>,
    chunk_seq: Option<i64>,
    row_ordinal: Option<i64>,
) -> Result<AfterPartitions, String> {
    match (kind, chunk_seq, row_ordinal) {
        (Some(1), Some(chunk), Some(row)) => Ok(AfterPartitions::Admit(InputPosition::new(
            input_stream_id,
            chunk,
            row,
        )?)),
        (Some(2), None, None) => Ok(AfterPartitions::FinishInput),
        (Some(3), Some(chunk), Some(row)) => Ok(AfterPartitions::Frontier(InputPosition::new(
            input_stream_id,
            chunk,
            row,
        )?)),
        _ => Err("Window continuation has an invalid partition target".into()),
    }
}

pub(super) fn encode_window_fields(
    continuation: WindowContinuation,
) -> Result<WindowFields, String> {
    let mut fields = WindowFields {
        phase: continuation.phase.code().value(),
        input_stream_id: continuation.input_stream_id,
        input_chunk_seq: continuation.input.map(|input| input.chunk_seq),
        input_row_ordinal: continuation.input.map(|input| input.row_ordinal),
        partition_queue_id: None,
        function_ordinal: None,
        output_ordinal: None,
        cursor_row_id: None,
        fold_ready: false,
        cursor_repeat: false,
        diff_leg: None,
        cleanup_ordinal: None,
        after_kind: None,
        after_chunk_seq: None,
        after_row_ordinal: None,
    };
    match continuation.phase {
        WindowPhase::Admit | WindowPhase::Frontier => {}
        WindowPhase::Enumerate {
            partition_queue_id,
            cursor,
            after_partitions,
        }
        | WindowPhase::Peers {
            partition_queue_id,
            cursor,
            after_partitions,
        }
        | WindowPhase::Frames {
            partition_queue_id,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.cursor_row_id = cursor.row_id;
            encode_window_after(&mut fields, after_partitions);
        }
        WindowPhase::FoldAggregate {
            partition_queue_id,
            function_ordinal,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.function_ordinal =
                Some(i32::try_from(function_ordinal).map_err(|_| "Window function exceeds i32")?);
            fields.output_ordinal = Some(cursor.output_ordinal);
            fields.cursor_row_id = cursor.last_frame_ordinal;
            fields.fold_ready = cursor.ready_to_finalize;
            encode_window_after(&mut fields, after_partitions);
        }
        WindowPhase::Evaluate {
            partition_queue_id,
            function_ordinal,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.function_ordinal =
                Some(i32::try_from(function_ordinal).map_err(|_| "Window function exceeds i32")?);
            fields.cursor_row_id = cursor.row_id;
            encode_window_after(&mut fields, after_partitions);
        }
        WindowPhase::Diff {
            partition_queue_id,
            leg,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.cursor_row_id = cursor.row_id;
            fields.cursor_repeat = cursor.repeat;
            fields.diff_leg = Some(match leg {
                DiffLeg::Remove => 1,
                DiffLeg::Add => 2,
            });
            encode_window_after(&mut fields, after_partitions);
        }
        WindowPhase::Cleanup {
            partition_queue_id,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.cleanup_ordinal = Some(
                i32::try_from(cursor.relation_ordinal)
                    .map_err(|_| "Window cleanup ordinal exceeds i32")?,
            );
            fields.cursor_row_id = cursor.row.row_id;
            encode_window_after(&mut fields, after_partitions);
        }
    }
    Ok(fields)
}

pub(super) fn encode_window_after(fields: &mut WindowFields, after: AfterPartitions) {
    match after {
        AfterPartitions::Admit(input) => {
            fields.after_kind = Some(1);
            fields.after_chunk_seq = Some(input.chunk_seq);
            fields.after_row_ordinal = Some(input.row_ordinal);
        }
        AfterPartitions::FinishInput => fields.after_kind = Some(2),
        AfterPartitions::Frontier(input) => {
            fields.after_kind = Some(3);
            fields.after_chunk_seq = Some(input.chunk_seq);
            fields.after_row_ordinal = Some(input.row_ordinal);
        }
    }
}

pub(super) fn replace_window_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    old: Option<WindowContinuation>,
    next: Option<WindowContinuation>,
) -> Result<(), String> {
    let old_fields = old.map(encode_window_fields).transpose()?;
    let next_fields = next.map(encode_window_fields).transpose()?;
    let old_arguments = old_fields.as_ref().map(window_field_arguments);
    let next_arguments = next_fields.as_ref().map(window_field_arguments);
    replace_continuation_cas(
        transaction,
        relation,
        CONTINUATION_COLUMNS,
        old_arguments.as_ref().map(|arguments| &arguments[..]),
        next_arguments.as_ref().map(|arguments| &arguments[..]),
        "Window",
    )
}

pub(super) fn window_field_arguments(fields: &WindowFields) -> [DatumWithOid<'_>; 15] {
    unsafe {
        [
            DatumWithOid::new(fields.phase, pg_sys::INT2OID),
            DatumWithOid::new(fields.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(fields.partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.function_ordinal, pg_sys::INT4OID),
            DatumWithOid::new(fields.output_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(fields.cursor_row_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.fold_ready, pg_sys::BOOLOID),
            DatumWithOid::new(fields.cursor_repeat, pg_sys::BOOLOID),
            DatumWithOid::new(fields.diff_leg, pg_sys::INT2OID),
            DatumWithOid::new(fields.cleanup_ordinal, pg_sys::INT4OID),
            DatumWithOid::new(fields.after_kind, pg_sys::INT2OID),
            DatumWithOid::new(fields.after_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.after_row_ordinal, pg_sys::INT8OID),
        ]
    }
}
