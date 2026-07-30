use super::*;

pub(super) fn compile_transform(
    transaction: &mut StepContext<'_, '_>,
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

pub(super) fn load_scan_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
) -> Result<Option<ScanContinuation>, String> {
    lock_continuation(
        transaction,
        relation,
        "phase,input_stream_id,input_chunk_seq,next_row_ordinal,next_bootstrap_seq,pending_frontier_lsn::text",
        "Scan",
        |table| {
            let table = table.first();
            let frontier = table
                .get::<String>(6)
                .map_err(|error| error.to_string())?
                .map(|value| {
                    parse_lsn(&value)
                        .map_err(|error| format!("invalid Scan continuation LSN: {error}"))
                })
                .transpose()?;
            Ok(ScanContinuation {
                phase: ScanPhase::decode(required_table(&table, 1, "Scan phase")?)?,
                input_stream_id: required_table(&table, 2, "Scan input stream")?,
                input_chunk_seq: table.get::<i64>(3).map_err(|error| error.to_string())?,
                next_row_ordinal: table.get::<i64>(4).map_err(|error| error.to_string())?,
                next_bootstrap_seq: table.get::<i64>(5).map_err(|error| error.to_string())?,
                pending_frontier_lsn: frontier,
            })
        },
    )
}

pub(super) fn load_transform_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
) -> Result<Option<TransformContinuation>, String> {
    lock_continuation(
        transaction,
        relation,
        "input_stream_id,input_chunk_seq,next_row_ordinal",
        "linear",
        |table| {
            let table = table.first();
            Ok(TransformContinuation {
                input_stream_id: required_table(&table, 1, "linear input stream")?,
                input_chunk_seq: required_table(&table, 2, "linear input chunk")?,
                next_row_ordinal: required_table(&table, 3, "linear row cursor")?,
            })
        },
    )
}

pub(super) fn replace_scan_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    previous: Option<&ScanContinuation>,
    next: &ScanContinuation,
) -> Result<(), String> {
    validate_scan_continuation(next)?;
    let previous = previous.map(scan_arguments);
    let next = scan_arguments(next);
    replace_continuation_cas(
        transaction,
        relation,
        SCAN_COLUMNS,
        previous.as_ref().map(|v| &v[..]),
        Some(&next),
        "Scan",
    )
}

pub(super) fn scan_arguments(value: &ScanContinuation) -> [DatumWithOid<'static>; 6] {
    let frontier = value.pending_frontier_lsn.map(format_lsn);
    unsafe {
        [
            DatumWithOid::new(value.phase as i16, pg_sys::INT2OID),
            DatumWithOid::new(value.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(value.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(value.next_row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(value.next_bootstrap_seq, pg_sys::INT8OID),
            DatumWithOid::new(frontier.as_deref(), pg_sys::TEXTOID),
        ]
    }
}

pub(super) fn replace_transform_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    previous: Option<TransformContinuation>,
    next: TransformContinuation,
) -> Result<(), String> {
    if next.input_stream_id <= 0 || next.input_chunk_seq <= 0 || next.next_row_ordinal < 0 {
        return Err("linear continuation contains an invalid cursor".into());
    }
    let previous = previous.map(transform_arguments);
    let next = transform_arguments(next);
    replace_continuation_cas(
        transaction,
        relation,
        TRANSFORM_COLUMNS,
        previous.as_ref().map(|value| &value[..]),
        Some(&next),
        "linear",
    )
}

pub(super) fn transform_arguments(value: TransformContinuation) -> [DatumWithOid<'static>; 3] {
    unsafe {
        [
            DatumWithOid::new(value.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(value.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(value.next_row_ordinal, pg_sys::INT8OID),
        ]
    }
}

pub(super) fn delete_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    expected_exists: bool,
) -> Result<(), String> {
    if !expected_exists {
        return Ok(());
    }
    clear_continuation_locked(transaction, relation, "linear")
}

pub(super) fn validate_scan_continuation(continuation: &ScanContinuation) -> Result<(), String> {
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

pub(super) fn validate_bootstrap_abi(
    transaction: &mut StepContext<'_, '_>,
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

pub(super) fn first_bootstrap_sequence(
    transaction: &mut StepContext<'_, '_>,
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

pub(super) fn assert_frontier_skips_no_source_chunk(
    transaction: &mut StepContext<'_, '_>,
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

pub(super) fn source_frontier_after_chunk(
    transaction: &mut StepContext<'_, '_>,
    completed: &ChunkMeta,
) -> Result<Option<u64>, String> {
    let input = transaction.input(0)?.clone();
    let Some(frontier) = input
        .available_source_frontier_lsn
        .filter(|frontier| *frontier > input.consumed_frontier_lsn)
    else {
        return Ok(None);
    };
    let next_input = crate::execution::chunk(transaction, &input, completed.sequence + 1)?;
    if next_input.is_none_or(|next| next.lsn > frontier) {
        Ok(Some(frontier))
    } else {
        Ok(None)
    }
}

pub(super) fn i64_from_u64(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "resource count exceeds bigint".into())
}

pub(super) fn i64_from_usize(value: usize) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "work budget exceeds bigint".into())
}
