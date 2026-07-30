//! Transactional Sink execution.
//!
//! One transaction quantum walks a bounded prefix of effect rows and applies
//! each row's bounded weight page. The arbitrary composite remains inside
//! PostgreSQL; Rust carries only stream positions, signed weights, measured
//! byte sizes, and the shared quantum budget.

use std::collections::BTreeMap;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{SpiHeapTupleData, SpiTupleTable};

use crate::logical::model::{
    BindingId, DataflowPlan, DataflowStage, OperatorSpec, SlotId, SlotType,
};
use crate::logical::{StepExecution, WorkBudget, WorkQuantum};
use crate::postgres::quote_identifier;

use super::{
    advance_input, next_chunk, AttributeRef, ChunkKind, ChunkMeta, InputPosition, RelationRef,
    StepTxn, WorkUsage,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinkContinuation {
    position: InputPosition,
    remaining_weight: Option<i64>,
    persisted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EffectHead {
    row_ordinal: i64,
    weight: i64,
    row_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinkAction {
    row_ordinal: i64,
    applied_weight: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WeightPage {
    applied_weight: i64,
    remaining_weight: Option<i64>,
    usage: WorkUsage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SinkMapping {
    insert_columns: String,
    select_columns: String,
    delete_predicate: String,
    effect_equality: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PayloadLayout {
    relation: RelationRef,
    attributes: Vec<AttributeRef>,
}

/// Execute one Sink checkpoint inside the caller's PostgreSQL transaction.
///
/// The caller must turn every returned error into a transaction-aborting
/// PostgreSQL ERROR. In particular, an error after result DML must never be
/// converted into an ordinary `Blocked` or `Idle` outcome.
pub(crate) fn execute(
    mut transaction: StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage_id: u32,
) -> Result<StepExecution, String> {
    let stage = sink_stage(plan, stage_id)?;
    if transaction.inputs().len() != 1 {
        return Err("Sink must have exactly one input".into());
    }
    let input = transaction.input(0)?.clone();
    if input.producer != super::ProducerKind::Operator {
        return Err("Sink input is not an operator effect stream".into());
    }
    let continuation_relation = transaction.continuation_storage()?;
    validate_continuation_abi(&mut transaction, &continuation_relation)?;
    let continuation =
        load_continuation(&mut transaction, &continuation_relation, input.stream_id)?;
    if transaction.checkpoint_had_continuation() != continuation.persisted {
        return Err("Sink checkpoint disagrees with its typed continuation".into());
    }
    if continuation.position.chunk_seq != input.next_chunk_seq {
        return Err("Sink continuation is not at its input cursor".into());
    }

    let chunk = next_chunk(&mut transaction, 0)?
        .ok_or_else(|| "Sink continuation or pending input references no chunk".to_string())?;
    let payload_storage = transaction.payload_storage(input.stream_id)?;
    let payload = PayloadLayout {
        attributes: transaction.composite_attributes(&payload_storage.row_type)?,
        relation: payload_storage.relation,
    };
    match chunk.kind {
        ChunkKind::Frontier => {
            consume_frontier(transaction, &continuation_relation, continuation, &chunk)
        }
        ChunkKind::Data => consume_data(
            transaction,
            plan,
            stage,
            &continuation_relation,
            continuation,
            &chunk,
            &payload,
        ),
    }
}

fn consume_frontier(
    mut transaction: StepTxn<'_, '_>,
    continuation_relation: &RelationRef,
    continuation: SinkContinuation,
    chunk: &ChunkMeta,
) -> Result<StepExecution, String> {
    if continuation.position.row_ordinal != 0 || continuation.remaining_weight.is_some() {
        return Err("Sink frontier has an invalid continuation".into());
    }
    if chunk.rows != 0 || chunk.bytes != 0 {
        return Err("Sink frontier contains payload".into());
    }
    advance_completed_chunk(&mut transaction, chunk, chunk.lsn)?;
    clear_continuation(&mut transaction, continuation_relation, continuation)?;
    transaction.finish(false, WorkUsage::default())
}

fn consume_data(
    mut transaction: StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    continuation_relation: &RelationRef,
    continuation: SinkContinuation,
    chunk: &ChunkMeta,
    payload: &PayloadLayout,
) -> Result<StepExecution, String> {
    let first_ordinal = continuation.position.row_ordinal;
    let first_ordinal_u64 =
        u64::try_from(first_ordinal).map_err(|_| "Sink continuation has a negative row")?;
    if first_ordinal_u64 >= chunk.rows {
        return Err("Sink continuation is outside its data chunk".into());
    }
    if first_ordinal == 0 {
        validate_payload_metadata(&mut transaction, payload, chunk)?;
    }

    let result = transaction.result_storage()?;
    let result_attributes = transaction.relation_attributes(result.oid())?;
    let mapping = sink_mapping(plan, stage, &payload.attributes, &result_attributes)?;
    let quantum_budget = transaction.budget();
    let heads = effect_heads(
        &mut transaction,
        payload,
        chunk,
        first_ordinal,
        quantum_budget.max_input_rows,
    )?;
    let mut quantum = WorkQuantum::new(quantum_budget, quantum_budget.max_input_rows);
    let mut actions = Vec::with_capacity(heads.len());
    let mut next = Some(continuation);
    for head in heads {
        let current =
            next.ok_or_else(|| "Sink page continued after its chunk ended".to_string())?;
        if current.position.row_ordinal != head.row_ordinal {
            return Err("Sink page continuation skipped an effect row".into());
        }
        let remaining = quantum
            .remaining()
            .ok_or_else(|| "Sink quantum exhausted before its first effect".to_string())?;
        if !quantum.usage().is_empty()
            && (head.row_bytes
                > u64::try_from(remaining.max_input_bytes)
                    .map_err(|_| "Sink input byte budget exceeds u64")?
                || head.row_bytes
                    > u64::try_from(remaining.max_output_bytes)
                        .map_err(|_| "Sink output byte budget exceeds u64")?)
        {
            break;
        }
        let page = plan_weight_page(
            head.weight,
            current.remaining_weight,
            head.row_bytes,
            remaining,
        )?;
        quantum.record(page.usage)?;
        actions.push(SinkAction {
            row_ordinal: head.row_ordinal,
            applied_weight: page.applied_weight,
        });

        if let Some(remaining_weight) = page.remaining_weight {
            next = Some(SinkContinuation {
                position: InputPosition::new(
                    continuation.position.stream_id,
                    continuation.position.chunk_seq,
                    head.row_ordinal,
                )?,
                remaining_weight: Some(remaining_weight),
                persisted: true,
            });
            break;
        }

        let next_ordinal = head
            .row_ordinal
            .checked_add(1)
            .ok_or_else(|| "Sink row ordinal exhausted bigint".to_string())?;
        if u64::try_from(next_ordinal).map_err(|_| "Sink row ordinal became negative")?
            == chunk.rows
        {
            next = None;
            break;
        }
        next = Some(SinkContinuation {
            position: InputPosition::new(
                continuation.position.stream_id,
                continuation.position.chunk_seq,
                next_ordinal,
            )?,
            remaining_weight: None,
            persisted: true,
        });
        if quantum.remaining().is_none() {
            break;
        }
    }
    if actions.is_empty() {
        return Err("Sink page selected no effect rows".into());
    }
    let mutated = mutate_result_page(
        &mut transaction,
        &result,
        payload,
        &mapping,
        chunk,
        &actions,
    )?;
    if mutated != quantum.usage().output_rows {
        return Err(format!(
            "Sink expected {} result mutations, database returned {mutated}",
            quantum.usage().output_rows
        ));
    }
    if next.is_none() {
        let frontier = transaction.input(0)?.consumed_frontier_lsn;
        advance_completed_chunk(&mut transaction, chunk, frontier)?;
    }
    let has_continuation = next.is_some();
    replace_continuation(&mut transaction, continuation_relation, continuation, next)?;
    transaction.finish(has_continuation, quantum.usage())
}

fn sink_stage(plan: &DataflowPlan, stage_id: u32) -> Result<&DataflowStage, String> {
    let stage = plan
        .stages
        .get(usize::try_from(stage_id).map_err(|_| "Sink stage ID exceeds usize")?)
        .ok_or_else(|| format!("Sink stage {stage_id} is absent from its dataflow"))?;
    if !matches!(stage.spec, OperatorSpec::Sink)
        || stage.inputs.len() != 1
        || stage.schema.inputs.is_empty()
        || !stage.schema.outputs.is_empty()
    {
        return Err(format!("stage {stage_id} is not a valid Sink"));
    }
    Ok(stage)
}

fn load_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    input_stream_id: i64,
) -> Result<SinkContinuation, String> {
    let query = format!(
        r#"
        SELECT input_stream_id, input_chunk_seq, row_ordinal, remaining_weight
        FROM {}
        WHERE singleton
        FOR UPDATE
        "#,
        relation.sql()
    );
    let rows = transaction.lock(&query, &[])?;
    match rows.len() {
        0 => {
            let input = transaction.input(0)?;
            Ok(SinkContinuation {
                position: InputPosition::new(input_stream_id, input.next_chunk_seq, 0)?,
                remaining_weight: None,
                persisted: false,
            })
        }
        1 => {
            let row = rows.first();
            let stream_id = required::<i64>(&row, 1, "Sink continuation stream")?;
            let chunk_seq = required::<i64>(&row, 2, "Sink continuation chunk")?;
            let row_ordinal = required::<i64>(&row, 3, "Sink continuation row")?;
            let remaining_weight = row.get::<i64>(4).map_err(|error| error.to_string())?;
            if stream_id != input_stream_id || remaining_weight == Some(0) {
                return Err("Sink continuation contains invalid durable state".into());
            }
            Ok(SinkContinuation {
                position: InputPosition::new(stream_id, chunk_seq, row_ordinal)?,
                remaining_weight,
                persisted: true,
            })
        }
        _ => Err("Sink continuation relation contains more than one row".into()),
    }
}

fn replace_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    expected: SinkContinuation,
    next: Option<SinkContinuation>,
) -> Result<(), String> {
    match (expected.persisted, next) {
        (false, Some(next)) => {
            let arguments = unsafe {
                [
                    DatumWithOid::new(next.position.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(next.position.chunk_seq, pg_sys::INT8OID),
                    DatumWithOid::new(next.position.row_ordinal, pg_sys::INT8OID),
                    optional_i64(next.remaining_weight),
                ]
            };
            let query = format!(
                r#"
                INSERT INTO {}(
                  singleton,input_stream_id,input_chunk_seq,
                  row_ordinal,remaining_weight
                )
                VALUES(true,$1,$2,$3,$4)
                RETURNING singleton
                "#,
                relation.sql()
            );
            require_one_mutation(transaction.write(&query, &arguments)?, "insert")?;
        }
        (true, Some(next)) => {
            let arguments = continuation_cas_arguments(expected, Some(next));
            let query = format!(
                r#"
                UPDATE {} AS continuation
                SET input_stream_id=$5,
                    input_chunk_seq=$6,
                    row_ordinal=$7,
                    remaining_weight=$8
                WHERE continuation.singleton
                  AND continuation.input_stream_id=$1
                  AND continuation.input_chunk_seq=$2
                  AND continuation.row_ordinal=$3
                  AND continuation.remaining_weight IS NOT DISTINCT FROM $4
                RETURNING continuation.singleton
                "#,
                relation.sql()
            );
            require_one_mutation(transaction.write(&query, &arguments)?, "update")?;
        }
        (true, None) => clear_continuation(transaction, relation, expected)?,
        (false, None) => {}
    }
    Ok(())
}

fn clear_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    expected: SinkContinuation,
) -> Result<(), String> {
    if !expected.persisted {
        return Ok(());
    }
    let arguments = unsafe {
        [
            DatumWithOid::new(expected.position.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(expected.position.chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(expected.position.row_ordinal, pg_sys::INT8OID),
            optional_i64(expected.remaining_weight),
        ]
    };
    let query = format!(
        r#"
        DELETE FROM {} AS continuation
        WHERE continuation.singleton
          AND continuation.input_stream_id=$1
          AND continuation.input_chunk_seq=$2
          AND continuation.row_ordinal=$3
          AND continuation.remaining_weight IS NOT DISTINCT FROM $4
        RETURNING continuation.singleton
        "#,
        relation.sql()
    );
    require_one_mutation(transaction.write(&query, &arguments)?, "delete")
}

fn continuation_cas_arguments(
    expected: SinkContinuation,
    next: Option<SinkContinuation>,
) -> [DatumWithOid<'static>; 8] {
    let next = next.expect("Sink continuation update has a successor");
    unsafe {
        [
            DatumWithOid::new(expected.position.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(expected.position.chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(expected.position.row_ordinal, pg_sys::INT8OID),
            optional_i64(expected.remaining_weight),
            DatumWithOid::new(next.position.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(next.position.chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(next.position.row_ordinal, pg_sys::INT8OID),
            optional_i64(next.remaining_weight),
        ]
    }
}

fn optional_i64(value: Option<i64>) -> DatumWithOid<'static> {
    unsafe {
        match value {
            Some(value) => DatumWithOid::new(value, pg_sys::INT8OID),
            None => DatumWithOid::null_oid(pg_sys::INT8OID),
        }
    }
}

fn require_one_mutation(rows: SpiTupleTable<'_>, operation: &str) -> Result<(), String> {
    if rows.len() != 1 {
        return Err(format!(
            "Sink continuation {operation} did not compare-and-set one row"
        ));
    }
    Ok(())
}

fn validate_continuation_abi(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(relation.oid())?;
    let expected = [
        ("singleton", pg_sys::BOOLOID, true),
        ("input_stream_id", pg_sys::INT8OID, true),
        ("input_chunk_seq", pg_sys::INT8OID, true),
        ("row_ordinal", pg_sys::INT8OID, true),
        ("remaining_weight", pg_sys::INT8OID, false),
    ];
    if attributes.len() != expected.len()
        || attributes
            .iter()
            .zip(expected)
            .any(|(actual, (name, type_oid, not_null))| {
                actual.name != name || actual.type_oid != type_oid || actual.not_null != not_null
            })
    {
        return Err("Sink continuation relation has an invalid ABI".into());
    }
    let arguments = unsafe { [DatumWithOid::new(relation.oid(), pg_sys::OIDOID)] };
    let constraints = transaction.read(
        r#"
        SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_constraint AS constraint_catalog
                 WHERE constraint_catalog.conrelid=$1
                   AND constraint_catalog.contype='p'
                   AND constraint_catalog.conkey=ARRAY[1]::smallint[]
               ),
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_constraint AS constraint_catalog
                 WHERE constraint_catalog.conrelid=$1
                   AND constraint_catalog.contype='f'
                   AND constraint_catalog.confrelid=
                         'shiba_internal.effect_stream_chunks'::regclass
                   AND constraint_catalog.conkey=ARRAY[2,3]::smallint[]
                   AND constraint_catalog.confkey=ARRAY[1,2]::smallint[]
                   AND constraint_catalog.confdeltype='r'
               )
        "#,
        &arguments,
    )?;
    let constraints = constraints.first();
    if !required::<bool>(&constraints, 1, "Sink continuation primary key")?
        || !required::<bool>(&constraints, 2, "Sink continuation chunk reference")?
    {
        return Err("Sink continuation relation lacks its authority constraints".into());
    }
    Ok(())
}

fn validate_payload_metadata(
    transaction: &mut StepTxn<'_, '_>,
    payload: &PayloadLayout,
    chunk: &ChunkMeta,
) -> Result<(), String> {
    let query = format!(
        r#"
        SELECT count(*)::bigint,
               min(row_ordinal)::bigint,
               max(row_ordinal)::bigint,
               coalesce(sum(shiba_internal.effect_row_bytes(row_value)),0)::bigint
        FROM {}
        WHERE stream_id=$1 AND chunk_seq=$2
        "#,
        payload.relation.sql()
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
        ]
    };
    let facts = transaction.read(&query, &arguments)?.first();
    let rows = nonnegative(required::<i64>(&facts, 1, "payload rows")?, "payload rows")?;
    let first = facts.get::<i64>(2).map_err(|error| error.to_string())?;
    let last = facts.get::<i64>(3).map_err(|error| error.to_string())?;
    let bytes = nonnegative(
        required::<i64>(&facts, 4, "payload bytes")?,
        "payload bytes",
    )?;
    let expected_last = i64::try_from(chunk.rows)
        .map_err(|_| "payload row count exceeds bigint")?
        .checked_sub(1);
    if rows != chunk.rows || bytes != chunk.bytes || first != Some(0) || last != expected_last {
        return Err("Sink payload does not match immutable chunk metadata".into());
    }
    Ok(())
}

fn effect_heads(
    transaction: &mut StepTxn<'_, '_>,
    payload: &PayloadLayout,
    chunk: &ChunkMeta,
    first_row_ordinal: i64,
    max_rows: usize,
) -> Result<Vec<EffectHead>, String> {
    let query = format!(
        r#"
        SELECT row_ordinal,weight,
               shiba_internal.effect_row_bytes(row_value)::bigint
        FROM {}
        WHERE stream_id=$1
          AND chunk_seq=$2
          AND row_ordinal >= $3
        ORDER BY row_ordinal
        LIMIT $4
        "#,
        payload.relation.sql()
    );
    let max_rows = i64::try_from(max_rows).map_err(|_| "Sink page row limit exceeds bigint")?;
    let arguments = unsafe {
        [
            DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
            DatumWithOid::new(first_row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.read(&query, &arguments)?;
    if rows.is_empty() {
        return Err("Sink effect position has no payload suffix".into());
    }
    let mut heads = Vec::with_capacity(rows.len());
    let mut expected_ordinal = first_row_ordinal;
    for row in rows {
        let row_ordinal = required_row::<i64>(&row, 1, "effect row ordinal")?;
        let weight = required_row::<i64>(&row, 2, "effect weight")?;
        let row_bytes = nonnegative(
            required_row::<i64>(&row, 3, "effect row bytes")?,
            "effect row bytes",
        )?;
        if row_ordinal != expected_ordinal || weight == 0 || row_bytes == 0 {
            return Err("Sink effect page is non-contiguous or invalid".into());
        }
        heads.push(EffectHead {
            row_ordinal,
            weight,
            row_bytes,
        });
        expected_ordinal = expected_ordinal
            .checked_add(1)
            .ok_or_else(|| "Sink effect ordinal exhausted bigint".to_string())?;
    }
    Ok(heads)
}

fn sink_mapping(
    plan: &DataflowPlan,
    sink: &DataflowStage,
    payload: &[AttributeRef],
    target: &[AttributeRef],
) -> Result<SinkMapping, String> {
    let input = sink
        .inputs
        .first()
        .ok_or_else(|| "Sink has no input edge".to_string())?;
    let upstream = plan
        .stages
        .get(
            usize::try_from(input.upstream_stage_id)
                .map_err(|_| "upstream stage ID exceeds usize")?,
        )
        .ok_or_else(|| "Sink input references an absent upstream stage".to_string())?;
    if input.upstream_stage_id as usize >= plan.stages.len()
        || payload.len() != upstream.schema.outputs.len()
        || target.len() != sink.schema.inputs.len()
        || sink.schema.inputs.len() != input.bindings.len()
    {
        return Err("Sink live row shapes do not match its plan".into());
    }

    let mut bindings = BTreeMap::<BindingId, SlotId>::new();
    for binding in &input.bindings {
        if bindings
            .insert(binding.target_binding, binding.source_slot)
            .is_some()
        {
            return Err("Sink input contains duplicate BindingIds".into());
        }
    }
    let mut slots = BTreeMap::<SlotId, usize>::new();
    for (ordinal, output) in upstream.schema.outputs.iter().enumerate() {
        if slots.insert(output.slot, ordinal).is_some() {
            return Err("Sink upstream schema contains duplicate SlotIds".into());
        }
    }

    let mut insert_columns = Vec::with_capacity(target.len());
    let mut select_columns = Vec::with_capacity(target.len());
    let mut delete_predicate = Vec::with_capacity(target.len());
    let mut effect_equality = Vec::with_capacity(payload.len());
    for (ordinal, (input_slot, target_attribute)) in
        sink.schema.inputs.iter().zip(target).enumerate()
    {
        if input_slot.input != 0 {
            return Err("Sink input schema references another port".into());
        }
        let source_slot = bindings
            .get(&input_slot.binding)
            .ok_or_else(|| "Sink BindingId has no input-edge mapping".to_string())?;
        let source_ordinal = *slots
            .get(source_slot)
            .ok_or_else(|| "Sink binding references an absent upstream SlotId".to_string())?;
        let output_slot = &upstream.schema.outputs[source_ordinal];
        let payload_attribute = &payload[source_ordinal];
        if !same_type(&input_slot.type_, target_attribute)
            || input_slot.type_ != output_slot.type_
            || !same_type(&output_slot.type_, payload_attribute)
            || target_attribute.number != i16::try_from(ordinal + 1).unwrap_or(i16::MAX)
            || payload_attribute.number != i16::try_from(source_ordinal + 1).unwrap_or(i16::MAX)
        {
            return Err("Sink BindingId mapping changed type or attribute identity".into());
        }
        let target_name = quote_identifier(&target_attribute.name);
        let payload_name = quote_identifier(&payload_attribute.name);
        insert_columns.push(target_name.clone());
        select_columns.push(format!("(effect.row_value).{payload_name}"));
        delete_predicate.push(format!(
            "target.{target_name} IS NOT DISTINCT FROM (effect.row_value).{payload_name}"
        ));
        effect_equality.push(format!(
            "(previous_effect.row_value).{payload_name} \
             IS NOT DISTINCT FROM (effect.row_value).{payload_name}"
        ));
    }
    Ok(SinkMapping {
        insert_columns: insert_columns.join(","),
        select_columns: select_columns.join(","),
        delete_predicate: delete_predicate.join(" AND "),
        effect_equality: effect_equality.join(" AND "),
    })
}

fn same_type(expected: &SlotType, actual: &AttributeRef) -> bool {
    actual.type_oid.to_u32() == expected.type_oid
        && actual.typmod == expected.typmod
        && actual.collation_oid.to_u32() == expected.collation_oid
}

fn mutate_result_page(
    transaction: &mut StepTxn<'_, '_>,
    result: &RelationRef,
    payload: &PayloadLayout,
    mapping: &SinkMapping,
    chunk: &ChunkMeta,
    actions: &[SinkAction],
) -> Result<u64, String> {
    if actions.is_empty() || actions.iter().any(|action| action.applied_weight == 0) {
        return Err("Sink result page is empty or contains a zero weight".into());
    }
    let action_values = actions
        .iter()
        .map(|action| format!("({},{})", action.row_ordinal, action.applied_weight))
        .collect::<Vec<_>>()
        .join(",");
    let arguments = unsafe {
        [
            DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
        ]
    };
    let query = format!(
        r#"
        WITH action_values(row_ordinal,applied_weight) AS (
          VALUES {action_values}
        ),
        effects AS MATERIALIZED (
          SELECT action.row_ordinal,action.applied_weight,payload.row_value
          FROM action_values AS action
          JOIN {payload} AS payload
            ON payload.stream_id=$1
           AND payload.chunk_seq=$2
           AND payload.row_ordinal=action.row_ordinal
        ),
        inserted AS (
          INSERT INTO {result}({columns})
          SELECT {values}
          FROM effects AS effect
          CROSS JOIN LATERAL pg_catalog.generate_series(
            1,effect.applied_weight
          ) AS copy(ordinal)
          WHERE effect.applied_weight > 0
          RETURNING 1
        ),
        negative_actions AS MATERIALIZED (
          SELECT effect.*,
                 (
                   SELECT coalesce(
                     sum(-previous_effect.applied_weight),0
                   )::bigint
                   FROM effects AS previous_effect
                   WHERE previous_effect.applied_weight < 0
                     AND previous_effect.row_ordinal < effect.row_ordinal
                     AND {effect_equality}
                 ) AS copies_before
          FROM effects AS effect
          WHERE effect.applied_weight < 0
        ),
        victims AS MATERIALIZED (
          SELECT victim.ctid
          FROM negative_actions AS effect
          CROSS JOIN LATERAL (
            SELECT target.ctid
            FROM {result} AS target
            WHERE {predicate}
            ORDER BY target.ctid
            OFFSET effect.copies_before
            LIMIT -effect.applied_weight
            FOR UPDATE OF target
          ) AS victim
        ),
        deleted AS (
          DELETE FROM {result} AS target
          USING victims
          WHERE target.ctid=victims.ctid
          RETURNING 1
        )
        SELECT (SELECT count(*)::bigint FROM inserted) +
               (SELECT count(*)::bigint FROM deleted)
        "#,
        action_values = action_values,
        payload = payload.relation.sql(),
        result = result.sql(),
        columns = mapping.insert_columns,
        values = mapping.select_columns,
        effect_equality = mapping.effect_equality,
        predicate = mapping.delete_predicate,
    );
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Sink set page returned no summary".into());
    }
    nonnegative(
        required::<i64>(&rows.first(), 1, "Sink mutation count")?,
        "Sink mutation count",
    )
}

fn plan_weight_page(
    effect_weight: i64,
    saved_remaining_weight: Option<i64>,
    row_bytes: u64,
    budget: WorkBudget,
) -> Result<WeightPage, String> {
    if effect_weight == 0 || row_bytes == 0 {
        return Err("Sink cannot page a zero effect".into());
    }
    let remaining = saved_remaining_weight.unwrap_or(effect_weight);
    if remaining == 0
        || remaining.signum() != effect_weight.signum()
        || i128::from(remaining).abs() > i128::from(effect_weight).abs()
    {
        return Err("Sink remaining weight is not a suffix of its effect".into());
    }
    let max_rows =
        u64::try_from(budget.max_output_rows).map_err(|_| "Sink row budget exceeds u64")?;
    let max_bytes =
        u64::try_from(budget.max_output_bytes).map_err(|_| "Sink byte budget exceeds u64")?;
    let byte_copies = if row_bytes > max_bytes {
        1
    } else {
        max_bytes / row_bytes
    };
    let copies = u64::try_from(i128::from(remaining).abs())
        .map_err(|_| "Sink remaining weight exceeds u64")?
        .min(max_rows)
        .min(byte_copies)
        .min(i64::MAX as u64);
    if copies == 0 {
        return Err("positive Sink budgets selected no result mutation".into());
    }
    let signed_copies =
        i64::try_from(copies).map_err(|_| "Sink page count exceeds signed bigint")?;
    let applied_weight = if remaining > 0 {
        signed_copies
    } else {
        -signed_copies
    };
    let remaining = i128::from(remaining) - i128::from(applied_weight);
    let remaining_weight = if remaining == 0 {
        None
    } else {
        Some(i64::try_from(remaining).map_err(|_| "Sink remaining weight exceeds bigint")?)
    };
    let output_bytes = row_bytes
        .checked_mul(copies)
        .ok_or_else(|| "Sink page byte count overflowed u64".to_string())?;
    Ok(WeightPage {
        applied_weight,
        remaining_weight,
        usage: WorkUsage {
            input_rows: 1,
            input_bytes: row_bytes,
            output_rows: copies,
            output_bytes,
        },
    })
}

fn advance_completed_chunk(
    transaction: &mut StepTxn<'_, '_>,
    chunk: &ChunkMeta,
    new_frontier_lsn: u64,
) -> Result<(), String> {
    let input = transaction.input(0)?.clone();
    if chunk.stream_id != input.stream_id
        || chunk.sequence != input.next_chunk_seq
        || new_frontier_lsn < input.consumed_frontier_lsn
    {
        return Err("Sink completed chunk does not match its input cursor".into());
    }
    let next_chunk = input
        .next_chunk_seq
        .checked_add(1)
        .ok_or_else(|| "Sink input chunk sequence exhausted".to_string())?;
    advance_input(
        transaction,
        0,
        next_chunk,
        new_frontier_lsn,
        WorkUsage {
            input_rows: chunk.rows,
            input_bytes: chunk.bytes,
            ..WorkUsage::default()
        },
    )
}

fn required<T: FromDatum + IntoDatum>(
    table: &SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    table
        .get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}

fn required_row<T: FromDatum + IntoDatum>(
    row: &SpiHeapTupleData<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    row.get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}

fn nonnegative(value: i64, name: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{name} is negative"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(rows: usize, bytes: usize) -> WorkBudget {
        WorkBudget::new(1, 16, rows, bytes)
    }

    #[test]
    fn positive_weight_is_split_by_both_output_limits() {
        let page = plan_weight_page(10, None, 4, budget(3, 9)).unwrap();
        assert_eq!(page.applied_weight, 2);
        assert_eq!(page.remaining_weight, Some(8));
        assert_eq!(page.usage.output_rows, 2);
        assert_eq!(page.usage.output_bytes, 8);
    }

    #[test]
    fn negative_weight_keeps_a_signed_durable_suffix() {
        let page = plan_weight_page(-7, Some(-5), 3, budget(2, 20)).unwrap();
        assert_eq!(page.applied_weight, -2);
        assert_eq!(page.remaining_weight, Some(-3));
    }

    #[test]
    fn one_oversized_row_is_the_only_byte_exception() {
        let page = plan_weight_page(4, None, 17, budget(8, 16)).unwrap();
        assert_eq!(page.applied_weight, 1);
        assert_eq!(page.remaining_weight, Some(3));
        assert_eq!(page.usage.output_rows, 1);
        assert_eq!(page.usage.output_bytes, 17);
        page.usage.validate(budget(8, 16)).unwrap();
    }

    #[test]
    fn minimum_bigint_weight_resumes_without_overflow() {
        let first = plan_weight_page(i64::MIN, None, 1, budget(3, 10)).unwrap();
        assert_eq!(first.applied_weight, -3);
        assert_eq!(first.remaining_weight, Some(i64::MIN + 3));
        let last = plan_weight_page(-3, Some(-1), 1, budget(3, 10)).unwrap();
        assert_eq!(last.applied_weight, -1);
        assert_eq!(last.remaining_weight, None);
    }

    #[test]
    fn remaining_weight_must_be_a_signed_suffix() {
        assert!(plan_weight_page(5, Some(-1), 1, budget(1, 1)).is_err());
        assert!(plan_weight_page(5, Some(6), 1, budget(1, 1)).is_err());
        assert!(plan_weight_page(-5, Some(-6), 1, budget(1, 1)).is_err());
        assert!(plan_weight_page(5, Some(0), 1, budget(1, 1)).is_err());
    }
}
