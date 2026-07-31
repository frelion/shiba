#[cfg(any(feature = "pg17", feature = "pg18"))]
use crate::execution::register::column_sql;
#[cfg(any(feature = "pg17", feature = "pg18"))]
use crate::planner::model::{JoinSpec, SlotType};
#[cfg(any(feature = "pg17", feature = "pg18"))]
use crate::postgres::quote_identifier;

#[cfg(any(feature = "pg17", feature = "pg18"))]
pub(crate) fn provision(
    client: &mut pgrx::spi::SpiClient<'_>,
    result_oid: pgrx::pg_sys::Oid,
    stage_id: i32,
    stage: &crate::planner::model::DataflowStage,
    input_streams: &[i64],
    output_stream: i64,
) -> Result<(), String> {
    use crate::planner::model::OperatorSpec;

    use crate::execution::register::{
        catalog_continuation, catalog_state, column_sql, qualified_internal, resolve_relation_oid,
        type_sql,
    };
    use crate::execution::storage;

    if result_oid == pgrx::pg_sys::InvalidOid {
        return Err("Join provisioning received an invalid result OID".into());
    }
    if stage_id < 0 {
        return Err("Join provisioning received a negative stage ID".into());
    }
    let OperatorSpec::Join(spec) = &stage.spec else {
        return Err("Join provisioning received another operator".into());
    };
    if stage.inputs.len() != 2 || input_streams.len() != 2 {
        return Err(format!(
            "Join stage {stage_id} does not have exactly two durable inputs"
        ));
    }
    if spec.outputs.len() != stage.schema.outputs.len() {
        return Err(format!(
            "Join stage {stage_id} output expressions do not match its schema"
        ));
    }

    for input in &stage.schema.inputs {
        column_sql(client, &input.type_)?;
    }
    for output in &stage.schema.outputs {
        type_sql(client, &output.type_)?;
    }

    let left_payload = storage::payload(client, input_streams[0])?;
    let right_payload = storage::payload(client, input_streams[1])?;
    let output_payload = storage::payload(client, output_stream)?;
    let output_attributes = storage::composite_attributes(client, &output_payload.row_type)?;
    if output_attributes.len() != stage.schema.outputs.len()
        || output_attributes
            .iter()
            .zip(&stage.schema.outputs)
            .any(|(attribute, output)| {
                attribute.type_oid.to_u32() != output.type_.type_oid
                    || attribute.typmod != output.type_.typmod
                    || attribute.collation_oid.to_u32() != output.type_.collation_oid
            })
    {
        return Err(format!(
            "Join stage {stage_id} output payload changed its plan schema"
        ));
    }

    let result_id = result_oid.to_u32();
    let left_state = qualified_internal(&format!("join_left_state_r{result_id}_s{stage_id}"));
    let right_state = qualified_internal(&format!("join_right_state_r{result_id}_s{stage_id}"));
    let left_key_types = join_key_types(stage, spec, 0)?;
    let right_key_types = join_key_types(stage, spec, 1)?;
    create_join_state(
        client,
        stage_id,
        "left",
        &left_state,
        left_payload.row_type.sql(),
        &left_key_types,
    )?;
    create_join_state(
        client,
        stage_id,
        "right",
        &right_state,
        right_payload.row_type.sql(),
        &right_key_types,
    )?;

    let continuation = qualified_internal(&format!("join_continuation_r{result_id}_s{stage_id}"));
    client
        .update(
            &format!(
                r#"
                CREATE TABLE {continuation}(
                  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                  phase smallint NOT NULL CHECK(phase BETWEEN 1 AND 5),
                  input_side smallint NOT NULL CHECK(input_side IN (0,1)),
                  left_stream_id bigint NOT NULL CHECK(left_stream_id > 0),
                  left_chunk_seq bigint NOT NULL CHECK(left_chunk_seq > 0),
                  left_row_ordinal bigint NOT NULL CHECK(left_row_ordinal >= 0),
                  right_stream_id bigint NOT NULL CHECK(right_stream_id > 0),
                  right_chunk_seq bigint NOT NULL CHECK(right_chunk_seq > 0),
                  right_row_ordinal bigint NOT NULL CHECK(right_row_ordinal >= 0),
                  event_weight bigint,
                  event_bytes bigint,
                  own_row_id bigint,
                  own_multiplicity bigint,
                  own_match_count bigint,
                  own_unknown_count bigint,
                  candidate_after bigint,
                  accumulated_match_count bigint,
                  accumulated_unknown_count bigint,
                  pending_row_id bigint,
                  pending_multiplicity bigint,
                  pending_truth smallint,
                  pending_old_match bigint,
                  pending_old_unknown bigint,
                  pending_new_match bigint,
                  pending_new_unknown bigint,
                  frontier_lsn pg_lsn,
                  CHECK(
                    (input_side = 0 AND right_row_ordinal = 0)
                    OR (input_side = 1 AND left_row_ordinal = 0)
                  ),
                  CHECK(
                    (
                      phase = 1
                      AND event_weight IS NULL
                      AND event_bytes IS NULL
                      AND own_row_id IS NULL
                      AND own_multiplicity IS NULL
                      AND own_match_count IS NULL
                      AND own_unknown_count IS NULL
                      AND candidate_after IS NULL
                      AND accumulated_match_count IS NULL
                      AND accumulated_unknown_count IS NULL
                      AND pending_row_id IS NULL
                      AND pending_multiplicity IS NULL
                      AND pending_truth IS NULL
                      AND pending_old_match IS NULL
                      AND pending_old_unknown IS NULL
                      AND pending_new_match IS NULL
                      AND pending_new_unknown IS NULL
                      AND frontier_lsn IS NULL
                    )
                    OR (
                      phase IN (2,3,4)
                      AND event_weight IS NOT NULL
                      AND event_weight <> 0
                      AND event_bytes IS NOT NULL
                      AND event_bytes > 0
                      AND own_multiplicity IS NOT NULL
                      AND own_match_count IS NOT NULL
                      AND own_match_count >= 0
                      AND own_unknown_count IS NOT NULL
                      AND own_unknown_count >= 0
                      AND (
                        (
                          own_row_id IS NULL
                          AND own_multiplicity = 0
                          AND own_match_count = 0
                          AND own_unknown_count = 0
                          AND event_weight > 0
                        )
                        OR (
                          own_row_id > 0
                          AND own_multiplicity > 0
                          AND own_multiplicity::numeric
                                + event_weight::numeric
                              BETWEEN 0 AND 9223372036854775807::numeric
                        )
                      )
                      AND (candidate_after IS NULL OR candidate_after > 0)
                      AND accumulated_match_count IS NOT NULL
                      AND accumulated_match_count >= 0
                      AND accumulated_unknown_count IS NOT NULL
                      AND accumulated_unknown_count >= 0
                      AND frontier_lsn IS NULL
                      AND (
                        (
                          phase IN (2,4)
                          AND pending_row_id IS NULL
                          AND pending_multiplicity IS NULL
                          AND pending_truth IS NULL
                          AND pending_old_match IS NULL
                          AND pending_old_unknown IS NULL
                          AND pending_new_match IS NULL
                          AND pending_new_unknown IS NULL
                        )
                        OR (
                          phase = 3
                          AND pending_row_id IS NOT NULL
                          AND pending_row_id > coalesce(candidate_after,0)
                          AND pending_multiplicity IS NOT NULL
                          AND pending_multiplicity > 0
                          AND pending_truth = 1
                          AND pending_old_match IS NOT NULL
                          AND pending_old_match >= 0
                          AND pending_old_unknown IS NOT NULL
                          AND pending_old_unknown >= 0
                          AND pending_new_match IS NOT NULL
                          AND pending_new_match >= 0
                          AND pending_new_unknown = pending_old_unknown
                          AND pending_new_match::numeric
                                = pending_old_match::numeric
                                  + event_weight::numeric
                        )
                      )
                    )
                    OR (
                      phase = 5
                      AND event_weight IS NULL
                      AND event_bytes IS NULL
                      AND own_row_id IS NULL
                      AND own_multiplicity IS NULL
                      AND own_match_count IS NULL
                      AND own_unknown_count IS NULL
                      AND candidate_after IS NULL
                      AND accumulated_match_count IS NULL
                      AND accumulated_unknown_count IS NULL
                      AND pending_row_id IS NULL
                      AND pending_multiplicity IS NULL
                      AND pending_truth IS NULL
                      AND pending_old_match IS NULL
                      AND pending_old_unknown IS NULL
                      AND pending_new_match IS NULL
                      AND pending_new_unknown IS NULL
                      AND frontier_lsn IS NOT NULL
                    )
                  )
                )
                "#
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Join stage {stage_id} continuation: {error}"))?;
    protect_join_relation(client, stage_id, "continuation", &continuation)?;

    let left_oid = resolve_relation_oid(client, &left_state)?;
    let right_oid = resolve_relation_oid(client, &right_state)?;
    let continuation_oid = resolve_relation_oid(client, &continuation)?;
    catalog_state(client, result_oid, stage_id, 0, left_oid)?;
    catalog_state(client, result_oid, stage_id, 1, right_oid)?;
    catalog_continuation(client, result_oid, stage_id, continuation_oid)
}

#[cfg(any(feature = "pg17", feature = "pg18"))]
fn join_key_types(
    stage: &crate::planner::model::DataflowStage,
    spec: &JoinSpec,
    input: u16,
) -> Result<Vec<SlotType>, String> {
    let by_binding = stage
        .schema
        .inputs
        .iter()
        .map(|slot| (slot.binding, slot))
        .collect::<std::collections::HashMap<_, _>>();
    spec.equi_keys
        .iter()
        .map(|key| {
            let binding = if input == 0 {
                key.left_binding
            } else {
                key.right_binding
            };
            let slot = by_binding.get(&binding).ok_or_else(|| {
                format!(
                    "Join equality key references missing BindingId {}",
                    binding.0
                )
            })?;
            if slot.input != input {
                return Err(format!(
                    "Join equality key BindingId {} belongs to input {}, expected {}",
                    binding.0, slot.input, input
                ));
            }
            Ok(slot.type_.clone())
        })
        .collect()
}

#[cfg(any(feature = "pg17", feature = "pg18"))]
fn create_join_state(
    client: &mut pgrx::spi::SpiClient<'_>,
    stage_id: i32,
    side: &str,
    relation: &str,
    row_type: &str,
    key_types: &[SlotType],
) -> Result<(), String> {
    let mut columns = vec![
        "row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY".to_string(),
        "row_key bytea NOT NULL UNIQUE".to_string(),
        format!("row_value {row_type} NOT NULL"),
    ];
    for (ordinal, type_) in key_types.iter().enumerate() {
        columns.push(format!("key_{ordinal} {}", column_sql(client, type_)?));
    }
    columns.extend([
        "multiplicity bigint NOT NULL CHECK(multiplicity > 0)".to_string(),
        "match_count bigint NOT NULL CHECK(match_count >= 0)".to_string(),
        "unknown_count bigint NOT NULL CHECK(unknown_count >= 0)".to_string(),
    ]);
    client
        .update(
            &format!("CREATE TABLE {relation}({})", columns.join(",")),
            None,
            &[],
        )
        .map_err(|error| {
            format!("could not create Join stage {stage_id} {side} arrangement: {error}")
        })?;
    if !key_types.is_empty() {
        let relation_name = relation
            .rsplit('.')
            .next()
            .unwrap_or("state")
            .trim_matches('"');
        let index_name = quote_identifier(&format!("{relation_name}_key_idx"));
        let index_columns = (0..key_types.len())
            .map(|ordinal| format!("key_{ordinal}"))
            .chain(std::iter::once("row_id".to_string()))
            .collect::<Vec<_>>();
        client
            .update(
                &format!(
                    "CREATE INDEX {index_name} ON {relation} ({})",
                    index_columns.join(",")
                ),
                None,
                &[],
            )
            .map_err(|error| {
                format!("could not index Join stage {stage_id} {side} arrangement: {error}")
            })?;
    }
    protect_join_relation(client, stage_id, side, relation)
}

#[cfg(any(feature = "pg17", feature = "pg18"))]
fn protect_join_relation(
    client: &mut pgrx::spi::SpiClient<'_>,
    stage_id: i32,
    label: &str,
    relation: &str,
) -> Result<(), String> {
    client
        .update(
            &format!("REVOKE ALL ON TABLE {relation} FROM PUBLIC"),
            None,
            &[],
        )
        .map_err(|error| {
            format!("could not protect Join stage {stage_id} {label} storage: {error}")
        })?;
    Ok(())
}
