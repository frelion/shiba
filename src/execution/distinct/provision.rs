use super::*;

/// Create the exact-key state, physical representative bag, pending effect
/// queue, and phaseful continuation used by one Distinct stage.
pub(crate) fn provision(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    stage: &DataflowStage,
    input_streams: &[i64],
    output_stream: i64,
) -> Result<(), String> {
    let OperatorSpec::Distinct(spec) = &stage.spec else {
        return Err("Distinct provisioner received another operator".into());
    };
    if result_oid == pg_sys::InvalidOid
        || stage_id < 0
        || input_streams.len() != 1
        || input_streams[0] <= 0
        || output_stream <= 0
        || spec.keys.is_empty()
        || spec.outputs.is_empty()
    {
        return Err(format!(
            "Distinct stage {stage_id} has an invalid storage contract"
        ));
    }
    let output = crate::execution::storage::payload(client, output_stream)?;
    let output_attributes =
        crate::execution::storage::composite_attributes(client, &output.row_type)?;
    if output_attributes.len() != stage.schema.outputs.len() {
        return Err("Distinct output payload does not match its plan schema".into());
    }

    let result_number = result_oid.to_u32();
    let key_columns = (1..=spec.keys.len())
        .map(|index| format!("key_{index}"))
        .collect::<Vec<_>>();
    let key_definitions = key_columns
        .iter()
        .zip(&spec.keys)
        .map(|(column, key)| {
            Ok(format!(
                "{} {}",
                quote_identifier(column),
                column_sql(client, &key.type_)?
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let key_index = key_columns
        .iter()
        .zip(&spec.keys)
        .map(|(column, key)| {
            resolve_btree_client(client, key, "Distinct").map(|order| order.index_column(column))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let state = create_distinct_state(
        client,
        result_oid,
        stage_id,
        0,
        &format!("distinct_groups_r{result_number}_s{stage_id}"),
        &format!(
            "group_state_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
             {},
             output_key bytea,
             output_row {},
             multiplicity bigint NOT NULL DEFAULT 0 CHECK(multiplicity >= 0),
             CHECK((multiplicity=0)=(output_key IS NULL)),
             CHECK((multiplicity=0)=((output_row)::text IS NULL))",
            key_definitions.join(","),
            output.row_type.sql(),
        ),
    )?;
    let state_index =
        quote_identifier(&format!("distinct_groups_key_r{result_number}_s{stage_id}"));
    client
        .update(
            &format!(
                "CREATE UNIQUE INDEX {state_index} ON {state}({})
                 NULLS NOT DISTINCT",
                key_index.join(",")
            ),
            None,
            &[],
        )
        .map_err(|error| {
            format!("Distinct stage {stage_id} keys lack exact B-tree semantics: {error}")
        })?;
    create_distinct_state(
        client,
        result_oid,
        stage_id,
        1,
        &format!("distinct_bag_r{result_number}_s{stage_id}"),
        &format!(
            "bag_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
             group_state_id bigint NOT NULL REFERENCES {state}(group_state_id)
               ON DELETE RESTRICT,
             output_key bytea NOT NULL,
             output_row {} NOT NULL,
             multiplicity bigint NOT NULL CHECK(multiplicity > 0),
             UNIQUE(group_state_id,output_key)",
            output.row_type.sql(),
        ),
    )?;
    create_distinct_state(
        client,
        result_oid,
        stage_id,
        2,
        &format!("distinct_effect_queue_r{result_number}_s{stage_id}"),
        &format!(
            "queue_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
             output_key bytea NOT NULL,
             weight bigint NOT NULL CHECK(weight IN (-1,1)),
             output_row {} NOT NULL,
             row_bytes bigint NOT NULL CHECK(row_bytes > 0),
             causal_lsn pg_lsn NOT NULL",
            output.row_type.sql(),
        ),
    )?;
    create_distinct_state(
        client,
        result_oid,
        stage_id,
        3,
        &format!("distinct_touched_r{result_number}_s{stage_id}"),
        "group_state_id bigint PRIMARY KEY,
         net_weight numeric NOT NULL",
    )?;

    let continuation_name = format!("continuation_r{result_number}_s{stage_id}");
    let continuation = qualified_internal(&continuation_name);
    client
        .update(
            &format!(
                "CREATE TABLE {continuation}(
                   singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                   phase smallint NOT NULL CHECK(phase IN (1,2)),
                   input_stream_id bigint NOT NULL,
                   input_chunk_seq bigint NOT NULL CHECK(input_chunk_seq > 0),
                   next_row_ordinal bigint NOT NULL CHECK(next_row_ordinal >= 0),
                   FOREIGN KEY(input_stream_id,input_chunk_seq)
                     REFERENCES shiba_internal.effect_stream_chunks(
                       stream_id,chunk_seq
                     ) ON DELETE RESTRICT
                 )"
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Distinct continuation: {error}"))?;
    revoke_distinct_relation(client, &continuation, "continuation")?;
    let continuation_oid = resolve_relation_oid(client, &continuation)?;
    catalog_continuation(client, result_oid, stage_id, continuation_oid)
}

fn create_distinct_state(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    slot: i32,
    name: &str,
    body: &str,
) -> Result<String, String> {
    let relation = qualified_internal(name);
    client
        .update(&format!("CREATE TABLE {relation}({body})"), None, &[])
        .map_err(|error| {
            format!("could not create Distinct stage {stage_id} state slot {slot}: {error}")
        })?;
    revoke_distinct_relation(client, &relation, "state")?;
    let relation_oid = resolve_relation_oid(client, &relation)?;
    catalog_state(client, result_oid, stage_id, slot, relation_oid)?;
    Ok(relation)
}

fn revoke_distinct_relation(
    client: &mut SpiClient<'_>,
    relation: &str,
    label: &str,
) -> Result<(), String> {
    client
        .update(
            &format!("REVOKE ALL ON TABLE {relation} FROM PUBLIC"),
            None,
            &[],
        )
        .map_err(|error| format!("could not protect Distinct {label}: {error}"))?;
    Ok(())
}
