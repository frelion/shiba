use super::*;

/// Provision the only storage layout understood by this kernel.
///
/// There is intentionally no schema-version branch: a relation that does not
/// have this exact typed ABI is rejected by `execute`.
pub(crate) fn provision(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    stage: &DataflowStage,
    input_streams: &[i64],
    output_stream: i64,
) -> Result<(), String> {
    let OperatorSpec::TopN(spec) = &stage.spec else {
        return Err("TopN provisioner received another operator".into());
    };
    if result_oid == pg_sys::InvalidOid
        || stage_id < 0
        || input_streams.len() != 1
        || input_streams[0] <= 0
        || output_stream <= 0
        || spec.outputs.is_empty()
    {
        return Err(format!(
            "TopN stage {stage_id} has an invalid storage contract"
        ));
    }
    let input_payload = crate::execution::storage::payload(client, input_streams[0])?;
    let output_payload = crate::execution::storage::payload(client, output_stream)?;
    let output_attributes =
        crate::execution::storage::composite_attributes(client, &output_payload.row_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
    let prefix = format!("r{}_s{stage_id}", result_oid.to_u32());

    let input_name = format!("topn_input_{prefix}");
    let input = qualified_internal(&input_name);
    let mut key_definitions = Vec::with_capacity(spec.order_by.len());
    let mut index_columns = Vec::with_capacity(spec.order_by.len() + 1);
    for (index, order) in spec.order_by.iter().enumerate() {
        let name = format!("key_{}", index + 1);
        let mut definition = format!(
            "{} {}",
            quote_identifier(&name),
            column_sql(client, &order.type_)?
        );
        if !order.type_.nullable {
            definition.push_str(" NOT NULL");
        }
        key_definitions.push(definition);
        index_columns.push(resolve_btree_client(client, order, "TopN")?.index_column(&name));
    }
    index_columns.push("entry_id ASC".into());
    let key_suffix = if key_definitions.is_empty() {
        String::new()
    } else {
        format!(",{}", key_definitions.join(","))
    };
    create_topn_relation(
        client,
        &input,
        &format!(
            r#"
            entry_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            row_key bytea NOT NULL UNIQUE,
            row_value {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity > 0 AND multiplicity=pg_catalog.trunc(multiplicity)
            )
            {key_suffix}
            "#,
            input_payload.row_type.sql()
        ),
        "input",
    )?;
    let order_index = quote_identifier(&format!("topn_order_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {order_index} ON {input}({})",
                index_columns.join(",")
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create TopN ordering index: {error}"))?;
    let input_oid = resolve_relation_oid(client, &input)?;
    catalog_state(client, result_oid, stage_id, 0, input_oid)?;

    let candidate_name = format!("topn_candidate_{prefix}");
    let candidate = qualified_internal(&candidate_name);
    create_topn_relation(
        client,
        &candidate,
        &format!(
            r#"
            candidate_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            generation_id bigint NOT NULL CHECK(generation_id > 0),
            output_key bytea NOT NULL,
            output_row {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity > 0 AND multiplicity=pg_catalog.trunc(multiplicity)
            ),
            UNIQUE(generation_id,output_key)
            "#,
            output_payload.row_type.sql()
        ),
        "candidate",
    )?;
    let candidate_index = quote_identifier(&format!("topn_candidate_page_{prefix}"));
    client
        .update(
            &format!("CREATE INDEX {candidate_index} ON {candidate}(generation_id,candidate_id)"),
            None,
            &[],
        )
        .map_err(|error| format!("could not create TopN candidate page index: {error}"))?;
    let candidate_oid = resolve_relation_oid(client, &candidate)?;
    catalog_state(client, result_oid, stage_id, 1, candidate_oid)?;

    let visible_name = format!("topn_visible_{prefix}");
    let visible = qualified_internal(&visible_name);
    create_topn_relation(
        client,
        &visible,
        &format!(
            r#"
            visible_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            output_key bytea NOT NULL UNIQUE,
            output_row {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity > 0 AND multiplicity=pg_catalog.trunc(multiplicity)
            )
            "#,
            output_payload.row_type.sql()
        ),
        "visible",
    )?;
    let visible_oid = resolve_relation_oid(client, &visible)?;
    catalog_state(client, result_oid, stage_id, 2, visible_oid)?;

    let control_name = format!("topn_control_{prefix}");
    let control = qualified_internal(&control_name);
    create_topn_relation(
        client,
        &control,
        r#"
        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
        dirty boolean NOT NULL DEFAULT false,
        causal_lsn pg_lsn,
        CHECK(dirty = (causal_lsn IS NOT NULL))
        "#,
        "control",
    )?;
    client
        .update(
            &format!("INSERT INTO {control}(singleton) VALUES(true)"),
            Some(1),
            &[],
        )
        .map_err(|error| format!("could not seed TopN control state: {error}"))?;
    let control_oid = resolve_relation_oid(client, &control)?;
    catalog_state(client, result_oid, stage_id, 3, control_oid)?;

    let continuation_name = format!("topn_continuation_{prefix}");
    let continuation = qualified_internal(&continuation_name);
    create_topn_relation(
        client,
        &continuation,
        r#"
        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
        phase smallint NOT NULL CHECK(phase BETWEEN 1 AND 5),
        input_stream_id bigint NOT NULL CHECK(input_stream_id > 0),
        input_chunk_seq bigint CHECK(input_chunk_seq > 0),
        input_row_ordinal bigint CHECK(input_row_ordinal >= 0),
        generation_id bigint CHECK(generation_id > 0),
        cursor_row_id bigint CHECK(cursor_row_id > 0),
        cursor_repeat boolean NOT NULL DEFAULT false,
        offset_remaining numeric CHECK(
          offset_remaining >= 0
          AND offset_remaining=pg_catalog.trunc(offset_remaining)
        ),
        limit_remaining numeric CHECK(
          limit_remaining >= 0
          AND limit_remaining=pg_catalog.trunc(limit_remaining)
        ),
        tie_boundary_row_id bigint CHECK(tie_boundary_row_id > 0),
        diff_leg smallint CHECK(diff_leg IN (1,2)),
        after_kind smallint CHECK(after_kind IN (1,2,3)),
        after_chunk_seq bigint CHECK(after_chunk_seq > 0),
        after_row_ordinal bigint CHECK(after_row_ordinal >= 0),
        FOREIGN KEY(input_stream_id,input_chunk_seq)
          REFERENCES shiba_internal.effect_stream_chunks(stream_id,chunk_seq)
          ON DELETE RESTRICT,
        CHECK(
          (phase IN (1,5) AND input_chunk_seq IS NOT NULL
           AND input_row_ordinal IS NOT NULL
           AND generation_id IS NULL AND cursor_row_id IS NULL
           AND NOT cursor_repeat
           AND offset_remaining IS NULL AND limit_remaining IS NULL
           AND tie_boundary_row_id IS NULL AND diff_leg IS NULL
           AND after_kind IS NULL AND after_chunk_seq IS NULL
           AND after_row_ordinal IS NULL)
          OR
          (phase=2 AND input_chunk_seq IS NULL AND input_row_ordinal IS NULL
           AND generation_id IS NOT NULL
           AND NOT cursor_repeat
           AND offset_remaining IS NOT NULL AND limit_remaining IS NOT NULL
           AND diff_leg IS NULL AND after_kind IS NOT NULL)
          OR
          (phase=3 AND input_chunk_seq IS NULL AND input_row_ordinal IS NULL
           AND generation_id IS NOT NULL
           AND offset_remaining IS NULL AND limit_remaining IS NULL
           AND tie_boundary_row_id IS NULL AND diff_leg IS NOT NULL
           AND after_kind IS NOT NULL)
          OR
          (phase=4 AND input_chunk_seq IS NULL AND input_row_ordinal IS NULL
           AND generation_id IS NOT NULL
           AND NOT cursor_repeat
           AND offset_remaining IS NULL AND limit_remaining IS NULL
           AND tie_boundary_row_id IS NULL AND diff_leg IS NULL
           AND after_kind IS NOT NULL)
        ),
        CHECK(NOT cursor_repeat OR (phase=3 AND cursor_row_id IS NOT NULL)),
        CHECK(
          after_kind IS NULL
          OR (after_kind=1 AND after_chunk_seq IS NOT NULL
              AND after_row_ordinal IS NOT NULL)
          OR (after_kind=2 AND after_chunk_seq IS NULL
              AND after_row_ordinal IS NULL)
          OR (after_kind=3 AND after_chunk_seq IS NOT NULL
              AND after_row_ordinal=0)
        )
        "#,
        "continuation",
    )?;
    let continuation_oid = resolve_relation_oid(client, &continuation)?;
    catalog_continuation(client, result_oid, stage_id, continuation_oid)?;
    Ok(())
}

fn create_topn_relation(
    client: &mut SpiClient<'_>,
    relation: &str,
    body: &str,
    label: &str,
) -> Result<(), String> {
    client
        .update(&format!("CREATE TABLE {relation}({body})"), None, &[])
        .map_err(|error| format!("could not create TopN {label} relation: {error}"))?;
    client
        .update(
            &format!("REVOKE ALL ON TABLE {relation} FROM PUBLIC"),
            None,
            &[],
        )
        .map_err(|error| format!("could not protect TopN {label} relation: {error}"))?;
    Ok(())
}
