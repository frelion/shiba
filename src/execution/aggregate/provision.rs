use super::*;

/// Create the typed, durable relations used by one Aggregate stage.
///
/// This is deliberately a Rust control path. PostgreSQL creates and owns the
/// relations, but no SQL provisioner interprets the serialized plan.
pub(crate) fn provision(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    stage: &DataflowStage,
    input_streams: &[i64],
    output_stream: i64,
) -> Result<(), String> {
    let OperatorSpec::Aggregate(spec) = &stage.spec else {
        return Err("Aggregate provisioner received another operator".into());
    };
    if result_oid == pg_sys::InvalidOid
        || stage_id < 0
        || input_streams.len() != 1
        || input_streams[0] <= 0
        || output_stream <= 0
        || spec.aggregates.is_empty()
        || spec
            .aggregates
            .iter()
            .any(|aggregate| !aggregate.direct_args.is_empty())
    {
        return Err(format!(
            "Aggregate stage {stage_id} has an invalid storage contract"
        ));
    }

    let input = crate::execution::storage::payload(client, input_streams[0])?;
    let output = crate::execution::storage::payload(client, output_stream)?;
    let output_attributes =
        crate::execution::storage::composite_attributes(client, &output.row_type)?;
    if output_attributes.len() != spec.groups.len() + spec.aggregates.len()
        || output_attributes
            .iter()
            .take(spec.groups.len())
            .zip(&spec.groups)
            .any(|(attribute, group)| !attribute_matches_slot(attribute, &group.key.type_))
        || output_attributes
            .iter()
            .skip(spec.groups.len())
            .zip(&spec.aggregates)
            .any(|(attribute, aggregate)| !attribute_matches_slot(attribute, &aggregate.type_))
    {
        return Err("Aggregate output payload does not match its plan schema".into());
    }

    let identity = aggregate_identity_columns(client, spec)?;
    let identity_names = identity
        .iter()
        .map(|(name, _)| quote_identifier(name))
        .collect::<Vec<_>>();
    let identity_definitions = identity
        .iter()
        .map(|(name, definition)| format!("{} {definition}", quote_identifier(name)))
        .collect::<Vec<_>>();
    let identity_csv = identity_names.join(",");
    let identity_index_csv = if spec.groups.is_empty() {
        identity_csv.clone()
    } else {
        spec.groups
            .iter()
            .zip(&identity)
            .map(|(group, (name, _))| {
                resolve_btree_client(client, &group.key, "Aggregate GROUP BY")
                    .map(|capability| capability.index_column(name))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(",")
    };
    let result_number = result_oid.to_u32();

    let groups = create_aggregate_relation(
        client,
        result_oid,
        stage_id,
        1,
        &format!("aggregate_groups_r{result_number}_s{stage_id}"),
        &format!(
            "group_state_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
             {},
             published_present boolean NOT NULL DEFAULT false,
             published_key bytea,
             published_output {},
             pending_present boolean NOT NULL DEFAULT false,
             pending_key bytea,
             pending_output {},
             CHECK(published_present=(published_key IS NOT NULL)),
             CHECK(
               published_present=(
                 published_output IS DISTINCT FROM NULL::{}
               )
             ),
             CHECK(pending_present=(pending_key IS NOT NULL)),
             CHECK(
               pending_present=(
                 pending_output IS DISTINCT FROM NULL::{}
               )
             ),
             CHECK(NOT (published_present AND pending_present))",
            identity_definitions.join(","),
            output.row_type.sql(),
            output.row_type.sql(),
            output.row_type.sql(),
            output.row_type.sql()
        ),
    )?;
    create_aggregate_unique_index(
        client,
        &groups,
        &format!("aggregate_groups_key_r{result_number}_s{stage_id}"),
        &identity_index_csv,
        "group state",
    )?;

    let mut bag_definitions = Vec::new();
    for (aggregate_index, aggregate) in spec.aggregates.iter().enumerate() {
        for (index, order) in aggregate.order_by.iter().enumerate() {
            bag_definitions.push(format!(
                "{} {}",
                quote_identifier(&format!("agg_{}_order_{}", aggregate_index + 1, index + 1)),
                column_sql(client, &order.type_)?
            ));
        }
        for (index, distinct) in aggregate.distinct.iter().enumerate() {
            bag_definitions.push(format!(
                "{} {}",
                quote_identifier(&format!(
                    "agg_{}_distinct_{}",
                    aggregate_index + 1,
                    index + 1
                )),
                column_sql(client, &distinct.type_)?
            ));
        }
    }
    let bag_value_definitions = if bag_definitions.is_empty() {
        String::new()
    } else {
        format!(",{}", bag_definitions.join(","))
    };
    let bag = create_aggregate_relation(
        client,
        result_oid,
        stage_id,
        0,
        &format!("aggregate_bag_r{result_number}_s{stage_id}"),
        &format!(
            "row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
             group_state_id bigint NOT NULL REFERENCES {groups}(group_state_id)
               ON DELETE RESTRICT,
             row_image bytea NOT NULL UNIQUE,
             row_value {} NOT NULL,
             multiplicity bigint NOT NULL CHECK(multiplicity > 0)
             {bag_value_definitions}",
            input.row_type.sql(),
        ),
    )?;
    let bag_index = quote_identifier(&format!("aggregate_bag_group_r{result_number}_s{stage_id}"));
    client
        .update(
            &format!("CREATE INDEX {bag_index} ON {bag}(group_state_id,row_id)"),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Aggregate bag group index: {error}"))?;

    for (aggregate_index, aggregate) in spec.aggregates.iter().enumerate() {
        let ordinal = aggregate_index + 1;
        let output_attribute = &output_attributes[spec.groups.len() + aggregate_index];
        let transition_type = aggregate_transition_column(
            client,
            aggregate.function_oid,
            output_attribute.type_oid,
            aggregate.args.len(),
            aggregate.input_collation_oid,
        )?;
        let effective_order = aggregate_effective_order(ordinal, aggregate)?;
        if !effective_order.is_empty() {
            let mut index_columns = vec!["group_state_id ASC".into()];
            for key in &effective_order {
                index_columns.push(
                    resolve_btree_client(client, key.expression, "Aggregate")?
                        .index_column(&key.column),
                );
            }
            index_columns.push("row_id ASC".into());
            let index = quote_identifier(&format!(
                "aggregate_bag_order_r{result_number}_s{stage_id}_a{ordinal}"
            ));
            client
                .update(
                    &format!("CREATE INDEX {index} ON {bag}({})", index_columns.join(",")),
                    None,
                    &[],
                )
                .map_err(|error| {
                    format!("could not create Aggregate {ordinal} bounded rebuild index: {error}")
                })?;
        }
        let mut work_definitions = vec![format!(
            "group_state_id bigint PRIMARY KEY REFERENCES {groups}(group_state_id)
               ON DELETE RESTRICT"
        )];
        work_definitions.push(format!("transition_state {transition_type}"));
        work_definitions.push("no_trans_value boolean NOT NULL".into());
        work_definitions.push("has_cursor boolean NOT NULL DEFAULT false".into());
        work_definitions.push("cursor_row_id bigint CHECK(cursor_row_id > 0)".into());
        work_definitions
            .push("remaining_multiplicity bigint CHECK(remaining_multiplicity > 0)".into());
        work_definitions.push("complete boolean NOT NULL DEFAULT false".into());
        for (index, key) in effective_order.iter().enumerate() {
            work_definitions.push(format!(
                "{} {}",
                quote_identifier(&format!("cursor_order_{}", index + 1)),
                column_sql(client, &key.expression.type_)?
            ));
        }
        work_definitions.push("has_distinct_cursor boolean NOT NULL DEFAULT false".into());
        work_definitions.push("distinct_transitioned boolean NOT NULL DEFAULT false".into());
        for (index, distinct) in aggregate.distinct.iter().enumerate() {
            work_definitions.push(format!(
                "{} {}",
                quote_identifier(&format!("cursor_distinct_{}", index + 1)),
                column_sql(client, &distinct.type_)?
            ));
        }
        work_definitions.push("CHECK(has_cursor = (cursor_row_id IS NOT NULL))".into());
        work_definitions.push("CHECK(has_distinct_cursor OR NOT distinct_transitioned)".into());
        let work = create_aggregate_relation(
            client,
            result_oid,
            stage_id,
            i32::try_from(2 + aggregate_index)
                .map_err(|_| "Aggregate work slot exceeds integer")?,
            &format!("aggregate_work_r{result_number}_s{stage_id}_a{ordinal}"),
            &work_definitions.join(","),
        )?;
        let _ = work;
    }

    create_aggregate_relation(
        client,
        result_oid,
        stage_id,
        2000,
        &format!("aggregate_dirty_r{result_number}_s{stage_id}"),
        &format!(
            "queue_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
             causal_lsn pg_lsn NOT NULL,
             group_state_id bigint NOT NULL UNIQUE
               REFERENCES {groups}(group_state_id) ON DELETE RESTRICT"
        ),
    )?;

    let continuation_name = format!("aggregate_cont_r{result_number}_s{stage_id}");
    let continuation = qualified_internal(&continuation_name);
    client
        .update(
            &format!(
                "CREATE TABLE {continuation}(
                   singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                   phase smallint NOT NULL CHECK(phase BETWEEN 1 AND 4),
                   input_stream_id bigint NOT NULL CHECK(input_stream_id > 0),
                   input_chunk_seq bigint CHECK(input_chunk_seq > 0),
                   input_row_ordinal bigint CHECK(input_row_ordinal >= 0),
                   group_queue_id bigint CHECK(group_queue_id > 0),
                   aggregate_ordinal integer
                     CHECK(aggregate_ordinal BETWEEN 1 AND {}),
                   emit_leg smallint CHECK(emit_leg IN (1,2)),
                   after_kind smallint CHECK(after_kind IN (1,2,3)),
                   after_chunk_seq bigint CHECK(after_chunk_seq > 0),
                   after_row_ordinal bigint CHECK(after_row_ordinal >= 0),
                   CHECK(
                     (phase IN (1,4)
                       AND input_chunk_seq IS NOT NULL
                       AND input_row_ordinal IS NOT NULL
                       AND group_queue_id IS NULL
                       AND aggregate_ordinal IS NULL
                       AND emit_leg IS NULL
                       AND after_kind IS NULL
                       AND after_chunk_seq IS NULL
                       AND after_row_ordinal IS NULL)
                     OR
                     (phase=2
                       AND input_chunk_seq IS NULL
                       AND input_row_ordinal IS NULL
                       AND group_queue_id IS NOT NULL
                       AND aggregate_ordinal IS NOT NULL
                       AND emit_leg IS NULL
                       AND after_kind IS NOT NULL)
                     OR
                     (phase=3
                       AND input_chunk_seq IS NULL
                       AND input_row_ordinal IS NULL
                       AND group_queue_id IS NOT NULL
                       AND aggregate_ordinal IS NULL
                       AND emit_leg IS NOT NULL
                       AND after_kind IS NOT NULL)
                   ),
                   CHECK(
                     after_kind IS NULL
                     OR (after_kind=1
                         AND after_chunk_seq IS NOT NULL
                         AND after_row_ordinal IS NOT NULL)
                     OR (after_kind=2
                         AND after_chunk_seq IS NULL
                         AND after_row_ordinal IS NULL)
                     OR (after_kind=3
                         AND after_chunk_seq IS NOT NULL
                         AND after_row_ordinal=0)
                   ),
                   FOREIGN KEY(input_stream_id,input_chunk_seq)
                     REFERENCES shiba_internal.effect_stream_chunks(
                       stream_id,chunk_seq
                     ) ON DELETE RESTRICT
                 )",
                spec.aggregates.len()
            ),
            None,
            &[],
        )
        .map_err(|error| {
            format!("could not create Aggregate stage {stage_id} continuation: {error}")
        })?;
    revoke_aggregate_relation(client, &continuation, "continuation")?;
    let continuation_oid = resolve_relation_oid(client, &continuation)?;
    catalog_continuation(client, result_oid, stage_id, continuation_oid)
}

fn aggregate_identity_columns(
    client: &mut SpiClient<'_>,
    spec: &AggregateSpec,
) -> Result<Vec<(String, String)>, String> {
    if spec.groups.is_empty() {
        return Ok(vec![(
            "global_group".into(),
            "boolean NOT NULL DEFAULT true CHECK(global_group)".into(),
        )]);
    }
    spec.groups
        .iter()
        .enumerate()
        .map(|(index, group)| {
            Ok((
                format!("group_{}", index + 1),
                column_sql(client, &group.key.type_)?,
            ))
        })
        .collect()
}

fn aggregate_transition_column(
    client: &mut SpiClient<'_>,
    function_oid: u32,
    output_type_oid: pg_sys::Oid,
    argument_count: usize,
    input_collation_oid: u32,
) -> Result<String, String> {
    let function_oid = pg_sys::Oid::from_u32(function_oid);
    if function_oid == pg_sys::InvalidOid {
        return Err("Aggregate function OID is invalid".into());
    }
    let arguments = unsafe {
        [
            DatumWithOid::new(function_oid, pg_sys::OIDOID),
            DatumWithOid::new(output_type_oid, pg_sys::OIDOID),
        ]
    };
    let rows = client
        .select(AGGREGATE_CAPABILITY_SQL, Some(1), &arguments)
        .map_err(|error| format!("could not resolve Aggregate capability: {error}"))?;
    let capability = decode_aggregate_capability(
        rows,
        function_oid.to_u32(),
        argument_count,
        input_collation_oid,
    )?;
    column_sql(
        client,
        &crate::planner::model::SlotType {
            type_oid: capability.transition_type_oid.to_u32(),
            typmod: -1,
            collation_oid: capability.transition_collation_oid.to_u32(),
            nullable: true,
        },
    )
}

fn create_aggregate_relation(
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
            format!("could not create Aggregate stage {stage_id} state slot {slot}: {error}")
        })?;
    revoke_aggregate_relation(client, &relation, "state")?;
    let relation_oid = resolve_relation_oid(client, &relation)?;
    catalog_state(client, result_oid, stage_id, slot, relation_oid)?;
    Ok(relation)
}

fn create_aggregate_unique_index(
    client: &mut SpiClient<'_>,
    relation: &str,
    name: &str,
    columns: &str,
    label: &str,
) -> Result<(), String> {
    // An index is always created in its table's schema; PostgreSQL accepts
    // only an unqualified index name in this grammar position.
    let index = quote_identifier(name);
    client
        .update(
            &format!("CREATE UNIQUE INDEX {index} ON {relation}({columns}) NULLS NOT DISTINCT"),
            None,
            &[],
        )
        .map_err(|error| format!("Aggregate {label} lacks durable B-tree semantics: {error}"))?;
    Ok(())
}

fn revoke_aggregate_relation(
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
        .map_err(|error| format!("could not protect Aggregate {label}: {error}"))?;
    Ok(())
}

pub(super) fn aggregate_nonnegative(value: i64) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| "Aggregate returned a negative count".into())
}
