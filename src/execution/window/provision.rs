use super::*;

/// Create the sole Window storage ABI understood by `execute`.
pub(crate) fn provision(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    stage: &DataflowStage,
    input_streams: &[i64],
    output_stream: i64,
) -> Result<(), String> {
    let OperatorSpec::Window(spec) = &stage.spec else {
        return Err("Window provisioner received another operator".into());
    };
    if result_oid == pg_sys::InvalidOid
        || stage_id < 0
        || input_streams.len() != 1
        || input_streams[0] <= 0
        || output_stream <= 0
        || spec.functions.is_empty()
    {
        return Err(format!(
            "Window stage {stage_id} has an invalid storage contract"
        ));
    }
    validate_window_frame(spec)?;
    let input_payload = crate::execution::storage::payload(client, input_streams[0])?;
    let output_payload = crate::execution::storage::payload(client, output_stream)?;
    let output_attributes =
        crate::execution::storage::composite_attributes(client, &output_payload.row_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
    let prefix = format!("r{}_s{stage_id}", result_oid.to_u32());

    let mut partition_definitions = Vec::with_capacity(spec.partition_by.len());
    let mut partition_columns = Vec::with_capacity(spec.partition_by.len());
    for (index, key) in spec.partition_by.iter().enumerate() {
        let name = format!("partition_{}", index + 1);
        let mut definition = format!(
            "{} {}",
            quote_identifier(&name),
            column_sql(client, &key.type_)?
        );
        if !key.type_.nullable {
            definition.push_str(" NOT NULL");
        }
        partition_definitions.push(definition);
        partition_columns.push(quote_identifier(&name));
    }
    let partition_suffix = if partition_definitions.is_empty() {
        String::new()
    } else {
        format!(",{}", partition_definitions.join(","))
    };
    let partitions = create_window_state(
        client,
        result_oid,
        stage_id,
        0,
        &format!("window_partitions_{prefix}"),
        &format!(
            r#"
            partition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            dirty boolean NOT NULL DEFAULT false,
            causal_lsn pg_lsn,
            row_count numeric NOT NULL DEFAULT 0 CHECK(
              row_count>=0 AND row_count<=9223372036854775807::numeric
              AND row_count=pg_catalog.trunc(row_count)
            )
            {partition_suffix}
            "#
        ),
    )?;
    if partition_columns.is_empty() {
        client
            .update(
                &format!("INSERT INTO {partitions}(dirty,row_count) VALUES(false,0)"),
                Some(1),
                &[],
            )
            .map_err(|error| format!("could not seed Window global partition: {error}"))?;
    } else {
        let index = quote_identifier(&format!("window_partition_keys_{prefix}"));
        client
            .update(
                &format!(
                    "CREATE UNIQUE INDEX {index} ON {partitions}({}) NULLS NOT DISTINCT",
                    partition_columns.join(",")
                ),
                None,
                &[],
            )
            .map_err(|error| format!("could not create Window partition index: {error}"))?;
    }
    let dirty_partition_index = quote_identifier(&format!("window_dirty_partitions_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {dirty_partition_index} \
                 ON {partitions}(partition_id) WHERE dirty"
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window dirty-partition index: {error}"))?;

    let mut order_definitions = Vec::with_capacity(spec.order_by.len());
    let mut order_index = Vec::with_capacity(spec.order_by.len() + 2);
    order_index.push("partition_id ASC".into());
    for (index, order) in spec.order_by.iter().enumerate() {
        let name = format!("order_{}", index + 1);
        let mut definition = format!(
            "{} {}",
            quote_identifier(&name),
            column_sql(client, &order.type_)?
        );
        if !order.type_.nullable {
            definition.push_str(" NOT NULL");
        }
        order_definitions.push(definition);
        order_index.push(resolve_btree_client(client, order, "Window")?.index_column(&name));
    }
    order_index.push("entry_id ASC".into());
    let order_suffix = if order_definitions.is_empty() {
        String::new()
    } else {
        format!(",{}", order_definitions.join(","))
    };
    let input = create_window_state(
        client,
        result_oid,
        stage_id,
        1,
        &format!("window_input_{prefix}"),
        &format!(
            r#"
            entry_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            row_key bytea NOT NULL UNIQUE,
            row_value {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity>0 AND multiplicity<=9223372036854775807::numeric
              AND multiplicity=pg_catalog.trunc(multiplicity)
            ),
            partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
              ON DELETE RESTRICT
            {order_suffix}
            "#,
            input_payload.row_type.sql()
        ),
    )?;
    let input_index = quote_identifier(&format!("window_input_order_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {input_index} ON {input}({})",
                order_index.join(",")
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window input order index: {error}"))?;

    let mut function_columns = Vec::with_capacity(spec.functions.len());
    let mut capabilities = Vec::with_capacity(spec.functions.len());
    for (index, function) in spec.functions.iter().enumerate() {
        function_columns.push(format!(
            "{} {}",
            quote_identifier(&format!("function_{}", index + 1)),
            column_sql(client, &function.type_)?
        ));
        capabilities.push(resolve_window_function_client(client, function)?);
    }
    let ordered = create_window_state(
        client,
        result_oid,
        stage_id,
        2,
        &format!("window_ordered_{prefix}"),
        &format!(
            r#"
            ordinal bigint PRIMARY KEY CHECK(ordinal>0),
            entry_id bigint NOT NULL REFERENCES {input}(entry_id) ON DELETE RESTRICT,
            copy_ordinal bigint NOT NULL CHECK(copy_ordinal>0),
            peer_id bigint,
            {},
            UNIQUE(entry_id,copy_ordinal)
            "#,
            function_columns.join(",")
        ),
    )?;
    let peer_index = quote_identifier(&format!("window_ordered_peer_{prefix}"));
    client
        .update(
            &format!("CREATE INDEX {peer_index} ON {ordered}(peer_id,ordinal)"),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window peer index: {error}"))?;

    let _peers = create_window_state(
        client,
        result_oid,
        stage_id,
        3,
        &format!("window_peers_{prefix}"),
        r#"
        peer_id bigint PRIMARY KEY CHECK(peer_id>0),
        first_ordinal bigint NOT NULL CHECK(first_ordinal>0),
        last_ordinal bigint NOT NULL CHECK(last_ordinal>=first_ordinal)
        "#,
    )?;
    let _frames = create_window_state(
        client,
        result_oid,
        stage_id,
        4,
        &format!("window_frames_{prefix}"),
        r#"
        ordinal bigint PRIMARY KEY CHECK(ordinal>0),
        start_1 bigint,end_1 bigint,start_2 bigint,end_2 bigint,
        start_3 bigint,end_3 bigint,
        frame_count bigint NOT NULL CHECK(frame_count>=0)
        "#,
    )?;
    let candidate = create_window_state(
        client,
        result_oid,
        stage_id,
        5,
        &format!("window_candidate_{prefix}"),
        &format!(
            r#"
            candidate_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
              ON DELETE RESTRICT,
            output_key bytea NOT NULL,
            output_row {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity>0 AND multiplicity=pg_catalog.trunc(multiplicity)
            ),
            UNIQUE(partition_id,output_key)
            "#,
            output_payload.row_type.sql()
        ),
    )?;
    let candidate_page_index = quote_identifier(&format!("window_candidate_page_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {candidate_page_index} \
                 ON {candidate}(partition_id,candidate_id)"
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window candidate page index: {error}"))?;
    let visible = create_window_state(
        client,
        result_oid,
        stage_id,
        6,
        &format!("window_visible_{prefix}"),
        &format!(
            r#"
            visible_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
              ON DELETE RESTRICT,
            output_key bytea NOT NULL,
            output_row {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity>0 AND multiplicity=pg_catalog.trunc(multiplicity)
            ),
            UNIQUE(partition_id,output_key)
            "#,
            output_payload.row_type.sql()
        ),
    )?;
    let visible_page_index = quote_identifier(&format!("window_visible_page_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {visible_page_index} \
                 ON {visible}(partition_id,visible_id)"
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window visible page index: {error}"))?;
    for (index, capability) in capabilities.iter().enumerate() {
        match capability {
            WindowFunctionCapability::Aggregate(capability) => {
                let transition_column_sql = column_sql(
                    client,
                    &SlotType {
                        type_oid: capability.transition_type_oid.to_u32(),
                        typmod: -1,
                        collation_oid: capability.transition_collation_oid.to_u32(),
                        nullable: true,
                    },
                )?;
                create_window_state(
                    client,
                    result_oid,
                    stage_id,
                    i32::try_from(1001 + index)
                        .map_err(|_| "Window accumulator slot exceeds integer")?,
                    &format!("window_accumulator_{prefix}_f{}", index + 1),
                    &format!(
                        r#"
                        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                        partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
                          ON DELETE RESTRICT,
                        output_ordinal bigint NOT NULL CHECK(output_ordinal>0),
                        state_value {},
                        no_trans_value boolean NOT NULL,
                        UNIQUE(partition_id,output_ordinal)
                        "#,
                        transition_column_sql
                    ),
                )?;
            }
            WindowFunctionCapability::Native(NativeWindow::Ntile) => {
                create_window_state(
                    client,
                    result_oid,
                    stage_id,
                    i32::try_from(2001 + index)
                        .map_err(|_| "Window ntile state slot exceeds integer")?,
                    &format!("window_ntile_{prefix}_f{}", index + 1),
                    &format!(
                        r#"
                        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                        partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
                          ON DELETE RESTRICT,
                        bucket_count bigint,
                        first_ordinal bigint CHECK(first_ordinal>0),
                        CHECK((bucket_count IS NULL)=(first_ordinal IS NULL))
                        "#
                    ),
                )?;
            }
            WindowFunctionCapability::Native(_) => {}
        }
    }

    let continuation = qualified_internal(&format!("window_continuation_{prefix}"));
    client
        .update(
            &format!(
                r#"
                CREATE TABLE {continuation}(
                  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                  phase smallint NOT NULL CHECK(phase BETWEEN 1 AND 9),
                  input_stream_id bigint NOT NULL CHECK(input_stream_id>0),
                  input_chunk_seq bigint CHECK(input_chunk_seq>0),
                  input_row_ordinal bigint CHECK(input_row_ordinal>=0),
                  partition_queue_id bigint CHECK(partition_queue_id>0),
                  function_ordinal integer CHECK(function_ordinal>0),
                  output_ordinal bigint CHECK(output_ordinal>0),
                  cursor_row_id bigint CHECK(cursor_row_id>0),
                  fold_ready boolean NOT NULL DEFAULT false,
                  cursor_repeat boolean NOT NULL DEFAULT false,
                  diff_leg smallint CHECK(diff_leg IN (1,2)),
                  cleanup_ordinal integer CHECK(cleanup_ordinal>=0),
                  after_kind smallint CHECK(after_kind IN (1,2,3)),
                  after_chunk_seq bigint CHECK(after_chunk_seq>0),
                  after_row_ordinal bigint CHECK(after_row_ordinal>=0),
                  FOREIGN KEY(input_stream_id,input_chunk_seq)
                    REFERENCES shiba_internal.effect_stream_chunks(stream_id,chunk_seq)
                    ON DELETE RESTRICT,
                  CHECK(
                    (phase IN (1,9) AND input_chunk_seq IS NOT NULL
                     AND input_row_ordinal IS NOT NULL
                     AND partition_queue_id IS NULL AND function_ordinal IS NULL
                     AND output_ordinal IS NULL
                     AND cursor_row_id IS NULL AND diff_leg IS NULL
                     AND cleanup_ordinal IS NULL AND after_kind IS NULL
                     AND after_chunk_seq IS NULL AND after_row_ordinal IS NULL)
                    OR
                    (phase IN (2,3,4) AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL AND function_ordinal IS NULL
                     AND output_ordinal IS NULL
                     AND diff_leg IS NULL AND cleanup_ordinal IS NULL
                     AND after_kind IS NOT NULL)
                    OR
                    (phase=5 AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL
                     AND function_ordinal IS NOT NULL AND output_ordinal IS NOT NULL
                     AND diff_leg IS NULL
                     AND cleanup_ordinal IS NULL AND after_kind IS NOT NULL)
                    OR
                    (phase=6 AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL
                     AND function_ordinal IS NOT NULL AND output_ordinal IS NULL
                     AND diff_leg IS NULL
                     AND cleanup_ordinal IS NULL AND after_kind IS NOT NULL)
                    OR
                    (phase=7 AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL
                     AND function_ordinal IS NULL AND output_ordinal IS NULL
                     AND diff_leg IS NOT NULL
                     AND cleanup_ordinal IS NULL AND after_kind IS NOT NULL)
                    OR
                    (phase=8 AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL
                     AND function_ordinal IS NULL AND output_ordinal IS NULL
                     AND diff_leg IS NULL
                     AND cleanup_ordinal IS NOT NULL AND after_kind IS NOT NULL)
                  ),
                  CHECK(phase=5 OR NOT fold_ready),
                  CHECK(
                    NOT cursor_repeat
                    OR (phase=7 AND cursor_row_id IS NOT NULL)
                  ),
                  CHECK(
                    after_kind IS NULL
                    OR (after_kind=1 AND after_chunk_seq IS NOT NULL
                        AND after_row_ordinal IS NOT NULL)
                    OR (after_kind=2 AND after_chunk_seq IS NULL
                        AND after_row_ordinal IS NULL)
                    OR (after_kind=3 AND after_chunk_seq IS NOT NULL
                        AND after_row_ordinal=0)
                  )
                )
                "#
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window continuation: {error}"))?;
    revoke_window_relation(client, &continuation, "continuation")?;
    let continuation_oid = resolve_relation_oid(client, &continuation)?;
    catalog_continuation(client, result_oid, stage_id, continuation_oid)?;
    Ok(())
}

pub(super) fn create_window_state(
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
            format!("could not create Window stage {stage_id} state slot {slot}: {error}")
        })?;
    revoke_window_relation(client, &relation, "state")?;
    let oid = resolve_relation_oid(client, &relation)?;
    catalog_state(client, result_oid, stage_id, slot, oid)?;
    Ok(relation)
}

pub(super) fn revoke_window_relation(
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
        .map_err(|error| format!("could not protect Window {label}: {error}"))?;
    Ok(())
}

pub(super) fn resolve_window_function_client(
    client: &mut SpiClient<'_>,
    function: &WindowExpr,
) -> Result<WindowFunctionCapability, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(pg_sys::Oid::from(function.function_oid), pg_sys::OIDOID),
            DatumWithOid::new(pg_sys::Oid::from(function.type_.type_oid), pg_sys::OIDOID),
        ]
    };
    if function.aggregate {
        let rows = client
            .select(AGGREGATE_CAPABILITY_SQL, None, &arguments)
            .map_err(|error| format!("could not resolve Window aggregate: {error}"))?;
        return decode_aggregate_capability(
            rows,
            function.function_oid,
            function.args.len(),
            function.input_collation_oid,
        )
        .map(WindowFunctionCapability::Aggregate);
    }
    if function.filter.is_some() || function.star {
        return Err("native Window function cannot use FILTER or star".into());
    }
    let rows = client
        .select(
            r#"
            SELECT procedure.proname::text,procedure.pronargs::integer
            FROM pg_catalog.pg_proc AS procedure
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid=procedure.pronamespace
            WHERE procedure.oid=$1 AND procedure.prokind='w'
              AND procedure.provolatile='i' AND namespace.nspname='pg_catalog'
            "#,
            None,
            &arguments[..1],
        )
        .map_err(|error| format!("could not resolve native Window function: {error}"))?;
    decode_native_window(rows, function).map(WindowFunctionCapability::Native)
}

pub(super) fn decode_native_window(
    rows: SpiTupleTable<'_>,
    function: &WindowExpr,
) -> Result<NativeWindow, String> {
    if rows.len() != 1 {
        return Err("Window function has no trusted native capability".into());
    }
    let row = rows.first();
    let name: String = window_required(&row, 1, "Window function name")?;
    let arity: i32 = window_required(&row, 2, "Window function arity")?;
    if usize::try_from(arity).ok() != Some(function.args.len()) {
        return Err("Window function arity changed".into());
    }
    match (name.as_str(), function.args.len()) {
        ("row_number", 0) => Ok(NativeWindow::RowNumber),
        ("rank", 0) => Ok(NativeWindow::Rank),
        ("dense_rank", 0) => Ok(NativeWindow::DenseRank),
        ("percent_rank", 0) => Ok(NativeWindow::PercentRank),
        ("cume_dist", 0) => Ok(NativeWindow::CumeDist),
        ("ntile", 1) => Ok(NativeWindow::Ntile),
        ("lag", 1..=3) => Ok(NativeWindow::Lag),
        ("lead", 1..=3) => Ok(NativeWindow::Lead),
        ("first_value", 1) => Ok(NativeWindow::FirstValue),
        ("last_value", 1) => Ok(NativeWindow::LastValue),
        ("nth_value", 2) => Ok(NativeWindow::NthValue),
        _ => Err(format!("Window function {name} has no bounded capability")),
    }
}

/// Execute exactly one durable Window action.
pub(crate) const KERNEL: crate::execution::KernelFn = crate::execution::KernelFn::new(
    crate::execution::KernelContract::new(
        &[crate::execution::InputContract::Operator],
        crate::execution::OutputContract::EffectStream,
    ),
    step,
);
