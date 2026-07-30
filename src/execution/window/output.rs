use super::*;

pub(super) fn run_window_evaluate(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    function_ordinal: u32,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let index = usize::try_from(function_ordinal - 1)
        .map_err(|_| "Window function ordinal exceeds usize")?;
    let function = expressions
        .functions
        .get(index)
        .ok_or_else(|| "Window evaluation function is outside its plan".to_string())?;
    let WindowFunctionCapability::Native(native) = &function.capability else {
        return Err("aggregate Window function entered native evaluation".into());
    };
    let mut extra_state_rows = 0_u64;
    let ntile_state = if *native == NativeWindow::Ntile {
        Some(
            storage
                .ntile_states
                .get(index)
                .and_then(Option::as_ref)
                .ok_or_else(|| "Window ntile omitted its durable state".to_string())?,
        )
    } else {
        None
    };
    if cursor.row_id.is_none() {
        if let Some(state) = ntile_state {
            let rows = transaction.write(
                &format!(
                    "INSERT INTO {}(partition_id,bucket_count,first_ordinal) \
                     VALUES($1,NULL,NULL) RETURNING 1",
                    state.sql()
                ),
                &unsafe { [DatumWithOid::new(partition_queue_id, pg_sys::INT8OID)] },
            )?;
            if rows.len() != 1 {
                return Err("Window ntile did not initialize one durable state row".into());
            }
            extra_state_rows = 1;
        }
    }
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window evaluation row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window evaluation row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window evaluation byte budget")?;
    let function_column = quote_identifier(&format!("function_{function_ordinal}"));
    let final_function = index + 1 == expressions.functions.len();
    let output_key = canonical_row_key_sql("output_rows.output_row", &storage.output_type);
    let (computation, native_state_status) = if let Some(state) = ntile_state {
        let bucket_argument = function
            .current_arguments
            .first()
            .ok_or_else(|| "Window ntile omitted its bucket argument".to_string())?;
        let value = window_ntile_value("fold.bucket_count", "fold.first_ordinal");
        (
            format!(
                r#"
                fold(step,ordinal,bucket_count,first_ordinal) AS (
                  SELECT 0::bigint,NULL::bigint,
                         state.bucket_count,state.first_ordinal
                  FROM {state} AS state
                  WHERE state.singleton AND state.partition_id=$1
                  UNION ALL
                  SELECT selected.page_ordinal,selected.ordinal,next.bucket_count,
                         CASE WHEN fold.first_ordinal IS NOT NULL
                              THEN fold.first_ordinal
                              WHEN next.bucket_count IS NOT NULL
                              THEN selected.ordinal
                         END
                  FROM fold
                  JOIN selected ON selected.page_ordinal=fold.step+1
                  JOIN {input} AS current_input
                    ON current_input.entry_id=selected.entry_id
                  CROSS JOIN LATERAL (
                    SELECT CASE WHEN fold.bucket_count IS NOT NULL
                                THEN fold.bucket_count
                                ELSE ({bucket_argument})::bigint
                           END AS bucket_count
                    OFFSET 0
                  ) AS next
                ),
                computed AS MATERIALIZED (
                  SELECT ordered.ordinal,({value})::{result_type} AS function_value
                  FROM fold
                  JOIN {ordered} AS ordered ON ordered.ordinal=fold.ordinal
                  JOIN {partitions} AS partition ON partition.partition_id=$1
                  WHERE fold.step>0
                ),
                final_fold AS MATERIALIZED (
                  SELECT step,bucket_count,first_ordinal
                  FROM fold ORDER BY step DESC LIMIT 1
                ),
                native_state_changed AS (
                  UPDATE {state} AS state
                  SET bucket_count=final_fold.bucket_count,
                      first_ordinal=final_fold.first_ordinal
                  FROM final_fold
                  WHERE state.singleton AND state.partition_id=$1
                    AND final_fold.step>0
                  RETURNING 1
                )
                "#,
                state = state.sql(),
                input = storage.input.sql(),
                ordered = storage.ordered.sql(),
                partitions = storage.partitions.sql(),
                result_type = function.result_type,
            ),
            format!(
                "WHEN (SELECT count(*) FROM {} \
                 WHERE singleton AND partition_id=$1)<>1 \
                 THEN 'missing_ntile_state'",
                state.sql()
            ),
        )
    } else {
        let value = window_native_value(storage, function, *native)?;
        (
            format!(
                r#"
                computed AS MATERIALIZED (
                  SELECT ordered.ordinal,({value}) AS function_value
                  FROM selected
                  JOIN {ordered} AS ordered
                    ON ordered.ordinal=selected.ordinal
                  JOIN {input} AS current_input
                    ON current_input.entry_id=ordered.entry_id
                  JOIN {peers} AS peer
                    ON peer.peer_id=ordered.peer_id
                  JOIN {frames} AS frame
                    ON frame.ordinal=ordered.ordinal
                  JOIN {partitions} AS partition ON partition.partition_id=$1
                ),
                native_state_changed AS (SELECT 1 WHERE false)
                "#,
                ordered = storage.ordered.sql(),
                input = storage.input.sql(),
                peers = storage.peers.sql(),
                frames = storage.frames.sql(),
                partitions = storage.partitions.sql(),
            ),
            String::new(),
        )
    };
    let candidate_write = if final_function {
        format!(
            r#"
            output_rows AS MATERIALIZED (
              SELECT updated.ordinal,
                     ROW({outputs})::{output_type} AS output_row
              FROM updated
              JOIN {input} AS input_row
                ON input_row.entry_id=updated.entry_id
            ),
            keyed AS MATERIALIZED (
              SELECT output_rows.*,
                     {output_key} AS output_key
              FROM output_rows
            ),
            collapsed AS MATERIALIZED (
              SELECT output_key,min(ordinal) AS representative_ordinal,
                     count(*)::numeric AS multiplicity
              FROM keyed GROUP BY output_key
            ),
            candidate_rows AS MATERIALIZED (
              SELECT collapsed.output_key,keyed.output_row,collapsed.multiplicity
              FROM collapsed JOIN keyed
                ON keyed.output_key=collapsed.output_key
               AND keyed.ordinal=collapsed.representative_ordinal
            ),
            candidate_changed AS (
              INSERT INTO {candidate} AS target(
                partition_id,output_key,output_row,multiplicity
              )
              SELECT $1,output_key,output_row,multiplicity FROM candidate_rows
              ON CONFLICT(partition_id,output_key) DO UPDATE
              SET output_row=EXCLUDED.output_row,
                  multiplicity=target.multiplicity+EXCLUDED.multiplicity
              RETURNING 1
            )
            "#,
            outputs = expressions.outputs,
            output_type = storage.output_type.sql(),
            input = storage.input.sql(),
            candidate = storage.candidate.sql(),
            output_key = output_key,
        )
    } else {
        "candidate_changed AS (SELECT 1 WHERE false)".into()
    };
    let query = format!(
        r#"
        WITH RECURSIVE source AS MATERIALIZED (
          SELECT ordered.ordinal,ordered.entry_id,ordered.peer_id,
                 current_input.row_value,
                 shiba_internal.effect_row_bytes(current_input.row_value) AS row_bytes
          FROM {ordered} AS ordered
          JOIN {input} AS current_input USING(entry_id)
          WHERE $2 IS NULL OR ordered.ordinal>$2
          ORDER BY ordered.ordinal
          LIMIT $5
        ),
        measured AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY ordinal) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal <= $3
            AND (page_ordinal=1 OR running_bytes <= $4)
        ),
        {computation},
        updated AS (
          UPDATE {ordered} AS ordered
          SET {function_column}=computed.function_value
          FROM computed
          WHERE ordered.ordinal=computed.ordinal
          RETURNING ordered.*
        ),
        {candidate_write}
        SELECT CASE
                 WHEN NOT EXISTS(
                   SELECT 1 FROM {partitions}
                   WHERE partition_id=$1 AND dirty
                 ) THEN 'missing_partition'
                 WHEN $2 IS NOT NULL AND NOT EXISTS(
                   SELECT 1 FROM {ordered} WHERE ordinal=$2
                 ) THEN 'cursor_mismatch'
                 {native_state_status}
                 ELSE 'ok'
               END,
               count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(ordinal ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM updated)
                 +(SELECT count(*)::bigint FROM candidate_changed)
                 +(SELECT count(*)::bigint FROM native_state_changed)
        FROM selected
        "#,
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
        partitions = storage.partitions.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Window evaluation returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window evaluation status")?;
    if status != "ok" {
        return Err(format!("Window evaluation returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window evaluation rows")?,
        "Window evaluation rows",
    )?;
    let bytes = window_nonnegative(
        window_required(&row, 3, "Window evaluation bytes")?,
        "Window evaluation bytes",
    )?;
    let last_row_id = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window evaluation completion")?;
    let mut state_rows = window_nonnegative(
        window_required(&row, 6, "Window evaluation mutations")?,
        "Window evaluation mutations",
    )?;
    if state_rows < processed + u64::from(ntile_state.is_some() && processed > 0) {
        return Err("Window evaluation did not update every selected row".into());
    }
    state_rows = state_rows
        .checked_add(extra_state_rows)
        .ok_or_else(|| "Window evaluation state count overflow".to_string())?;
    if complete {
        if let Some(state) = ntile_state {
            let rows = transaction.write(
                &format!(
                    "DELETE FROM {} WHERE singleton AND partition_id=$1 RETURNING 1",
                    state.sql()
                ),
                &unsafe { [DatumWithOid::new(partition_queue_id, pg_sys::INT8OID)] },
            )?;
            if rows.len() != 1 {
                return Err("Window ntile did not release one durable state row".into());
            }
            state_rows = state_rows
                .checked_add(1)
                .ok_or_else(|| "Window evaluation state count overflow".to_string())?;
        }
    }
    Ok(window_internal_page(
        processed,
        bytes,
        state_rows,
        last_row_id,
        complete,
    ))
}

pub(super) fn window_native_value(
    storage: &WindowStorage,
    function: &WindowFunctionPlan,
    native: NativeWindow,
) -> Result<String, String> {
    let output_type = &function.result_type;
    let value = match native {
        NativeWindow::RowNumber => "ordered.ordinal".into(),
        NativeWindow::Rank => "peer.first_ordinal".into(),
        NativeWindow::DenseRank => "ordered.peer_id".into(),
        NativeWindow::PercentRank => "CASE WHEN partition.row_count<=1 THEN 0::double precision \
             ELSE (peer.first_ordinal-1)::double precision \
                  /(partition.row_count-1)::double precision END"
            .into(),
        NativeWindow::CumeDist => {
            "peer.last_ordinal::double precision/partition.row_count::double precision".into()
        }
        NativeWindow::Ntile => {
            return Err("Window ntile entered stateless evaluation".into());
        }
        NativeWindow::Lag | NativeWindow::Lead => {
            let offset = function
                .current_arguments
                .get(1)
                .cloned()
                .unwrap_or_else(|| "1".into());
            let target = if native == NativeWindow::Lag {
                format!("ordered.ordinal::numeric-({offset})::numeric")
            } else {
                format!("ordered.ordinal::numeric+({offset})::numeric")
            };
            let default = function
                .current_arguments
                .get(2)
                .cloned()
                .unwrap_or_else(|| format!("NULL::{output_type}"));
            window_target_value(
                storage,
                &target,
                &function.target_arguments[0],
                &default,
                output_type,
                Some(&offset),
            )
        }
        NativeWindow::FirstValue => window_target_value(
            storage,
            "coalesce(frame.start_1,frame.start_2,frame.start_3)",
            &function.target_arguments[0],
            &format!("NULL::{output_type}"),
            output_type,
            None,
        ),
        NativeWindow::LastValue => window_target_value(
            storage,
            "coalesce(frame.end_3,frame.end_2,frame.end_1)",
            &function.target_arguments[0],
            &format!("NULL::{output_type}"),
            output_type,
            None,
        ),
        NativeWindow::NthValue => {
            let nth = &function.current_arguments[1];
            let target = format!(
                r#"
                CASE WHEN ({nth}) IS NULL THEN NULL
                     WHEN ({nth})::bigint<=0
                       THEN 1::bigint/(ordered.ordinal-ordered.ordinal)
                     WHEN ({nth})::bigint<=coalesce(frame.end_1-frame.start_1+1,0)
                       THEN frame.start_1+({nth})::bigint-1
                     WHEN ({nth})::bigint<=coalesce(frame.end_1-frame.start_1+1,0)
                          +coalesce(frame.end_2-frame.start_2+1,0)
                       THEN frame.start_2+({nth})::bigint
                          -coalesce(frame.end_1-frame.start_1+1,0)-1
                     WHEN ({nth})::bigint<=frame.frame_count
                       THEN frame.start_3+({nth})::bigint
                          -coalesce(frame.end_1-frame.start_1+1,0)
                          -coalesce(frame.end_2-frame.start_2+1,0)-1
                     ELSE NULL
                END
                "#
            );
            window_target_value(
                storage,
                &target,
                &function.target_arguments[0],
                &format!("NULL::{output_type}"),
                output_type,
                None,
            )
        }
    };
    Ok(format!("({value})::{output_type}"))
}

pub(super) fn window_ntile_value(buckets: &str, first_ordinal: &str) -> String {
    let active_ordinal = format!("(ordered.ordinal-({first_ordinal})+1)");
    let total_rows = "partition.row_count::bigint";
    format!(
        r#"
        CASE WHEN ({buckets}) IS NULL THEN NULL::bigint
          WHEN ({buckets})<=0
            THEN 1::bigint/(ordered.ordinal-ordered.ordinal)
          WHEN {active_ordinal}
               <= (({total_rows}/({buckets}))+1)
                  *({total_rows}%({buckets}))
          THEN ({active_ordinal}-1)
               /(({total_rows}/({buckets}))+1)+1
          ELSE ({total_rows}%({buckets}))
               +({active_ordinal}
                 -(({total_rows}/({buckets}))+1)
                  *({total_rows}%({buckets}))-1)
                 /({total_rows}/({buckets}))+1
        END
        "#,
    )
}

pub(super) fn window_target_value(
    storage: &WindowStorage,
    target_ordinal: &str,
    target_value: &str,
    default_value: &str,
    output_type: &str,
    nullable_offset: Option<&str>,
) -> String {
    let lookup = format!(
        r#"
        (
          SELECT CASE WHEN target_ordered.ordinal IS NULL
                      THEN ({default_value})::{output_type}
                      ELSE ({target_value})::{output_type}
                 END
          FROM (SELECT 1) AS seed
          LEFT JOIN {ordered} AS target_ordered
            ON target_ordered.ordinal=({target_ordinal})
          LEFT JOIN {input} AS target_input
            ON target_input.entry_id=target_ordered.entry_id
        )
        "#,
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
    );
    nullable_offset.map_or(lookup.clone(), |offset| {
        format!("CASE WHEN ({offset}) IS NULL THEN NULL::{output_type} ELSE {lookup} END")
    })
}

pub(super) fn run_window_diff(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    partition_queue_id: i64,
    leg: DiffLeg,
    cursor: WindowDiffCursor,
) -> Result<WindowDiffPage, String> {
    cursor.validate()?;
    let causal_arguments = unsafe { [DatumWithOid::new(partition_queue_id, pg_sys::INT8OID)] };
    let causal_rows = transaction.read(
        &format!(
            "SELECT causal_lsn::text FROM {} \
             WHERE partition_id=$1 AND dirty AND causal_lsn IS NOT NULL",
            storage.partitions.sql()
        ),
        &causal_arguments,
    )?;
    if causal_rows.len() != 1 {
        return Err("Window dirty partition has no unique causal LSN".into());
    }
    let lsn: String = window_required(&causal_rows.first(), 1, "Window partition causal LSN")?;
    let output = transaction.output()?.clone();
    let budget = transaction.budget();
    let max_rows = i64::min(
        i64::min(
            window_i64_budget(budget.max_input_rows, "Window diff input rows")?,
            window_i64_budget(budget.max_output_rows, "Window diff output rows")?,
        ),
        output.target_rows,
    );
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window diff row budget overflow".to_string())?;
    let max_bytes = i64::min(
        i64::min(
            window_i64_budget(budget.max_input_bytes, "Window diff input bytes")?,
            window_i64_budget(budget.max_output_bytes, "Window diff output bytes")?,
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
                WHERE visible.partition_id=$1
                  AND {cursor_predicate}
                ORDER BY visible.visible_id LIMIT $5
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
                  ON candidate.partition_id=$1
                 AND candidate.output_key=bounded_prefix.output_key
                "#,
                candidate = storage.candidate.sql(),
            ),
            format!(
                r#"
                deleted AS (
                  DELETE FROM {visible} AS visible USING differences
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
                WHERE candidate.partition_id=$1
                  AND {cursor_predicate}
                ORDER BY candidate.candidate_id LIMIT $5
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
                  ON visible.partition_id=$1
                 AND visible.output_key=bounded_prefix.output_key
                "#,
                visible = storage.visible.sql(),
            ),
            format!(
                r#"
                changed AS (
                  INSERT INTO {visible} AS target(
                    partition_id,output_key,output_row,multiplicity
                  )
                  SELECT $1,output_key,output_row,slice::numeric FROM differences
                  ON CONFLICT(partition_id,output_key) DO UPDATE
                  SET output_row=EXCLUDED.output_row,
                      multiplicity=target.multiplicity+EXCLUDED.multiplicity
                  RETURNING 1
                ),
                deleted AS (SELECT 1 WHERE false)
                "#,
                visible = storage.visible.sql(),
            ),
            "differences.slice",
        ),
    };
    let query = format!(
        r#"
        WITH source AS MATERIALIZED ({source}),
        numbered AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY row_id) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_id) AS running_bytes
          FROM source
        ),
        bounded_prefix AS MATERIALIZED (
          SELECT numbered.*
          FROM numbered
          WHERE page_ordinal<=$3
            AND (page_ordinal=1 OR running_bytes<=$4)
        ),
        joined AS MATERIALIZED ({compared}),
        marked AS MATERIALIZED (
          SELECT joined.*,
                 min(CASE WHEN delta>9223372036854775807::numeric
                          THEN page_ordinal END) OVER () AS first_huge_ordinal
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
        appended AS MATERIALIZED (
          SELECT append.outcome,append.appended_chunk_seq
          FROM stats
          CROSS JOIN LATERAL shiba_internal.append_effect_stream_chunk(
            $7,$8,'data',stats.emitted_rows,stats.emitted_bytes,$6::pg_lsn
          ) AS append
          WHERE stats.emitted_rows>0
        ),
        payload_insert AS (
          INSERT INTO {output_payload}(
            stream_id,chunk_seq,row_ordinal,weight,row_value
          )
          SELECT $7,appended.appended_chunk_seq,
                 row_number() OVER (ORDER BY differences.page_ordinal)-1,
                 {weight},differences.output_row
          FROM differences CROSS JOIN appended
          WHERE appended.outcome='appended'
          RETURNING 1
        ),
        {mutation}
        SELECT stats.compared_rows,stats.compared_bytes,stats.last_id,
               (SELECT count(*) FROM source)
                 =(SELECT count(*) FROM bounded_prefix)
                 AND (SELECT count(*) FROM bounded_prefix)=stats.compared_rows
                 AND NOT stats.repeat_cursor,
               stats.repeat_cursor,stats.emitted_rows,stats.emitted_bytes,
               appended.outcome,appended.appended_chunk_seq,
               (SELECT count(*)::bigint FROM payload_insert),
               (SELECT count(*)::bigint FROM changed)
                 +(SELECT count(*)::bigint FROM deleted)
        FROM stats LEFT JOIN appended ON true
        "#,
        output_payload = storage.output_payload.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
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
        return Err("Window diff returned no summary".into());
    }
    let row = rows.first();
    let compared_rows = window_nonnegative(
        window_required(&row, 1, "Window compared rows")?,
        "Window compared rows",
    )?;
    let compared_bytes = window_nonnegative(
        window_required(&row, 2, "Window compared bytes")?,
        "Window compared bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 4, "Window diff completion")?;
    let repeat_cursor = window_required(&row, 5, "Window residual cursor")?;
    let emitted = window_nonnegative(
        window_required(&row, 6, "Window diff rows")?,
        "Window diff rows",
    )?;
    let emitted_bytes = window_nonnegative(
        window_required(&row, 7, "Window diff bytes")?,
        "Window diff bytes",
    )?;
    let append_outcome = row.get::<String>(8).map_err(|error| error.to_string())?;
    let appended_sequence = row.get::<i64>(9).map_err(|error| error.to_string())?;
    let inserted = window_nonnegative(
        window_required(&row, 10, "Window payload inserts")?,
        "Window payload inserts",
    )?;
    let mutated = window_nonnegative(
        window_required(&row, 11, "Window visible mutations")?,
        "Window visible mutations",
    )?;
    let output_facts = if emitted == 0 {
        if append_outcome.is_some() || appended_sequence.is_some() || inserted != 0 || mutated != 0
        {
            return Err("Window appended or mutated an empty diff".into());
        }
        OutputFacts::None
    } else {
        if append_outcome.as_deref() != Some("appended")
            || appended_sequence != Some(output.next_chunk_seq)
            || inserted != emitted
            || mutated != emitted
        {
            return Err("Window diff append is inconsistent".into());
        }
        OutputFacts::Data {
            chunk_seq: output.next_chunk_seq,
        }
    };
    Ok(WindowDiffPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: compared_rows,
                input_bytes: compared_bytes,
                output_rows: emitted,
                output_bytes: emitted_bytes,
            },
            state_rows: mutated,
            continuation_rows: 1,
            output: output_facts,
        },
        last_row_id,
        complete,
        repeat_cursor,
    })
}

pub(super) fn run_window_cleanup(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    cursor: WindowCleanupCursor,
    after_partitions: AfterPartitions,
) -> Result<WindowCleanup, String> {
    let final_ordinal = 3;
    let relation = match cursor.relation_ordinal {
        0 => Some((
            &storage.candidate,
            "candidate_id",
            format!("partition_id={partition_queue_id}"),
            "shiba_internal.effect_row_bytes(output_row)".to_string(),
        )),
        1 => Some((
            &storage.ordered,
            "ordinal",
            "true".into(),
            format!(
                "coalesce((SELECT shiba_internal.effect_row_bytes(input.row_value) \
                 FROM {} AS input WHERE input.entry_id=target.entry_id),24)",
                storage.input.sql()
            ),
        )),
        2 => Some((&storage.peers, "peer_id", "true".into(), "24".into())),
        3 => Some((&storage.frames, "ordinal", "true".into(), "64".into())),
        _ => None,
    };
    let mut page = if let Some((relation, identity, predicate, bytes)) = relation {
        run_window_cleanup_relation(
            transaction,
            relation,
            identity,
            &predicate,
            &bytes,
            cursor.row,
        )?
    } else {
        window_internal_page(0, 0, 0, None, true)
    };
    let mut next_partition_queue_id = None;
    if page.complete && cursor.relation_ordinal == final_ordinal {
        for accumulator in storage.accumulators.iter().flatten() {
            let rows = transaction.read(
                &format!("SELECT count(*)::bigint FROM {}", accumulator.sql()),
                &[],
            )?;
            if window_required::<i64>(&rows.first(), 1, "Window accumulator rows")? != 0 {
                return Err("Window cleanup found an unfinished aggregate fold".into());
            }
        }
        for state in storage.ntile_states.iter().flatten() {
            let rows = transaction.read(
                &format!("SELECT count(*)::bigint FROM {}", state.sql()),
                &[],
            )?;
            if window_required::<i64>(&rows.first(), 1, "Window ntile state rows")? != 0 {
                return Err("Window cleanup found unfinished ntile evaluation".into());
            }
        }
        let keep_empty = expressions.partition_columns.is_empty();
        let query = format!(
            r#"
            WITH removed AS (
              DELETE FROM {partitions}
              WHERE partition_id=$1 AND dirty AND row_count=0
                AND NOT $2::boolean
              RETURNING 1
            ),
            cleaned AS (
              UPDATE {partitions}
              SET dirty=false,causal_lsn=NULL
              WHERE partition_id=$1 AND dirty
                AND (row_count<>0 OR $2::boolean)
              RETURNING 1
            )
            SELECT (SELECT count(*)::bigint FROM removed)
                     +(SELECT count(*)::bigint FROM cleaned),
                   (SELECT min(partition_id)::bigint
                    FROM {partitions}
                    WHERE dirty AND partition_id>$1)
            "#,
            partitions = storage.partitions.sql(),
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(keep_empty, pg_sys::BOOLOID),
            ]
        };
        let rows = transaction.write(&query, &arguments)?;
        if rows.len() != 1 {
            return Err("Window partition finalization returned no summary".into());
        }
        let row = rows.first();
        let finalized = window_nonnegative(
            window_required(&row, 1, "Window finalized partitions")?,
            "Window finalized partitions",
        )?;
        next_partition_queue_id = row.get(2).map_err(|error| error.to_string())?;
        if finalized != 1 {
            return Err("Window finalization did not consume one dirty partition".into());
        }
        page.facts.state_rows = page
            .facts
            .state_rows
            .checked_add(finalized)
            .ok_or_else(|| "Window cleanup state count overflow".to_string())?;
    }
    page.facts.continuation_rows = u64::from(
        !page.complete
            || cursor.relation_ordinal != final_ordinal
            || next_partition_queue_id.is_some()
            || !matches!(after_partitions, AfterPartitions::FinishInput),
    );
    Ok(WindowCleanup {
        page,
        next_partition_queue_id,
    })
}

pub(super) fn run_window_cleanup_relation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    identity: &str,
    predicate: &str,
    bytes: &str,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window cleanup row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window cleanup row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window cleanup byte budget")?;
    let identity = quote_identifier(identity);
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          SELECT target.{identity} AS row_id,({bytes})::bigint AS row_bytes
          FROM {relation} AS target
          WHERE ({predicate}) AND ($1 IS NULL OR target.{identity}>=$1)
          ORDER BY target.{identity}
          LIMIT $4
        ),
        measured AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY row_id) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_id) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal<=$2 AND (page_ordinal=1 OR running_bytes<=$3)
        ),
        deleted AS (
          DELETE FROM {relation} AS target USING selected
          WHERE target.{identity}=selected.row_id
          RETURNING 1
        )
        SELECT count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(row_id ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM deleted)
        FROM selected
        "#,
        relation = relation.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Window relation cleanup returned no summary".into());
    }
    let row = rows.first();
    let deleted = window_nonnegative(
        window_required(&row, 1, "Window cleanup rows")?,
        "Window cleanup rows",
    )?;
    let row_bytes = window_nonnegative(
        window_required(&row, 2, "Window cleanup bytes")?,
        "Window cleanup bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 4, "Window cleanup completion")?;
    let mutations = window_nonnegative(
        window_required(&row, 5, "Window cleanup deletes")?,
        "Window cleanup deletes",
    )?;
    if mutations != deleted {
        return Err("Window cleanup delete count is inconsistent".into());
    }
    Ok(window_internal_page(
        deleted,
        row_bytes,
        mutations,
        last_row_id,
        complete,
    ))
}

pub(super) fn run_window_frontier(
    transaction: &mut StepContext<'_, '_>,
    input: InputPosition,
) -> Result<PrimitiveFacts, String> {
    if input.row_ordinal != 0 {
        return Err("Window frontier has a row cursor".into());
    }
    let input_state = transaction.input(0)?.clone();
    let frontier = chunk(transaction, &input_state, input.chunk_seq)?
        .ok_or_else(|| "Window frontier chunk is missing".to_string())?;
    if frontier.kind != ChunkKind::Frontier || frontier.stream_id != input.stream_id {
        return Err("Window frontier continuation references data".into());
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
        continuation_rows: 0,
        output,
        ..PrimitiveFacts::default()
    })
}
