use super::*;

pub(super) fn run_window_admission(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    input: InputPosition,
) -> Result<WindowAdmission, String> {
    let input_state = transaction.input(0)?.clone();
    let input_chunk = chunk(transaction, &input_state, input.chunk_seq)?
        .ok_or_else(|| "Window admission references a missing input chunk".to_string())?;
    if input_chunk.kind != ChunkKind::Data || input_chunk.stream_id != input.stream_id {
        return Err("Window admission does not reference a data chunk".into());
    }
    if input.row_ordinal == 0 {
        payload_facts(transaction, &storage.input_payload, &input_chunk)?;
    }
    let chunk_rows =
        i64::try_from(input_chunk.rows).map_err(|_| "Window chunk rows exceed bigint")?;
    if input.row_ordinal >= chunk_rows {
        return Err("Window admission cursor is outside its chunk".into());
    }
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window admission row budget")?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window admission byte budget")?;
    let causal_lsn = format_lsn(input_chunk.lsn);
    let evaluated = window_admission_evaluated_sql(storage, expressions);
    let partition_columns = expressions
        .partition_columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>();
    let partition_values = partition_columns
        .iter()
        .map(|column| format!("evaluated.{column}"))
        .collect::<Vec<_>>()
        .join(",");
    let partition_touch = if partition_columns.is_empty() {
        format!(
            "UPDATE {} SET dirty=true RETURNING partition_id",
            storage.partitions.sql()
        )
    } else {
        format!(
            r#"
            INSERT INTO {partitions} AS target({columns},dirty,row_count)
            SELECT DISTINCT {values},true,0::numeric
            FROM evaluated
            ON CONFLICT({columns}) DO UPDATE SET dirty=true
            RETURNING partition_id
            "#,
            partitions = storage.partitions.sql(),
            columns = partition_columns.join(","),
            values = partition_values,
        )
    };
    let touch_query = format!(
        r#"
        WITH {evaluated},
        touched AS ({partition_touch})
        SELECT count(*)::bigint FROM touched
        "#
    );
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
    let touched_rows = transaction.write(&touch_query, &arguments)?;
    if touched_rows.len() != 1
        || window_required::<i64>(&touched_rows.first(), 1, "Window touched partitions")? <= 0
    {
        return Err("Window admission did not resolve a partition".into());
    }

    let partition_predicate = if partition_columns.is_empty() {
        "true".into()
    } else {
        partition_columns
            .iter()
            .map(|column| format!("partition.{column} IS NOT DISTINCT FROM evaluated.{column}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let order_columns = expressions
        .order_columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>();
    let insert_orders = if order_columns.is_empty() {
        String::new()
    } else {
        format!(",{}", order_columns.join(","))
    };
    let decision_orders = if order_columns.is_empty() {
        String::new()
    } else {
        format!(
            ",{}",
            order_columns
                .iter()
                .map(|column| format!("decision.{column}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let representative_orders = if order_columns.is_empty() {
        String::new()
    } else {
        format!(
            ",{}",
            order_columns
                .iter()
                .map(|column| format!("representative.{column}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let update_orders = order_columns
        .iter()
        .map(|column| format!("{column}=EXCLUDED.{column}"))
        .collect::<Vec<_>>();
    let update_orders = if update_orders.is_empty() {
        String::new()
    } else {
        format!(",{}", update_orders.join(","))
    };
    let query = format!(
        r#"
        WITH {evaluated},
        assigned AS MATERIALIZED (
          SELECT evaluated.*,partition.partition_id
          FROM evaluated
          JOIN {partitions} AS partition ON {partition_predicate}
        ),
        prefixes AS MATERIALIZED (
          SELECT assigned.*,
                 sum(weight::numeric) OVER (
                   PARTITION BY row_key ORDER BY row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS key_prefix
          FROM assigned
        ),
        collapsed AS MATERIALIZED (
          SELECT row_key,min(row_ordinal) AS representative_ordinal,
                 sum(weight::numeric) AS net_weight,min(key_prefix) AS min_prefix
          FROM prefixes GROUP BY row_key
        ),
        representative AS MATERIALIZED (
          SELECT assigned.*
          FROM assigned JOIN collapsed
            ON collapsed.row_key=assigned.row_key
           AND collapsed.representative_ordinal=assigned.row_ordinal
        ),
        existing AS MATERIALIZED (
          SELECT state.entry_id,state.row_key,state.partition_id,state.multiplicity
          FROM {state} AS state JOIN collapsed USING(row_key)
          FOR UPDATE OF state
        ),
        decision AS MATERIALIZED (
          SELECT collapsed.*,representative.row_value,
                 representative.partition_id {representative_orders},
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
        partition_delta AS MATERIALIZED (
          SELECT partition_id,sum(new_multiplicity-old_multiplicity) AS delta
          FROM decision GROUP BY partition_id
        ),
        partition_decision AS MATERIALIZED (
          SELECT partition.partition_id,
                 partition.row_count+partition_delta.delta AS new_count
          FROM {partitions} AS partition
          JOIN partition_delta USING(partition_id)
          FOR UPDATE OF partition
        ),
        status AS MATERIALIZED (
          SELECT CASE
                   WHEN EXISTS(
                     SELECT 1 FROM decision WHERE minimum_multiplicity<0
                   ) THEN 'negative'
                   WHEN EXISTS(
                     SELECT 1 FROM partition_decision
                     WHERE new_count<0 OR new_count>9223372036854775807::numeric
                   ) THEN 'partition_overflow'
                   ELSE 'ok'
                 END AS value
        ),
        removed AS (
          DELETE FROM {state} AS state
          USING decision,status
          WHERE status.value='ok' AND decision.new_multiplicity=0
            AND state.entry_id=decision.entry_id
          RETURNING 1
        ),
        changed AS (
          INSERT INTO {state} AS target(
            row_key,row_value,multiplicity,partition_id{insert_orders}
          )
          SELECT decision.row_key,decision.row_value,decision.new_multiplicity,
                 decision.partition_id{decision_orders}
          FROM decision,status
          WHERE status.value='ok' AND decision.new_multiplicity>0
          ON CONFLICT(row_key) DO UPDATE
          SET row_value=EXCLUDED.row_value,
              multiplicity=EXCLUDED.multiplicity,
              partition_id=EXCLUDED.partition_id{update_orders}
          RETURNING 1
        ),
        partition_changed AS (
          UPDATE {partitions} AS partition
          SET row_count=partition_decision.new_count,
              dirty=true,
              causal_lsn=CASE
                WHEN partition.causal_lsn IS NULL THEN $6::pg_lsn
                ELSE greatest(partition.causal_lsn,$6::pg_lsn)
              END
          FROM partition_decision,status
          WHERE status.value='ok'
            AND partition.partition_id=partition_decision.partition_id
          RETURNING partition.partition_id
        )
        SELECT (SELECT value FROM status),
               count(*)::bigint,min(row_ordinal)::bigint,max(row_ordinal)::bigint,
               coalesce(sum(row_bytes),0)::bigint,
               (SELECT count(*)::bigint FROM removed)
                 +(SELECT count(*)::bigint FROM changed)
                 +(SELECT count(*)::bigint FROM partition_changed),
               (SELECT min(partition_id)::bigint
                FROM {partitions} WHERE dirty)
        FROM bounded
        "#,
        partitions = storage.partitions.sql(),
        state = storage.input.sql(),
    );
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Window admission returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window admission status")?;
    if status != "ok" {
        return Err(format!("Window admission returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window admitted rows")?,
        "Window admitted rows",
    )?;
    let first = window_required::<i64>(&row, 3, "Window first admitted row")?;
    let last = window_required::<i64>(&row, 4, "Window last admitted row")?;
    let input_bytes = window_nonnegative(
        window_required(&row, 5, "Window admitted bytes")?,
        "Window admitted bytes",
    )?;
    let state_rows = window_nonnegative(
        window_required(&row, 6, "Window state mutations")?,
        "Window state mutations",
    )?;
    let first_partition_queue_id = window_required(&row, 7, "Window first dirty partition")?;
    if processed == 0
        || first != input.row_ordinal
        || last
            != input
                .row_ordinal
                .checked_add(i64::try_from(processed).map_err(|_| "Window page too large")?)
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| "Window input ordinal overflow".to_string())?
    {
        return Err("Window admission returned inconsistent row facts".into());
    }
    let next_row = last
        .checked_add(1)
        .ok_or_else(|| "Window input ordinal exhausted".to_string())?;
    let usage = WorkUsage {
        input_rows: processed,
        input_bytes,
        ..WorkUsage::default()
    };
    let drain_reached = transaction.record_admission(usage)?;
    let target = if next_row < chunk_rows {
        let next = InputPosition::new(input.stream_id, input.chunk_seq, next_row)?;
        if drain_reached {
            WindowAdmissionTarget::Drain {
                first_partition_queue_id,
                after_partitions: AfterPartitions::Admit(next),
            }
        } else {
            WindowAdmissionTarget::Continue(next)
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
        let next = chunk(transaction, &input_state, input_chunk.sequence + 1)?;
        match next {
            Some(next) if next.kind == ChunkKind::Frontier => WindowAdmissionTarget::Drain {
                first_partition_queue_id,
                after_partitions: AfterPartitions::Frontier(InputPosition::new(
                    next.stream_id,
                    next.sequence,
                    0,
                )?),
            },
            _ if drain_reached => WindowAdmissionTarget::Drain {
                first_partition_queue_id,
                after_partitions: AfterPartitions::FinishInput,
            },
            _ => WindowAdmissionTarget::Idle,
        }
    } else {
        return Err("Window admission advanced beyond its input chunk".into());
    };
    Ok(WindowAdmission {
        facts: PrimitiveFacts {
            usage,
            state_rows,
            output: OutputFacts::None,
        },
        target,
    })
}

pub(super) fn window_admission_evaluated_sql(
    storage: &WindowStorage,
    expressions: &WindowExpressions,
) -> String {
    let partition_select = expressions
        .partition_expressions
        .iter()
        .zip(&expressions.partition_columns)
        .map(|(expression, column)| format!("{expression} AS {}", quote_identifier(column)));
    let order_select = expressions
        .order_expressions
        .iter()
        .zip(&expressions.order_columns)
        .map(|(expression, column)| format!("{expression} AS {}", quote_identifier(column)));
    let keys = partition_select
        .chain(order_select)
        .collect::<Vec<_>>()
        .join(",");
    let keys = if keys.is_empty() {
        String::new()
    } else {
        format!(",{keys}")
    };
    let row_key = canonical_row_key_sql("input_row.row_value", &storage.input_type);
    format!(
        r#"
        source AS MATERIALIZED (
          SELECT input_row.row_ordinal,input_row.weight,input_row.row_value,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {payload} AS input_row
          WHERE input_row.stream_id=$1 AND input_row.chunk_seq=$2
            AND input_row.row_ordinal >= $3
          ORDER BY input_row.row_ordinal
          LIMIT $4
        ),
        measured AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY row_ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_ordinal) AS running_bytes
          FROM source
        ),
        bounded AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal=1 OR running_bytes <= $5
        ),
        evaluated AS MATERIALIZED (
          SELECT input_row.*,
                 {row_key} AS row_key{keys}
          FROM bounded AS input_row
        )
        "#,
        payload = storage.input_payload.sql(),
        row_key = row_key,
    )
}

// Atomic bounded Window enumeration primitive: materialize one ordered page
// for a partition; dynamic ordering and row types remain operator-specific.
pub(super) fn run_window_enumeration(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window enumeration row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window enumeration row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window enumeration byte budget")?;
    let entry_order = expressions.order_by.replace("input_row.", "entry_prefix.");
    let logical_order = format!("{entry_order},copy.copy_ordinal");
    let source_order_columns = expressions
        .order_columns
        .iter()
        .map(|column| format!(",entry_prefix.{}", quote_identifier(column)))
        .collect::<String>();
    let query = format!(
        r#"
        WITH partition AS MATERIALIZED (
          SELECT partition_id,row_count
          FROM {partitions}
          WHERE partition_id=$1 AND dirty
        ),
        boundary AS MATERIALIZED (
          SELECT input_row.*,ordered.copy_ordinal,ordered.ordinal
          FROM {ordered} AS ordered
          JOIN {input} AS input_row USING(entry_id)
          WHERE ordered.ordinal=$2
        ),
        entries AS MATERIALIZED (
          SELECT input_row.*,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes,
                 CASE
                   WHEN input_row.entry_id=(SELECT entry_id FROM boundary)
                     THEN (SELECT copy_ordinal+1 FROM boundary)
                   ELSE 1
                 END::bigint AS start_copy
          FROM {input} AS input_row
          JOIN partition USING(partition_id)
          WHERE $2 IS NULL
             OR (
               input_row.entry_id=(SELECT entry_id FROM boundary)
               AND (SELECT copy_ordinal FROM boundary)<input_row.multiplicity
             )
             OR EXISTS(
               SELECT 1 FROM boundary WHERE {keyset_after}
             )
          ORDER BY {entry_order}
          LIMIT $5
        ),
        entry_prefix AS MATERIALIZED (
          SELECT entries.*,
                 coalesce(
                   sum((multiplicity::bigint-start_copy+1)::numeric) OVER (
                     ORDER BY {prefix_order}
                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ),
                   0::numeric
                 ) AS available_before
          FROM entries
        ),
        source AS MATERIALIZED (
          SELECT entry_prefix.entry_id,copy.copy_ordinal,
                 entry_prefix.row_value,entry_prefix.row_bytes
                 {source_order_columns}
          FROM entry_prefix
          CROSS JOIN LATERAL pg_catalog.generate_series(
            entry_prefix.start_copy,
            least(
              entry_prefix.multiplicity,
              entry_prefix.start_copy::numeric
                +greatest($5::numeric-entry_prefix.available_before,0::numeric)-1
            )::bigint
          ) AS copy(copy_ordinal)
          WHERE entry_prefix.available_before<$5::numeric
          ORDER BY {logical_order}
          LIMIT $5
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY {source_order}) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY {source_order}) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal <= $3
            AND (page_ordinal=1 OR running_bytes <= $4)
        ),
        base AS MATERIALIZED (
          SELECT coalesce(max(ordinal),0)::bigint AS last_ordinal
          FROM {ordered}
        ),
        inserted AS (
          INSERT INTO {ordered}(ordinal,entry_id,copy_ordinal,peer_id)
          SELECT base.last_ordinal+selected.page_ordinal,
                 selected.entry_id,selected.copy_ordinal,NULL
          FROM selected CROSS JOIN base
          RETURNING ordinal
        ),
        summary AS MATERIALIZED (
          SELECT count(*)::bigint AS processed,
                 coalesce(sum(row_bytes),0)::bigint AS input_bytes,
                 (SELECT max(ordinal) FROM inserted) AS last_id,
                 (SELECT count(*) FROM source)=(SELECT count(*) FROM selected)
                   AS source_complete
          FROM selected
        )
        SELECT CASE
                 WHEN $2 IS DISTINCT FROM NULL
                      AND $2 IS DISTINCT FROM (SELECT last_ordinal FROM base)
                   THEN 'cursor_mismatch'
                 WHEN NOT EXISTS(SELECT 1 FROM partition) THEN 'missing_partition'
                 ELSE 'ok'
               END,
               summary.processed,summary.input_bytes,summary.last_id,
               summary.source_complete,
               (SELECT count(*)::bigint FROM inserted)
        FROM summary
        "#,
        partitions = storage.partitions.sql(),
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
        keyset_after = expressions.keyset_after,
        entry_order = expressions.order_by,
        prefix_order = expressions.order_by.replace("input_row.", "entries."),
        logical_order = logical_order,
        source_order = logical_order
            .replace("entry_prefix.", "source.")
            .replace("copy.", "source."),
        source_order_columns = source_order_columns,
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
        return Err("Window enumeration returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window enumeration status")?;
    if status != "ok" {
        return Err(format!("Window enumeration returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window enumerated rows")?,
        "Window enumerated rows",
    )?;
    let bytes = window_nonnegative(
        window_required(&row, 3, "Window enumerated bytes")?,
        "Window enumerated bytes",
    )?;
    let last_row_id = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window enumeration completion")?;
    let inserted = window_nonnegative(
        window_required(&row, 6, "Window ordered inserts")?,
        "Window ordered inserts",
    )?;
    if inserted != processed {
        return Err("Window enumeration insert count is inconsistent".into());
    }
    Ok(window_internal_page(
        processed,
        bytes,
        inserted,
        last_row_id,
        complete,
    ))
}

pub(super) fn window_internal_page(
    input_rows: u64,
    input_bytes: u64,
    state_rows: u64,
    last_row_id: Option<i64>,
    complete: bool,
) -> WindowPage {
    WindowPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows,
                input_bytes,
                ..WorkUsage::default()
            },
            state_rows,
            output: OutputFacts::None,
        },
        last_row_id,
        complete,
    }
}

// Atomic bounded Window peer primitive: resolve one peer boundary page while
// advancing only the typed window cursor.
pub(super) fn run_window_peers(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window peer row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window peer row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window peer byte budget")?;
    let peer_keys = expressions
        .order_columns
        .iter()
        .map(|column| {
            let column = quote_identifier(column);
            format!(",input_row.{column}")
        })
        .collect::<String>();
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          SELECT ordered.ordinal,ordered.entry_id,input_row.row_value{peer_keys},
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {ordered} AS ordered
          JOIN {input} AS input_row USING(entry_id)
          WHERE ($2 IS NULL OR ordered.ordinal>$2)
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
        marked AS MATERIALIZED (
          SELECT next_row.*,
                 CASE
                   WHEN next_row.ordinal=1 THEN 1
                   WHEN ({peer_equal}) THEN 0
                   ELSE 1
                 END AS starts_peer
          FROM selected AS next_row
          LEFT JOIN {ordered} AS previous_ordered
            ON previous_ordered.ordinal=next_row.ordinal-1
          LEFT JOIN {input} AS boundary_row
            ON boundary_row.entry_id=previous_ordered.entry_id
        ),
        base AS MATERIALIZED (
          SELECT coalesce(
                   (SELECT peer_id FROM {ordered} WHERE ordinal=$2),
                   0
                 )::bigint AS peer_id
        ),
        assigned AS MATERIALIZED (
          SELECT marked.*,
                 base.peer_id+sum(starts_peer) OVER (ORDER BY ordinal) AS peer_id
          FROM marked CROSS JOIN base
        ),
        updated AS (
          UPDATE {ordered} AS ordered
          SET peer_id=assigned.peer_id
          FROM assigned
          WHERE ordered.ordinal=assigned.ordinal
          RETURNING 1
        ),
        peer_ranges AS MATERIALIZED (
          SELECT peer_id,min(ordinal) AS first_ordinal,max(ordinal) AS last_ordinal
          FROM assigned GROUP BY peer_id
        ),
        peer_changed AS (
          INSERT INTO {peers} AS target(peer_id,first_ordinal,last_ordinal)
          SELECT peer_id,first_ordinal,last_ordinal FROM peer_ranges
          ON CONFLICT(peer_id) DO UPDATE
          SET first_ordinal=least(
                target.first_ordinal,EXCLUDED.first_ordinal
              ),
              last_ordinal=greatest(
                target.last_ordinal,EXCLUDED.last_ordinal
              )
          RETURNING 1
        )
        SELECT CASE
                 WHEN NOT EXISTS(
                   SELECT 1 FROM {partitions}
                   WHERE partition_id=$1 AND dirty
                 ) THEN 'missing_partition'
                 WHEN $2 IS NOT NULL AND NOT EXISTS(
                   SELECT 1 FROM {ordered}
                   WHERE ordinal=$2 AND peer_id IS NOT NULL
                 ) THEN 'cursor_mismatch'
                 ELSE 'ok'
               END,
               count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(ordinal ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM updated)
                 +(SELECT count(*)::bigint FROM peer_changed)
        FROM selected
        "#,
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
        peers = storage.peers.sql(),
        partitions = storage.partitions.sql(),
        peer_equal = expressions.peer_equal,
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
        return Err("Window peer build returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window peer status")?;
    if status != "ok" {
        return Err(format!("Window peer build returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window peer rows")?,
        "Window peer rows",
    )?;
    let bytes = window_nonnegative(
        window_required(&row, 3, "Window peer bytes")?,
        "Window peer bytes",
    )?;
    let last_row_id = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window peer completion")?;
    let state_rows = window_nonnegative(
        window_required(&row, 6, "Window peer mutations")?,
        "Window peer mutations",
    )?;
    Ok(window_internal_page(
        processed,
        bytes,
        state_rows,
        last_row_id,
        complete,
    ))
}

// Atomic bounded Window frame primitive: resolve one frame page against the
// dynamic ordered state and return its complete mutation summary.
pub(super) fn run_window_frames(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    spec: &WindowSpec,
    partition_queue_id: i64,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window frame row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window frame row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window frame byte budget")?;
    let (base_start, base_end, offset_valid) =
        window_frame_base_expressions(storage, expressions, spec)?;
    let intervals = window_frame_intervals(spec);
    let query = format!(
        r#"
        WITH partition AS MATERIALIZED (
          SELECT partition_id,row_count::bigint AS partition_rows
          FROM {partitions} WHERE partition_id=$1 AND dirty
        ),
        source AS MATERIALIZED (
          SELECT ordered.ordinal,ordered.peer_id,input_row.row_value,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {ordered} AS ordered
          JOIN {input} AS input_row USING(entry_id)
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
        based AS MATERIALIZED (
          SELECT current_input.*,peer.first_ordinal,peer.last_ordinal,
                 partition.partition_rows,
                 ({base_start})::bigint AS base_start,
                 ({base_end})::bigint AS base_end,
                 ({offset_valid}) AS offset_valid
          FROM selected AS current_input
          CROSS JOIN partition
          JOIN {peers} AS peer USING(peer_id)
        ),
        split AS MATERIALIZED (
          SELECT based.*,{intervals}
          FROM based
        ),
        normalized AS MATERIALIZED (
          SELECT split.*,
                 CASE WHEN raw_start_1<=raw_end_1 THEN raw_start_1 END AS start_1,
                 CASE WHEN raw_start_1<=raw_end_1 THEN raw_end_1 END AS end_1,
                 CASE WHEN raw_start_2<=raw_end_2 THEN raw_start_2 END AS start_2,
                 CASE WHEN raw_start_2<=raw_end_2 THEN raw_end_2 END AS end_2,
                 CASE WHEN raw_start_3<=raw_end_3 THEN raw_start_3 END AS start_3,
                 CASE WHEN raw_start_3<=raw_end_3 THEN raw_end_3 END AS end_3
          FROM split
        ),
        status AS MATERIALIZED (
          SELECT CASE
                   WHEN NOT EXISTS(SELECT 1 FROM partition) THEN 'missing_partition'
                   WHEN EXISTS(SELECT 1 FROM based WHERE NOT offset_valid)
                     THEN 'invalid_offset'
                   ELSE 'ok'
                 END AS value
        ),
        inserted AS (
          INSERT INTO {frames}(
            ordinal,start_1,end_1,start_2,end_2,start_3,end_3,frame_count
          )
          SELECT ordinal,start_1,end_1,start_2,end_2,start_3,end_3,
                 coalesce(end_1-start_1+1,0)
                   +coalesce(end_2-start_2+1,0)
                   +coalesce(end_3-start_3+1,0)
          FROM normalized,status WHERE status.value='ok'
          RETURNING ordinal
        )
        SELECT (SELECT value FROM status),
               count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(ordinal ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM inserted)
        FROM selected
        "#,
        partitions = storage.partitions.sql(),
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
        peers = storage.peers.sql(),
        frames = storage.frames.sql(),
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
        return Err("Window frame build returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window frame status")?;
    if status != "ok" {
        return Err(format!("Window frame build returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window frame rows")?,
        "Window frame rows",
    )?;
    let bytes = window_nonnegative(
        window_required(&row, 3, "Window frame bytes")?,
        "Window frame bytes",
    )?;
    let last_row_id = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window frame completion")?;
    let inserted = window_nonnegative(
        window_required(&row, 6, "Window frame inserts")?,
        "Window frame inserts",
    )?;
    if inserted != processed {
        return Err("Window frame insert count is inconsistent".into());
    }
    Ok(window_internal_page(
        processed,
        bytes,
        inserted,
        last_row_id,
        complete,
    ))
}

pub(super) fn window_frame_base_expressions(
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    spec: &WindowSpec,
) -> Result<(String, String, String), String> {
    let options = spec.frame.options;
    let start_offset = expressions.frame_start_offset.as_deref().unwrap_or("NULL");
    let end_offset = expressions.frame_end_offset.as_deref().unwrap_or("NULL");
    let mode_rows = options & pg_sys::FRAMEOPTION_ROWS != 0;
    let mode_groups = options & pg_sys::FRAMEOPTION_GROUPS != 0;
    let start = if options & pg_sys::FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
        "1".into()
    } else if options & pg_sys::FRAMEOPTION_START_CURRENT_ROW != 0 {
        if mode_rows {
            "current_input.ordinal".into()
        } else {
            "peer.first_ordinal".into()
        }
    } else if options & pg_sys::FRAMEOPTION_START_OFFSET_PRECEDING != 0 {
        if mode_rows {
            format!("greatest(1::numeric,current_input.ordinal::numeric-({start_offset})::numeric)")
        } else if mode_groups {
            format!(
                "coalesce((SELECT first_ordinal FROM {peers} \
                 WHERE peer_id=greatest(1::numeric,current_input.peer_id::numeric-({start_offset})::numeric)::bigint), \
                 partition.partition_rows+1)",
                peers = storage.peers.sql()
            )
        } else {
            return Err("Window RANGE offset escaped capability validation".into());
        }
    } else if mode_rows {
        format!(
            "least(partition.partition_rows::numeric+1,current_input.ordinal::numeric+({start_offset})::numeric)"
        )
    } else if mode_groups {
        format!(
            "coalesce((SELECT first_ordinal FROM {peers} \
             WHERE peer_id::numeric=current_input.peer_id::numeric+({start_offset})::numeric), \
             partition.partition_rows+1)",
            peers = storage.peers.sql()
        )
    } else {
        return Err("Window RANGE offset escaped capability validation".into());
    };
    let end = if options & pg_sys::FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
        "partition.partition_rows".into()
    } else if options & pg_sys::FRAMEOPTION_END_CURRENT_ROW != 0 {
        if mode_rows {
            "current_input.ordinal".into()
        } else {
            "peer.last_ordinal".into()
        }
    } else if options & pg_sys::FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
        if mode_rows {
            format!("greatest(0::numeric,current_input.ordinal::numeric-({end_offset})::numeric)")
        } else if mode_groups {
            format!(
                "coalesce((SELECT last_ordinal FROM {peers} \
                 WHERE peer_id=current_input.peer_id-({end_offset})::bigint),0)",
                peers = storage.peers.sql()
            )
        } else {
            return Err("Window RANGE offset escaped capability validation".into());
        }
    } else if mode_rows {
        format!(
            "least(partition.partition_rows::numeric,current_input.ordinal::numeric+({end_offset})::numeric)"
        )
    } else if mode_groups {
        format!(
            "coalesce((SELECT last_ordinal FROM {peers} \
             WHERE peer_id::numeric=current_input.peer_id::numeric+({end_offset})::numeric), \
             partition.partition_rows)",
            peers = storage.peers.sql()
        )
    } else {
        return Err("Window RANGE offset escaped capability validation".into());
    };
    let mut valid = Vec::new();
    if spec.frame.start_offset.is_some() {
        valid.push(format!(
            "({start_offset}) IS NOT NULL AND ({start_offset})::numeric>=0 \
             AND ({start_offset})::numeric=pg_catalog.trunc(({start_offset})::numeric)"
        ));
    }
    if spec.frame.end_offset.is_some() {
        valid.push(format!(
            "({end_offset}) IS NOT NULL AND ({end_offset})::numeric>=0 \
             AND ({end_offset})::numeric=pg_catalog.trunc(({end_offset})::numeric)"
        ));
    }
    Ok((
        start,
        end,
        if valid.is_empty() {
            "true".into()
        } else {
            valid.join(" AND ")
        },
    ))
}

pub(super) fn window_frame_intervals(spec: &WindowSpec) -> String {
    let options = spec.frame.options;
    let pairs = if options & pg_sys::FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
        vec![
            ("based.base_start", "least(based.base_end,based.ordinal-1)"),
            (
                "greatest(based.base_start,based.ordinal+1)",
                "based.base_end",
            ),
        ]
    } else if options & pg_sys::FRAMEOPTION_EXCLUDE_GROUP != 0 {
        vec![
            (
                "based.base_start",
                "least(based.base_end,based.first_ordinal-1)",
            ),
            (
                "greatest(based.base_start,based.last_ordinal+1)",
                "based.base_end",
            ),
        ]
    } else if options & pg_sys::FRAMEOPTION_EXCLUDE_TIES != 0 {
        vec![
            (
                "based.base_start",
                "least(based.base_end,based.first_ordinal-1)",
            ),
            (
                "greatest(based.base_start,based.ordinal)",
                "least(based.base_end,based.ordinal)",
            ),
            (
                "greatest(based.base_start,based.last_ordinal+1)",
                "based.base_end",
            ),
        ]
    } else {
        vec![("based.base_start", "based.base_end")]
    };
    (0..3)
        .flat_map(|index| {
            let (start, end) = pairs.get(index).copied().unwrap_or(("NULL", "NULL"));
            [
                format!("{start}::bigint AS raw_start_{}", index + 1),
                format!("{end}::bigint AS raw_end_{}", index + 1),
            ]
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WindowFoldPrimitive {
    processed_rows: u64,
    processed_bytes: u64,
    last_frame_ordinal: Option<i64>,
    complete: bool,
    state_rows: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WindowFinalizePrimitive {
    applied: bool,
    work_bytes: u64,
    state_rows: u64,
}

pub(super) fn validate_window_fold_status(status: &str) -> Result<(), String> {
    if status == "ok" {
        Ok(())
    } else {
        Err(format!("Window aggregate fold returned {status}"))
    }
}

pub(super) fn validate_window_finalize_decision(
    applied: bool,
    work_bytes: u64,
    remaining_rows: usize,
    remaining_bytes: usize,
    allow_oversized_item: bool,
) -> Result<(), String> {
    let remaining_bytes = u64::try_from(remaining_bytes)
        .map_err(|_| "Window finalize remaining byte budget exceeds u64")?;
    if applied {
        if remaining_rows == 0 {
            return Err("Window aggregate finalization exceeded its remaining rows".into());
        }
        if work_bytes > remaining_bytes && !allow_oversized_item {
            return Err("Window aggregate finalization exceeded its remaining bytes".into());
        }
    } else if remaining_rows > 0 && (work_bytes <= remaining_bytes || allow_oversized_item) {
        return Err("Window aggregate finalization blocked despite available budget".into());
    }
    Ok(())
}

// Atomic bounded Window fold primitive: apply one aggregate function page to
// durable fold state; output publication is handled by the shared context.
pub(super) fn run_window_aggregate_fold(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    function_ordinal: u32,
    cursor: WindowFoldCursor,
) -> Result<WindowFoldPage, String> {
    let index = usize::try_from(function_ordinal - 1)
        .map_err(|_| "Window function ordinal exceeds usize")?;
    let function = expressions
        .functions
        .get(index)
        .ok_or_else(|| "Window aggregate fold is outside its plan".to_string())?;
    let WindowFunctionCapability::Aggregate(capability) = &function.capability else {
        return Err("native Window function entered aggregate fold".into());
    };
    let accumulator = storage
        .accumulators
        .get(index)
        .and_then(Option::as_ref)
        .ok_or_else(|| "Window aggregate has no accumulator relation".to_string())?;
    let partition_arguments = unsafe { [DatumWithOid::new(partition_queue_id, pg_sys::INT8OID)] };
    let partition_rows = transaction.read(
        &format!(
            "SELECT row_count::bigint FROM {} \
             WHERE partition_id=$1 AND dirty",
            storage.partitions.sql()
        ),
        &partition_arguments,
    )?;
    if partition_rows.len() != 1 {
        return Err("Window aggregate fold has no unique dirty partition".into());
    }
    let partition_rows: i64 = window_required(&partition_rows.first(), 1, "Window partition rows")?;
    if partition_rows < 0 || cursor.output_ordinal > i64::max(partition_rows, 1) {
        return Err("Window aggregate output ordinal is outside its partition".into());
    }
    if partition_rows == 0 {
        if cursor.output_ordinal != 1
            || cursor.last_frame_ordinal.is_some()
            || cursor.ready_to_finalize
        {
            return Err("empty Window partition has an aggregate fold cursor".into());
        }
        let rows = transaction.read(
            &format!("SELECT count(*)::bigint FROM {}", accumulator.sql()),
            &[],
        )?;
        if window_required::<i64>(&rows.first(), 1, "Window accumulator rows")? != 0 {
            return Err("empty Window partition retained aggregate state".into());
        }
        return Ok(WindowFoldPage {
            facts: PrimitiveFacts {
                ..PrimitiveFacts::default()
            },
            next_cursor: None,
            work_items: 1,
        });
    }

    let budget = transaction.budget();
    let max_rows =
        u64::try_from(budget.max_input_rows).map_err(|_| "Window fold row budget exceeds u64")?;
    let max_bytes =
        u64::try_from(budget.max_input_bytes).map_err(|_| "Window fold byte budget exceeds u64")?;
    let mut state_rows = 0_u64;
    let mut processed_rows = 0_u64;
    let mut processed_bytes = 0_u64;
    let mut work_items = 0_usize;
    let mut next_cursor = Some(cursor);

    while work_items < WINDOW_FOLD_WORK_ITEM_CAP
        && processed_rows < max_rows
        && processed_bytes < max_bytes
    {
        let current = next_cursor
            .ok_or_else(|| "completed Window aggregate retained a fold cursor".to_string())?;
        work_items += 1;
        let state_arguments = unsafe {
            [
                DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(current.output_ordinal, pg_sys::INT8OID),
            ]
        };
        let initialized = if current.last_frame_ordinal.is_none() && !current.ready_to_finalize {
            let initial = initial_state_sql(capability);
            let no_trans_value =
                capability.transition_is_strict && capability.initial_literal.is_none();
            let inserted = transaction.write(
                &format!(
                    "INSERT INTO {}(
                       singleton,partition_id,output_ordinal,state_value,no_trans_value
                     ) VALUES(true,$1,$2,{initial},$3)
                     RETURNING singleton",
                    accumulator.sql()
                ),
                &unsafe {
                    [
                        DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
                        DatumWithOid::new(current.output_ordinal, pg_sys::INT8OID),
                        DatumWithOid::new(no_trans_value, pg_sys::BOOLOID),
                    ]
                },
            )?;
            if inserted.len() != 1 {
                return Err("Window aggregate fold did not initialize one accumulator".into());
            }
            state_rows = state_rows
                .checked_add(1)
                .ok_or_else(|| "Window aggregate fold state count overflow".to_string())?;
            true
        } else {
            let rows = transaction.read(
                &format!(
                    "SELECT count(*)::bigint FROM {} \
                     WHERE singleton AND partition_id=$1 AND output_ordinal=$2",
                    accumulator.sql()
                ),
                &state_arguments,
            )?;
            if window_required::<i64>(&rows.first(), 1, "Window accumulator rows")? != 1 {
                return Err("Window aggregate fold lost its accumulator".into());
            }
            false
        };

        let ready = if current.ready_to_finalize {
            current
        } else {
            let remaining_rows = usize::try_from(max_rows - processed_rows)
                .map_err(|_| "Window fold remaining row budget exceeds usize")?;
            let remaining_bytes = usize::try_from(max_bytes - processed_bytes)
                .map_err(|_| "Window fold remaining byte budget exceeds usize")?;
            let allow_oversized_row = processed_rows == 0;
            let folded = window_fold_page(
                transaction,
                storage,
                accumulator,
                function,
                capability,
                partition_queue_id,
                current.output_ordinal,
                current.last_frame_ordinal,
                remaining_rows,
                remaining_bytes,
                allow_oversized_row,
            )?;
            let remaining_rows = u64::try_from(remaining_rows)
                .map_err(|_| "Window fold remaining row budget exceeds u64")?;
            let remaining_bytes = u64::try_from(remaining_bytes)
                .map_err(|_| "Window fold remaining byte budget exceeds u64")?;
            if folded.processed_rows > remaining_rows
                || (folded.processed_bytes > remaining_bytes
                    && !(allow_oversized_row && folded.processed_rows == 1))
            {
                return Err("Window aggregate fold exceeded its remaining step budget".into());
            }
            processed_rows = processed_rows
                .checked_add(folded.processed_rows)
                .ok_or_else(|| "Window aggregate fold row count overflow".to_string())?;
            processed_bytes = processed_bytes
                .checked_add(folded.processed_bytes)
                .ok_or_else(|| "Window aggregate fold byte count overflow".to_string())?;
            state_rows = state_rows
                .checked_add(folded.state_rows)
                .ok_or_else(|| "Window aggregate fold state count overflow".to_string())?;

            if !folded.complete {
                if folded.processed_rows == 0 {
                    if !initialized || current.last_frame_ordinal.is_some() {
                        return Err("resumed Window aggregate fold made no progress".into());
                    }
                    let deleted = transaction.write(
                        &format!(
                            "DELETE FROM {} \
                             WHERE singleton AND partition_id=$1 AND output_ordinal=$2 \
                             RETURNING singleton",
                            accumulator.sql()
                        ),
                        &state_arguments,
                    )?;
                    if deleted.len() != 1 {
                        return Err(
                            "Window aggregate fold could not release an unstarted accumulator"
                                .into(),
                        );
                    }
                    state_rows = state_rows
                        .checked_add(1)
                        .ok_or_else(|| "Window aggregate fold state count overflow".to_string())?;
                    next_cursor = Some(current);
                } else {
                    next_cursor = Some(WindowFoldCursor {
                        output_ordinal: current.output_ordinal,
                        last_frame_ordinal: folded.last_frame_ordinal,
                        ready_to_finalize: false,
                    });
                }
                break;
            }
            WindowFoldCursor {
                output_ordinal: current.output_ordinal,
                last_frame_ordinal: folded.last_frame_ordinal.or(current.last_frame_ordinal),
                ready_to_finalize: true,
            }
        };
        next_cursor = Some(ready);

        if processed_rows == max_rows || processed_bytes >= max_bytes {
            break;
        }
        let remaining_rows = usize::try_from(max_rows - processed_rows)
            .map_err(|_| "Window finalize remaining row budget exceeds usize")?;
        let remaining_bytes = usize::try_from(max_bytes - processed_bytes)
            .map_err(|_| "Window finalize remaining byte budget exceeds usize")?;
        let finalized = window_finalize_fold(
            transaction,
            storage,
            expressions,
            accumulator,
            function,
            capability,
            partition_queue_id,
            current.output_ordinal,
            function_ordinal,
            remaining_rows,
            remaining_bytes,
            processed_rows == 0,
        )?;
        if !finalized.applied {
            break;
        }
        processed_rows = processed_rows
            .checked_add(1)
            .ok_or_else(|| "Window aggregate finalization row count overflow".to_string())?;
        processed_bytes = processed_bytes
            .checked_add(finalized.work_bytes)
            .ok_or_else(|| "Window aggregate finalization byte count overflow".to_string())?;
        state_rows = state_rows
            .checked_add(finalized.state_rows)
            .ok_or_else(|| "Window aggregate finalization state count overflow".to_string())?;
        next_cursor = if current.output_ordinal == partition_rows {
            None
        } else {
            Some(WindowFoldCursor {
                output_ordinal: current
                    .output_ordinal
                    .checked_add(1)
                    .ok_or_else(|| "Window output ordinal overflow".to_string())?,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            })
        };
        if next_cursor.is_none() {
            break;
        }
    }

    Ok(WindowFoldPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: processed_rows,
                input_bytes: processed_bytes,
                ..WorkUsage::default()
            },
            state_rows,
            output: OutputFacts::None,
        },
        next_cursor,
        work_items,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn window_fold_page(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    accumulator: &RelationRef,
    function: &WindowFunctionPlan,
    capability: &AggregateCapability,
    partition_queue_id: i64,
    output_ordinal: i64,
    last_frame_ordinal: Option<i64>,
    max_rows: usize,
    max_bytes: usize,
    allow_oversized_row: bool,
) -> Result<WindowFoldPrimitive, String> {
    let arguments_nonnull = if function.current_arguments.is_empty() {
        "true".into()
    } else {
        function
            .current_arguments
            .iter()
            .map(|argument| format!("({argument}) IS NOT NULL"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let transition_call = format!(
        "{}(fold.state_value{})",
        capability.transition_function,
        if function.current_arguments.is_empty() {
            String::new()
        } else {
            format!(",{}", function.current_arguments.join(","))
        }
    );
    let (next_state, next_no_trans) = if capability.transition_is_strict {
        let advance = if capability.initial_literal.is_none() {
            let first = function.current_arguments.first().ok_or_else(|| {
                "strict aggregate with NULL initial state has no argument".to_string()
            })?;
            format!(
                "CASE WHEN fold.no_trans_value \
                      THEN ({first})::{} \
                      WHEN fold.state_value IS NULL \
                      THEN fold.state_value \
                      ELSE {transition_call} END",
                capability.transition_type
            )
        } else {
            format!(
                "CASE WHEN fold.state_value IS NULL \
                      THEN fold.state_value ELSE {transition_call} END"
            )
        };
        (
            format!(
                "CASE WHEN ({filter}) IS TRUE \
                      THEN CASE WHEN {arguments_nonnull} \
                                THEN {advance} ELSE fold.state_value END \
                      ELSE fold.state_value END",
                filter = function.filter,
            ),
            format!(
                "CASE WHEN ({filter}) IS TRUE AND ({arguments_nonnull}) \
                      THEN false ELSE fold.no_trans_value END",
                filter = function.filter,
            ),
        )
    } else {
        (
            format!(
                "CASE WHEN ({}) IS TRUE THEN {transition_call} \
                      ELSE fold.state_value END",
                function.filter
            ),
            "false".into(),
        )
    };
    let max_rows = window_i64_budget(max_rows, "Window fold row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window fold row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(max_bytes, "Window fold byte budget")?;
    let query = format!(
        r#"
        WITH RECURSIVE frame AS MATERIALIZED (
          SELECT start_1,end_1,start_2,end_2,start_3,end_3
          FROM {frames}
          WHERE ordinal=$2
        ),
        interval_1 AS MATERIALIZED (
          SELECT interval_row.ordinal,interval_row.entry_id,current_input.row_value,
                 shiba_internal.effect_row_bytes(current_input.row_value)
                   AS row_bytes
          FROM frame
          CROSS JOIN LATERAL (
            SELECT ordered.ordinal,ordered.entry_id
            FROM {ordered} AS ordered
            WHERE frame.start_1 IS NOT NULL
              AND ordered.ordinal BETWEEN frame.start_1 AND frame.end_1
              AND ($3 IS NULL OR ordered.ordinal>$3)
            ORDER BY ordered.ordinal
            LIMIT $6
          ) AS interval_row
          JOIN {input} AS current_input
            ON current_input.entry_id=interval_row.entry_id
        ),
        interval_2 AS MATERIALIZED (
          SELECT interval_row.ordinal,interval_row.entry_id,current_input.row_value,
                 shiba_internal.effect_row_bytes(current_input.row_value)
                   AS row_bytes
          FROM frame
          CROSS JOIN LATERAL (
            SELECT ordered.ordinal,ordered.entry_id
            FROM {ordered} AS ordered
            WHERE frame.start_2 IS NOT NULL
              AND ordered.ordinal BETWEEN frame.start_2 AND frame.end_2
              AND ($3 IS NULL OR ordered.ordinal>$3)
            ORDER BY ordered.ordinal
            LIMIT $6
          ) AS interval_row
          JOIN {input} AS current_input
            ON current_input.entry_id=interval_row.entry_id
        ),
        interval_3 AS MATERIALIZED (
          SELECT interval_row.ordinal,interval_row.entry_id,current_input.row_value,
                 shiba_internal.effect_row_bytes(current_input.row_value)
                   AS row_bytes
          FROM frame
          CROSS JOIN LATERAL (
            SELECT ordered.ordinal,ordered.entry_id
            FROM {ordered} AS ordered
            WHERE frame.start_3 IS NOT NULL
              AND ordered.ordinal BETWEEN frame.start_3 AND frame.end_3
              AND ($3 IS NULL OR ordered.ordinal>$3)
            ORDER BY ordered.ordinal
            LIMIT $6
          ) AS interval_row
          JOIN {input} AS current_input
            ON current_input.entry_id=interval_row.entry_id
        ),
        source AS MATERIALIZED (
          SELECT intervals.*
          FROM (
            SELECT * FROM interval_1
            UNION ALL
            SELECT * FROM interval_2
            UNION ALL
            SELECT * FROM interval_3
          ) AS intervals
          ORDER BY ordinal
          LIMIT $6
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY ordinal) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal<=$4
            AND (($7::boolean AND page_ordinal=1) OR running_bytes<=$5)
        ),
        fold(step,state_value,no_trans_value,last_frame_ordinal) AS (
          SELECT 0::bigint,accumulator.state_value,
                 accumulator.no_trans_value,NULL::bigint
          FROM {accumulator} AS accumulator
          WHERE accumulator.singleton
            AND accumulator.partition_id=$1
            AND accumulator.output_ordinal=$2
          UNION ALL
          SELECT selected.page_ordinal,{next_state},{next_no_trans},
                 selected.ordinal
          FROM fold
          JOIN selected ON selected.page_ordinal=fold.step+1
          CROSS JOIN LATERAL (
            SELECT selected.row_value AS row_value
          ) AS current_input
        ),
        final_fold AS MATERIALIZED (
          SELECT * FROM fold ORDER BY step DESC LIMIT 1
        ),
        updated AS (
          UPDATE {accumulator} AS accumulator
          SET state_value=final_fold.state_value,
              no_trans_value=final_fold.no_trans_value
          FROM final_fold
          WHERE accumulator.singleton
            AND accumulator.partition_id=$1
            AND accumulator.output_ordinal=$2
            AND final_fold.step>0
          RETURNING 1
        )
        SELECT CASE
                 WHEN (SELECT count(*) FROM frame)<>1
                   THEN 'missing_frame'
                 WHEN (
                   SELECT count(*) FROM {accumulator}
                   WHERE singleton AND partition_id=$1 AND output_ordinal=$2
                 )<>1 THEN 'missing_accumulator'
                 ELSE 'ok'
               END,
               count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(ordinal ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM updated)
        FROM selected
        "#,
        accumulator = accumulator.sql(),
        frames = storage.frames.sql(),
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
    );
    let rows = transaction.write(&query, &unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(output_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(last_frame_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
            DatumWithOid::new(allow_oversized_row, pg_sys::BOOLOID),
        ]
    })?;
    if rows.len() != 1 {
        return Err("Window aggregate fold returned no page summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window aggregate fold status")?;
    validate_window_fold_status(&status)?;
    let processed_rows = window_nonnegative(
        window_required(&row, 2, "Window aggregate fold rows")?,
        "Window aggregate fold rows",
    )?;
    let processed_bytes = window_nonnegative(
        window_required(&row, 3, "Window aggregate fold bytes")?,
        "Window aggregate fold bytes",
    )?;
    let last_frame_ordinal = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window aggregate fold completion")?;
    let state_rows = window_nonnegative(
        window_required(&row, 6, "Window aggregate fold mutations")?,
        "Window aggregate fold mutations",
    )?;
    if state_rows != u64::from(processed_rows > 0) {
        return Err("Window aggregate fold mutation count is inconsistent".into());
    }
    Ok(WindowFoldPrimitive {
        processed_rows,
        processed_bytes,
        last_frame_ordinal,
        complete,
        state_rows,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn window_finalize_fold(
    transaction: &mut StepContext<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    accumulator: &RelationRef,
    function: &WindowFunctionPlan,
    capability: &AggregateCapability,
    partition_queue_id: i64,
    output_ordinal: i64,
    function_ordinal: u32,
    remaining_rows: usize,
    remaining_bytes: usize,
    allow_oversized_item: bool,
) -> Result<WindowFinalizePrimitive, String> {
    let state = "accumulator.state_value";
    let value = capability.final_function.as_ref().map_or_else(
        || state.into(),
        |final_function| format!("{final_function}({state})"),
    );
    let function_column = quote_identifier(&format!("function_{function_ordinal}"));
    let function_bytes = scalar_work_bytes_sql("finalized.function_value");
    let is_last_function = usize::try_from(function_ordinal)
        .ok()
        .is_some_and(|ordinal| ordinal == expressions.functions.len());
    let (candidate_prepare, candidate_bytes, candidate_write) = if is_last_function {
        let output_key = canonical_row_key_sql("output_rows.output_row", &storage.output_type);
        let projected_functions = expressions
            .functions
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let column = quote_identifier(&format!("function_{}", index + 1));
                if index + 1 == usize::try_from(function_ordinal).unwrap_or(usize::MAX) {
                    format!(",finalized.function_value AS {column}")
                } else {
                    format!(",ordered.{column}")
                }
            })
            .collect::<String>();
        (
            format!(
                r#"
                projected AS MATERIALIZED (
                  SELECT ordered.ordinal,ordered.entry_id,ordered.copy_ordinal,
                         ordered.peer_id{projected_functions}
                  FROM {ordered} AS ordered CROSS JOIN finalized
                  WHERE ordered.ordinal=$2
                ),
                output_rows AS MATERIALIZED (
                  SELECT updated.ordinal,
                         ROW({outputs})::{output_type} AS output_row
                  FROM projected AS updated
                  JOIN {input} AS input_row
                    ON input_row.entry_id=updated.entry_id
                ),
                keyed AS MATERIALIZED (
                  SELECT output_rows.*,
                         {output_key} AS output_key
                  FROM output_rows
                ),
                "#,
                ordered = storage.ordered.sql(),
                outputs = expressions.outputs,
                output_type = storage.output_type.sql(),
                input = storage.input.sql(),
            ),
            "shiba_internal.effect_row_bytes(keyed.output_row)".to_string(),
            format!(
                r#"
                candidate_changed AS (
                  INSERT INTO {candidate} AS target(
                    partition_id,output_key,output_row,multiplicity
                  )
                  SELECT $1,keyed.output_key,keyed.output_row,1::numeric
                  FROM keyed,decision
                  WHERE decision.permitted
                  ON CONFLICT(partition_id,output_key) DO UPDATE
                  SET output_row=EXCLUDED.output_row,
                      multiplicity=target.multiplicity+1::numeric
                  RETURNING 1
                ),
                "#,
                candidate = storage.candidate.sql(),
            ),
        )
    } else {
        (
            String::new(),
            "0::bigint".to_string(),
            r#"
            candidate_changed AS (SELECT 1 WHERE false),
            "#
            .to_string(),
        )
    };
    let query = format!(
        r#"
        WITH finalized AS MATERIALIZED (
          SELECT ({value})::{result_type} AS function_value
          FROM {accumulator} AS accumulator
          WHERE accumulator.singleton
            AND accumulator.partition_id=$1
            AND accumulator.output_ordinal=$2
        ),
        {candidate_prepare}
        materialized AS MATERIALIZED (
          SELECT (
                   {function_bytes}+{candidate_bytes}
                 )::bigint AS work_bytes
          FROM finalized{candidate_from}
        ),
        decision AS MATERIALIZED (
          SELECT materialized.work_bytes,
                 $3::bigint>=1
                   AND (materialized.work_bytes<=$4::bigint OR $5::boolean)
                   AS permitted
          FROM materialized
        ),
        state_updated AS (
          UPDATE {ordered} AS ordered
          SET {function_column}=finalized.function_value
          FROM finalized,decision
          WHERE ordered.ordinal=$2 AND decision.permitted
          RETURNING 1
        ),
        {candidate_write}
        deleted AS (
          DELETE FROM {accumulator} AS accumulator
          USING decision
          WHERE accumulator.singleton
            AND accumulator.partition_id=$1
            AND accumulator.output_ordinal=$2
            AND decision.permitted
          RETURNING 1
        )
        SELECT CASE
                 WHEN (SELECT count(*) FROM finalized)<>1
                   THEN 'missing_accumulator'
                 WHEN (SELECT count(*) FROM materialized)<>1
                   THEN 'missing_output'
                 WHEN (SELECT permitted FROM decision)
                   THEN 'applied'
                 ELSE 'blocked'
               END,
               coalesce((SELECT work_bytes FROM decision),0)::bigint,
               (SELECT count(*)::bigint FROM state_updated)
                 +(SELECT count(*)::bigint FROM candidate_changed)
                 +(SELECT count(*)::bigint FROM deleted)
        "#,
        result_type = function.result_type,
        accumulator = accumulator.sql(),
        ordered = storage.ordered.sql(),
        candidate_from = if is_last_function { ",keyed" } else { "" },
    );
    let rows = transaction.write(&query, &unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(output_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(
                window_i64_budget(remaining_rows, "Window finalize row budget")?,
                pg_sys::INT8OID,
            ),
            DatumWithOid::new(
                window_i64_budget(remaining_bytes, "Window finalize byte budget")?,
                pg_sys::INT8OID,
            ),
            DatumWithOid::new(allow_oversized_item, pg_sys::BOOLOID),
        ]
    })?;
    if rows.len() != 1 {
        return Err("Window aggregate finalization returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window aggregate finalization status")?;
    if status != "applied" && status != "blocked" {
        return Err(format!("Window aggregate finalization returned {status}"));
    }
    let work_bytes = window_nonnegative(
        window_required(&row, 2, "Window aggregate finalization bytes")?,
        "Window aggregate finalization bytes",
    )?;
    let state_rows = window_nonnegative(
        window_required(&row, 3, "Window aggregate finalization mutations")?,
        "Window aggregate finalization mutations",
    )?;
    let expected = if is_last_function { 3 } else { 2 };
    let applied = status == "applied";
    if state_rows != if applied { expected } else { 0 } {
        return Err("Window aggregate finalization mutation count is inconsistent".into());
    }
    if work_bytes == 0 {
        return Err("Window aggregate finalization returned no materialized bytes".into());
    }
    validate_window_finalize_decision(
        applied,
        work_bytes,
        remaining_rows,
        remaining_bytes,
        allow_oversized_item,
    )?;
    Ok(WindowFinalizePrimitive {
        applied,
        work_bytes,
        state_rows,
    })
}
