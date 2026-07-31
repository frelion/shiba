use super::*;

pub(crate) const KERNEL: crate::execution::KernelFn = crate::execution::KernelFn::new(
    crate::execution::KernelContract::with_phases(
        &[crate::execution::InputContract::Operator],
        crate::execution::OutputContract::EffectStream,
        &[
            crate::execution::LifecyclePhase::Admit,
            crate::execution::LifecyclePhase::Drain,
            crate::execution::LifecyclePhase::Frontier,
        ],
    ),
    step,
);

pub(crate) fn step(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage_id: u32,
) -> Result<StepReceipt, String> {
    let stage = plan
        .stages
        .get(usize::try_from(stage_id).map_err(|_| "Distinct stage ID exceeds usize")?)
        .ok_or_else(|| format!("dataflow has no Distinct stage {stage_id}"))?;
    let OperatorSpec::Distinct(spec) = &stage.spec else {
        return Err("Distinct executor received another operator".into());
    };
    if transaction.inputs().len() != 1
        || transaction.input(0)?.producer != ProducerKind::Operator
        || stage.inputs.len() != 1
    {
        return Err("Distinct must have one operator input".into());
    }
    let input = transaction.input(0)?.clone();
    let continuation_relation = transaction.continuation_storage()?;
    validate_continuation_abi(transaction, &continuation_relation)?;
    let mut stored = load_continuation(
        transaction,
        &continuation_relation,
        input.stream_id,
        input.next_chunk_seq,
    )?;
    crate::execution::validate_continuation_authority(transaction, stored.persisted)?;
    let chunk = next_chunk(transaction, 0)?
        .ok_or_else(|| "runnable Distinct has no input chunk".to_string())?;
    if stored.persisted {
        if chunk.kind != ChunkKind::Data || stored.value.phase == DistinctPhase::Frontier {
            return Err("Distinct continuation is not pinned to a data chunk".into());
        }
    } else {
        stored.value.phase = match chunk.kind {
            ChunkKind::Data => DistinctPhase::Apply,
            ChunkKind::Frontier => DistinctPhase::Frontier,
        };
    }
    let queue = transaction.state_storage(2)?;
    let touched = transaction.state_storage(3)?;
    require_empty_touched(transaction, &touched)?;

    if stored.value.phase == DistinctPhase::Frontier {
        if stored.value.input.row_ordinal != 0 {
            return Err("Distinct frontier has a row continuation".into());
        }
        require_empty_queue(transaction, &queue)?;
        let output = append_frontier(transaction, chunk.lsn)?;
        advance_input(
            transaction,
            0,
            chunk.sequence + 1,
            chunk.lsn,
            WorkUsage::default(),
        )?;
        replace_continuation(transaction, &continuation_relation, stored, None)?;
        DistinctMachine.apply(
            stored.value,
            DistinctActionResult::FrontierForwarded(PrimitiveFacts {
                output,
                ..PrimitiveFacts::default()
            }),
            transaction.budget(),
        )?;
        return transaction.transition_facts(
            KernelPhase::Frontier,
            PrimitiveFacts {
                output,
                ..PrimitiveFacts::default()
            },
        );
    }

    let input_storage = transaction.payload_storage(input.stream_id)?;
    if stored.value.phase == DistinctPhase::Apply && stored.value.input.row_ordinal == 0 {
        payload_facts(transaction, &input_storage.relation, &chunk)?;
    }
    let output = transaction.output()?.clone();
    let output_storage = transaction.payload_storage(output.stream_id)?;
    let state = transaction.state_storage(0)?;
    let bag = transaction.state_storage(1)?;
    validate_state_abi(
        transaction,
        &state,
        spec.keys.len(),
        output_storage.row_type.oid(),
        spec,
    )?;
    validate_bag_abi(transaction, &bag, output_storage.row_type.oid())?;
    validate_queue_abi(transaction, &queue, output_storage.row_type.oid())?;
    validate_touched_abi(transaction, &touched)?;

    if stored.value.phase == DistinctPhase::Drain {
        let drained = drain_queue(
            transaction,
            &output_storage.relation,
            &output_storage.row_type,
            &queue,
        )?;
        let next = if drained.remaining_effects > 0 {
            Some(stored.value)
        } else {
            finish_input_position(transaction, &input, &chunk, stored.value.input.row_ordinal)?
        };
        let facts = PrimitiveFacts { ..drained.facts };
        let transition = DistinctMachine.apply(
            stored.value,
            DistinctActionResult::Drained(AppliedPrefix {
                facts,
                occupancy: OccupancyDiff {
                    touched_keys: 0,
                    external_effects: facts.usage.output_rows,
                },
                next,
            }),
            transaction.budget(),
        )?;
        let DistinctTransition::Committed { continuation, .. } = transition;
        replace_continuation(transaction, &continuation_relation, stored, continuation)?;
        return transaction.transition_facts(KernelPhase::Drain, facts);
    }

    require_empty_queue(transaction, &queue)?;
    let bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: &input_storage.row_type,
            alias: "input_row",
        }],
    )?;
    let keys = spec
        .keys
        .iter()
        .map(|key| compile_scalar_expression(&key.expr, &bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let key_orders = spec
        .keys
        .iter()
        .map(|key| resolve_btree_step(transaction, key, "Distinct"))
        .collect::<Result<Vec<_>, _>>()?;
    let output_attributes = transaction.composite_attributes(&output_storage.row_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
    let expressions =
        compile_named_outputs(&stage.schema.outputs, &spec.outputs, &bindings, "Distinct")?;
    let facts = run_prefix(
        transaction,
        &chunk,
        stored.value.input.row_ordinal,
        &PrefixSql {
            input: &input_storage.relation,
            output_type: &output_storage.row_type,
            state: &state,
            bag: &bag,
            queue: &queue,
            touched: &touched,
            keys: &keys,
            key_orders: &key_orders,
            expressions: &expressions,
        },
    )?;
    let next = if facts.queued_effects > 0 {
        Some(DistinctContinuation {
            input: InputPosition::new(input.stream_id, chunk.sequence, facts.next_row)?,
            phase: DistinctPhase::Drain,
        })
    } else {
        finish_input_position(transaction, &input, &chunk, facts.next_row)?
    };
    let primitive = PrimitiveFacts {
        usage: facts.usage,
        state_rows: facts.state_rows,
        output: OutputFacts::None,
    };
    let result = DistinctActionResult::Applied(AppliedPrefix {
        facts: primitive,
        occupancy: OccupancyDiff {
            touched_keys: facts.touched_keys,
            external_effects: facts.queued_effects,
        },
        next,
    });
    let transition = DistinctMachine.apply(stored.value, result, transaction.budget())?;
    let DistinctTransition::Committed { continuation, .. } = transition;
    replace_continuation(transaction, &continuation_relation, stored, continuation)?;
    transaction.transition_facts(KernelPhase::Admit, primitive)
}

fn finish_input_position(
    transaction: &mut StepContext<'_, '_>,
    input: &crate::execution::InputState,
    chunk: &ChunkMeta,
    next_row: i64,
) -> Result<Option<DistinctContinuation>, String> {
    let chunk_rows = i64::try_from(chunk.rows).map_err(|_| "Distinct chunk rows exceed bigint")?;
    if next_row < chunk_rows {
        return Ok(Some(DistinctContinuation {
            input: InputPosition::new(input.stream_id, chunk.sequence, next_row)?,
            phase: DistinctPhase::Apply,
        }));
    }
    if next_row != chunk_rows {
        return Err("Distinct prefix advanced beyond its input chunk".into());
    }
    advance_input(
        transaction,
        0,
        chunk.sequence + 1,
        input.consumed_frontier_lsn,
        WorkUsage {
            input_rows: chunk.rows,
            input_bytes: chunk.bytes,
            ..WorkUsage::default()
        },
    )?;
    Ok(None)
}

// Atomic bounded Distinct admission primitive: update key/bag/queue state for
// one input prefix. Effect publication remains a separate shared boundary.
fn run_prefix(
    transaction: &mut StepContext<'_, '_>,
    chunk: &ChunkMeta,
    first_row: i64,
    sql: &PrefixSql<'_>,
) -> Result<PrefixFacts, String> {
    let PrefixSql {
        input,
        output_type,
        state,
        bag,
        queue,
        touched,
        keys,
        key_orders,
        expressions,
    } = sql;
    if keys.is_empty() || keys.len() != key_orders.len() || expressions.is_empty() {
        return Err("Distinct has no exact key or output expression".into());
    }
    let budget = transaction.budget();
    let key_columns = (1..=keys.len())
        .map(|index| format!("key_{index}"))
        .collect::<Vec<_>>();
    let key_column_list = key_columns.join(",");
    let qualified_key_column_list = key_columns
        .iter()
        .map(|column| format!("evaluated_base.{}", quote_identifier(column)))
        .collect::<Vec<_>>()
        .join(",");
    let key_select = keys
        .iter()
        .enumerate()
        .map(|(index, expression)| format!("{expression} AS key_{}", index + 1))
        .collect::<Vec<_>>()
        .join(",");
    let key_order = key_orders
        .iter()
        .enumerate()
        .map(|(index, order)| {
            format!(
                "evaluated_base.key_{} USING {} NULLS {}",
                index + 1,
                order.sort_operator,
                if order.nulls_first { "FIRST" } else { "LAST" }
            )
        })
        .chain(std::iter::once("evaluated_base.row_ordinal".into()))
        .collect::<Vec<_>>()
        .join(",");
    let conflict_keys = key_columns
        .iter()
        .zip(key_orders.iter())
        .map(|(column, order)| format!("{} {}", quote_identifier(column), order.opclass))
        .collect::<Vec<_>>()
        .join(",");
    let group_match = key_orders
        .iter()
        .enumerate()
        .map(|(index, order)| {
            distinct_null_safe_equality(
                &format!("resolved_groups.key_{}", index + 1),
                &format!("evaluated_base.key_{}", index + 1),
                &order.equality_operator,
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let output_key = canonical_row_key_sql("incoming.output_row", output_type);
    let lsn = format_lsn(chunk.lsn);
    let query = format!(
        r#"
        WITH candidates AS MATERIALIZED (
          SELECT input_row.row_ordinal,input_row.weight,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS input_bytes,
                 ROW({expressions})::{output_type} AS output_row,
                 {key_select}
          FROM {input} AS input_row
          WHERE input_row.stream_id=$1 AND input_row.chunk_seq=$2
            AND input_row.row_ordinal >= $3
          ORDER BY input_row.row_ordinal
          LIMIT $4
        ),
        measured AS (
          SELECT candidates.*,
                 row_number() OVER (ORDER BY row_ordinal) AS page_ordinal,
                 sum(input_bytes) OVER (ORDER BY row_ordinal) AS running_input_bytes
          FROM candidates
        ),
        incoming AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal=1 OR running_input_bytes <= $5
        ),
        evaluated_base AS MATERIALIZED (
          SELECT incoming.*,{output_key} AS output_key
          FROM incoming
        ),
        collapsed AS MATERIALIZED (
          SELECT DISTINCT ON ({qualified_key_column_list})
                 {qualified_key_column_list}
          FROM evaluated_base
          ORDER BY {key_order}
        ),
        resolved_groups AS MATERIALIZED (
          INSERT INTO {state} AS groups({key_column_list})
          SELECT {key_column_list} FROM collapsed
          ON CONFLICT({conflict_keys}) DO UPDATE
          SET multiplicity=groups.multiplicity
          RETURNING group_state_id,{key_column_list},multiplicity
        ),
        evaluated AS MATERIALIZED (
          SELECT evaluated_base.*,resolved_groups.group_state_id
          FROM evaluated_base
          JOIN resolved_groups ON {group_match}
        ),
        key_prefixes AS (
          SELECT evaluated.*,
                 sum(weight::numeric) OVER (
                   PARTITION BY group_state_id ORDER BY row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS key_prefix
          FROM evaluated
        ),
        physical_prefixes AS (
          SELECT key_prefixes.*,
                 sum(weight::numeric) OVER (
                   PARTITION BY group_state_id,output_key ORDER BY row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS physical_prefix
          FROM key_prefixes
        ),
        key_collapsed AS MATERIALIZED (
          SELECT group_state_id,sum(weight::numeric) AS net_weight,
                 min(key_prefix) AS min_prefix,max(key_prefix) AS max_prefix
          FROM key_prefixes GROUP BY group_state_id
        ),
        physical_collapsed AS MATERIALIZED (
          SELECT group_state_id,output_key,
                 (array_agg(output_row ORDER BY row_ordinal))[1] AS incoming_output_row,
                 sum(weight::numeric) AS net_weight,
                 min(physical_prefix) AS min_prefix,
                 max(physical_prefix) AS max_prefix
          FROM physical_prefixes
          GROUP BY group_state_id,output_key
        ),
        locked_bag AS MATERIALIZED (
          SELECT locked.*
          FROM physical_collapsed
          JOIN LATERAL (
            SELECT bag.*
            FROM {bag} AS bag
            WHERE bag.group_state_id=physical_collapsed.group_state_id
              AND bag.output_key=physical_collapsed.output_key
            LIMIT 1
            FOR UPDATE
          ) AS locked ON true
        ),
        key_decision AS MATERIALIZED (
          SELECT key_collapsed.group_state_id,
                 resolved_groups.multiplicity::numeric AS old_multiplicity,
                 resolved_groups.multiplicity::numeric+key_collapsed.net_weight
                   AS new_multiplicity,
                 resolved_groups.multiplicity::numeric+key_collapsed.min_prefix
                   AS minimum_multiplicity,
                 resolved_groups.multiplicity::numeric+key_collapsed.max_prefix
                   AS maximum_multiplicity
          FROM key_collapsed
          JOIN resolved_groups USING(group_state_id)
        ),
        physical_decision AS MATERIALIZED (
          SELECT physical_collapsed.*,locked_bag.bag_id,
                 locked_bag.output_row AS stored_output_row,
                 coalesce(locked_bag.multiplicity,0)::numeric AS old_multiplicity,
                 coalesce(locked_bag.multiplicity,0)::numeric
                   +physical_collapsed.net_weight AS new_multiplicity,
                 coalesce(locked_bag.multiplicity,0)::numeric
                   +physical_collapsed.min_prefix AS minimum_multiplicity,
                 coalesce(locked_bag.multiplicity,0)::numeric
                   +physical_collapsed.max_prefix AS maximum_multiplicity
          FROM physical_collapsed
          LEFT JOIN locked_bag
            ON locked_bag.group_state_id=physical_collapsed.group_state_id
           AND locked_bag.output_key=physical_collapsed.output_key
        ),
        validation AS MATERIALIZED (
          SELECT CASE
            WHEN (SELECT count(*) FROM evaluated)<>
                 (SELECT count(*) FROM incoming)
              OR (SELECT count(*) FROM resolved_groups)<>
                 (SELECT count(*) FROM collapsed)
              OR (SELECT count(*) FROM key_decision)<>
                 (SELECT count(*) FROM key_collapsed) THEN 'corrupt'
            WHEN EXISTS (
              SELECT 1 FROM key_decision WHERE minimum_multiplicity<0
            ) OR EXISTS (
              SELECT 1 FROM physical_decision WHERE minimum_multiplicity<0
            ) THEN 'negative'
            WHEN EXISTS (
              SELECT 1 FROM key_decision
              WHERE maximum_multiplicity>9223372036854775807
            ) OR EXISTS (
              SELECT 1 FROM physical_decision
              WHERE maximum_multiplicity>9223372036854775807
            ) THEN 'overflow'
            ELSE 'ok' END AS status
        ),
        bag_removed AS (
          DELETE FROM {bag} AS bag USING physical_decision,validation
          WHERE validation.status='ok'
            AND bag.bag_id=physical_decision.bag_id
            AND physical_decision.new_multiplicity=0
          RETURNING 1
        ),
        bag_changed AS (
          UPDATE {bag} AS bag
          SET multiplicity=physical_decision.new_multiplicity::bigint
          FROM physical_decision,validation
          WHERE validation.status='ok'
            AND bag.bag_id=physical_decision.bag_id
            AND physical_decision.new_multiplicity>0
          RETURNING 1
        ),
        bag_created AS (
          INSERT INTO {bag}(group_state_id,output_key,output_row,multiplicity)
          SELECT physical_decision.group_state_id,physical_decision.output_key,
                 physical_decision.incoming_output_row,
                 physical_decision.new_multiplicity::bigint
          FROM physical_decision,validation
          WHERE validation.status='ok'
            AND physical_decision.bag_id IS NULL
            AND physical_decision.new_multiplicity>0
          RETURNING 1
        ),
        touched_written AS (
          INSERT INTO {touched}(group_state_id,net_weight)
          SELECT key_collapsed.group_state_id,key_collapsed.net_weight
          FROM key_collapsed,validation
          WHERE validation.status='ok'
          RETURNING 1
        )
        SELECT validation.status,
               (SELECT count(*)::bigint FROM key_decision),
               (SELECT count(*)::bigint FROM physical_decision),
               (SELECT max(row_ordinal)+1 FROM incoming),
               (SELECT count(*)::bigint FROM incoming),
               (SELECT coalesce(sum(input_bytes),0)::bigint FROM incoming),
               (SELECT count(*)::bigint FROM bag_removed)
                 +(SELECT count(*)::bigint FROM bag_changed)
                 +(SELECT count(*)::bigint FROM bag_created)
                 +(SELECT count(*)::bigint FROM resolved_groups)
                 +(SELECT count(*)::bigint FROM touched_written)
        FROM validation
        "#,
        expressions = expressions.join(","),
        output_type = output_type.sql(),
        key_select = key_select,
        key_column_list = key_column_list,
        qualified_key_column_list = qualified_key_column_list,
        key_order = key_order,
        conflict_keys = conflict_keys,
        input = input.sql(),
        output_key = output_key,
        state = state.sql(),
        group_match = group_match,
        bag = bag.sql(),
        touched = touched.sql(),
    );
    let rows = transaction.write(&query, &unsafe {
        [
            DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
            DatumWithOid::new(first_row, pg_sys::INT8OID),
            DatumWithOid::new(i64_from_usize(budget.max_input_rows)?, pg_sys::INT8OID),
            DatumWithOid::new(i64_from_usize(budget.max_input_bytes)?, pg_sys::INT8OID),
        ]
    })?;
    if rows.len() != 1 {
        return Err("Distinct Apply returned no summary".into());
    }
    let row = rows.first();
    let status = required::<String>(&row, 1, "Distinct Apply status")?;
    if status != "ok" {
        return Err(format!(
            "Distinct multiplicity transition returned {status}"
        ));
    }
    let touched_keys = nonnegative(required::<i64>(&row, 2, "Distinct touched keys")?)?;
    let touched_physical =
        nonnegative(required::<i64>(&row, 3, "Distinct touched physical rows")?)?;
    let next_row = required::<i64>(&row, 4, "Distinct next row")?;
    let input_rows = nonnegative(required::<i64>(&row, 5, "Distinct input rows")?)?;
    let input_bytes = nonnegative(required::<i64>(&row, 6, "Distinct input bytes")?)?;
    let mutations = nonnegative(required::<i64>(&row, 7, "Distinct mutations")?)?;
    if touched_keys > input_rows || touched_physical > input_rows {
        return Err("Distinct Apply exceeded its per-input state bound".into());
    }

    // A new SPI statement observes the bag mutations. It performs one
    // `(group_state_id,output_key) LIMIT 1` probe per touched SQL group, so a
    // group with many SQL-equal physical representations remains bounded.
    let reconciled =
        reconcile_representatives(transaction, state, bag, queue, touched, output_type, &lsn)?;
    if reconciled.queued_effects > input_rows.saturating_mul(2) {
        return Err("Distinct queued more than two effects per admitted row".into());
    }
    Ok(PrefixFacts {
        usage: WorkUsage {
            input_rows,
            input_bytes,
            ..WorkUsage::default()
        },
        next_row,
        touched_keys,
        queued_effects: reconciled.queued_effects,
        state_rows: mutations
            .checked_add(reconciled.state_rows)
            .ok_or_else(|| "Distinct state count overflowed".to_string())?,
    })
}

// Atomic bounded Distinct reconciliation primitive: resolve touched-key
// representatives and enqueue only the resulting durable differences.
fn reconcile_representatives(
    transaction: &mut StepContext<'_, '_>,
    state: &RelationRef,
    bag: &RelationRef,
    queue: &RelationRef,
    touched: &RelationRef,
    output_type: &TypeRef,
    lsn: &str,
) -> Result<ReconcileFacts, String> {
    let budget = transaction.budget();
    let canonical_key = canonical_row_key_sql("desired.output_row", output_type);
    let rows = transaction.write(
        &format!(
            r#"
            WITH touched_page AS MATERIALIZED (
              DELETE FROM {touched}
              RETURNING group_state_id,net_weight
            ),
            locked AS MATERIALIZED (
              SELECT locked_group.*
              FROM touched_page
              JOIN LATERAL (
                SELECT groups.*
                FROM {state} AS groups
                WHERE groups.group_state_id=touched_page.group_state_id
                LIMIT 1
                FOR UPDATE
              ) AS locked_group ON true
            ),
            desired AS MATERIALIZED (
              SELECT locked.group_state_id,locked.output_key AS old_output_key,
                     locked.output_row AS old_output_row,
                     locked.multiplicity::numeric AS old_multiplicity,
                     locked.multiplicity::numeric+touched_page.net_weight
                       AS new_multiplicity,
                     representative.output_key,representative.output_row
              FROM locked
              JOIN touched_page USING(group_state_id)
              LEFT JOIN LATERAL (
                SELECT bag.output_key,bag.output_row
                FROM {bag} AS bag
                WHERE bag.group_state_id=locked.group_state_id
                ORDER BY bag.output_key
                LIMIT 1
              ) AS representative ON true
            ),
            validation AS MATERIALIZED (
              SELECT CASE WHEN EXISTS (
                SELECT 1
                WHERE (SELECT count(*) FROM desired)<>
                      (SELECT count(*) FROM touched_page)
              ) OR EXISTS (
                SELECT 1 FROM desired
                WHERE desired.new_multiplicity<0
                   OR desired.new_multiplicity>9223372036854775807
                   OR (desired.old_multiplicity>0)<>
                      (desired.old_output_key IS NOT NULL)
                   OR (desired.new_multiplicity>0)<>
                      (desired.output_key IS NOT NULL)
                   OR (
                     desired.output_key IS NOT NULL
                     AND desired.output_key<>{canonical_key}
                   )
              ) THEN 'corrupt' ELSE 'ok' END AS status
            ),
            effects AS MATERIALIZED (
              SELECT desired.group_state_id,1 AS leg,
                     desired.old_output_key AS output_key,
                     -1::bigint AS weight,desired.old_output_row AS output_row
              FROM desired
              WHERE desired.old_output_key IS NOT NULL
                AND (
                  desired.output_key IS NULL
                  OR desired.old_output_key<>desired.output_key
                )
              UNION ALL
              SELECT desired.group_state_id,
                     CASE WHEN desired.old_output_key IS NULL THEN 1 ELSE 2 END,
                     desired.output_key,1::bigint,desired.output_row
              FROM desired
              WHERE desired.output_key IS NOT NULL
                AND (
                  desired.old_output_key IS NULL
                  OR desired.old_output_key<>desired.output_key
                )
            ),
            state_changed AS (
              UPDATE {state} AS groups
              SET output_key=desired.output_key,output_row=desired.output_row,
                  multiplicity=desired.new_multiplicity::bigint
              FROM desired,validation
              WHERE validation.status='ok'
                AND groups.group_state_id=desired.group_state_id
                AND desired.new_multiplicity>0
              RETURNING 1
            ),
            state_removed AS (
              DELETE FROM {state} AS groups
              USING desired,validation
              WHERE validation.status='ok'
                AND groups.group_state_id=desired.group_state_id
                AND desired.new_multiplicity=0
              RETURNING 1
            ),
            queued AS (
              INSERT INTO {queue}(
                output_key,weight,output_row,row_bytes,causal_lsn
              )
              SELECT effects.output_key,effects.weight,effects.output_row,
                     shiba_internal.effect_row_bytes(effects.output_row),$1::pg_lsn
              FROM effects,validation
              WHERE validation.status='ok'
              ORDER BY effects.group_state_id,effects.leg
              RETURNING 1
            )
            SELECT validation.status,
                   (SELECT count(*)::bigint FROM desired),
                   (SELECT count(*)::bigint FROM queued),
                   (SELECT count(*)::bigint FROM state_changed)
                     +(SELECT count(*)::bigint FROM state_removed)
                     +(SELECT count(*)::bigint FROM queued)
                     +(SELECT count(*)::bigint FROM touched_page)
            FROM validation
            "#,
            state = state.sql(),
            bag = bag.sql(),
            queue = queue.sql(),
            touched = touched.sql(),
            canonical_key = canonical_key,
        ),
        &unsafe { [DatumWithOid::new(lsn, pg_sys::TEXTOID)] },
    )?;
    if rows.len() != 1 {
        return Err("Distinct representative reconciliation returned no summary".into());
    }
    let row = rows.first();
    let status = required::<String>(&row, 1, "Distinct reconciliation status")?;
    if status != "ok" {
        return Err(format!(
            "Distinct representative reconciliation returned {status}"
        ));
    }
    let touched = nonnegative(required::<i64>(&row, 2, "Distinct reconciled groups")?)?;
    let queued_effects = nonnegative(required::<i64>(&row, 3, "Distinct reconciled effects")?)?;
    let state_rows = nonnegative(required::<i64>(
        &row,
        4,
        "Distinct reconciliation mutations",
    )?)?;
    if touched
        > u64::try_from(budget.max_input_rows)
            .map_err(|_| "Distinct input row budget exceeds u64")?
    {
        return Err("Distinct reconciliation exceeded its touched-group bound".into());
    }
    Ok(ReconcileFacts {
        queued_effects,
        state_rows,
    })
}

fn distinct_null_safe_equality(left: &str, right: &str, equality: &str) -> String {
    format!(
        "(
           ({left} IS NULL AND {right} IS NULL)
           OR
           ({left} IS NOT NULL AND {right} IS NOT NULL
            AND ({left} {equality} {right}) IS TRUE)
         )"
    )
}

fn require_empty_queue(
    transaction: &mut StepContext<'_, '_>,
    queue: &RelationRef,
) -> Result<(), String> {
    let rows = transaction.read(
        &format!(
            "SELECT queue_id FROM {} ORDER BY queue_id LIMIT 1",
            queue.sql()
        ),
        &[],
    )?;
    if !rows.is_empty() {
        return Err("Distinct Apply/frontier found undrained effects".into());
    }
    Ok(())
}

fn require_empty_touched(
    transaction: &mut StepContext<'_, '_>,
    touched: &RelationRef,
) -> Result<(), String> {
    let rows = transaction.read(
        &format!(
            "SELECT group_state_id FROM {} ORDER BY group_state_id LIMIT 1",
            touched.sql()
        ),
        &[],
    )?;
    if !rows.is_empty() {
        return Err("Distinct found committed touched-key scratch state".into());
    }
    Ok(())
}

// Atomic bounded Distinct drain primitive: consume one queue page and write its
// payload; StepContext owns effect-stream publication and output sequencing.
fn drain_queue(
    transaction: &mut StepContext<'_, '_>,
    output_payload: &RelationRef,
    output_type: &TypeRef,
    queue: &RelationRef,
) -> Result<DrainFacts, String> {
    let output = transaction.output()?.clone();
    let budget = transaction.budget();
    let max_rows = i64::min(i64_from_usize(budget.max_output_rows)?, output.target_rows);
    let max_bytes = i64::min(
        i64_from_usize(budget.max_output_bytes)?,
        output.target_bytes,
    );
    let canonical_key = canonical_row_key_sql("selected.output_row", output_type);
    let rows = transaction.write(
        &format!(
            r#"
            WITH candidates AS MATERIALIZED (
              SELECT queue_id,output_key,weight,output_row,row_bytes,causal_lsn
              FROM {queue}
              ORDER BY queue_id
              LIMIT $3
            ),
            measured AS (
              SELECT candidates.*,
                     row_number() OVER (ORDER BY queue_id) AS page_ordinal,
                     sum(row_bytes) OVER (ORDER BY queue_id) AS running_bytes
              FROM candidates
            ),
            selected AS MATERIALIZED (
              SELECT * FROM measured
              WHERE page_ordinal=1 OR running_bytes <= $4
            ),
            summary AS MATERIALIZED (
              SELECT count(*)::bigint AS emitted,
                     coalesce(sum(row_bytes),0)::bigint AS emitted_bytes,
                     min(causal_lsn) AS causal_lsn,
                     min(causal_lsn)=max(causal_lsn) AS one_causal_lsn,
                     bool_and(output_key={canonical_key}) AS canonical_keys
              FROM selected
            ),
            payload_insert AS (
              INSERT INTO {output_payload}(
                stream_id,chunk_seq,row_ordinal,weight,row_value
              )
              SELECT $1,$2,
                     row_number() OVER (ORDER BY selected.queue_id)-1,
                     selected.weight,selected.output_row
              FROM selected,summary
              WHERE summary.emitted>0 AND summary.one_causal_lsn
                AND summary.canonical_keys
              RETURNING 1
            ),
            removed AS (
              DELETE FROM {queue} AS queue
              USING selected,summary
              WHERE summary.emitted>0 AND summary.one_causal_lsn
                AND summary.canonical_keys
                AND queue.queue_id=selected.queue_id
              RETURNING 1
            )
            SELECT summary.emitted,summary.emitted_bytes,
                   summary.one_causal_lsn,summary.canonical_keys,
                   summary.causal_lsn::text,
                   (SELECT count(*)::bigint FROM payload_insert),
                   (SELECT count(*)::bigint FROM removed),
                   (SELECT count(*)::bigint
                    FROM {queue} AS pending
                    WHERE NOT EXISTS (
                      SELECT 1 FROM selected
                      WHERE selected.queue_id=pending.queue_id
                    ))
            FROM summary
            "#,
            queue = queue.sql(),
            output_payload = output_payload.sql(),
            canonical_key = canonical_key,
        ),
        &unsafe {
            [
                DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(output.next_chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(max_rows, pg_sys::INT8OID),
                DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            ]
        },
    )?;
    if rows.len() != 1 {
        return Err("Distinct Drain returned no summary".into());
    }
    let row = rows.first();
    let emitted = nonnegative(required::<i64>(&row, 1, "Distinct emitted effects")?)?;
    let emitted_bytes = nonnegative(required::<i64>(&row, 2, "Distinct emitted bytes")?)?;
    let one_causal_lsn = required::<bool>(&row, 3, "Distinct causal LSN summary")?;
    let canonical_keys = required::<bool>(&row, 4, "Distinct canonical effect keys")?;
    let causal_lsn = parse_lsn(&required::<String>(&row, 5, "Distinct causal LSN")?)?;
    let inserted = nonnegative(required::<i64>(&row, 6, "Distinct payload inserts")?)?;
    let removed = nonnegative(required::<i64>(&row, 7, "Distinct queue deletes")?)?;
    let remaining = nonnegative(required::<i64>(&row, 8, "Distinct remaining effects")?)?;
    if emitted == 0
        || emitted_bytes == 0
        || !one_causal_lsn
        || !canonical_keys
        || inserted != emitted
        || removed != emitted
    {
        return Err("Distinct bounded effect Drain is inconsistent".into());
    }
    transaction.record_output_append(
        OutputAppendTarget::New {
            sequence: output.next_chunk_seq,
        },
        emitted,
        emitted_bytes,
        causal_lsn,
    )?;
    Ok(DrainFacts {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                output_rows: emitted,
                output_bytes: emitted_bytes,
                ..WorkUsage::default()
            },
            state_rows: removed,
            output: OutputFacts::Data {
                chunk_seq: output.next_chunk_seq,
            },
        },
        remaining_effects: remaining,
    })
}

fn validate_state_abi(
    transaction: &mut StepContext<'_, '_>,
    state: &RelationRef,
    key_count: usize,
    output_type_oid: pg_sys::Oid,
    spec: &crate::planner::model::DistinctSpec,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(state.oid())?;
    if attributes.len() != key_count + 4
        || attributes[0].name != "group_state_id"
        || attributes[0].type_oid != pg_sys::INT8OID
        || attributes[key_count + 1].name != "output_key"
        || attributes[key_count + 1].type_oid != pg_sys::BYTEAOID
        || attributes[key_count + 2].name != "output_row"
        || attributes[key_count + 2].type_oid != output_type_oid
        || attributes[key_count + 3].name != "multiplicity"
        || attributes[key_count + 3].type_oid != pg_sys::INT8OID
    {
        return Err("Distinct typed state relation has an invalid ABI".into());
    }
    for (index, key) in spec.keys.iter().enumerate() {
        let attribute = &attributes[index + 1];
        if attribute.name != format!("key_{}", index + 1)
            || !attribute_matches_slot(attribute, &key.type_)
        {
            return Err("Distinct typed key state changed ABI".into());
        }
    }
    Ok(())
}

fn validate_bag_abi(
    transaction: &mut StepContext<'_, '_>,
    bag: &RelationRef,
    output_type_oid: pg_sys::Oid,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(bag.oid())?;
    let expected = [
        ("bag_id", pg_sys::INT8OID),
        ("group_state_id", pg_sys::INT8OID),
        ("output_key", pg_sys::BYTEAOID),
        ("output_row", output_type_oid),
        ("multiplicity", pg_sys::INT8OID),
    ];
    if attributes.len() != expected.len()
        || attributes
            .iter()
            .zip(expected)
            .any(|(actual, (name, type_oid))| {
                actual.name != name || actual.type_oid != type_oid || !actual.not_null
            })
    {
        return Err("Distinct physical representative bag has an invalid ABI".into());
    }
    Ok(())
}

fn validate_queue_abi(
    transaction: &mut StepContext<'_, '_>,
    queue: &RelationRef,
    output_type_oid: pg_sys::Oid,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(queue.oid())?;
    let expected = [
        ("queue_id", pg_sys::INT8OID),
        ("output_key", pg_sys::BYTEAOID),
        ("weight", pg_sys::INT8OID),
        ("output_row", output_type_oid),
        ("row_bytes", pg_sys::INT8OID),
        ("causal_lsn", pg_sys::PG_LSNOID),
    ];
    if attributes.len() != expected.len()
        || attributes
            .iter()
            .zip(expected)
            .any(|(actual, (name, type_oid))| {
                actual.name != name || actual.type_oid != type_oid || !actual.not_null
            })
    {
        return Err("Distinct pending effect queue has an invalid ABI".into());
    }
    Ok(())
}

fn validate_touched_abi(
    transaction: &mut StepContext<'_, '_>,
    touched: &RelationRef,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(touched.oid())?;
    let expected = [
        ("group_state_id", pg_sys::INT8OID),
        ("net_weight", pg_sys::NUMERICOID),
    ];
    if attributes.len() != expected.len()
        || attributes
            .iter()
            .zip(expected)
            .any(|(actual, (name, type_oid))| {
                actual.name != name || actual.type_oid != type_oid || !actual.not_null
            })
    {
        return Err("Distinct touched-key scratch relation has an invalid ABI".into());
    }
    Ok(())
}

fn validate_continuation_abi(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
) -> Result<(), String> {
    validate_typed_continuation_abi(transaction, relation, CONTINUATION_COLUMNS, "Distinct")
}

fn load_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    stream_id: i64,
    chunk_seq: i64,
) -> Result<StoredContinuation, String> {
    let value = lock_continuation(
        transaction,
        relation,
        "phase,input_stream_id,input_chunk_seq,next_row_ordinal",
        "Distinct",
        |rows| {
            let row = rows.first();
            let phase = match required::<i16>(&row, 1, "Distinct continuation phase")? {
                1 => DistinctPhase::Apply,
                2 => DistinctPhase::Drain,
                _ => return Err("Distinct continuation has an invalid phase".into()),
            };
            let position = InputPosition::new(
                required::<i64>(&row, 2, "Distinct continuation stream")?,
                required::<i64>(&row, 3, "Distinct continuation chunk")?,
                required::<i64>(&row, 4, "Distinct continuation row")?,
            )?;
            if position.stream_id != stream_id || position.chunk_seq != chunk_seq {
                return Err("Distinct continuation is not at its input cursor".into());
            }
            Ok(DistinctContinuation {
                input: position,
                phase,
            })
        },
    )?;
    Ok(match value {
        None => StoredContinuation {
            value: DistinctContinuation {
                input: InputPosition::new(stream_id, chunk_seq, 0)?,
                phase: DistinctPhase::Apply,
            },
            persisted: false,
        },
        Some(value) => StoredContinuation {
            value,
            persisted: true,
        },
    })
}

fn replace_continuation(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    old: StoredContinuation,
    next: Option<DistinctContinuation>,
) -> Result<(), String> {
    let old_fields = old
        .persisted
        .then(|| continuation_arguments(old.value))
        .transpose()?;
    let next_fields = next.map(continuation_arguments).transpose()?;
    replace_continuation_cas(
        transaction,
        relation,
        CONTINUATION_COLUMNS,
        old_fields.as_ref().map(|fields| &fields[..]),
        next_fields.as_ref().map(|fields| &fields[..]),
        "Distinct",
    )
}

fn continuation_arguments(
    continuation: DistinctContinuation,
) -> Result<[DatumWithOid<'static>; 4], String> {
    let phase = phase_code(continuation.phase)?;
    // Every continuation scalar is copied into PostgreSQL immediately; these
    // owned values therefore need no borrowed datum lifetime.
    Ok(unsafe {
        [
            DatumWithOid::new(phase, pg_sys::INT2OID),
            DatumWithOid::new(continuation.input.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(continuation.input.chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(continuation.input.row_ordinal, pg_sys::INT8OID),
        ]
    })
}

fn phase_code(phase: DistinctPhase) -> Result<i16, String> {
    match phase {
        DistinctPhase::Apply => Ok(1),
        DistinctPhase::Drain => Ok(2),
        DistinctPhase::Frontier => Err("Distinct frontier cannot be persisted".into()),
    }
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

fn nonnegative(value: i64) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| "database returned a negative resource count".into())
}

fn i64_from_usize(value: usize) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "Distinct budget exceeds bigint".into())
}
