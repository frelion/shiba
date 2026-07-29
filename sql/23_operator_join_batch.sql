CREATE FUNCTION shiba._assert_join_transition(valid boolean)
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NOT coalesce(valid,false) THEN
      RAISE EXCEPTION 'Shiba JOIN arrangement multiplicity became negative'
        USING ERRCODE='P0S01';
    END IF;
    RETURN true;
END;
$$;

-- Apply one source commit as an exact relational JOIN transition.
--
-- The physical compiler owns one typed UNLOGGED join_delta stage per DAG.
-- This function computes exact versioned multiplicity differences directly:
-- delta-left × old-right, old-left × delta-right, their cross term, and the
-- old/new match-presence boundary terms required by outer/semi/anti joins.
-- It writes only the net difference to the Stage, then applies the input
-- transition to the durable arrangement. A second statement atomically
-- consumes and removes the Stage rows into aggregate/distinct state and the
-- sink. No source-table snapshot is consulted: it may already include commits
-- newer than p_commit_lsn.
-- Prepared Join programs live in the singleton Runtime session. Release an
-- obsolete physical-plan generation when its DAG is dropped or recompiled so
-- a long-lived Runtime does not accumulate session plan memory.
CREATE FUNCTION shiba_internal._deallocate_join_physical_plans(
    result_relation oid,
    physical_plan_id bigint
)
RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    prepared_name text;
    released integer := 0;
    stage_name name := format(
      'shiba_join_stage_r%s_p%s',result_relation,physical_plan_id
    )::name;
    consume_name name := format(
      'shiba_join_consume_r%s_p%s',result_relation,physical_plan_id
    )::name;
    preflight_name name := format(
      'shiba_join_preflight_r%s_p%s',result_relation,physical_plan_id
    )::name;
BEGIN
    FOR prepared_name IN
      SELECT name
      FROM pg_prepared_statements
      WHERE name IN (
        stage_name::text,consume_name::text,preflight_name::text
      )
      ORDER BY name
    LOOP
      EXECUTE format('DEALLOCATE %I',prepared_name);
      released := released+1;
    END LOOP;
    RETURN released;
END;
$$;

CREATE FUNCTION shiba._apply_join_commit_temp_free(
    result_relation oid,
    execution_descriptor jsonb,
    p_commit_lsn pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    stream_view shiba_internal.stream_views%ROWTYPE;
    join_view shiba_internal.inner_join_views%ROWTYPE;
    left_source_oid oid := (execution_descriptor->>'left_source_oid')::oid;
    right_source_oid oid := (execution_descriptor->>'right_source_oid')::oid;
    left_name text;
    right_name text;
    result_name text;
    requested_stage_name text;
    stage_name text;
    group_expression text;
    sum_input_expression text;
    count_input_expression text;
    left_pre_filter_sql text;
    right_pre_filter_sql text;
    post_filter_sql text;
    join_filter_sql text;
    having_sql text;
    visible_sql text;
    applicable_events bigint;
    left_event_count bigint;
    right_event_count bigint;
    left_arrangement_rows bigint;
    right_arrangement_rows bigint;
    candidate_screen_upper_bound numeric;
    candidate_upper_bound numeric;
    candidate_preflight_sql text;
    physical_plan_id bigint;
    preflight_plan_name name;
    stage_plan_name name;
    consume_plan_name name;
    prepared_count integer;
    prepared_name text;
    stage_statement_sql text;
    consume_statement_sql text;
    preflight_execute_sql text;
    stage_execute_sql text;
    consume_execute_sql text;
    staged_rows bigint;
    arrangement_rows bigint;
    max_stage_rows bigint := coalesce(
      nullif(current_setting('shiba.max_stage_rows',true),'')::bigint,
      1000000
    );
BEGIN
    SELECT * INTO STRICT stream_view
    FROM shiba_internal.stream_views
    WHERE result_oid=result_relation;
    SELECT * INTO STRICT join_view
    FROM shiba_internal.inner_join_views
    WHERE result_oid=result_relation;

    IF stream_view.view_kind<>'aggregate'
       OR stream_view.source_oid<>left_source_oid
       OR join_view.right_source_oid<>right_source_oid
       OR join_view.join_type IS DISTINCT FROM execution_descriptor->>'join_type'
       OR join_view.sum_source<>'left' THEN
      RAISE EXCEPTION
        'logical plan JOIN descriptor disagrees with metadata for result %',
        result_relation
        USING ERRCODE='P0S01';
    END IF;

    SELECT count(*),
           count(*) FILTER (WHERE event.source_oid=left_source_oid),
           count(*) FILTER (WHERE event.source_oid=right_source_oid)
    INTO applicable_events,left_event_count,right_event_count
    FROM shiba_internal.effective_change_log event
    WHERE event.commit_lsn=p_commit_lsn
      AND event.source_oid IN (left_source_oid,right_source_oid);
    IF applicable_events=0 THEN
      RAISE EXCEPTION
        'Shiba DAG % inbox commit % has no applicable change-log events',
        result_relation,p_commit_lsn
        USING ERRCODE='P0S01';
    END IF;
    SELECT
      count(*) FILTER (WHERE input_side='left'),
      count(*) FILTER (WHERE input_side='right')
    INTO left_arrangement_rows,right_arrangement_rows
    FROM shiba_internal.join_arrangements
    WHERE result_oid=result_relation;
    candidate_screen_upper_bound :=
        left_event_count::numeric*right_arrangement_rows::numeric
      + right_event_count::numeric*left_arrangement_rows::numeric
      + left_event_count::numeric*right_event_count::numeric;
    IF join_view.join_type<>'inner' THEN
      candidate_screen_upper_bound := candidate_screen_upper_bound
        +left_arrangement_rows+right_arrangement_rows
        +left_event_count+right_event_count;
    END IF;

    SELECT plan.plan_id INTO STRICT physical_plan_id
    FROM shiba_internal.physical_plans AS plan
    WHERE plan.result_oid=result_relation;
    preflight_plan_name :=
      format('shiba_join_preflight_r%s_p%s',
             result_relation,physical_plan_id)::name;
    stage_plan_name :=
      format('shiba_join_stage_r%s_p%s',
             result_relation,physical_plan_id)::name;
    consume_plan_name :=
      format('shiba_join_consume_r%s_p%s',
             result_relation,physical_plan_id)::name;
    preflight_execute_sql := format(
      'EXECUTE %I (%s::oid,%s::oid,%s::oid,%L::pg_lsn)',
      preflight_plan_name,result_relation,left_source_oid,right_source_oid,
      p_commit_lsn::text
    );
    stage_execute_sql := format(
      'EXECUTE %I (%s::oid,%s::oid,%s::oid,%L::pg_lsn)',
      stage_plan_name,result_relation,left_source_oid,right_source_oid,
      p_commit_lsn::text
    );
    consume_execute_sql := format(
      'EXECUTE %I (%s::oid,%L::pg_lsn,%L::boolean)',
      consume_plan_name,result_relation,p_commit_lsn::text,
      stream_view.count_distinct
    );
    requested_stage_name :=
      shiba._physical_stage_name(result_relation,'join_delta');
    SELECT format('%I.%I',namespace.nspname,relation.relname)
    INTO STRICT stage_name
    FROM pg_class relation
    JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
    WHERE relation.oid=to_regclass(requested_stage_name)
      AND relation.relpersistence='u';

    SELECT count(*) INTO prepared_count
    FROM pg_prepared_statements
    WHERE name IN (
      preflight_plan_name::text,stage_plan_name::text,consume_plan_name::text
    );
    IF prepared_count=3 THEN
      IF candidate_screen_upper_bound>max_stage_rows THEN
        EXECUTE preflight_execute_sql INTO STRICT candidate_upper_bound;
        IF candidate_upper_bound>max_stage_rows THEN
          RAISE EXCEPTION
            'Shiba JOIN commit % for result % may generate % candidates, limit %',
            p_commit_lsn,result_relation::regclass,
            candidate_upper_bound,max_stage_rows
            USING ERRCODE='53400',
                  HINT='Increase shiba.max_stage_rows, split the source transaction, or reduce JOIN fanout.';
        END IF;
      END IF;
      EXECUTE stage_execute_sql INTO STRICT staged_rows,arrangement_rows;
      IF staged_rows>max_stage_rows THEN
        RAISE EXCEPTION
          'Shiba JOIN commit % for result % produced % Stage rows, limit %',
          p_commit_lsn,result_relation::regclass,staged_rows,max_stage_rows
          USING ERRCODE='53400',
                HINT='Increase shiba.max_stage_rows or reduce JOIN fanout.';
      END IF;
      IF staged_rows>=1024 THEN
        EXECUTE format('ANALYZE %s',stage_name);
      END IF;
      EXECUTE consume_execute_sql;
      -- The consume program leaves the Stage empty. Reset a large-batch
      -- estimate so the next small commit does not reuse a generic plan
      -- costed as though the previous large Stage were still populated.
      IF staged_rows>=1024 THEN
        EXECUTE format('ANALYZE %s',stage_name);
      END IF;
      PERFORM shiba_internal._compact_physical_stages(result_relation);
      RETURN;
    END IF;

    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT left_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=left_source_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT right_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=right_source_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT result_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=result_relation;

    SELECT coalesce((
      SELECT predicate_sql
      FROM shiba_internal.stream_filters
      WHERE result_oid=result_relation
        AND input_side='left'
        AND phase='pre'
    ),'true') INTO left_pre_filter_sql;
    SELECT coalesce((
      SELECT predicate_sql
      FROM shiba_internal.stream_filters
      WHERE result_oid=result_relation
        AND input_side='right'
        AND phase='pre'
    ),'true') INTO right_pre_filter_sql;

    -- Registration validates these expressions.  Inline them so the input
    -- and output relations remain set-oriented rather than invoking dynamic
    -- PL/pgSQL once per row.
    SELECT string_agg(
      '(' || CASE filter.input_side
        WHEN 'left' THEN replace(
          filter.predicate_sql,'(input.row)','(left_input.row)'
        )
        ELSE replace(
          filter.predicate_sql,'(input.row)','(right_input.row)'
        )
      END || ')',
      ' AND ' ORDER BY filter.input_side
    )
    INTO post_filter_sql
    FROM shiba_internal.stream_filters filter
    WHERE filter.result_oid=result_relation
      AND filter.phase='post';
    SELECT predicate_sql INTO join_filter_sql
    FROM shiba_internal.stream_join_filters
    WHERE result_oid=result_relation;
    IF join_filter_sql IS NOT NULL THEN
      join_filter_sql := replace(
        join_filter_sql,format('input_%s',left_source_oid),'left_input'
      );
      join_filter_sql := replace(
        join_filter_sql,format('input_%s',right_source_oid),'right_input'
      );
      post_filter_sql := concat_ws(
        ' AND ',post_filter_sql,'(' || join_filter_sql || ')'
      );
    END IF;
    post_filter_sql := coalesce(post_filter_sql,'true');

    -- Bound the candidate set before constructing it.  Count unique changed
    -- rows and retained rows per affected join key, rather than multiplying
    -- each side's event count by the entire opposite arrangement.  The latter
    -- badly overestimates sparse one-to-one updates and can quarantine healthy
    -- DAGs.  This preflight only groups the bounded commit payload and scans
    -- indexed arrangement keys; it never enumerates the Cartesian candidates.
    candidate_preflight_sql := format(
      $candidate_preflight$
      WITH left_event_rows AS MATERIALIZED (
        SELECT coalesce(
                 to_jsonb((input.row).%2$I),'null'::jsonb
               ) AS join_key,
               event.row_data,
               sum(event.delta)::bigint AS weight
        FROM shiba_internal.effective_change_log event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(NULL::%1$s,event.row_data) AS row
        ) input
        WHERE event.commit_lsn=$4
          AND event.source_oid=$2
          AND event.delta IN (-1,1)
          AND jsonb_typeof(event.row_data)='object'
          AND coalesce((%3$s),false)
        GROUP BY 1,event.row_data
        HAVING sum(event.delta)<>0
      ),
      right_event_rows AS MATERIALIZED (
        SELECT coalesce(
                 to_jsonb((input.row).%5$I),'null'::jsonb
               ) AS join_key,
               event.row_data,
               sum(event.delta)::bigint AS weight
        FROM shiba_internal.effective_change_log event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(NULL::%4$s,event.row_data) AS row
        ) input
        WHERE event.commit_lsn=$4
          AND event.source_oid=$3
          AND event.delta IN (-1,1)
          AND jsonb_typeof(event.row_data)='object'
          AND coalesce((%6$s),false)
        GROUP BY 1,event.row_data
        HAVING sum(event.delta)<>0
      ),
      affected_keys AS MATERIALIZED (
        SELECT join_key FROM left_event_rows
        UNION
        SELECT join_key FROM right_event_rows
      ),
      right_global AS MATERIALIZED (
        SELECT
          coalesce(sum(state.multiplicity),0)::bigint AS old_any_count,
          coalesce(sum(state.multiplicity)
            FILTER (WHERE state.join_key='null'::jsonb),0)::bigint
            AS old_null_count,
          coalesce((SELECT sum(weight) FROM right_event_rows),0)::bigint
            AS any_delta,
          coalesce((SELECT sum(weight) FROM right_event_rows
                    WHERE join_key='null'::jsonb),0)::bigint AS null_delta
        FROM shiba_internal.join_arrangements state
        WHERE %7$L='null_anti'
          AND state.result_oid=$1
          AND state.input_side='right'
      ),
      expanded_keys AS MATERIALIZED (
        SELECT join_key FROM affected_keys
        UNION
        SELECT DISTINCT state.join_key
        FROM shiba_internal.join_arrangements state
        CROSS JOIN right_global global
        WHERE %7$L='null_anti'
          AND state.result_oid=$1
          AND state.input_side='left'
          AND (global.old_null_count>0)
                IS DISTINCT FROM
              (global.old_null_count+global.null_delta>0)
        UNION
        SELECT 'null'::jsonb
        FROM right_global global
        WHERE %7$L='null_anti'
          AND (global.old_any_count>0)
                IS DISTINCT FROM
              (global.old_any_count+global.any_delta>0)
      ),
      left_event_counts AS (
        SELECT join_key,count(*)::numeric AS row_count,
               sum(weight)::bigint AS weight
        FROM left_event_rows GROUP BY join_key
      ),
      right_event_counts AS (
        SELECT join_key,count(*)::numeric AS row_count,
               sum(weight)::bigint AS weight
        FROM right_event_rows GROUP BY join_key
      ),
      left_state_counts AS (
        SELECT key.join_key,
               count(state.*)::numeric AS row_count,
               coalesce(sum(state.multiplicity),0)::bigint
                 AS multiplicity
        FROM expanded_keys key
        LEFT JOIN shiba_internal.join_arrangements state
          ON state.result_oid=$1
         AND state.input_side='left'
         AND state.join_key=key.join_key
        GROUP BY key.join_key
      ),
      right_state_counts AS (
        SELECT key.join_key,
               count(state.*)::numeric AS row_count,
               coalesce(sum(state.multiplicity),0)::bigint
                 AS multiplicity
        FROM expanded_keys key
        LEFT JOIN shiba_internal.join_arrangements state
          ON state.result_oid=$1
         AND state.input_side='right'
         AND state.join_key=key.join_key
        GROUP BY key.join_key
      ),
      key_presence AS (
        SELECT key.join_key,
               left_state.row_count AS left_rows,
               left_state.multiplicity AS old_left_count,
               (
                 left_state.multiplicity
                   +coalesce(left_event.weight,0)
               )::bigint AS new_left_count,
               right_state.row_count AS right_rows,
               right_state.multiplicity AS old_right_count,
               (
                 right_state.multiplicity
                   +coalesce(right_event.weight,0)
               )::bigint AS new_right_count,
               coalesce(left_event.row_count,0)::numeric
                 AS left_event_rows,
               coalesce(right_event.row_count,0)::numeric
                 AS right_event_rows
        FROM expanded_keys key
        JOIN left_state_counts left_state USING(join_key)
        JOIN right_state_counts right_state USING(join_key)
        LEFT JOIN left_event_counts left_event USING(join_key)
        LEFT JOIN right_event_counts right_event USING(join_key)
      ),
      visibility AS (
        SELECT presence.*,
               CASE %7$L
                 WHEN 'left' THEN
                   presence.join_key='null'::jsonb
                     OR presence.old_right_count=0
                 WHEN 'full' THEN
                   presence.join_key='null'::jsonb
                     OR presence.old_right_count=0
                 WHEN 'semi' THEN
                   presence.join_key<>'null'::jsonb
                     AND presence.old_right_count>0
                 WHEN 'anti' THEN
                   presence.join_key='null'::jsonb
                     OR presence.old_right_count=0
                 WHEN 'null_anti' THEN
                   global.old_any_count=0
                   OR (
                     presence.join_key<>'null'::jsonb
                     AND global.old_null_count=0
                     AND presence.old_right_count=0
                   )
                 ELSE false
               END AS old_left_visible,
               CASE %7$L
                 WHEN 'left' THEN
                   presence.join_key='null'::jsonb
                     OR presence.new_right_count=0
                 WHEN 'full' THEN
                   presence.join_key='null'::jsonb
                     OR presence.new_right_count=0
                 WHEN 'semi' THEN
                   presence.join_key<>'null'::jsonb
                     AND presence.new_right_count>0
                 WHEN 'anti' THEN
                   presence.join_key='null'::jsonb
                     OR presence.new_right_count=0
                 WHEN 'null_anti' THEN
                   global.old_any_count+global.any_delta=0
                   OR (
                     presence.join_key<>'null'::jsonb
                     AND global.old_null_count+global.null_delta=0
                     AND presence.new_right_count=0
                   )
                 ELSE false
               END AS new_left_visible,
               (
                 presence.join_key='null'::jsonb
                   OR presence.old_left_count=0
               ) AS old_right_visible,
               (
                 presence.join_key='null'::jsonb
                   OR presence.new_left_count=0
               ) AS new_right_visible
        FROM key_presence presence
        CROSS JOIN right_global global
      ),
      matched_candidates AS (
        SELECT CASE
          WHEN %7$L IN ('inner','left','right','full') THEN
            coalesce((
              SELECT sum(left_event_rows*right_rows)
              FROM visibility
              WHERE join_key<>'null'::jsonb
            ),0)
            +coalesce((
              SELECT sum(right_event_rows*left_rows)
              FROM visibility
              WHERE join_key<>'null'::jsonb
            ),0)
            +coalesce((
              SELECT sum(left_event_rows*right_event_rows)
              FROM visibility
              WHERE join_key<>'null'::jsonb
            ),0)
          ELSE 0::numeric
        END AS row_count
      ),
      boundary_candidates AS (
        SELECT
          coalesce(sum(
            CASE
              WHEN %7$L IN ('left','full','semi','anti','null_anti')
                   AND new_left_visible
              THEN left_event_rows ELSE 0
            END
            +CASE
               WHEN %7$L IN ('left','full','semi','anti','null_anti')
                    AND old_left_visible IS DISTINCT FROM new_left_visible
               THEN left_rows ELSE 0
             END
            +CASE
               WHEN %7$L IN ('right','full') AND new_right_visible
               THEN right_event_rows ELSE 0
             END
            +CASE
               WHEN %7$L IN ('right','full')
                    AND old_right_visible IS DISTINCT FROM new_right_visible
               THEN right_rows ELSE 0
             END
          ),0)::numeric AS row_count
        FROM visibility
      )
      SELECT matched.row_count+boundary.row_count
      FROM matched_candidates matched
      CROSS JOIN boundary_candidates boundary
      $candidate_preflight$,
      left_name,
      join_view.left_join_column,
      left_pre_filter_sql,
      right_name,
      join_view.right_join_column,
      right_pre_filter_sql,
      join_view.join_type
    );
    -- A failed first compilation can leave a partial program generation.
    -- Rebuild preflight, Stage, and consume plans together.
    FOR prepared_name IN
      SELECT name
      FROM pg_prepared_statements
      WHERE name IN (
        preflight_plan_name::text,
        stage_plan_name::text,
        consume_plan_name::text
      )
      ORDER BY name
    LOOP
      EXECUTE format('DEALLOCATE %I',prepared_name);
    END LOOP;
    EXECUTE format(
      'PREPARE %I (oid,oid,oid,pg_lsn) AS %s',
      preflight_plan_name,candidate_preflight_sql
    );
    IF candidate_screen_upper_bound>max_stage_rows THEN
      EXECUTE preflight_execute_sql INTO STRICT candidate_upper_bound;
      IF candidate_upper_bound>max_stage_rows THEN
        RAISE EXCEPTION
          'Shiba JOIN commit % for result % may generate % candidates, limit %',
          p_commit_lsn,result_relation::regclass,
          candidate_upper_bound,max_stage_rows
          USING ERRCODE='53400',
                HINT='Increase shiba.max_stage_rows, split the source transaction, or reduce JOIN fanout.';
      END IF;
    END IF;

    stage_statement_sql := format(
      $stage_statement$
      WITH input_events AS MATERIALIZED (
        SELECT event.sequence AS ordinality,
               'left'::text AS input_side,
               coalesce(
                 to_jsonb((input.row).%2$I),'null'::jsonb
               ) AS join_key,
               event.row_data,
               event.delta::bigint AS multiplicity_delta
        FROM shiba_internal.effective_change_log event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%1$s,event.row_data
          ) AS row
        ) input
        WHERE event.commit_lsn=$4
          AND event.source_oid=$2
          AND event.delta IN (-1,1)
          AND jsonb_typeof(event.row_data)='object'
          AND coalesce((%3$s),false)
        UNION ALL
        SELECT event.sequence AS ordinality,
               'right'::text AS input_side,
               coalesce(
                 to_jsonb((input.row).%5$I),'null'::jsonb
               ) AS join_key,
               event.row_data,
               event.delta::bigint AS multiplicity_delta
        FROM shiba_internal.effective_change_log event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%4$s,event.row_data
          ) AS row
        ) input
        WHERE event.commit_lsn=$4
          AND event.source_oid=$3
          AND event.delta IN (-1,1)
          AND jsonb_typeof(event.row_data)='object'
          AND coalesce((%6$s),false)
      ),
      running_input AS MATERIALIZED (
        SELECT event.*,
               sum(event.multiplicity_delta) OVER (
                 PARTITION BY event.input_side,event.join_key,event.row_data
                 ORDER BY event.ordinality
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               )::bigint AS multiplicity_prefix
        FROM input_events event
      ),
      input_folded AS MATERIALIZED (
        SELECT input_side,join_key,row_data,
               sum(multiplicity_delta)::bigint AS multiplicity_delta,
               min(multiplicity_prefix)::bigint
                 AS multiplicity_min_prefix
        FROM running_input
        GROUP BY input_side,join_key,row_data
      ),
      input_transition AS MATERIALIZED (
        SELECT delta.input_side,delta.join_key,delta.row_data,
               delta.multiplicity_delta,
               coalesce(old.multiplicity,0)::bigint AS old_multiplicity,
               (
                 coalesce(old.multiplicity,0)
                   + delta.multiplicity_delta
               )::bigint AS new_multiplicity
        FROM input_folded delta
        LEFT JOIN shiba_internal.join_arrangements old
          ON old.result_oid=$1
         AND old.input_side=delta.input_side
         AND old.join_key=delta.join_key
         AND old.row_data=delta.row_data
        WHERE shiba._assert_join_transition(
          coalesce(old.multiplicity,0)
            + delta.multiplicity_min_prefix>=0
          AND coalesce(old.multiplicity,0)
            + delta.multiplicity_delta>=0
          AND (
            old.result_oid IS NOT NULL
            OR delta.multiplicity_min_prefix>=0
          )
        )
      ),
      right_presence AS MATERIALIZED (
        SELECT
          (
            %7$L='null_anti'
            AND EXISTS (
              SELECT 1
              FROM shiba_internal.join_arrangements old
              WHERE old.result_oid=$1 AND old.input_side='right'
            )
          ) AS old_any,
          (
            %7$L='null_anti'
            AND EXISTS (
              SELECT 1
              FROM shiba_internal.join_arrangements old
              WHERE old.result_oid=$1
                AND old.input_side='right'
                AND old.join_key='null'::jsonb
            )
          ) AS old_null,
          (
            %7$L='null_anti'
            AND (
              EXISTS (
                SELECT 1 FROM input_transition changed
                WHERE changed.input_side='right'
                  AND changed.new_multiplicity>0
              )
              OR EXISTS (
                SELECT 1
                FROM shiba_internal.join_arrangements old
                WHERE old.result_oid=$1
                  AND old.input_side='right'
                  AND NOT EXISTS (
                    SELECT 1 FROM input_folded changed
                    WHERE changed.input_side='right'
                      AND changed.join_key=old.join_key
                      AND changed.row_data=old.row_data
                  )
              )
            )
          ) AS new_any,
          (
            %7$L='null_anti'
            AND (
              EXISTS (
                SELECT 1 FROM input_transition changed
                WHERE changed.input_side='right'
                  AND changed.join_key='null'::jsonb
                  AND changed.new_multiplicity>0
              )
              OR EXISTS (
                SELECT 1
                FROM shiba_internal.join_arrangements old
                WHERE old.result_oid=$1
                  AND old.input_side='right'
                  AND old.join_key='null'::jsonb
                  AND NOT EXISTS (
                    SELECT 1 FROM input_folded changed
                    WHERE changed.input_side='right'
                      AND changed.join_key=old.join_key
                      AND changed.row_data=old.row_data
                  )
              )
            )
          ) AS new_null
      ),
      affected_keys AS MATERIALIZED (
        SELECT DISTINCT join_key FROM input_folded
      ),
      expanded_keys AS MATERIALIZED (
        SELECT join_key FROM affected_keys
        UNION
        SELECT DISTINCT left_row.join_key
        FROM shiba_internal.join_arrangements left_row
        CROSS JOIN right_presence presence
        WHERE %7$L='null_anti'
          AND left_row.result_oid=$1
          AND left_row.input_side='left'
          AND presence.old_null IS DISTINCT FROM presence.new_null
        UNION
        SELECT 'null'::jsonb
        FROM right_presence presence
        WHERE %7$L='null_anti'
          AND presence.old_any IS DISTINCT FROM presence.new_any
      ),
      left_delta AS MATERIALIZED (
        SELECT changed.join_key,changed.row_data,typed.row,
               (
                 changed.new_multiplicity-changed.old_multiplicity
               )::bigint AS weight
        FROM input_transition changed
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%1$s,changed.row_data
          ) AS row
        ) typed
        WHERE changed.input_side='left'
          AND changed.new_multiplicity<>changed.old_multiplicity
      ),
      right_delta AS MATERIALIZED (
        SELECT changed.join_key,changed.row_data,typed.row,
               (
                 changed.new_multiplicity-changed.old_multiplicity
               )::bigint AS weight
        FROM input_transition changed
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%4$s,changed.row_data
          ) AS row
        ) typed
        WHERE changed.input_side='right'
          AND changed.new_multiplicity<>changed.old_multiplicity
      ),
      right_key_counts AS MATERIALIZED (
        SELECT key.join_key,
               coalesce(sum(old.multiplicity),0)::bigint AS old_count
        FROM expanded_keys key
        LEFT JOIN shiba_internal.join_arrangements old
          ON old.result_oid=$1
         AND old.input_side='right'
         AND old.join_key=key.join_key
        GROUP BY key.join_key
      ),
      right_key_presence AS MATERIALIZED (
        SELECT counts.join_key,counts.old_count,
               (
                 counts.old_count+coalesce((
                   SELECT sum(delta.weight)
                   FROM right_delta delta
                   WHERE delta.join_key=counts.join_key
                 ),0)
               )::bigint AS new_count
        FROM right_key_counts counts
      ),
      left_key_counts AS MATERIALIZED (
        SELECT key.join_key,
               coalesce(sum(old.multiplicity),0)::bigint AS old_count
        FROM expanded_keys key
        LEFT JOIN shiba_internal.join_arrangements old
          ON old.result_oid=$1
         AND old.input_side='left'
         AND old.join_key=key.join_key
        GROUP BY key.join_key
      ),
      left_key_presence AS MATERIALIZED (
        SELECT counts.join_key,counts.old_count,
               (
                 counts.old_count+coalesce((
                   SELECT sum(delta.weight)
                   FROM left_delta delta
                   WHERE delta.join_key=counts.join_key
                 ),0)
               )::bigint AS new_count
        FROM left_key_counts counts
      ),
      left_visibility AS MATERIALIZED (
        SELECT presence.join_key,
               CASE %7$L
                 WHEN 'left' THEN
                   presence.join_key='null'::jsonb
                     OR presence.old_count=0
                 WHEN 'full' THEN
                   presence.join_key='null'::jsonb
                     OR presence.old_count=0
                 WHEN 'semi' THEN
                   presence.join_key<>'null'::jsonb
                     AND presence.old_count>0
                 WHEN 'anti' THEN
                   presence.join_key='null'::jsonb
                     OR presence.old_count=0
                 WHEN 'null_anti' THEN
                   NOT global.old_any
                   OR (
                     presence.join_key<>'null'::jsonb
                     AND NOT global.old_null
                     AND presence.old_count=0
                   )
                 ELSE false
               END AS old_visible,
               CASE %7$L
                 WHEN 'left' THEN
                   presence.join_key='null'::jsonb
                     OR presence.new_count=0
                 WHEN 'full' THEN
                   presence.join_key='null'::jsonb
                     OR presence.new_count=0
                 WHEN 'semi' THEN
                   presence.join_key<>'null'::jsonb
                     AND presence.new_count>0
                 WHEN 'anti' THEN
                   presence.join_key='null'::jsonb
                     OR presence.new_count=0
                 WHEN 'null_anti' THEN
                   NOT global.new_any
                   OR (
                     presence.join_key<>'null'::jsonb
                     AND NOT global.new_null
                     AND presence.new_count=0
                   )
                 ELSE false
               END AS new_visible
        FROM right_key_presence presence
        CROSS JOIN right_presence global
      ),
      right_visibility AS MATERIALIZED (
        SELECT presence.join_key,
               (
                 presence.join_key='null'::jsonb
                   OR presence.old_count=0
               ) AS old_visible,
               (
                 presence.join_key='null'::jsonb
                   OR presence.new_count=0
               ) AS new_visible
        FROM left_key_presence presence
      ),
      direct_join_candidates AS MATERIALIZED (
        SELECT delta.row_data AS left_data,
               old.row_data AS right_data,
               delta.row AS left_row,
               typed.row AS right_row,
               (delta.weight*old.multiplicity)::bigint AS weight
        FROM left_delta delta
        CROSS JOIN LATERAL (
          SELECT arrangement.row_data,arrangement.multiplicity
          FROM shiba_internal.join_arrangements arrangement
          WHERE arrangement.result_oid=$1
            AND arrangement.input_side='right'
            AND delta.join_key<>'null'::jsonb
            AND arrangement.join_key=delta.join_key
          OFFSET 0
        ) old
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%4$s,old.row_data
          ) AS row
        ) typed
        WHERE %7$L IN ('inner','left','right','full')
        UNION ALL
        SELECT old.row_data AS left_data,
               delta.row_data AS right_data,
               typed.row AS left_row,
               delta.row AS right_row,
               (old.multiplicity*delta.weight)::bigint AS weight
        FROM right_delta delta
        CROSS JOIN LATERAL (
          SELECT arrangement.row_data,arrangement.multiplicity
          FROM shiba_internal.join_arrangements arrangement
          WHERE arrangement.result_oid=$1
            AND arrangement.input_side='left'
            AND arrangement.join_key<>'null'::jsonb
            AND arrangement.join_key=delta.join_key
          OFFSET 0
        ) old
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%1$s,old.row_data
          ) AS row
        ) typed
        WHERE %7$L IN ('inner','left','right','full')
        UNION ALL
        SELECT left_changed.row_data AS left_data,
               right_changed.row_data AS right_data,
               left_changed.row AS left_row,
               right_changed.row AS right_row,
               (left_changed.weight*right_changed.weight)::bigint AS weight
        FROM left_delta left_changed
        JOIN right_delta right_changed
          ON left_changed.join_key<>'null'::jsonb
         AND left_changed.join_key=right_changed.join_key
        WHERE %7$L IN ('inner','left','right','full')
        UNION ALL
        SELECT delta.row_data AS left_data,
               NULL::jsonb AS right_data,
               delta.row AS left_row,
               NULL::%4$s AS right_row,
               delta.weight AS weight
        FROM left_delta delta
        JOIN left_visibility visibility USING(join_key)
        WHERE %7$L IN (
          'left','full','semi','anti','null_anti'
        )
          AND visibility.new_visible
        UNION ALL
        SELECT old.row_data AS left_data,
               NULL::jsonb AS right_data,
               typed.row AS left_row,
               NULL::%4$s AS right_row,
               (
                 old.multiplicity*(
                   visibility.new_visible::integer
                     -visibility.old_visible::integer
                 )
               )::bigint AS weight
        FROM left_visibility visibility
        JOIN shiba_internal.join_arrangements old
          ON old.result_oid=$1
         AND old.input_side='left'
         AND old.join_key=visibility.join_key
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%1$s,old.row_data
          ) AS row
        ) typed
        WHERE %7$L IN (
          'left','full','semi','anti','null_anti'
        )
          AND visibility.old_visible
                IS DISTINCT FROM visibility.new_visible
        UNION ALL
        SELECT NULL::jsonb AS left_data,
               delta.row_data AS right_data,
               NULL::%1$s AS left_row,
               delta.row AS right_row,
               delta.weight AS weight
        FROM right_delta delta
        JOIN right_visibility visibility USING(join_key)
        WHERE %7$L IN ('right','full')
          AND visibility.new_visible
        UNION ALL
        SELECT NULL::jsonb AS left_data,
               old.row_data AS right_data,
               NULL::%1$s AS left_row,
               typed.row AS right_row,
               (
                 old.multiplicity*(
                   visibility.new_visible::integer
                     -visibility.old_visible::integer
                 )
               )::bigint AS weight
        FROM right_visibility visibility
        JOIN shiba_internal.join_arrangements old
          ON old.result_oid=$1
         AND old.input_side='right'
         AND old.join_key=visibility.join_key
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%4$s,old.row_data
          ) AS row
        ) typed
        WHERE %7$L IN ('right','full')
          AND visibility.old_visible
                IS DISTINCT FROM visibility.new_visible
      ),
      direct_inner_visible AS MATERIALIZED (
        SELECT candidate.left_data,candidate.right_data,candidate.weight
        FROM direct_join_candidates candidate
        CROSS JOIN LATERAL (
          SELECT candidate.left_row AS row
        ) left_input
        CROSS JOIN LATERAL (
          SELECT candidate.right_row AS row
        ) right_input
        WHERE coalesce((%8$s),false)
      ),
      direct_inner_delta AS MATERIALIZED (
        SELECT left_data,right_data,sum(weight)::bigint AS weight
        FROM direct_inner_visible
        GROUP BY left_data,right_data
        HAVING sum(weight)<>0
      ),
      net_delta AS MATERIALIZED (
        SELECT left_data,right_data,weight
        FROM direct_inner_delta
      ),
      stage_written AS (
        INSERT INTO %9$s (
          commit_lsn,sequence,weight,left_row,right_row
        )
        SELECT $4,
               row_number() OVER ()::bigint,
               delta.weight,left_typed.row,right_typed.row
        FROM net_delta delta
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%1$s,delta.left_data
          ) AS row
        ) left_typed
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%4$s,delta.right_data
          ) AS row
        ) right_typed
        RETURNING 1
      ),
      arrangement_applied AS (
        MERGE INTO shiba_internal.join_arrangements AS arrangement
        USING input_transition transition
          ON arrangement.result_oid=$1
         AND arrangement.input_side=transition.input_side
         AND arrangement.join_key=transition.join_key
         AND arrangement.row_data=transition.row_data
        WHEN MATCHED AND transition.new_multiplicity=0 THEN DELETE
        WHEN MATCHED THEN UPDATE
          SET multiplicity=transition.new_multiplicity
        WHEN NOT MATCHED AND transition.new_multiplicity>0 THEN
          INSERT (
            result_oid,input_side,join_key,row_data,multiplicity
          )
          VALUES (
            $1,transition.input_side,transition.join_key,
            transition.row_data,transition.new_multiplicity
          )
        RETURNING 1
      )
      SELECT
        (SELECT count(*) FROM stage_written),
        (SELECT count(*) FROM arrangement_applied)
      $stage_statement$,
      left_name,
      join_view.left_join_column,
      left_pre_filter_sql,
      right_name,
      join_view.right_join_column,
      right_pre_filter_sql,
      join_view.join_type,
      post_filter_sql,
      stage_name
    );
    EXECUTE format(
      'PREPARE %I (oid,oid,oid,pg_lsn) AS %s',
      stage_plan_name,stage_statement_sql
    );
    EXECUTE stage_execute_sql INTO STRICT staged_rows,arrangement_rows;
    IF staged_rows>max_stage_rows THEN
      RAISE EXCEPTION
        'Shiba JOIN commit % for result % produced % Stage rows, limit %',
        p_commit_lsn,result_relation::regclass,staged_rows,max_stage_rows
        USING ERRCODE='53400',
              HINT='Increase shiba.max_stage_rows or reduce JOIN fanout.';
    END IF;
    IF staged_rows>=1024 THEN
      EXECUTE format('ANALYZE %s',stage_name);
    END IF;

    group_expression := CASE join_view.group_source
      WHEN 'left' THEN format('(stage.left_row).%I',join_view.group_column)
      ELSE format('(stage.right_row).%I',join_view.group_column)
    END;
    sum_input_expression :=
      format('(stage.left_row).%I',stream_view.sum_input_column);
    count_input_expression := CASE
      WHEN NOT stream_view.count_distinct THEN 'NULL::jsonb'
      WHEN stream_view.count_input_source='left'
        THEN format('to_jsonb((stage.left_row).%I)',
                    stream_view.count_input_column)
      ELSE format('to_jsonb((stage.right_row).%I)',
                  stream_view.count_input_column)
    END;

    SELECT predicate_sql INTO having_sql
    FROM shiba_internal.stream_having
    WHERE result_oid=result_relation;
    visible_sql := CASE
      WHEN having_sql IS NULL THEN 'true'
      ELSE format('coalesce((%s),false)',having_sql)
    END;

    consume_statement_sql := format(
      $statement$
      WITH consumed_stage AS MATERIALIZED (
        DELETE FROM %1$s
        RETURNING sequence,weight,left_row,right_row
      ),
      stage_rows AS MATERIALIZED (
        SELECT stage.sequence,stage.weight,
               coalesce(to_jsonb(%2$s),'null'::jsonb) AS group_key,
               %4$s AS value_key,
               CASE WHEN %3$s IS NULL
                 THEN 0 ELSE stage.weight
               END::bigint AS sum_nonnull_delta,
               (stage.weight*coalesce((%3$s)::numeric,0))::numeric
                 AS sum_delta
        FROM consumed_stage stage
      ),
      group_contribution AS MATERIALIZED (
        SELECT group_key,
               sum(weight)::bigint AS row_count_delta,
               sum(sum_nonnull_delta)::bigint AS sum_nonnull_delta,
               sum(sum_delta)::numeric AS sum_delta
        FROM stage_rows
        GROUP BY group_key
      ),
      key_contribution AS MATERIALIZED (
        SELECT group_key,value_key,
               sum(weight)::bigint AS multiplicity_delta
        FROM stage_rows
        WHERE $3
          AND value_key IS NOT NULL
          AND value_key<>'null'::jsonb
        GROUP BY group_key,value_key
      ),
      key_transition AS MATERIALIZED (
        SELECT contribution.group_key,contribution.value_key,
               coalesce(state.multiplicity,0)::bigint
                 AS old_multiplicity,
               (
                 coalesce(state.multiplicity,0)
                   + contribution.multiplicity_delta
               )::bigint AS new_multiplicity
        FROM key_contribution contribution
        LEFT JOIN shiba_internal.distinct_state state
          ON state.result_oid=$1
         AND state.group_key=contribution.group_key
         AND state.value_key=contribution.value_key
        WHERE shiba._assert_join_transition(
          coalesce(state.multiplicity,0)
            + contribution.multiplicity_delta>=0
        )
      ),
      key_count_delta AS MATERIALIZED (
        SELECT group_key,
               sum(
                 CASE
                   WHEN old_multiplicity=0
                    AND new_multiplicity>0 THEN 1
                   WHEN old_multiplicity>0
                    AND new_multiplicity=0 THEN -1
                   ELSE 0
                 END
               )::bigint AS count_value_delta
        FROM key_transition
        GROUP BY group_key
      ),
      contribution AS MATERIALIZED (
        SELECT grouped.group_key,grouped.row_count_delta,
               CASE WHEN $3
                 THEN coalesce(keys.count_value_delta,0)
                 ELSE grouped.row_count_delta
               END::bigint AS count_value_delta,
               grouped.sum_nonnull_delta,grouped.sum_delta
        FROM group_contribution grouped
        LEFT JOIN key_count_delta keys USING(group_key)
      ),
      transition AS MATERIALIZED (
        SELECT contribution.group_key,
               (
                 coalesce(state.row_count,0)
                   + contribution.row_count_delta
               )::bigint AS row_count,
               (
                 coalesce(state.count_value,0)
                   + contribution.count_value_delta
               )::bigint AS count_value,
               (
                 coalesce(state.sum_nonnull_count,0)
                   + contribution.sum_nonnull_delta
               )::bigint AS sum_nonnull_count,
               (
                 coalesce(state.sum_value,0)
                   + contribution.sum_delta
               )::numeric AS sum_value
        FROM contribution
        LEFT JOIN shiba_internal.aggregate_state state
          ON state.result_oid=$1
         AND state.group_key=contribution.group_key
        WHERE shiba._assert_join_transition(
          coalesce(state.row_count,0)
            + contribution.row_count_delta>=0
          AND coalesce(state.count_value,0)
            + contribution.count_value_delta>=0
          AND coalesce(state.sum_nonnull_count,0)
            + contribution.sum_nonnull_delta>=0
          AND (
            (
              $3
              AND coalesce(state.count_value,0)
                    + contribution.count_value_delta
                  <= coalesce(state.row_count,0)
                       + contribution.row_count_delta
            )
            OR (
              NOT $3
              AND coalesce(state.count_value,0)
                    + contribution.count_value_delta
                  = coalesce(state.row_count,0)
                      + contribution.row_count_delta
            )
          )
          AND coalesce(state.sum_nonnull_count,0)
                + contribution.sum_nonnull_delta
              <= coalesce(state.row_count,0)
                   + contribution.row_count_delta
          AND (
            coalesce(state.row_count,0)
              + contribution.row_count_delta<>0
            OR (
              coalesce(state.count_value,0)
                + contribution.count_value_delta=0
              AND coalesce(state.sum_nonnull_count,0)
                + contribution.sum_nonnull_delta=0
            )
          )
        )
      ),
      distinct_applied AS (
        MERGE INTO shiba_internal.distinct_state target
        USING key_transition next
          ON target.result_oid=$1
         AND target.group_key=next.group_key
         AND target.value_key=next.value_key
        WHEN MATCHED AND next.new_multiplicity=0 THEN DELETE
        WHEN MATCHED THEN UPDATE
          SET multiplicity=next.new_multiplicity
        WHEN NOT MATCHED AND next.new_multiplicity>0 THEN
          INSERT (result_oid,group_key,value_key,multiplicity)
          VALUES (
            $1,next.group_key,next.value_key,next.new_multiplicity
          )
        RETURNING 1
      ),
      aggregate_applied AS (
        MERGE INTO shiba_internal.aggregate_state target
        USING transition next
          ON target.result_oid=$1
         AND target.group_key=next.group_key
        WHEN MATCHED AND next.row_count=0 THEN DELETE
        WHEN MATCHED THEN UPDATE SET
          row_count=next.row_count,
          count_value=next.count_value,
          sum_nonnull_count=next.sum_nonnull_count,
          sum_value=next.sum_value
        WHEN NOT MATCHED AND next.row_count>0 THEN
          INSERT (
            result_oid,group_key,row_count,count_value,
            sum_nonnull_count,sum_value
          )
          VALUES (
            $1,next.group_key,next.row_count,next.count_value,
            next.sum_nonnull_count,next.sum_value
          )
        RETURNING 1
      ),
      visibility AS MATERIALIZED (
        SELECT (typed.row).%6$I AS group_value,
               state.count_value,
               CASE WHEN state.sum_nonnull_count=0
                 THEN NULL ELSE state.sum_value
               END AS sum_value,
               (state.row_count<>0 AND %9$s) AS visible
        FROM transition state
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%5$s,
            jsonb_build_object(%10$L,state.group_key)
          ) row
        ) typed
      ),
      sink_deleted AS (
        DELETE FROM %5$s sink
        USING visibility
        WHERE sink.%6$I IS NOT DISTINCT FROM visibility.group_value
          AND NOT visibility.visible
        RETURNING 1
      )
      INSERT INTO %5$s (%6$I,%7$I,%8$I)
      SELECT group_value,count_value,sum_value
      FROM visibility
      WHERE visible
      ON CONFLICT (%6$I) DO UPDATE
      SET %7$I=EXCLUDED.%7$I,%8$I=EXCLUDED.%8$I
      $statement$,
      stage_name,
      group_expression,
      sum_input_expression,
      count_input_expression,
      result_name,
      stream_view.result_group_column,
      stream_view.count_column,
      stream_view.sum_column,
      visible_sql,
      stream_view.result_group_column
    );
    EXECUTE format(
      'PREPARE %I (oid,pg_lsn,boolean) AS %s',
      consume_plan_name,consume_statement_sql
    );
    EXECUTE consume_execute_sql;
    IF staged_rows>=1024 THEN
      EXECUTE format('ANALYZE %s',stage_name);
    END IF;
    PERFORM shiba_internal._compact_physical_stages(result_relation);
END;
$$;
