CREATE FUNCTION shiba._apply_topn_delta(
    stream_view shiba_internal.stream_views,
    p_row_data jsonb,
    delta bigint,
    commit_lsn text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    topn_view shiba_internal.topn_views%ROWTYPE;
    source_name text;
    result_name text;
    prior_multiplicity bigint;
    quoted_outputs text;
    expressions text;
BEGIN
    SELECT * INTO STRICT topn_view
    FROM shiba_internal.topn_views WHERE result_oid=stream_view.result_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.source_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT result_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.result_oid;
    SELECT multiplicity INTO prior_multiplicity
    FROM shiba_internal.topn_rows
    WHERE result_oid=stream_view.result_oid AND row_data=p_row_data;
    prior_multiplicity := coalesce(prior_multiplicity,0);
    IF delta>0 THEN
      INSERT INTO shiba_internal.topn_rows(result_oid,row_data,multiplicity)
      VALUES(stream_view.result_oid,p_row_data,delta)
      ON CONFLICT(result_oid,row_data) DO UPDATE
      SET multiplicity=topn_rows.multiplicity+EXCLUDED.multiplicity;
    ELSE
      IF prior_multiplicity=0 THEN
        RAISE EXCEPTION 'Shiba TopN state is missing a retracted row'
          USING ERRCODE='data_corrupted';
      ELSIF prior_multiplicity+delta<0 THEN
        RAISE EXCEPTION 'Shiba TopN multiplicity became negative'
          USING ERRCODE='data_corrupted';
      ELSIF prior_multiplicity+delta=0 THEN
        DELETE FROM shiba_internal.topn_rows
        WHERE result_oid=stream_view.result_oid AND row_data=p_row_data;
      ELSE
        UPDATE shiba_internal.topn_rows
        SET multiplicity=prior_multiplicity+delta
        WHERE result_oid=stream_view.result_oid AND row_data=p_row_data;
      END IF;
    END IF;
    EXECUTE format('DELETE FROM %s',result_name);
    SELECT string_agg(format('%I',output_column),',' ORDER BY ordinal),
           string_agg(format('input.%I',source_column),',' ORDER BY ordinal)
    INTO quoted_outputs,expressions
    FROM unnest(topn_view.source_columns,topn_view.output_columns)
      WITH ORDINALITY columns(source_column,output_column,ordinal);
    EXECUTE format(
      'INSERT INTO %s (%s)
       SELECT %s
       FROM shiba_internal.topn_rows state
       CROSS JOIN LATERAL jsonb_populate_record(NULL::%s,state.row_data) input
       CROSS JOIN LATERAL generate_series(1,state.multiplicity) copy(n)
       WHERE state.result_oid=$1
       ORDER BY input.%I %s NULLS %s,state.row_data::text,copy.n
       OFFSET %s LIMIT %s',
      result_name,quoted_outputs,expressions,source_name,
      topn_view.order_column,upper(topn_view.order_direction),
      CASE topn_view.nulls_first WHEN true THEN 'FIRST' ELSE 'LAST' END,
      topn_view.limit_offset,topn_view.limit_count
    ) USING stream_view.result_oid;
END;
$$;

CREATE FUNCTION shiba._apply_distinct_delta(
    stream_view shiba_internal.stream_views,
    p_row_data jsonb,
    delta bigint,
    commit_lsn text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    distinct_view shiba_internal.distinct_views%ROWTYPE;
    source_name text;
    result_name text;
    key_arguments text;
    state_row_key jsonb;
    prior_multiplicity bigint;
BEGIN
    SELECT * INTO STRICT distinct_view
    FROM shiba_internal.distinct_views WHERE result_oid=stream_view.result_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.source_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT result_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.result_oid;
    SELECT string_agg(
      format('%L,to_jsonb((typed.row).%I)',output_column,source_column),
      ',' ORDER BY ordinal
    ) INTO key_arguments
    FROM unnest(distinct_view.source_columns,distinct_view.output_columns)
      WITH ORDINALITY columns(source_column,output_column,ordinal);
    EXECUTE format(
      'SELECT jsonb_build_object(%s)
       FROM (SELECT jsonb_populate_record(NULL::%s,$1) row) typed',
      key_arguments,source_name
    ) USING p_row_data INTO STRICT state_row_key;

    SELECT multiplicity INTO prior_multiplicity
    FROM shiba_internal.projection_state
    WHERE result_oid=stream_view.result_oid
      AND row_key=state_row_key;
    prior_multiplicity := coalesce(prior_multiplicity,0);
    IF delta>0 THEN
      INSERT INTO shiba_internal.projection_state(result_oid,row_key,multiplicity)
      VALUES(stream_view.result_oid,state_row_key,delta)
      ON CONFLICT(result_oid,row_key) DO UPDATE
      SET multiplicity=projection_state.multiplicity+EXCLUDED.multiplicity;
      IF prior_multiplicity=0 THEN
        EXECUTE format(
          'INSERT INTO %s SELECT (jsonb_populate_record(NULL::%s,$1)).*',
          result_name,result_name
        ) USING state_row_key;
      END IF;
    ELSE
      IF prior_multiplicity=0 THEN
        RAISE EXCEPTION 'Shiba DISTINCT state is missing a retracted row'
          USING ERRCODE='data_corrupted';
      ELSIF prior_multiplicity+delta<0 THEN
        RAISE EXCEPTION 'Shiba DISTINCT multiplicity became negative'
          USING ERRCODE='data_corrupted';
      ELSIF prior_multiplicity+delta=0 THEN
        DELETE FROM shiba_internal.projection_state
        WHERE result_oid=stream_view.result_oid AND row_key=state_row_key;
        EXECUTE format('DELETE FROM %s target WHERE to_jsonb(target)=$1',result_name)
        USING state_row_key;
      ELSE
        UPDATE shiba_internal.projection_state
        SET multiplicity=prior_multiplicity+delta
        WHERE result_oid=stream_view.result_oid AND row_key=state_row_key;
      END IF;
    END IF;
END;
$$;

CREATE FUNCTION shiba._apply_window_delta(
    stream_view shiba_internal.stream_views,
    p_row_data jsonb,
    delta bigint,
    commit_lsn text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    window_view shiba_internal.window_views%ROWTYPE;
    source_name text;
    result_name text;
    state_partition_key jsonb;
    prior_multiplicity bigint;
    quoted_outputs text;
    expressions text;
BEGIN
    SELECT * INTO STRICT window_view
    FROM shiba_internal.window_views WHERE result_oid=stream_view.result_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.source_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT result_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.result_oid;
    EXECUTE format(
      'SELECT coalesce(to_jsonb((typed.row).%I),''null''::jsonb)
       FROM (SELECT jsonb_populate_record(NULL::%s,$1) row) typed',
      window_view.partition_column,source_name
    ) USING p_row_data INTO STRICT state_partition_key;

    IF delta>0 THEN
      INSERT INTO shiba_internal.window_rows
        (result_oid,partition_key,row_data,multiplicity)
      VALUES(stream_view.result_oid,state_partition_key,p_row_data,delta)
      ON CONFLICT(result_oid,partition_key,row_data) DO UPDATE
      SET multiplicity=window_rows.multiplicity+EXCLUDED.multiplicity;
    ELSE
      SELECT multiplicity INTO prior_multiplicity
      FROM shiba_internal.window_rows
      WHERE result_oid=stream_view.result_oid
        AND window_rows.partition_key=state_partition_key
        AND window_rows.row_data=p_row_data;
      IF prior_multiplicity IS NULL THEN
        RAISE EXCEPTION 'Shiba window state is missing a retracted row'
          USING ERRCODE='data_corrupted';
      ELSIF prior_multiplicity+delta<0 THEN
        RAISE EXCEPTION 'Shiba window multiplicity became negative'
          USING ERRCODE='data_corrupted';
      ELSIF prior_multiplicity+delta=0 THEN
        DELETE FROM shiba_internal.window_rows
        WHERE result_oid=stream_view.result_oid
          AND window_rows.partition_key=state_partition_key
          AND window_rows.row_data=p_row_data;
      ELSE
        UPDATE shiba_internal.window_rows
        SET multiplicity=prior_multiplicity+delta
        WHERE result_oid=stream_view.result_oid
          AND window_rows.partition_key=state_partition_key
          AND window_rows.row_data=p_row_data;
      END IF;
    END IF;

    EXECUTE format(
      'DELETE FROM %s
       WHERE coalesce(to_jsonb(%I),''null''::jsonb)=$1',
      result_name,window_view.result_partition_column
    ) USING state_partition_key;
    SELECT string_agg(format('%I',column_name),',' ORDER BY ordinal)
    INTO quoted_outputs
    FROM unnest(window_view.output_columns) WITH ORDINALITY output(column_name,ordinal);
    SELECT string_agg(expression,',' ORDER BY ordinal)
    INTO expressions
    FROM unnest(window_view.target_expressions) WITH ORDINALITY target(expression,ordinal);
    EXECUTE format(
      'INSERT INTO %s (%s)
       SELECT %s
       FROM shiba_internal.window_rows state
       CROSS JOIN LATERAL jsonb_populate_record(NULL::%s,state.row_data) input
       CROSS JOIN LATERAL generate_series(1,state.multiplicity) copy(n)
       WHERE state.result_oid=$1 AND state.partition_key=$2',
      result_name,quoted_outputs,expressions,source_name
    ) USING stream_view.result_oid,state_partition_key;

END;
$$;

CREATE FUNCTION shiba._apply_inner_join_delta(
    result_relation oid, p_input_side text, input_row jsonb, p_delta bigint,
    commit_lsn text, defer_sink boolean DEFAULT false
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    view_row shiba_internal.stream_views%ROWTYPE;
    join_row shiba_internal.inner_join_views%ROWTYPE;
    key_value text;
    prior_multiplicity bigint;
    same_total bigint := 0;
    opposite_total bigint := 0;
    opposite text := CASE p_input_side WHEN 'left' THEN 'right' ELSE 'left' END;
    match_row record;
    preserves_input boolean;
    preserves_opposite boolean;
    right_total bigint := 0;
    right_null_total bigint := 0;
    old_match_total bigint;
    new_match_total bigint;
    new_right_total bigint;
    new_right_null_total bigint;
    left_key text;
    old_visible boolean;
    new_visible boolean;
BEGIN
    SELECT * INTO STRICT view_row FROM shiba_internal.stream_views WHERE result_oid = result_relation;
    SELECT * INTO STRICT join_row FROM shiba_internal.inner_join_views WHERE result_oid = result_relation;
    key_value := input_row ->> CASE p_input_side WHEN 'left' THEN join_row.left_join_column ELSE join_row.right_join_column END;
    preserves_input := join_row.join_type = 'full'
      OR (join_row.join_type = 'left' AND p_input_side = 'left')
      OR (join_row.join_type = 'right' AND p_input_side = 'right');
    preserves_opposite := join_row.join_type = 'full'
      OR (join_row.join_type = 'left' AND opposite = 'left')
      OR (join_row.join_type = 'right' AND opposite = 'right');
    IF key_value IS NOT NULL THEN
        SELECT coalesce(sum(multiplicity),0) INTO same_total
        FROM shiba_internal.join_arrangements
        WHERE result_oid=result_relation AND input_side=p_input_side AND join_key=key_value;
        SELECT coalesce(sum(multiplicity),0) INTO opposite_total
        FROM shiba_internal.join_arrangements
        WHERE result_oid=result_relation AND input_side=opposite AND join_key=key_value;
    END IF;
    IF p_delta < 0 THEN
        SELECT multiplicity INTO prior_multiplicity
        FROM shiba_internal.join_arrangements
        WHERE result_oid = result_relation AND input_side = p_input_side
          AND join_key = COALESCE(key_value, '') AND row_data = input_row;
        -- A slot may replay a commit after a worker crash boundary.  Do not
        -- accept an absent row: inbox acknowledgement and state changes are
        -- atomic, so this indicates an encoding or state-integrity bug.
        IF prior_multiplicity IS NULL THEN
            RAISE EXCEPTION 'Shiba JOIN state is missing a retracted row'
                USING ERRCODE='data_corrupted';
        END IF;
    END IF;

    IF join_row.join_type='null_anti' THEN
      SELECT coalesce(sum(multiplicity),0),
             coalesce(sum(multiplicity) FILTER (
               WHERE row_data ->> join_row.right_join_column IS NULL
             ),0)
      INTO right_total,right_null_total
      FROM shiba_internal.join_arrangements
      WHERE result_oid=result_relation AND input_side='right';
      IF p_input_side='left' THEN
        old_visible := right_total=0
          OR (key_value IS NOT NULL AND right_null_total=0 AND opposite_total=0);
        IF old_visible THEN
          PERFORM shiba._apply_inner_join_aggregate(
            view_row,join_row,input_row,'{}'::jsonb,p_delta,defer_sink
          );
        END IF;
      ELSE
        new_right_total := right_total+p_delta;
        new_right_null_total := right_null_total
          + CASE WHEN key_value IS NULL THEN p_delta ELSE 0 END;
        FOR match_row IN
          SELECT row_data,multiplicity
          FROM shiba_internal.join_arrangements
          WHERE result_oid=result_relation AND input_side='left'
        LOOP
          left_key := match_row.row_data ->> join_row.left_join_column;
          IF left_key IS NULL THEN
            old_match_total := 0;
          ELSE
            SELECT coalesce(sum(multiplicity),0) INTO old_match_total
            FROM shiba_internal.join_arrangements
            WHERE result_oid=result_relation AND input_side='right'
              AND join_key=left_key;
          END IF;
          new_match_total := old_match_total
            + CASE WHEN key_value IS NOT NULL AND left_key=key_value THEN p_delta ELSE 0 END;
          old_visible := right_total=0
            OR (left_key IS NOT NULL AND right_null_total=0 AND old_match_total=0);
          new_visible := new_right_total=0
            OR (left_key IS NOT NULL AND new_right_null_total=0 AND new_match_total=0);
          IF old_visible IS DISTINCT FROM new_visible THEN
            PERFORM shiba._apply_inner_join_aggregate(
              view_row,join_row,match_row.row_data,'{}'::jsonb,
              match_row.multiplicity * CASE WHEN new_visible THEN 1 ELSE -1 END,
              defer_sink
            );
          END IF;
        END LOOP;
      END IF;
    ELSIF join_row.join_type IN ('semi','anti') THEN
      IF p_input_side='left' THEN
        IF (join_row.join_type='semi' AND key_value IS NOT NULL AND opposite_total>0)
           OR (join_row.join_type='anti' AND (key_value IS NULL OR opposite_total=0)) THEN
          PERFORM shiba._apply_inner_join_aggregate(
            view_row,join_row,input_row,'{}'::jsonb,p_delta,defer_sink
          );
        END IF;
      ELSIF key_value IS NOT NULL
        AND ((p_delta>0 AND same_total=0)
          OR (p_delta<0 AND same_total+p_delta=0)) THEN
        FOR match_row IN
          SELECT row_data,multiplicity
          FROM shiba_internal.join_arrangements
          WHERE result_oid=result_relation
            AND input_side='left' AND join_key=key_value
        LOOP
          PERFORM shiba._apply_inner_join_aggregate(
            view_row,join_row,match_row.row_data,'{}'::jsonb,
            match_row.multiplicity *
              CASE
                WHEN join_row.join_type='semi' AND p_delta>0 THEN 1
                WHEN join_row.join_type='semi' THEN -1
                WHEN p_delta>0 THEN -1
                ELSE 1
              END,
            defer_sink
          );
        END LOOP;
      END IF;
    ELSE
      -- SQL equality never matches NULL.  We still retain the row so an
      -- UPDATE from NULL to a value has correct future arrangement state.
      IF key_value IS NOT NULL THEN
        FOR match_row IN
            SELECT row_data, multiplicity
            FROM shiba_internal.join_arrangements AS arrangement
            WHERE arrangement.result_oid = result_relation AND arrangement.input_side = opposite AND arrangement.join_key = key_value
        LOOP
            IF p_input_side = 'left' THEN
                PERFORM shiba._apply_inner_join_aggregate(
                  view_row,join_row,input_row,match_row.row_data,
                  p_delta*match_row.multiplicity,defer_sink
                );
            ELSE
                PERFORM shiba._apply_inner_join_aggregate(
                  view_row,join_row,match_row.row_data,input_row,
                  p_delta*match_row.multiplicity,defer_sink
                );
            END IF;
        END LOOP;
      END IF;

    -- A preserved row with no equality match contributes one NULL-extended
    -- joined row per source multiplicity.
      IF preserves_input AND (key_value IS NULL OR opposite_total = 0) THEN
        IF p_input_side = 'left' THEN
            PERFORM shiba._apply_inner_join_aggregate(
                view_row,join_row,input_row,'{}'::jsonb,p_delta,defer_sink
            );
        ELSE
            PERFORM shiba._apply_inner_join_aggregate(
                view_row,join_row,'{}'::jsonb,input_row,p_delta,defer_sink
            );
        END IF;
      END IF;

    -- The first row on one side replaces all preserved NULL-extended rows on
    -- the opposite side. Removing the last row restores them.
      IF key_value IS NOT NULL AND preserves_opposite
       AND ((p_delta > 0 AND same_total = 0)
         OR (p_delta < 0 AND same_total + p_delta = 0)) THEN
        FOR match_row IN
            SELECT row_data,multiplicity
            FROM shiba_internal.join_arrangements
            WHERE result_oid=result_relation AND input_side=opposite AND join_key=key_value
        LOOP
            IF opposite = 'left' THEN
                PERFORM shiba._apply_inner_join_aggregate(
                    view_row,join_row,match_row.row_data,'{}'::jsonb,
                    CASE WHEN p_delta > 0 THEN -match_row.multiplicity ELSE match_row.multiplicity END,
                    defer_sink
                );
            ELSE
                PERFORM shiba._apply_inner_join_aggregate(
                    view_row,join_row,'{}'::jsonb,match_row.row_data,
                    CASE WHEN p_delta > 0 THEN -match_row.multiplicity ELSE match_row.multiplicity END,
                    defer_sink
                );
            END IF;
        END LOOP;
      END IF;
    END IF;

    IF p_delta > 0 THEN
        INSERT INTO shiba_internal.join_arrangements (result_oid, input_side, join_key, row_data, multiplicity)
        VALUES (result_relation, p_input_side, COALESCE(key_value, ''), input_row, p_delta)
        ON CONFLICT (result_oid, input_side, join_key, row_data) DO UPDATE
        SET multiplicity = shiba_internal.join_arrangements.multiplicity + EXCLUDED.multiplicity;
    ELSE
        IF prior_multiplicity + p_delta < 0 THEN
            RAISE EXCEPTION 'Shiba JOIN state corruption: deleted row is absent from its arrangement'
                USING ERRCODE = 'data_corrupted';
        END IF;
        IF prior_multiplicity + p_delta = 0 THEN
            DELETE FROM shiba_internal.join_arrangements
            WHERE result_oid = result_relation AND input_side = p_input_side
              AND join_key = COALESCE(key_value, '') AND row_data = input_row;
        ELSE
            UPDATE shiba_internal.join_arrangements
            SET multiplicity = prior_multiplicity + p_delta
            WHERE result_oid = result_relation AND input_side = p_input_side
              AND join_key = COALESCE(key_value, '') AND row_data = input_row;
        END IF;
    END IF;

END; $$;

CREATE FUNCTION shiba._apply_inner_join_aggregate(
    view_row shiba_internal.stream_views, join_row shiba_internal.inner_join_views,
    left_row jsonb, right_row jsonb, delta bigint, defer_sink boolean DEFAULT false
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE left_name text; right_name text; group_expr text;
    state_group_key jsonb; state_sum_value numeric; state_count_input jsonb;
    count_input_expr text;
BEGIN
    IF join_row.sum_source <> 'left' THEN RAISE EXCEPTION 'Shiba MVP currently requires SUM input from the left JOIN source'; END IF;
    IF NOT shiba._joined_rows_pass_filters(view_row.result_oid,left_row,right_row) THEN
      RETURN;
    END IF;
    SELECT format('%I.%I', n.nspname, c.relname) INTO left_name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=view_row.source_oid;
    SELECT format('%I.%I', n.nspname, c.relname) INTO right_name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=join_row.right_source_oid;
    group_expr := CASE join_row.group_source WHEN 'left' THEN format('(l.row).%I', join_row.group_column) ELSE format('(r.row).%I', join_row.group_column) END;
    count_input_expr := CASE
      WHEN NOT view_row.count_distinct THEN 'NULL::jsonb'
      WHEN view_row.count_input_source='left' THEN format('to_jsonb((l.row).%I)',view_row.count_input_column)
      ELSE format('to_jsonb((r.row).%I)',view_row.count_input_column)
    END;
    EXECUTE format(
      'WITH l AS (SELECT jsonb_populate_record(NULL::%s, $1) row),
            r AS (SELECT jsonb_populate_record(NULL::%s, $2) row)
       SELECT coalesce(to_jsonb(%s), ''null''::jsonb),
              ((l.row).%I)::numeric,%s FROM l,r',
      left_name,right_name,group_expr,view_row.sum_input_column,count_input_expr
    ) USING left_row,right_row
      INTO STRICT state_group_key,state_sum_value,state_count_input;
    PERFORM shiba._apply_aggregate_state(
      view_row.result_oid,state_group_key,delta,state_sum_value,state_count_input
    );
    IF defer_sink THEN
      INSERT INTO pg_temp.shiba_join_batch_groups(result_oid,group_key)
      VALUES(view_row.result_oid,state_group_key)
      ON CONFLICT(result_oid,group_key) DO NOTHING;
    ELSE
      PERFORM shiba._sync_aggregate_sink(view_row,state_group_key);
    END IF;
END; $$;
