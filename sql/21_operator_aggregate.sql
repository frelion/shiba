CREATE FUNCTION shiba._protect_result_table()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- An extension owner is trusted PostgreSQL administration; a caller
    -- cannot forge current_user with SET as it could a custom GUC.
    IF current_user <> shiba_internal.extension_owner() THEN
        RAISE EXCEPTION 'cannot modify Shiba result table % directly', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
            USING ERRCODE = 'read_only_sql_transaction';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION shiba._apply_aggregate_state(
    result_relation oid,
    p_group_key jsonb,
    count_delta bigint,
    input_sum_value numeric,
    count_input_value jsonb DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    affected bigint;
    new_count bigint;
    new_sum_nonnull_count bigint;
    sum_nonnull_delta bigint := CASE WHEN input_sum_value IS NULL THEN 0 ELSE count_delta END;
    sum_delta numeric := count_delta * coalesce(input_sum_value,0);
    count_value_delta bigint;
    prior_distinct_multiplicity bigint;
    uses_distinct boolean;
BEGIN
    SELECT count_distinct INTO STRICT uses_distinct
    FROM shiba_internal.stream_views WHERE result_oid=result_relation;
    IF uses_distinct THEN
        count_value_delta := 0;
        IF count_input_value IS NOT NULL AND count_input_value <> 'null'::jsonb THEN
            SELECT multiplicity INTO prior_distinct_multiplicity
            FROM shiba_internal.distinct_state
            WHERE result_oid=result_relation AND group_key=p_group_key
              AND value_key=count_input_value;
            prior_distinct_multiplicity := coalesce(prior_distinct_multiplicity,0);
            IF count_delta > 0 THEN
                INSERT INTO shiba_internal.distinct_state
                    (result_oid,group_key,value_key,multiplicity)
                VALUES(result_relation,p_group_key,count_input_value,count_delta)
                ON CONFLICT(result_oid,group_key,value_key) DO UPDATE
                SET multiplicity=distinct_state.multiplicity+EXCLUDED.multiplicity;
                IF prior_distinct_multiplicity=0 THEN count_value_delta := 1; END IF;
            ELSE
                IF prior_distinct_multiplicity + count_delta < 0 THEN
                    RAISE EXCEPTION 'Shiba DISTINCT multiplicity became negative'
                        USING ERRCODE='data_corrupted';
                ELSIF prior_distinct_multiplicity + count_delta = 0 THEN
                    DELETE FROM shiba_internal.distinct_state
                    WHERE result_oid=result_relation AND group_key=p_group_key
                      AND value_key=count_input_value;
                    count_value_delta := -1;
                ELSE
                    UPDATE shiba_internal.distinct_state
                    SET multiplicity=prior_distinct_multiplicity+count_delta
                    WHERE result_oid=result_relation AND group_key=p_group_key
                      AND value_key=count_input_value;
                END IF;
            END IF;
        END IF;
    ELSE
        count_value_delta := count_delta;
    END IF;
    UPDATE shiba_internal.aggregate_state
    SET row_count = row_count + count_delta,
        count_value = count_value + count_value_delta,
        sum_nonnull_count = sum_nonnull_count + sum_nonnull_delta,
        sum_value = sum_value + sum_delta
    WHERE result_oid = result_relation
      AND aggregate_state.group_key = p_group_key
    RETURNING row_count,sum_nonnull_count INTO new_count,new_sum_nonnull_count;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected = 0 THEN
        IF count_delta <= 0 THEN
            RAISE EXCEPTION 'Shiba aggregate state retraction has no group'
                USING ERRCODE = 'data_corrupted';
        END IF;
        INSERT INTO shiba_internal.aggregate_state
            (result_oid, group_key, row_count, count_value, sum_nonnull_count, sum_value)
        VALUES
            (result_relation, p_group_key, count_delta, count_value_delta,
             sum_nonnull_delta, sum_delta);
        RETURN;
    END IF;
    IF new_count < 0 OR new_sum_nonnull_count < 0 THEN
        RAISE EXCEPTION 'Shiba aggregate state count became negative'
            USING ERRCODE = 'data_corrupted';
    ELSIF new_count = 0 THEN
        DELETE FROM shiba_internal.aggregate_state
        WHERE result_oid = result_relation
          AND aggregate_state.group_key = p_group_key;
        DELETE FROM shiba_internal.distinct_state
        WHERE result_oid=result_relation AND group_key=p_group_key;
    END IF;
END;
$$;

CREATE FUNCTION shiba._initialize_aggregate_state(result_relation oid)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    view_row shiba_internal.stream_views%ROWTYPE;
    join_row shiba_internal.inner_join_views%ROWTYPE;
    left_name text;
    right_name text;
    group_expression text;
    join_keyword text;
    from_expression text;
    where_expression text;
    count_expression text;
    count_input_expression text;
BEGIN
    SELECT * INTO STRICT view_row
    FROM shiba_internal.stream_views
    WHERE result_oid = result_relation;
    SELECT * INTO join_row
    FROM shiba_internal.inner_join_views
    WHERE result_oid = result_relation;
    SELECT format('%I.%I', n.nspname, c.relname) INTO STRICT left_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=view_row.source_oid;
    DELETE FROM shiba_internal.aggregate_state WHERE result_oid=result_relation;
    DELETE FROM shiba_internal.distinct_state WHERE result_oid=result_relation;
    IF join_row.result_oid IS NULL THEN
        count_expression := CASE WHEN view_row.count_distinct
          THEN format('count(DISTINCT x.%I)',view_row.count_input_column)
          ELSE 'count(*)'
        END;
        EXECUTE format(
            'INSERT INTO shiba_internal.aggregate_state
                 (result_oid, group_key, row_count, count_value, sum_nonnull_count, sum_value)
             SELECT $1, coalesce(to_jsonb(x.%I), ''null''::jsonb),
                    count(*), %s, count(x.%I), coalesce(sum(x.%I),0)::numeric
             FROM %s x
             WHERE shiba._row_passes_filter($1, ''left'', to_jsonb(x))
             GROUP BY x.%I',
            view_row.group_column,
            count_expression,
            view_row.sum_input_column,
            view_row.sum_input_column,
            left_name,
            view_row.group_column
        ) USING result_relation;
        IF view_row.count_distinct THEN
          EXECUTE format(
            'INSERT INTO shiba_internal.distinct_state
                 (result_oid,group_key,value_key,multiplicity)
             SELECT $1,coalesce(to_jsonb(x.%I),''null''::jsonb),
                    to_jsonb(x.%I),count(*)
             FROM %s x
             WHERE x.%I IS NOT NULL
               AND shiba._row_passes_filter($1,''left'',to_jsonb(x))
             GROUP BY x.%I,x.%I',
            view_row.group_column,view_row.count_input_column,left_name,
            view_row.count_input_column,view_row.group_column,view_row.count_input_column
          ) USING result_relation;
        END IF;
    ELSE
        SELECT format('%I.%I', n.nspname, c.relname) INTO STRICT right_name
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.oid=join_row.right_source_oid;
        group_expression := CASE join_row.group_source
          WHEN 'left' THEN format('l.%I',join_row.group_column)
          ELSE format('r.%I',join_row.group_column)
        END;
        join_keyword := CASE join_row.join_type
          WHEN 'inner' THEN 'JOIN'
          WHEN 'left' THEN 'LEFT JOIN'
          WHEN 'right' THEN 'RIGHT JOIN'
          WHEN 'full' THEN 'FULL JOIN'
          ELSE NULL
        END;
        IF join_row.join_type IN ('semi','anti','null_anti') THEN
          from_expression := format('%s l',left_name);
          IF join_row.join_type='null_anti' THEN
            where_expression := format(
              'shiba._row_passes_filter($1,''left'',to_jsonb(l))
               AND l.%I NOT IN (
                 SELECT r.%I FROM %s r
                 WHERE shiba._row_passes_filter($1,''right'',to_jsonb(r))
               )',
              join_row.left_join_column,join_row.right_join_column,right_name
            );
          ELSE
            where_expression := format(
              'shiba._row_passes_filter($1,''left'',to_jsonb(l))
               AND %sEXISTS (
                 SELECT 1 FROM %s r
                 WHERE l.%I=r.%I
                   AND shiba._row_passes_filter($1,''right'',to_jsonb(r))
               )',
              CASE join_row.join_type WHEN 'anti' THEN 'NOT ' ELSE '' END,
              right_name,join_row.left_join_column,join_row.right_join_column
            );
          END IF;
        ELSE
          from_expression := format(
            '%s l %s %s r ON l.%I=r.%I',
            left_name,join_keyword,right_name,
            join_row.left_join_column,join_row.right_join_column
          );
          where_expression :=
            'shiba._row_passes_filter($1,''left'',to_jsonb(l))
             AND shiba._row_passes_filter($1,''right'',to_jsonb(r))
             AND shiba._joined_rows_pass_filters($1,to_jsonb(l),to_jsonb(r))';
        END IF;
        count_input_expression := CASE WHEN view_row.count_distinct THEN
          CASE view_row.count_input_source
            WHEN 'left' THEN format('l.%I',view_row.count_input_column)
            ELSE format('r.%I',view_row.count_input_column)
          END
          ELSE NULL
        END;
        count_expression := CASE WHEN view_row.count_distinct
          THEN format('count(DISTINCT %s)',count_input_expression)
          ELSE 'count(*)'
        END;
        EXECUTE format(
            'INSERT INTO shiba_internal.aggregate_state
                 (result_oid, group_key, row_count, count_value, sum_nonnull_count, sum_value)
             SELECT $1, coalesce(to_jsonb(%s), ''null''::jsonb),
                    count(*), %s, count(l.%I), coalesce(sum(l.%I),0)::numeric
             FROM %s
             WHERE %s
             GROUP BY %s',
            group_expression,
            count_expression,
            view_row.sum_input_column,
            view_row.sum_input_column,
            from_expression,
            where_expression,
            group_expression
        ) USING result_relation;
        IF view_row.count_distinct THEN
          EXECUTE format(
            'INSERT INTO shiba_internal.distinct_state
                 (result_oid,group_key,value_key,multiplicity)
             SELECT $1,coalesce(to_jsonb(%s),''null''::jsonb),
                    to_jsonb(%s),count(*)
             FROM %s
             WHERE %s IS NOT NULL
               AND %s
             GROUP BY %s,%s',
            group_expression,count_input_expression,from_expression,
            count_input_expression,where_expression,
            group_expression,count_input_expression
          ) USING result_relation;
        END IF;
    END IF;
END;
$$;

CREATE FUNCTION shiba._sync_aggregate_sink(
    stream_view shiba_internal.stream_views,
    p_group_key jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    result_name text;
    state_count bigint;
    state_sum_nonnull_count bigint;
    state_sum numeric;
    having_sql text;
    visible boolean;
BEGIN
    SELECT format('%I.%I', n.nspname, c.relname) INTO STRICT result_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.result_oid;
    SELECT count_value,sum_nonnull_count,
           CASE WHEN sum_nonnull_count=0 THEN NULL ELSE sum_value END
    INTO state_count,state_sum_nonnull_count,state_sum
    FROM shiba_internal.aggregate_state
    WHERE result_oid=stream_view.result_oid AND group_key=p_group_key;
    IF NOT FOUND THEN
        visible := false;
    ELSE
        SELECT predicate_sql INTO having_sql
        FROM shiba_internal.stream_having
        WHERE result_oid=stream_view.result_oid;
        IF having_sql IS NULL THEN
            visible := true;
        ELSE
            EXECUTE format(
                'SELECT coalesce((%s),false)
                 FROM shiba_internal.aggregate_state state
                 WHERE result_oid=$1 AND group_key=$2',
                having_sql
            ) USING stream_view.result_oid,p_group_key INTO visible;
        END IF;
    END IF;
    IF NOT visible THEN
        EXECUTE format(
            'DELETE FROM %s WHERE coalesce(to_jsonb(%I),''null''::jsonb)=$1',
            result_name,stream_view.result_group_column
        ) USING p_group_key;
        RETURN;
    END IF;
    EXECUTE format(
        'INSERT INTO %s (%I,%I,%I)
         SELECT (typed.row).%I,$2,$3
         FROM (
           SELECT jsonb_populate_record(
             NULL::%s,jsonb_build_object(%L,$1)
           ) row
         ) typed
         ON CONFLICT (%I) DO UPDATE
         SET %I=EXCLUDED.%I,%I=EXCLUDED.%I',
        result_name,
        stream_view.result_group_column,
        stream_view.count_column,
        stream_view.sum_column,
        stream_view.result_group_column,
        result_name,
        stream_view.result_group_column,
        stream_view.result_group_column,
        stream_view.count_column,
        stream_view.count_column,
        stream_view.sum_column,
        stream_view.sum_column
    ) USING p_group_key,state_count,state_sum;
END;
$$;

CREATE FUNCTION shiba._apply_logged_delta(
    stream_view shiba_internal.stream_views,
    row_data jsonb,
    delta bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    source_name text;
    state_group_key jsonb;
    state_sum_value numeric;
    state_count_input jsonb;
    count_input_expression text;
BEGIN
    SELECT format('%I.%I', source_namespace.nspname, source.relname)
    INTO source_name
    FROM pg_class AS source
    JOIN pg_namespace AS source_namespace ON source_namespace.oid = source.relnamespace
    WHERE source.oid = stream_view.source_oid;

    count_input_expression := CASE WHEN stream_view.count_distinct
      THEN format('to_jsonb((input.row).%I)',stream_view.count_input_column)
      ELSE 'NULL::jsonb'
    END;
    EXECUTE format(
        'SELECT coalesce(to_jsonb((input.row).%I), ''null''::jsonb),
                ((input.row).%I)::numeric,
                %s
         FROM (SELECT jsonb_populate_record(NULL::%s, $1) AS row) input',
        stream_view.group_column,
        stream_view.sum_input_column,
        count_input_expression,
        source_name
    ) USING row_data INTO STRICT state_group_key, state_sum_value,state_count_input;
    PERFORM shiba._apply_aggregate_state(
        stream_view.result_oid, state_group_key, delta, state_sum_value,
        state_count_input
    );
    PERFORM shiba._sync_aggregate_sink(stream_view,state_group_key);
END;
$$;

-- A single-source aggregate commit can be combined by group. DISTINCT also
-- combines by (group,value), but must retain ordered-prefix validation because
-- a net-zero key may still contain an invalid retraction before its insertion.
-- State and sink rows are each touched at most once per affected group.
CREATE FUNCTION shiba._apply_single_source_aggregate_batch(
    stream_view shiba_internal.stream_views,
    events jsonb,
    only_insertions boolean
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    source_name text;
    filter_sql text;
    count_input_sql text;
    affected_groups jsonb[];
    row_count_deltas bigint[];
    count_value_deltas bigint[];
    sum_nonnull_deltas bigint[];
    sum_deltas numeric[];
    row_count_min_prefixes bigint[];
    sum_nonnull_min_prefixes bigint[];
    distinct_groups jsonb[];
    distinct_values jsonb[];
    distinct_new_multiplicities bigint[];
    distinct_state_is_valid boolean;
    state_is_valid boolean;
    affected_group jsonb;
BEGIN
    IF stream_view.view_kind <> 'aggregate' THEN
      RAISE EXCEPTION 'invalid Shiba aggregate batch specialization for result %',
        stream_view.result_oid
        USING ERRCODE='data_corrupted';
    END IF;
    IF EXISTS (
      SELECT 1 FROM shiba_internal.inner_join_views
      WHERE result_oid=stream_view.result_oid
    ) THEN
      RAISE EXCEPTION 'Shiba aggregate batch specialization does not accept JOIN results'
        USING ERRCODE='data_corrupted';
    END IF;

    SELECT format('%I.%I',source_namespace.nspname,source.relname)
    INTO STRICT source_name
    FROM pg_class source
    JOIN pg_namespace source_namespace ON source_namespace.oid=source.relnamespace
    WHERE source.oid=stream_view.source_oid;
    SELECT coalesce((
      SELECT predicate_sql
      FROM shiba_internal.stream_filters
      WHERE result_oid=stream_view.result_oid
        AND input_side='left'
        AND phase='pre'
    ),'true')
    INTO filter_sql;
    count_input_sql := CASE WHEN stream_view.count_distinct
      THEN format('to_jsonb((input.row).%I)',stream_view.count_input_column)
      ELSE 'NULL::jsonb'
    END;

    -- Pure insertion batches cannot violate an ordered non-negative prefix.
    -- Keep this common path free of the window and DISTINCT machinery below.
    IF only_insertions AND NOT stream_view.count_distinct THEN
      EXECUTE format(
        $statement$
        WITH typed_events AS MATERIALIZED (
          SELECT event.delta::bigint AS delta,event.row_data
          FROM jsonb_populate_recordset(
            NULL::shiba_internal.delta_event,$2
          ) event
          WHERE event.source_oid=$3
            AND event.delta=1
            AND jsonb_typeof(event.row_data)='object'
        ),
        contributions AS (
          SELECT coalesce(to_jsonb((input.row).%1$I),'null'::jsonb) AS group_key,
                 sum(event.delta)::bigint AS row_count_delta,
                 sum(
                   CASE WHEN (input.row).%2$I IS NULL
                     THEN 0 ELSE event.delta
                   END
                 )::bigint AS sum_nonnull_delta,
                 sum(
                   event.delta * coalesce(((input.row).%2$I)::numeric,0)
                 )::numeric AS sum_delta
          FROM typed_events event
          CROSS JOIN LATERAL (
            SELECT jsonb_populate_record(NULL::%3$s,event.row_data) AS row
          ) input
          WHERE coalesce((%4$s),false)
          GROUP BY coalesce(to_jsonb((input.row).%1$I),'null'::jsonb)
        )
        SELECT array_agg(group_key ORDER BY group_key::text),
               array_agg(row_count_delta ORDER BY group_key::text),
               array_agg(row_count_delta ORDER BY group_key::text),
               array_agg(sum_nonnull_delta ORDER BY group_key::text),
               array_agg(sum_delta ORDER BY group_key::text),
               array_agg(0::bigint ORDER BY group_key::text),
               array_agg(0::bigint ORDER BY group_key::text),
               NULL::jsonb[],NULL::jsonb[],NULL::bigint[],true
        FROM contributions
        $statement$,
        stream_view.group_column,
        stream_view.sum_input_column,
        source_name,
        filter_sql
      )
      USING stream_view.result_oid,events,stream_view.source_oid
      INTO affected_groups,row_count_deltas,count_value_deltas,
           sum_nonnull_deltas,sum_deltas,row_count_min_prefixes,
           sum_nonnull_min_prefixes,distinct_groups,distinct_values,
           distinct_new_multiplicities,distinct_state_is_valid;
    ELSE
      EXECUTE format(
      $statement$
      WITH typed_events AS MATERIALIZED (
        SELECT raw.ordinality,event.delta::bigint AS delta,event.row_data
        FROM jsonb_array_elements($2) WITH ORDINALITY raw(value,ordinality)
        CROSS JOIN LATERAL jsonb_populate_record(
          NULL::shiba_internal.delta_event,raw.value
        ) event
        WHERE event.source_oid=$3
          AND event.delta IN (-1,1)
          AND jsonb_typeof(event.row_data)='object'
      ),
      decoded_events AS MATERIALIZED (
        SELECT event.ordinality,
               coalesce(to_jsonb((input.row).%1$I),'null'::jsonb) AS group_key,
               %5$s AS value_key,
               event.delta AS row_count_delta,
               CASE WHEN (input.row).%2$I IS NULL
                 THEN 0 ELSE event.delta
               END::bigint AS sum_nonnull_delta,
               event.delta
                 * coalesce(((input.row).%2$I)::numeric,0) AS sum_delta
        FROM typed_events event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(NULL::%3$s,event.row_data) AS row
        ) input
        WHERE coalesce((%4$s),false)
      ),
      group_prefix_rows AS (
        SELECT decoded.*,
               sum(row_count_delta) OVER (
                 PARTITION BY group_key ORDER BY ordinality
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               )::bigint AS row_count_prefix,
               sum(sum_nonnull_delta) OVER (
                 PARTITION BY group_key ORDER BY ordinality
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               )::bigint AS sum_nonnull_prefix
        FROM decoded_events decoded
      ),
      group_contributions AS (
        SELECT group_key,
               sum(row_count_delta)::bigint AS row_count_delta,
               min(row_count_prefix)::bigint AS row_count_min_prefix,
               sum(sum_nonnull_delta)::bigint AS sum_nonnull_delta,
               min(sum_nonnull_prefix)::bigint AS sum_nonnull_min_prefix,
               sum(sum_delta)::numeric AS sum_delta
        FROM group_prefix_rows
        GROUP BY group_key
      ),
      key_prefix_rows AS (
        SELECT group_key,value_key,ordinality,row_count_delta,
               sum(row_count_delta) OVER (
                 PARTITION BY group_key,value_key ORDER BY ordinality
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               )::bigint AS multiplicity_prefix
        FROM decoded_events
        WHERE %6$L::boolean
          AND value_key IS NOT NULL
          AND value_key<>'null'::jsonb
      ),
      key_contributions AS (
        SELECT group_key,value_key,
               sum(row_count_delta)::bigint AS multiplicity_delta,
               min(multiplicity_prefix)::bigint AS multiplicity_min_prefix
        FROM key_prefix_rows
        GROUP BY group_key,value_key
      ),
      key_transitions AS (
        SELECT contribution.group_key,contribution.value_key,
               contribution.multiplicity_delta,
               contribution.multiplicity_min_prefix,
               coalesce(state.multiplicity,0)::bigint AS old_multiplicity,
               (
                 coalesce(state.multiplicity,0)
                 + contribution.multiplicity_delta
               )::bigint AS new_multiplicity,
               CASE
                 WHEN coalesce(state.multiplicity,0)=0
                      AND coalesce(state.multiplicity,0)
                          + contribution.multiplicity_delta>0 THEN 1
                 WHEN coalesce(state.multiplicity,0)>0
                      AND coalesce(state.multiplicity,0)
                          + contribution.multiplicity_delta=0 THEN -1
                 ELSE 0
               END::bigint AS count_value_delta
        FROM key_contributions contribution
        LEFT JOIN shiba_internal.distinct_state state
          ON state.result_oid=$1
         AND state.group_key=contribution.group_key
         AND state.value_key=contribution.value_key
      ),
      key_group_deltas AS (
        SELECT group_key,sum(count_value_delta)::bigint AS count_value_delta
        FROM key_transitions
        GROUP BY group_key
      ),
      contributions AS (
        SELECT grouped.group_key,
               grouped.row_count_delta,
               CASE WHEN %6$L::boolean
                 THEN coalesce(keys.count_value_delta,0)
                 ELSE grouped.row_count_delta
               END::bigint AS count_value_delta,
               grouped.sum_nonnull_delta,
               grouped.sum_delta,
               grouped.row_count_min_prefix,
               grouped.sum_nonnull_min_prefix
        FROM group_contributions grouped
        LEFT JOIN key_group_deltas keys USING (group_key)
      )
      SELECT array_agg(group_key ORDER BY group_key::text),
             array_agg(row_count_delta ORDER BY group_key::text),
             array_agg(count_value_delta ORDER BY group_key::text),
             array_agg(sum_nonnull_delta ORDER BY group_key::text),
             array_agg(sum_delta ORDER BY group_key::text),
             array_agg(row_count_min_prefix ORDER BY group_key::text),
             array_agg(sum_nonnull_min_prefix ORDER BY group_key::text),
             (
               SELECT array_agg(
                 group_key ORDER BY group_key::text,value_key::text
               )
               FROM key_transitions WHERE multiplicity_delta<>0
             ),
             (
               SELECT array_agg(
                 value_key ORDER BY group_key::text,value_key::text
               )
               FROM key_transitions WHERE multiplicity_delta<>0
             ),
             (
               SELECT array_agg(
                 new_multiplicity ORDER BY group_key::text,value_key::text
               )
               FROM key_transitions WHERE multiplicity_delta<>0
             ),
             coalesce((
               SELECT bool_and(
                 old_multiplicity+multiplicity_min_prefix>=0
                 AND new_multiplicity>=0
               )
               FROM key_transitions
             ),true)
      FROM contributions
      WHERE row_count_delta<>0
         OR count_value_delta<>0
         OR sum_nonnull_delta<>0
         OR sum_delta<>0
         OR row_count_min_prefix<0
         OR sum_nonnull_min_prefix<0
         OR EXISTS (
           SELECT 1 FROM key_transitions transition
           WHERE transition.group_key=contributions.group_key
             AND transition.multiplicity_delta<>0
         )
      $statement$,
      stream_view.group_column,
      stream_view.sum_input_column,
      source_name,
      filter_sql,
      count_input_sql,
      stream_view.count_distinct
    )
    USING stream_view.result_oid,events,stream_view.source_oid
    INTO affected_groups,row_count_deltas,count_value_deltas,
         sum_nonnull_deltas,sum_deltas,row_count_min_prefixes,
         sum_nonnull_min_prefixes,distinct_groups,distinct_values,
         distinct_new_multiplicities,distinct_state_is_valid;
    END IF;

    IF NOT distinct_state_is_valid THEN
      RAISE EXCEPTION 'Shiba DISTINCT multiplicity became negative'
        USING ERRCODE='data_corrupted';
    END IF;
    IF affected_groups IS NULL THEN
      RETURN;
    END IF;

    SELECT coalesce(bool_and(
             coalesce(state.row_count,0)+row_count_min_prefixes[slot]>=0
             AND coalesce(state.sum_nonnull_count,0)
                   +sum_nonnull_min_prefixes[slot]>=0
             AND final.row_count>=0
             AND final.count_value>=0
             AND final.sum_nonnull_count>=0
             AND (
               (stream_view.count_distinct
                AND final.count_value<=final.row_count)
               OR
               (NOT stream_view.count_distinct
                AND final.count_value=final.row_count)
             )
             AND final.sum_nonnull_count<=final.row_count
             AND (
               final.row_count<>0
               OR (
                 final.count_value=0
                 AND final.sum_nonnull_count=0
               )
             )
           ),true)
    INTO state_is_valid
    FROM generate_subscripts(affected_groups,1) slot
    LEFT JOIN shiba_internal.aggregate_state state
      ON state.result_oid=stream_view.result_oid
     AND state.group_key=affected_groups[slot]
    CROSS JOIN LATERAL (
      SELECT coalesce(state.row_count,0)+row_count_deltas[slot] AS row_count,
             coalesce(state.count_value,0)+count_value_deltas[slot]
               AS count_value,
             coalesce(state.sum_nonnull_count,0)+sum_nonnull_deltas[slot]
               AS sum_nonnull_count,
             coalesce(state.sum_value,0)+sum_deltas[slot] AS sum_value
    ) final;
    IF NOT state_is_valid THEN
      RAISE EXCEPTION 'Shiba aggregate batch produced invalid state'
        USING ERRCODE='data_corrupted';
    END IF;

    IF distinct_groups IS NOT NULL THEN
      DELETE FROM shiba_internal.distinct_state state
      USING generate_subscripts(distinct_groups,1) slot
      WHERE state.result_oid=stream_view.result_oid
        AND state.group_key=distinct_groups[slot]
        AND state.value_key=distinct_values[slot]
        AND distinct_new_multiplicities[slot]=0;
      UPDATE shiba_internal.distinct_state state
      SET multiplicity=distinct_new_multiplicities[slot]
      FROM generate_subscripts(distinct_groups,1) slot
      WHERE state.result_oid=stream_view.result_oid
        AND state.group_key=distinct_groups[slot]
        AND state.value_key=distinct_values[slot]
        AND distinct_new_multiplicities[slot]>0;
      INSERT INTO shiba_internal.distinct_state
        (result_oid,group_key,value_key,multiplicity)
      SELECT stream_view.result_oid,distinct_groups[slot],
             distinct_values[slot],distinct_new_multiplicities[slot]
      FROM generate_subscripts(distinct_groups,1) slot
      WHERE distinct_new_multiplicities[slot]>0
        AND NOT EXISTS (
          SELECT 1 FROM shiba_internal.distinct_state state
          WHERE state.result_oid=stream_view.result_oid
            AND state.group_key=distinct_groups[slot]
            AND state.value_key=distinct_values[slot]
        );
    END IF;

    UPDATE shiba_internal.aggregate_state state
    SET row_count=state.row_count+row_count_deltas[slot],
        count_value=state.count_value+count_value_deltas[slot],
        sum_nonnull_count=
          state.sum_nonnull_count+sum_nonnull_deltas[slot],
        sum_value=state.sum_value+sum_deltas[slot]
    FROM generate_subscripts(affected_groups,1) slot
    WHERE state.result_oid=stream_view.result_oid
      AND state.group_key=affected_groups[slot];
    INSERT INTO shiba_internal.aggregate_state
      (result_oid,group_key,row_count,count_value,sum_nonnull_count,sum_value)
    SELECT stream_view.result_oid,
           affected_groups[slot],
           row_count_deltas[slot],
           count_value_deltas[slot],
           sum_nonnull_deltas[slot],
           sum_deltas[slot]
    FROM generate_subscripts(affected_groups,1) slot
    WHERE row_count_deltas[slot]>0
      AND NOT EXISTS (
        SELECT 1
        FROM shiba_internal.aggregate_state state
        WHERE state.result_oid=stream_view.result_oid
          AND state.group_key=affected_groups[slot]
      );
    DELETE FROM shiba_internal.aggregate_state
    WHERE result_oid=stream_view.result_oid
      AND group_key=ANY(affected_groups)
      AND row_count=0;
    IF stream_view.count_distinct THEN
      DELETE FROM shiba_internal.distinct_state state
      WHERE state.result_oid=stream_view.result_oid
        AND state.group_key=ANY(affected_groups)
        AND NOT EXISTS (
          SELECT 1 FROM shiba_internal.aggregate_state aggregate
          WHERE aggregate.result_oid=stream_view.result_oid
            AND aggregate.group_key=state.group_key
        );
    END IF;
    FOREACH affected_group IN ARRAY affected_groups LOOP
      PERFORM shiba._sync_aggregate_sink(stream_view,affected_group);
    END LOOP;
END;
$$;
