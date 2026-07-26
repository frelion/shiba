CREATE FUNCTION shiba._register_stream_filter(
    result_relation oid,
    input_side text,
    source_relation oid,
    raw_predicate text,
    expected_alias name DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    compiled jsonb;
    predicate_sql text;
    aliases text[];
BEGIN
    IF input_side NOT IN ('left', 'right') THEN
        RAISE EXCEPTION 'unsupported Shiba filter input'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    compiled := shiba.compile_filter_expression(raw_predicate)::jsonb;
    predicate_sql := compiled ->> 'sql';
    SELECT coalesce(array_agg(value ORDER BY value), ARRAY[]::text[])
    INTO aliases
    FROM jsonb_array_elements_text(compiled -> 'aliases') AS alias(value);
    IF expected_alias IS NULL AND cardinality(aliases) <> 0 THEN
        RAISE EXCEPTION 'single-source Shiba filters use unqualified column names'
            USING ERRCODE = 'feature_not_supported';
    ELSIF expected_alias IS NOT NULL
      AND (cardinality(aliases) > 1 OR (cardinality(aliases) = 1 AND lower(aliases[1]) <> lower(expected_alias::text))) THEN
        RAISE EXCEPTION 'a Shiba JOIN filter must reference only one input alias'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    PERFORM shiba._register_compiled_stream_filter(
        result_relation, input_side, source_relation, predicate_sql
    );
END;
$$;

CREATE FUNCTION shiba._register_compiled_stream_filter(
    result_relation oid,
    input_side text,
    source_relation oid,
    predicate_sql text,
    filter_phase text DEFAULT 'pre'
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    source_name text;
BEGIN
    IF input_side NOT IN ('left', 'right') OR predicate_sql IS NULL
       OR filter_phase NOT IN ('pre','post') THEN
        RAISE EXCEPTION 'invalid compiled Shiba filter'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    SELECT format('%I.%I', n.nspname, c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = source_relation;
    EXECUTE format(
        'SELECT %s FROM (SELECT jsonb_populate_record(NULL::%s, $1) AS row) input',
        predicate_sql, source_name
    ) USING '{}'::jsonb;
    INSERT INTO shiba_internal.stream_filters
        (result_oid, input_side, source_oid, phase, predicate_sql)
    VALUES
        (result_relation, input_side, source_relation, filter_phase, predicate_sql);
END;
$$;

CREATE FUNCTION shiba._row_passes_filter(
    result_relation oid,
    p_input_side text,
    row_data jsonb
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    filter_row shiba_internal.stream_filters%ROWTYPE;
    source_name text;
    passes boolean;
BEGIN
    SELECT * INTO filter_row FROM shiba_internal.stream_filters
    WHERE result_oid = result_relation AND input_side = p_input_side;
    IF NOT FOUND THEN RETURN true; END IF;
    IF filter_row.phase <> 'pre' THEN RETURN true; END IF;
    SELECT format('%I.%I', n.nspname, c.relname)
    INTO source_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = filter_row.source_oid;
    EXECUTE format(
        'SELECT %s FROM (SELECT jsonb_populate_record(NULL::%s, $1) AS row) input',
        filter_row.predicate_sql, source_name
    ) USING row_data INTO passes;
    RETURN COALESCE(passes, false);
END;
$$;

CREATE FUNCTION shiba._joined_rows_pass_filters(
    result_relation oid,
    left_row jsonb,
    right_row jsonb
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    filter_row shiba_internal.stream_filters%ROWTYPE;
    source_name text;
    left_source_name text;
    right_source_name text;
    left_source_oid oid;
    right_source_oid oid;
    joined_predicate text;
    passes boolean;
BEGIN
    FOR filter_row IN
      SELECT * FROM shiba_internal.stream_filters
      WHERE result_oid=result_relation AND phase='post'
    LOOP
      SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid=filter_row.source_oid;
      EXECUTE format(
        'SELECT coalesce((%s),false)
         FROM (SELECT jsonb_populate_record(NULL::%s,$1) row) input',
        filter_row.predicate_sql,source_name
      ) USING CASE filter_row.input_side WHEN 'left' THEN left_row ELSE right_row END
        INTO passes;
      IF NOT passes THEN RETURN false; END IF;
    END LOOP;
    SELECT stream.source_oid,joined.right_source_oid,filter.predicate_sql
    INTO left_source_oid,right_source_oid,joined_predicate
    FROM shiba_internal.stream_views stream
    JOIN shiba_internal.inner_join_views joined USING(result_oid)
    JOIN shiba_internal.stream_join_filters filter USING(result_oid)
    WHERE stream.result_oid=result_relation;
    IF joined_predicate IS NOT NULL THEN
      SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT left_source_name
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid=left_source_oid;
      SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT right_source_name
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid=right_source_oid;
      EXECUTE format(
        'SELECT coalesce((%s),false)
         FROM (SELECT jsonb_populate_record(NULL::%s,$1) row) %I
         CROSS JOIN (SELECT jsonb_populate_record(NULL::%s,$2) row) %I',
        joined_predicate,left_source_name,format('input_%s',left_source_oid),
        right_source_name,format('input_%s',right_source_oid)
      ) USING left_row,right_row INTO passes;
      IF NOT passes THEN RETURN false; END IF;
    END IF;
    RETURN true;
END;
$$;

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

-- Aggregate is commutative within one source transaction when DISTINCT and
-- JOIN are absent. Convert the JSON transport to typed source rows, combine
-- all contributions for each group, update state once per group and then
-- synchronize each affected sink row once. Other physical operators keep the
-- ordered per-delta path below.
CREATE FUNCTION shiba._apply_single_source_aggregate_batch(
    stream_view shiba_internal.stream_views,
    events jsonb
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    source_name text;
    filter_sql text;
    affected_groups jsonb[];
    row_count_deltas bigint[];
    sum_nonnull_deltas bigint[];
    sum_deltas numeric[];
    state_is_valid boolean;
    affected_group jsonb;
BEGIN
    IF stream_view.view_kind <> 'aggregate' OR stream_view.count_distinct THEN
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
             array_agg(sum_nonnull_delta ORDER BY group_key::text),
             array_agg(sum_delta ORDER BY group_key::text)
      FROM contributions
      WHERE row_count_delta<>0
         OR sum_nonnull_delta<>0
         OR sum_delta<>0
      $statement$,
      stream_view.group_column,
      stream_view.sum_input_column,
      source_name,
      filter_sql
    )
    USING stream_view.result_oid,events,stream_view.source_oid
    INTO affected_groups,row_count_deltas,sum_nonnull_deltas,sum_deltas;

    IF affected_groups IS NULL THEN
      RETURN;
    END IF;

    SELECT coalesce(bool_and(
             final.row_count>=0
             AND final.count_value>=0
             AND final.sum_nonnull_count>=0
             AND final.count_value=final.row_count
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
             coalesce(state.count_value,0)+row_count_deltas[slot] AS count_value,
             coalesce(state.sum_nonnull_count,0)+sum_nonnull_deltas[slot]
               AS sum_nonnull_count,
             coalesce(state.sum_value,0)+sum_deltas[slot] AS sum_value
    ) final;
    IF NOT state_is_valid THEN
      RAISE EXCEPTION 'Shiba aggregate batch produced invalid state'
        USING ERRCODE='data_corrupted';
    END IF;

    UPDATE shiba_internal.aggregate_state state
    SET row_count=state.row_count+row_count_deltas[slot],
        count_value=state.count_value+row_count_deltas[slot],
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
           row_count_deltas[slot],
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
    FOREACH affected_group IN ARRAY affected_groups LOOP
      PERFORM shiba._sync_aggregate_sink(stream_view,affected_group);
    END LOOP;
END;
$$;

CREATE FUNCTION shiba._route_wal_delta(
    source_relation oid,
    row_data jsonb,
    delta integer,
    commit_lsn text,
    event_sequence integer
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
BEGIN
    INSERT INTO shiba_internal.dag_inbox (result_oid, commit_lsn, sequence, source_oid, delta, row_data)
    SELECT stream_view.result_oid, commit_lsn::pg_lsn, event_sequence, source_relation, delta, row_data
    FROM shiba_internal.stream_views AS stream_view
    LEFT JOIN shiba_internal.inner_join_views AS join_view
      ON join_view.result_oid = stream_view.result_oid
    WHERE stream_view.activation_lsn < commit_lsn::pg_lsn
      AND (stream_view.source_oid = source_relation OR join_view.right_source_oid = source_relation)
    ON CONFLICT DO NOTHING;
END;
$$;

CREATE FUNCTION shiba._canonicalize_row(source_relation oid, row_data jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    source_name text;
    canonical jsonb;
BEGIN
    SELECT format('%I.%I',n.nspname,c.relname)
    INTO STRICT source_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=source_relation;
    EXECUTE format(
      'SELECT jsonb_object_agg(entry.key,entry.value)
       FROM jsonb_each_text(
         to_jsonb(jsonb_populate_record(NULL::%s,$1))
       ) entry',
      source_name
    ) USING row_data INTO STRICT canonical;
    RETURN canonical;
END;
$$;

-- Apply one ordered source delta without advancing the durable DAG watermark.
-- Callers must hold the result advisory lock and advance view_progress only
-- after every delta in the source transaction has been applied successfully.
CREATE FUNCTION shiba._apply_dag_delta_state(
    result_relation oid,
    source_relation oid,
    row_data jsonb,
    delta integer,
    commit_lsn text
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    stream_view shiba_internal.stream_views%ROWTYPE;
    join_view shiba_internal.inner_join_views%ROWTYPE;
    input_side text;
BEGIN
    SELECT * INTO STRICT stream_view FROM shiba_internal.stream_views WHERE result_oid = result_relation;
    row_data := shiba._canonicalize_row(source_relation,row_data);
    IF stream_view.view_kind='window' THEN
        IF source_relation<>stream_view.source_oid THEN
          RAISE EXCEPTION 'Shiba window DAG inbox source does not belong to result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
        IF shiba._row_passes_filter(result_relation,'left',row_data) THEN
          PERFORM shiba._apply_window_delta(
            stream_view,row_data,delta,commit_lsn
          );
        END IF;
        RETURN;
    END IF;
    IF stream_view.view_kind='distinct' THEN
        IF source_relation<>stream_view.source_oid THEN
          RAISE EXCEPTION 'Shiba DISTINCT DAG inbox source does not belong to result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
        IF shiba._row_passes_filter(result_relation,'left',row_data) THEN
          PERFORM shiba._apply_distinct_delta(
            stream_view,row_data,delta,commit_lsn
          );
        END IF;
        RETURN;
    END IF;
    IF stream_view.view_kind='topn' THEN
        IF source_relation<>stream_view.source_oid THEN
          RAISE EXCEPTION 'Shiba TopN DAG inbox source does not belong to result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
        IF shiba._row_passes_filter(result_relation,'left',row_data) THEN
          PERFORM shiba._apply_topn_delta(
            stream_view,row_data,delta,commit_lsn
          );
        END IF;
        RETURN;
    END IF;
    SELECT * INTO join_view FROM shiba_internal.inner_join_views WHERE result_oid = result_relation;
    IF FOUND THEN
        IF source_relation = stream_view.source_oid THEN
            input_side := 'left';
        ELSIF source_relation = join_view.right_source_oid THEN
            input_side := 'right';
        ELSE
            RAISE EXCEPTION 'Shiba DAG inbox source does not belong to result %', result_relation
                USING ERRCODE = 'data_corrupted';
        END IF;
        IF NOT shiba._row_passes_filter(result_relation, input_side, row_data) THEN
            RETURN;
        END IF;
        PERFORM shiba._apply_inner_join_delta(result_relation, input_side, row_data, delta, commit_lsn);
    ELSE
        IF source_relation <> stream_view.source_oid THEN
            RAISE EXCEPTION 'Shiba DAG inbox source does not belong to result %', result_relation
                USING ERRCODE = 'data_corrupted';
        END IF;
        IF NOT shiba._row_passes_filter(result_relation, 'left', row_data) THEN
            RETURN;
        END IF;
        PERFORM shiba._apply_logged_delta(stream_view, row_data, delta);
    END IF;
END;
$$;

CREATE FUNCTION shiba._advance_dag_progress(
    result_relation oid,
    commit_lsn text
)
RETURNS void
LANGUAGE sql
AS $$
    INSERT INTO shiba_internal.view_progress (result_oid, applied_lsn, updated_at)
    VALUES (result_relation, commit_lsn::pg_lsn, clock_timestamp())
    ON CONFLICT (result_oid) DO UPDATE
    SET applied_lsn = EXCLUDED.applied_lsn,
        updated_at = EXCLUDED.updated_at
$$;

-- Compatibility entry point for a single delta. New workers use the batch
-- entry point below so a source commit crosses SPI and advances progress once.
CREATE FUNCTION shiba._apply_dag_delta(
    result_relation oid,
    source_relation oid,
    row_data jsonb,
    delta integer,
    commit_lsn text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(result_relation::bigint);
    PERFORM shiba._apply_dag_delta_state(
      result_relation,source_relation,row_data,delta,commit_lsn
    );
    PERFORM shiba._advance_dag_progress(result_relation,commit_lsn);
END;
$$;

-- Commit-level bridge to the existing ordered SQL state transitions. This is
-- deliberately not described as a vectorized operator: the loop preserves
-- the WAL order required by UPDATE (-1 followed by +1) and join boundary
-- semantics. It does, however, remove per-row SPI calls, lock acquisition and
-- progress writes, and is the stable boundary for future physical operators.
CREATE FUNCTION shiba._apply_dag_delta_batch(
    result_relation oid,
    events jsonb,
    commit_lsn text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    event record;
    event_count bigint := 0;
    stream_view shiba_internal.stream_views%ROWTYPE;
    use_aggregate_batch boolean := false;
BEGIN
    IF jsonb_typeof(events) IS DISTINCT FROM 'array' THEN
      RAISE EXCEPTION 'Shiba DAG delta batch must be a JSON array'
        USING ERRCODE='invalid_parameter_value';
    END IF;

    PERFORM pg_advisory_xact_lock(result_relation::bigint);
    -- Set-based setup is more expensive for small commits. Check the batch
    -- cardinality before reading metadata so the ordered small-commit path
    -- does not pay any physical-dispatch query overhead.
    IF jsonb_array_length(events)>=64 THEN
      SELECT * INTO STRICT stream_view
      FROM shiba_internal.stream_views
      WHERE result_oid=result_relation;
      use_aggregate_batch :=
        stream_view.view_kind='aggregate'
        AND NOT stream_view.count_distinct
        AND NOT EXISTS (
          SELECT 1 FROM shiba_internal.inner_join_views
          WHERE result_oid=result_relation
        );
    END IF;
    FOR event IN
      SELECT value,ordinality
      FROM jsonb_array_elements(events) WITH ORDINALITY input(value,ordinality)
      ORDER BY ordinality
    LOOP
      IF jsonb_typeof(event.value) IS DISTINCT FROM 'object'
         OR jsonb_typeof(event.value->'row_data') IS DISTINCT FROM 'object'
         OR (event.value->>'delta') IS NULL
         OR (event.value->>'source_oid') IS NULL THEN
        RAISE EXCEPTION 'invalid Shiba DAG event at batch position %',event.ordinality
          USING ERRCODE='invalid_parameter_value';
      END IF;
      IF (event.value->>'delta')::integer NOT IN (-1,1) THEN
        RAISE EXCEPTION 'invalid Shiba DAG differential weight at batch position %',event.ordinality
          USING ERRCODE='invalid_parameter_value';
      END IF;
      IF use_aggregate_batch THEN
        IF (event.value->>'source_oid')::oid<>stream_view.source_oid THEN
          RAISE EXCEPTION 'Shiba DAG inbox source does not belong to result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
      ELSE
        PERFORM shiba._apply_dag_delta_state(
          result_relation,
          (event.value->>'source_oid')::oid,
          event.value->'row_data',
          (event.value->>'delta')::integer,
          commit_lsn
        );
      END IF;
      event_count := event_count+1;
    END LOOP;

    IF event_count=0 THEN
      RAISE EXCEPTION 'Shiba DAG delta batch must not be empty'
        USING ERRCODE='invalid_parameter_value';
    END IF;
    IF use_aggregate_batch THEN
      PERFORM shiba._apply_single_source_aggregate_batch(stream_view,events);
    END IF;
    PERFORM shiba._advance_dag_progress(result_relation,commit_lsn);
END;
$$;

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
    result_relation oid, p_input_side text, input_row jsonb, p_delta bigint, commit_lsn text
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
            view_row,join_row,input_row,'{}'::jsonb,p_delta
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
              match_row.multiplicity * CASE WHEN new_visible THEN 1 ELSE -1 END
            );
          END IF;
        END LOOP;
      END IF;
    ELSIF join_row.join_type IN ('semi','anti') THEN
      IF p_input_side='left' THEN
        IF (join_row.join_type='semi' AND key_value IS NOT NULL AND opposite_total>0)
           OR (join_row.join_type='anti' AND (key_value IS NULL OR opposite_total=0)) THEN
          PERFORM shiba._apply_inner_join_aggregate(
            view_row,join_row,input_row,'{}'::jsonb,p_delta
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
              END
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
                PERFORM shiba._apply_inner_join_aggregate(view_row, join_row, input_row, match_row.row_data, p_delta * match_row.multiplicity);
            ELSE
                PERFORM shiba._apply_inner_join_aggregate(view_row, join_row, match_row.row_data, input_row, p_delta * match_row.multiplicity);
            END IF;
        END LOOP;
      END IF;

    -- A preserved row with no equality match contributes one NULL-extended
    -- joined row per source multiplicity.
      IF preserves_input AND (key_value IS NULL OR opposite_total = 0) THEN
        IF p_input_side = 'left' THEN
            PERFORM shiba._apply_inner_join_aggregate(
                view_row,join_row,input_row,'{}'::jsonb,p_delta
            );
        ELSE
            PERFORM shiba._apply_inner_join_aggregate(
                view_row,join_row,'{}'::jsonb,input_row,p_delta
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
                    CASE WHEN p_delta > 0 THEN -match_row.multiplicity ELSE match_row.multiplicity END
                );
            ELSE
                PERFORM shiba._apply_inner_join_aggregate(
                    view_row,join_row,'{}'::jsonb,match_row.row_data,
                    CASE WHEN p_delta > 0 THEN -match_row.multiplicity ELSE match_row.multiplicity END
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
    left_row jsonb, right_row jsonb, delta bigint
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
    PERFORM shiba._sync_aggregate_sink(view_row,state_group_key);
END; $$;
