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
                        USING ERRCODE='P0S01';
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
                USING ERRCODE='P0S01';
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
            USING ERRCODE='P0S01';
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
    JOIN pg_namespace AS source_namespace
      ON source_namespace.oid = source.relnamespace
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
    ) USING row_data
      INTO STRICT state_group_key,state_sum_value,state_count_input;
    PERFORM shiba._apply_aggregate_state(
      stream_view.result_oid,state_group_key,delta,state_sum_value,
      state_count_input
    );
    PERFORM shiba._sync_aggregate_sink(stream_view,state_group_key);
END;
$$;

CREATE FUNCTION shiba._assert_aggregate_transition(valid boolean)
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NOT coalesce(valid,false) THEN
      RAISE EXCEPTION 'Shiba aggregate batch produced invalid state'
        USING ERRCODE='P0S01';
    END IF;
    RETURN true;
END;
$$;

-- Canonical single-source aggregate executor.  Each statement handles at most
-- stage_chunk_rows input rows or folded keys/groups.  The caller still
-- owns one outer transaction for the complete DAG commit, so state, sink,
-- progress, inbox acknowledgement, and Stage cleanup remain atomic.
--
-- A chunk summary (total,min-prefix) is associative:
--   total(a || b) = total(a) + total(b)
--   min(a || b) = least(min(a), total(a) + min(b))
-- This preserves ordered retraction validation while bounding each window,
-- hash/group, MERGE, and sink statement by a configured chunk.
CREATE FUNCTION shiba._apply_single_source_aggregate_temp_free(
    stream_view shiba_internal.stream_views,
    p_commit_lsn pg_lsn,
    only_insertions boolean
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    source_name text;
    result_name text;
    filter_sql text;
    having_sql text;
    visible_sql text;
    count_input_sql text;
    chunk_rows integer := coalesce(
      nullif(current_setting('shiba.stage_chunk_rows',true),'')::integer,
      2048
    );
    max_stage_rows bigint := coalesce(
      nullif(current_setting('shiba.max_stage_rows',true),'')::bigint,
      1000000
    );
    lower_sequence integer := 0;
    upper_sequence integer;
    folded_rows bigint;
    folded_work bigint := 0;
BEGIN
    IF stream_view.view_kind <> 'aggregate' THEN
      RAISE EXCEPTION
        'invalid Shiba aggregate specialization for result %',
        stream_view.result_oid
        USING ERRCODE='P0S01';
    END IF;
    IF EXISTS (
      SELECT 1
      FROM shiba_internal.inner_join_views
      WHERE result_oid=stream_view.result_oid
    ) THEN
      RAISE EXCEPTION
        'Shiba single-source aggregate specialization does not accept JOIN results'
        USING ERRCODE='P0S01';
    END IF;

    -- Keep the API-compatible hint visible to PL/pgSQL.  Prefix validation is
    -- intentionally retained even for insertion-only callers so correctness
    -- does not depend on a dispatcher hint.
    PERFORM only_insertions;

    SELECT format('%I.%I',n.nspname,c.relname)
    INTO STRICT source_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.source_oid;
    SELECT format('%I.%I',n.nspname,c.relname)
    INTO STRICT result_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.result_oid;
    SELECT coalesce((
      SELECT predicate_sql
      FROM shiba_internal.stream_filters
      WHERE result_oid=stream_view.result_oid
        AND input_side='left'
        AND phase='pre'
    ),'true') INTO filter_sql;
    SELECT predicate_sql
    INTO having_sql
    FROM shiba_internal.stream_having
    WHERE result_oid=stream_view.result_oid;
    visible_sql := CASE
      WHEN having_sql IS NULL THEN 'true'
      ELSE format('coalesce((%s),false)',having_sql)
    END;
    count_input_sql := CASE WHEN stream_view.count_distinct
      THEN format('to_jsonb((input.row).%I)',stream_view.count_input_column)
      ELSE 'NULL::jsonb'
    END;

    IF chunk_rows<1 OR max_stage_rows<1 THEN
      RAISE EXCEPTION 'invalid Shiba Stage resource configuration'
        USING ERRCODE='53400';
    END IF;

    -- A result advisory lock is held by the dispatcher.  Clearing every row
    -- for that result prevents stale/re-entrant Stage data from being mixed
    -- with this replay; an error rolls the cleanup back with the outer apply.
    DELETE FROM shiba_internal.aggregate_distinct_fold_stage
    WHERE result_oid=stream_view.result_oid;
    DELETE FROM shiba_internal.aggregate_group_fold_stage
    WHERE result_oid=stream_view.result_oid;

    LOOP
      SELECT max(chunk.sequence)
      INTO upper_sequence
      FROM (
        SELECT event.sequence
        FROM shiba_internal.effective_change_log event
        WHERE event.commit_lsn=p_commit_lsn
          AND event.source_oid=stream_view.source_oid
          AND event.sequence>lower_sequence
        ORDER BY event.sequence
        LIMIT chunk_rows
      ) chunk;
      EXIT WHEN upper_sequence IS NULL;

      EXECUTE format(
        $fold$
        WITH decoded AS MATERIALIZED (
          SELECT event.sequence AS ordinality,
                 event.delta::bigint AS row_count_delta,
                 coalesce(
                   to_jsonb((input.row).%2$I),'null'::jsonb
                 ) AS group_key,
                 %3$s AS value_key,
                 CASE WHEN (input.row).%4$I IS NULL
                   THEN 0 ELSE event.delta
                 END::bigint AS sum_nonnull_delta,
                 (
                   event.delta
                     * coalesce(((input.row).%4$I)::numeric,0)
                 )::numeric AS sum_delta
          FROM shiba_internal.effective_change_log event
          CROSS JOIN LATERAL (
            SELECT jsonb_populate_record(
              NULL::%1$s,event.row_data
            ) AS row
          ) input
          WHERE event.commit_lsn=$2
            AND event.source_oid=$3
            AND event.sequence>$4
            AND event.sequence<=$5
            AND event.delta IN (-1,1)
            AND jsonb_typeof(event.row_data)='object'
            AND coalesce((%5$s),false)
        ),
        running AS MATERIALIZED (
          SELECT event.*,
                 sum(event.row_count_delta) OVER (
                   PARTITION BY event.group_key
                   ORDER BY event.ordinality
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                 )::bigint AS row_count_prefix,
                 sum(event.sum_nonnull_delta) OVER (
                   PARTITION BY event.group_key
                   ORDER BY event.ordinality
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                 )::bigint AS sum_nonnull_prefix,
                 CASE WHEN $6
                        AND event.value_key IS NOT NULL
                        AND event.value_key<>'null'::jsonb
                   THEN sum(event.row_count_delta) OVER (
                     PARTITION BY event.group_key,event.value_key
                     ORDER BY event.ordinality
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   )::bigint
                   ELSE NULL::bigint
                 END AS multiplicity_prefix
          FROM decoded event
        ),
        group_upsert AS (
          INSERT INTO shiba_internal.aggregate_group_fold_stage (
            result_oid,commit_lsn,group_key,row_count_delta,
            row_count_min_prefix,count_value_delta,sum_nonnull_delta,
            sum_nonnull_min_prefix,sum_delta
          )
          SELECT $1,$2,event.group_key,
                 sum(event.row_count_delta)::bigint,
                 min(event.row_count_prefix)::bigint,
                 CASE WHEN $6 THEN 0
                   ELSE sum(event.row_count_delta)::bigint
                 END,
                 sum(event.sum_nonnull_delta)::bigint,
                 min(event.sum_nonnull_prefix)::bigint,
                 sum(event.sum_delta)::numeric
          FROM running event
          GROUP BY event.group_key
          ON CONFLICT (result_oid,commit_lsn,group_key) DO UPDATE
          SET row_count_min_prefix=least(
                aggregate_group_fold_stage.row_count_min_prefix,
                aggregate_group_fold_stage.row_count_delta
                  + EXCLUDED.row_count_min_prefix
              ),
              row_count_delta=
                aggregate_group_fold_stage.row_count_delta
                  + EXCLUDED.row_count_delta,
              count_value_delta=
                aggregate_group_fold_stage.count_value_delta
                  + EXCLUDED.count_value_delta,
              sum_nonnull_min_prefix=least(
                aggregate_group_fold_stage.sum_nonnull_min_prefix,
                aggregate_group_fold_stage.sum_nonnull_delta
                  + EXCLUDED.sum_nonnull_min_prefix
              ),
              sum_nonnull_delta=
                aggregate_group_fold_stage.sum_nonnull_delta
                  + EXCLUDED.sum_nonnull_delta,
              sum_delta=
                aggregate_group_fold_stage.sum_delta+EXCLUDED.sum_delta
          RETURNING 1
        ),
        key_upsert AS (
          INSERT INTO shiba_internal.aggregate_distinct_fold_stage (
            result_oid,commit_lsn,group_key,value_key,
            multiplicity_delta,multiplicity_min_prefix
          )
          SELECT $1,$2,event.group_key,event.value_key,
                 sum(event.row_count_delta)::bigint,
                 min(event.multiplicity_prefix)::bigint
          FROM running event
          WHERE $6
            AND event.value_key IS NOT NULL
            AND event.value_key<>'null'::jsonb
          GROUP BY event.group_key,event.value_key
          ON CONFLICT (result_oid,commit_lsn,group_key,value_key) DO UPDATE
          SET multiplicity_min_prefix=least(
                aggregate_distinct_fold_stage.multiplicity_min_prefix,
                aggregate_distinct_fold_stage.multiplicity_delta
                  + EXCLUDED.multiplicity_min_prefix
              ),
              multiplicity_delta=
                aggregate_distinct_fold_stage.multiplicity_delta
                  + EXCLUDED.multiplicity_delta
          RETURNING 1
        )
        SELECT (SELECT count(*) FROM group_upsert)
                 +(SELECT count(*) FROM key_upsert)
        $fold$,
        source_name,
        stream_view.group_column,
        count_input_sql,
        stream_view.sum_input_column,
        filter_sql
      ) INTO STRICT folded_rows
        USING stream_view.result_oid,p_commit_lsn,stream_view.source_oid,
              lower_sequence,upper_sequence,stream_view.count_distinct;
      folded_work := folded_work+folded_rows;
      IF folded_work>max_stage_rows THEN
        RAISE EXCEPTION
          'Shiba aggregate commit % for result % exceeded Stage work limit %',
          p_commit_lsn,stream_view.result_oid::regclass,max_stage_rows
          USING ERRCODE='53400',
                HINT='Increase shiba.max_stage_rows or split the source transaction.';
      END IF;
      lower_sequence := upper_sequence;
    END LOOP;

    -- DISTINCT keys are independent once their complete ordered summaries
    -- have been folded.  Apply bounded key sets and accumulate zero-boundary
    -- changes into the corresponding group summary.
    IF stream_view.count_distinct THEN
      LOOP
        EXIT WHEN NOT EXISTS (
          SELECT 1
          FROM shiba_internal.aggregate_distinct_fold_stage
          WHERE result_oid=stream_view.result_oid
            AND commit_lsn=p_commit_lsn
        );
        WITH selected AS MATERIALIZED (
          SELECT stage.group_key,stage.value_key,
                 stage.multiplicity_delta,
                 stage.multiplicity_min_prefix
          FROM shiba_internal.aggregate_distinct_fold_stage stage
          WHERE stage.result_oid=stream_view.result_oid
            AND stage.commit_lsn=p_commit_lsn
          ORDER BY stage.group_key,stage.value_key
          LIMIT chunk_rows
        ),
        transition AS MATERIALIZED (
          SELECT selected.*,
                 coalesce(state.multiplicity,0)::bigint
                   AS old_multiplicity,
                 (
                   coalesce(state.multiplicity,0)
                     + selected.multiplicity_delta
                 )::bigint AS new_multiplicity,
                 shiba._assert_aggregate_transition(
                   coalesce(state.multiplicity,0)
                     + selected.multiplicity_min_prefix>=0
                   AND coalesce(state.multiplicity,0)
                     + selected.multiplicity_delta>=0
                 ) AS valid
          FROM selected
          LEFT JOIN shiba_internal.distinct_state state
            ON state.result_oid=stream_view.result_oid
           AND state.group_key=selected.group_key
           AND state.value_key=selected.value_key
        ),
        distinct_merged AS (
          MERGE INTO shiba_internal.distinct_state AS state
          USING (
            SELECT stream_view.result_oid AS result_oid,
                   transition.group_key,transition.value_key,
                   transition.new_multiplicity
            FROM transition
            WHERE transition.valid
          ) AS next
          ON state.result_oid=next.result_oid
         AND state.group_key=next.group_key
         AND state.value_key=next.value_key
          WHEN MATCHED AND next.new_multiplicity=0 THEN DELETE
          WHEN MATCHED THEN UPDATE
            SET multiplicity=next.new_multiplicity
          WHEN NOT MATCHED AND next.new_multiplicity>0 THEN
            INSERT (result_oid,group_key,value_key,multiplicity)
            VALUES (
              next.result_oid,next.group_key,next.value_key,
              next.new_multiplicity
            )
        ),
        processed AS (
          DELETE FROM shiba_internal.aggregate_distinct_fold_stage stage
          USING selected
          WHERE stage.result_oid=stream_view.result_oid
            AND stage.commit_lsn=p_commit_lsn
            AND stage.group_key=selected.group_key
            AND stage.value_key=selected.value_key
        ),
        group_delta AS MATERIALIZED (
          SELECT transition.group_key,
                 sum(
                   CASE
                     WHEN transition.old_multiplicity=0
                      AND transition.new_multiplicity>0 THEN 1
                     WHEN transition.old_multiplicity>0
                      AND transition.new_multiplicity=0 THEN -1
                     ELSE 0
                   END
                 )::bigint AS count_value_delta
          FROM transition
          WHERE transition.valid
          GROUP BY transition.group_key
        )
        UPDATE shiba_internal.aggregate_group_fold_stage stage
        SET count_value_delta=
              stage.count_value_delta+group_delta.count_value_delta
        FROM group_delta
        WHERE stage.result_oid=stream_view.result_oid
          AND stage.commit_lsn=p_commit_lsn
          AND stage.group_key=group_delta.group_key;
      END LOOP;
    END IF;

    -- Apply bounded group sets.  Sink values are derived from transition
    -- because writes made by aggregate_merged are hidden by this statement's
    -- snapshot.
    LOOP
      EXIT WHEN NOT EXISTS (
        SELECT 1
        FROM shiba_internal.aggregate_group_fold_stage
        WHERE result_oid=stream_view.result_oid
          AND commit_lsn=p_commit_lsn
      );
      EXECUTE format(
        $apply$
        WITH selected AS MATERIALIZED (
          SELECT stage.*
          FROM shiba_internal.aggregate_group_fold_stage stage
          WHERE stage.result_oid=$1
            AND stage.commit_lsn=$2
          ORDER BY stage.group_key
          LIMIT $4
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
          FROM selected contribution
          LEFT JOIN shiba_internal.aggregate_state state
            ON state.result_oid=$1
           AND state.group_key=contribution.group_key
          WHERE shiba._assert_aggregate_transition(
            coalesce(state.row_count,0)
              + contribution.row_count_min_prefix>=0
            AND coalesce(state.sum_nonnull_count,0)
              + contribution.sum_nonnull_min_prefix>=0
            AND coalesce(state.row_count,0)
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
              OR
              (
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
        aggregate_merged AS (
          MERGE INTO shiba_internal.aggregate_state AS state
          USING (
            SELECT $1::oid AS result_oid,transition.*
            FROM transition
          ) AS next
          ON state.result_oid=next.result_oid
         AND state.group_key=next.group_key
          WHEN MATCHED AND next.row_count=0 THEN DELETE
          WHEN MATCHED THEN UPDATE
            SET row_count=next.row_count,
                count_value=next.count_value,
                sum_nonnull_count=next.sum_nonnull_count,
                sum_value=next.sum_value
          WHEN NOT MATCHED AND next.row_count>0 THEN
            INSERT (
              result_oid,group_key,row_count,count_value,
              sum_nonnull_count,sum_value
            )
            VALUES (
              next.result_oid,next.group_key,next.row_count,
              next.count_value,next.sum_nonnull_count,next.sum_value
            )
        ),
        visibility AS MATERIALIZED (
          SELECT (typed.row).%2$I AS group_value,
                 state.count_value,
                 CASE WHEN state.sum_nonnull_count=0
                   THEN NULL ELSE state.sum_value
                 END AS sum_value,
                 (state.row_count<>0 AND %3$s) AS visible
          FROM transition state
          CROSS JOIN LATERAL (
            SELECT jsonb_populate_record(
              NULL::%1$s,
              jsonb_build_object(%6$L,state.group_key)
            ) row
          ) typed
        ),
        sink_deleted AS (
          DELETE FROM %1$s result
          USING visibility
          WHERE result.%2$I IS NOT DISTINCT FROM visibility.group_value
            AND NOT visibility.visible
        ),
        stage_deleted AS (
          DELETE FROM shiba_internal.aggregate_group_fold_stage stage
          USING selected
          WHERE stage.result_oid=$1
            AND stage.commit_lsn=$2
            AND stage.group_key=selected.group_key
        )
        INSERT INTO %1$s (%2$I,%4$I,%5$I)
        SELECT visibility.group_value,visibility.count_value,
               visibility.sum_value
        FROM visibility
        WHERE visibility.visible
        ON CONFLICT (%2$I) DO UPDATE
        SET %4$I=EXCLUDED.%4$I,%5$I=EXCLUDED.%5$I
        $apply$,
        result_name,
        stream_view.result_group_column,
        visible_sql,
        stream_view.count_column,
        stream_view.sum_column,
        stream_view.result_group_column
      ) USING stream_view.result_oid,p_commit_lsn,
              stream_view.count_distinct,chunk_rows;
    END LOOP;

    DELETE FROM shiba_internal.aggregate_distinct_fold_stage
    WHERE result_oid=stream_view.result_oid
      AND commit_lsn=p_commit_lsn;
    DELETE FROM shiba_internal.aggregate_group_fold_stage
    WHERE result_oid=stream_view.result_oid
      AND commit_lsn=p_commit_lsn;
END;
$$;

-- Dispatcher contract:
--   * existing callers may continue to call either function below;
--   * new dispatchers should call _apply_single_source_aggregate_temp_free
--     directly for every non-JOIN aggregate source commit;
--   * only_insertions is a validated optimization hint, not a semantic mode.
CREATE OR REPLACE FUNCTION shiba._apply_single_source_aggregate_batch(
    stream_view shiba_internal.stream_views,
    p_commit_lsn pg_lsn,
    only_insertions boolean
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
BEGIN
    PERFORM shiba._apply_single_source_aggregate_temp_free(
      stream_view,p_commit_lsn,only_insertions
    );
END;
$$;

CREATE OR REPLACE FUNCTION shiba._apply_single_source_aggregate_inline_fast(
    stream_view shiba_internal.stream_views,
    p_commit_lsn pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
BEGIN
    PERFORM shiba._apply_single_source_aggregate_temp_free(
      stream_view,p_commit_lsn,false
    );
END;
$$;
