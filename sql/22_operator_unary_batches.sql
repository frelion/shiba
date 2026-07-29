-- Raise from inside a MATERIALIZED transition relation before any unary
-- operator state or sink change can be derived from an invalid prefix.
CREATE FUNCTION shiba._assert_unary_batch_transition(
    p_valid boolean,
    p_operator text
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog
AS $$
BEGIN
    IF NOT coalesce(p_valid,false) THEN
      RAISE EXCEPTION 'Shiba % batch produced negative multiplicity',p_operator
        USING ERRCODE='P0S01';
    END IF;
    RETURN true;
END;
$$;

-- DISTINCT is a threshold over projected keys.  Fold the ordered source
-- transaction in fixed-size statements, composing each key's (sum,min-prefix)
-- summary in an UNLOGGED Stage. Apply that Stage in bounded key batches while
-- the caller's one outer transaction preserves source-commit atomicity.
CREATE FUNCTION shiba._apply_distinct_batch(
    stream_view shiba_internal.stream_views,
    p_commit_lsn pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    distinct_view shiba_internal.distinct_views%ROWTYPE;
    source_name text;
    result_name text;
    filter_sql text;
    key_arguments text;
    sink_key_predicate text;
    chunk_rows integer := coalesce(
      nullif(current_setting('shiba.stage_chunk_rows',true),'')::integer,
      2048
    );
    max_stage_rows bigint := coalesce(
      nullif(current_setting('shiba.max_stage_rows',true),'')::bigint,
      1000000
    );
    cursor_sequence integer := 0;
    next_sequence integer;
    input_rows bigint;
    folded_rows bigint;
    folded_work bigint := 0;
    applied_rows bigint;
    apply_cursor jsonb;
    next_apply_cursor jsonb;
    mutation_rows bigint;
BEGIN
    IF stream_view.view_kind<>'distinct' THEN
      RAISE EXCEPTION 'invalid Shiba DISTINCT batch specialization for result %',
        stream_view.result_oid
        USING ERRCODE='P0S01';
    END IF;
    SELECT * INTO STRICT distinct_view
    FROM shiba_internal.distinct_views
    WHERE result_oid=stream_view.result_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.source_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT result_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.result_oid;
    SELECT coalesce((
      SELECT predicate_sql
      FROM shiba_internal.stream_filters
      WHERE result_oid=stream_view.result_oid
        AND input_side='left'
        AND phase='pre'
    ),'true') INTO filter_sql;
    SELECT string_agg(
      format('%L,to_jsonb((event.row).%I)',output_column,source_column),
      ',' ORDER BY ordinal
    ) INTO STRICT key_arguments
    FROM unnest(distinct_view.source_columns,distinct_view.output_columns)
      WITH ORDINALITY columns(source_column,output_column,ordinal);
    SELECT string_agg(
      format(
        'target.%1$I IS NOT DISTINCT FROM (typed.row).%1$I',
        output_column
      ),
      ' AND ' ORDER BY ordinal
    ) INTO STRICT sink_key_predicate
    FROM unnest(distinct_view.output_columns)
      WITH ORDINALITY columns(output_column,ordinal);

    IF chunk_rows<1 OR max_stage_rows<1 THEN
      RAISE EXCEPTION 'invalid Shiba Stage resource configuration'
        USING ERRCODE='53400';
    END IF;

    DELETE FROM shiba_internal.distinct_fold_stage
    WHERE result_oid=stream_view.result_oid
      AND commit_lsn=p_commit_lsn;

    LOOP
      EXECUTE format(
        $fold_statement$
        WITH raw_chunk AS MATERIALIZED (
          SELECT event.sequence AS ordinality,event.delta,event.row_data
          FROM shiba_internal.all_change_log event
          WHERE event.commit_lsn=$2
            AND event.source_oid=$3
            AND event.sequence>$4
          ORDER BY event.sequence
          LIMIT $5
        ),
        typed_events AS MATERIALIZED (
          SELECT event.ordinality,
                 event.delta::bigint AS delta,input.row
          FROM raw_chunk event
          CROSS JOIN LATERAL (
            SELECT jsonb_populate_record(NULL::%1$s,event.row_data) AS row
          ) input
          WHERE coalesce((%2$s),false)
        ),
        keyed_events AS (
          SELECT event.ordinality,event.delta,projected.row_key
          FROM typed_events event
          CROSS JOIN LATERAL (
            SELECT jsonb_build_object(%3$s) AS row_key
          ) projected
        ),
        running AS (
          SELECT row_key,delta,
                 sum(delta) OVER (
                   PARTITION BY row_key ORDER BY ordinality
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                 )::bigint AS prefix
          FROM keyed_events
        ),
        contributions AS MATERIALIZED (
          SELECT row_key,sum(delta)::bigint AS multiplicity_delta,
                 min(prefix)::bigint AS minimum_prefix
          FROM running
          GROUP BY row_key
        ),
        stage_merged AS (
          INSERT INTO shiba_internal.distinct_fold_stage
            (
              result_oid,commit_lsn,row_key,multiplicity_delta,
              minimum_prefix
            )
          SELECT $1,$2,row_key,multiplicity_delta,minimum_prefix
          FROM contributions
          ON CONFLICT (result_oid,commit_lsn,row_key) DO UPDATE
          SET minimum_prefix=least(
                shiba_internal.distinct_fold_stage.minimum_prefix,
                shiba_internal.distinct_fold_stage.multiplicity_delta
                  + EXCLUDED.minimum_prefix
              ),
              multiplicity_delta=
                shiba_internal.distinct_fold_stage.multiplicity_delta
                  + EXCLUDED.multiplicity_delta
          RETURNING 1
        )
        SELECT
          coalesce((SELECT max(ordinality) FROM raw_chunk),$4),
          (SELECT count(*) FROM raw_chunk),
          (SELECT count(*) FROM stage_merged)
        $fold_statement$,
        source_name,filter_sql,key_arguments
      )
      INTO STRICT next_sequence,input_rows,folded_rows
      USING stream_view.result_oid,p_commit_lsn,stream_view.source_oid,
            cursor_sequence,chunk_rows;

      EXIT WHEN input_rows=0;
      cursor_sequence := next_sequence;
      folded_work := folded_work+folded_rows;
      IF folded_work>max_stage_rows THEN
        RAISE EXCEPTION
          'Shiba DISTINCT commit % for result % exceeded Stage work limit %',
          p_commit_lsn,stream_view.result_oid::regclass,max_stage_rows
          USING ERRCODE='53400',
                HINT='Increase shiba.max_stage_rows or split the source transaction.';
      END IF;
    END LOOP;

    LOOP
      EXECUTE format(
        $apply_statement$
        WITH batch AS MATERIALIZED (
          SELECT row_key,multiplicity_delta,minimum_prefix
          FROM shiba_internal.distinct_fold_stage
          WHERE result_oid=$1
            AND commit_lsn=$2
            AND ($4::jsonb IS NULL OR row_key>$4::jsonb)
          ORDER BY row_key
          LIMIT $3
        ),
        transitions AS MATERIALIZED (
          SELECT contribution.row_key,contribution.multiplicity_delta,
                 contribution.minimum_prefix,
                 coalesce(state.multiplicity,0)::bigint AS old_multiplicity,
                 (
                   coalesce(state.multiplicity,0)
                     + contribution.multiplicity_delta
                 )::bigint AS new_multiplicity
          FROM batch contribution
          LEFT JOIN shiba_internal.projection_state state
            ON state.result_oid=$1
           AND state.row_key=contribution.row_key
        ),
        validated AS MATERIALIZED (
          SELECT transition.*
          FROM transitions transition
          WHERE shiba._assert_unary_batch_transition(
            transition.old_multiplicity+transition.minimum_prefix>=0
              AND transition.new_multiplicity>=0,
            'DISTINCT'
          )
        ),
        state_upserts AS (
          INSERT INTO shiba_internal.projection_state
            (result_oid,row_key,multiplicity)
          SELECT $1,batch.row_key,batch.new_multiplicity
          FROM validated batch
          WHERE batch.multiplicity_delta<>0
            AND batch.new_multiplicity>0
          ON CONFLICT(result_oid,row_key) DO UPDATE
          SET multiplicity=EXCLUDED.multiplicity
          RETURNING row_key
        ),
        state_deletes AS (
          DELETE FROM shiba_internal.projection_state state
          USING validated batch
          WHERE state.result_oid=$1
            AND state.row_key=batch.row_key
            AND batch.multiplicity_delta<>0
            AND batch.new_multiplicity=0
          RETURNING state.row_key
        ),
        sink_deletes AS (
          DELETE FROM %1$s target
          USING validated batch
          CROSS JOIN LATERAL (
            SELECT jsonb_populate_record(
              NULL::%1$s,batch.row_key
            ) AS row
          ) typed
          WHERE %2$s
            AND batch.old_multiplicity>0
            AND batch.new_multiplicity=0
          RETURNING 1
        ),
        sink_inserts AS (
          INSERT INTO %1$s
          SELECT (jsonb_populate_record(NULL::%1$s,batch.row_key)).*
          FROM validated batch
          WHERE batch.old_multiplicity=0
            AND batch.new_multiplicity>0
          RETURNING 1
        ),
        mutations AS MATERIALIZED (
          SELECT
            (SELECT count(*) FROM state_upserts)
            +(SELECT count(*) FROM state_deletes)
            +(SELECT count(*) FROM sink_deletes)
            +(SELECT count(*) FROM sink_inserts) AS mutation_count
        )
        SELECT
          (SELECT count(*) FROM batch),
          (
            SELECT row_key
            FROM batch
            ORDER BY row_key DESC
            LIMIT 1
          ),
          (SELECT mutation_count FROM mutations)
        $apply_statement$,
        result_name,sink_key_predicate
      )
      INTO STRICT applied_rows,next_apply_cursor,mutation_rows
      USING stream_view.result_oid,p_commit_lsn,chunk_rows,apply_cursor;
      EXIT WHEN applied_rows=0;
      apply_cursor := next_apply_cursor;
    END LOOP;

    DELETE FROM shiba_internal.distinct_fold_stage
    WHERE result_oid=stream_view.result_oid
      AND commit_lsn=p_commit_lsn;
END;
$$;

-- TopN keeps a full multiset. The final state is derived from the statement
-- snapshot plus this commit's transition relation, so rebuilding the bounded
-- sink does not require reading state writes made earlier in the statement.
CREATE FUNCTION shiba._apply_topn_batch(
    stream_view shiba_internal.stream_views,
    p_commit_lsn pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    topn_view shiba_internal.topn_views%ROWTYPE;
    source_name text;
    result_name text;
    filter_sql text;
    quoted_outputs text;
    expressions text;
    state_row_upper_bound bigint;
    max_stage_rows bigint := coalesce(
      nullif(current_setting('shiba.max_stage_rows',true),'')::bigint,
      1000000
    );
BEGIN
    IF stream_view.view_kind<>'topn' THEN
      RAISE EXCEPTION 'invalid Shiba TopN batch specialization for result %',
        stream_view.result_oid
        USING ERRCODE='P0S01';
    END IF;
    SELECT * INTO STRICT topn_view
    FROM shiba_internal.topn_views
    WHERE result_oid=stream_view.result_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.source_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT result_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.result_oid;
    SELECT coalesce((
      SELECT predicate_sql
      FROM shiba_internal.stream_filters
      WHERE result_oid=stream_view.result_oid
        AND input_side='left'
        AND phase='pre'
    ),'true') INTO filter_sql;
    SELECT string_agg(format('%I',output_column),',' ORDER BY ordinal),
           string_agg(format('input.%I',source_column),',' ORDER BY ordinal)
    INTO STRICT quoted_outputs,expressions
    FROM unnest(topn_view.source_columns,topn_view.output_columns)
      WITH ORDINALITY columns(source_column,output_column,ordinal);

    EXECUTE format(
      $bound$
      SELECT
        (
          SELECT count(*)
          FROM shiba_internal.topn_rows state
          WHERE state.result_oid=$1
        )
        +(
          SELECT count(*)
          FROM shiba_internal.all_change_log event
          CROSS JOIN LATERAL (
            SELECT jsonb_populate_record(NULL::%1$s,event.row_data) AS row
          ) input
          WHERE event.commit_lsn=$2
            AND event.source_oid=$3
            AND event.delta>0
            AND coalesce((%2$s),false)
        )
      $bound$,
      source_name,filter_sql
    )
    INTO STRICT state_row_upper_bound
    USING stream_view.result_oid,p_commit_lsn,stream_view.source_oid;
    IF state_row_upper_bound>max_stage_rows
       OR topn_view.limit_offset::numeric+topn_view.limit_count::numeric
            > max_stage_rows::numeric THEN
      RAISE EXCEPTION
        'Shiba TopN commit % for result % exceeds ranked-state limit %',
        p_commit_lsn,stream_view.result_oid::regclass,max_stage_rows
        USING ERRCODE='53400',
              HINT='Increase shiba.max_stage_rows, reduce OFFSET/LIMIT, or split the source transaction.';
    END IF;

    EXECUTE format(
      $statement$
      WITH typed_events AS MATERIALIZED (
        SELECT event.sequence AS ordinality,
               event.delta::bigint AS delta,input.row
        FROM shiba_internal.all_change_log event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(NULL::%1$s,event.row_data) AS row
        ) input
        WHERE event.commit_lsn=$2
          AND event.source_oid=$3
          AND coalesce((%2$s),false)
      ),
      canonical_events AS (
        SELECT event.ordinality,event.delta,canonical.row_data
        FROM typed_events event
        CROSS JOIN LATERAL (
          SELECT jsonb_object_agg(entry.key,entry.value) AS row_data
          FROM jsonb_each_text(to_jsonb(event.row)) entry
        ) canonical
      ),
      running AS (
        SELECT row_data,delta,
               sum(delta) OVER (
                 PARTITION BY row_data ORDER BY ordinality
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               )::bigint AS prefix
        FROM canonical_events
      ),
      contributions AS (
        SELECT row_data,sum(delta)::bigint AS multiplicity_delta,
               min(prefix)::bigint AS minimum_prefix
        FROM running
        GROUP BY row_data
      ),
      transitions AS MATERIALIZED (
        SELECT contribution.row_data,contribution.multiplicity_delta,
               contribution.minimum_prefix,
               coalesce(state.multiplicity,0)::bigint AS old_multiplicity,
               (
                 coalesce(state.multiplicity,0)
                   + contribution.multiplicity_delta
               )::bigint AS new_multiplicity
        FROM contributions contribution
        LEFT JOIN shiba_internal.topn_rows state
          ON state.result_oid=$1
         AND state.row_data=contribution.row_data
      ),
      validated AS MATERIALIZED (
        SELECT transition.*
        FROM transitions transition
        WHERE shiba._assert_unary_batch_transition(
          transition.old_multiplicity+transition.minimum_prefix>=0
            AND transition.new_multiplicity>=0,
          'TopN'
        )
      ),
      changed AS MATERIALIZED (
        SELECT coalesce(bool_or(multiplicity_delta<>0),false) AS value
        FROM validated
      ),
      next_state AS NOT MATERIALIZED (
        SELECT state.row_data,state.multiplicity
        FROM shiba_internal.topn_rows state
        WHERE state.result_oid=$1
          AND NOT EXISTS (
            SELECT 1
            FROM validated batch
            WHERE batch.row_data=state.row_data
              AND batch.multiplicity_delta<>0
          )
        UNION ALL
        SELECT batch.row_data,batch.new_multiplicity
        FROM validated batch
        WHERE batch.multiplicity_delta<>0
          AND batch.new_multiplicity>0
      ),
      state_upserts AS (
        INSERT INTO shiba_internal.topn_rows
          (result_oid,row_data,multiplicity)
        SELECT $1,batch.row_data,batch.new_multiplicity
        FROM validated batch
        WHERE batch.multiplicity_delta<>0
          AND batch.new_multiplicity>0
        ON CONFLICT(result_oid,row_data) DO UPDATE
        SET multiplicity=EXCLUDED.multiplicity
        RETURNING row_data
      ),
      state_deletes AS (
        DELETE FROM shiba_internal.topn_rows state
        USING validated batch
        WHERE state.result_oid=$1
          AND state.row_data=batch.row_data
          AND batch.multiplicity_delta<>0
          AND batch.new_multiplicity=0
        RETURNING state.row_data
      ),
      sink_deletes AS (
        DELETE FROM %3$s
        WHERE (SELECT value FROM changed)
        RETURNING 1
      ),
      mutations AS MATERIALIZED (
        SELECT
          (SELECT count(*) FROM state_upserts)
          +(SELECT count(*) FROM state_deletes)
          +(SELECT count(*) FROM sink_deletes) AS mutation_count
      ),
      ranked_state AS MATERIALIZED (
        SELECT state.row_data,state.multiplicity,
               sum(state.multiplicity) OVER (
                 ORDER BY (typed.row).%6$I %7$s NULLS %8$s,
                          state.row_data::text
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               )::bigint AS end_position
        FROM next_state state
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(
            NULL::%1$s,state.row_data
          ) AS row
        ) typed
      ),
      selected_state AS (
        SELECT state.*,
               greatest(
                 state.end_position-state.multiplicity+1,
                 %9$s::bigint+1
               )::bigint AS first_position,
               least(
                 state.end_position,
                 %9$s::bigint+%10$s::bigint
               )::bigint AS last_position
        FROM ranked_state state
        WHERE state.end_position>%9$s::bigint
          AND state.end_position-state.multiplicity
                < %9$s::bigint+%10$s::bigint
      )
      INSERT INTO %3$s (%4$s)
      SELECT %5$s
      FROM selected_state state
      CROSS JOIN LATERAL
        jsonb_populate_record(NULL::%1$s,state.row_data) input
      CROSS JOIN LATERAL generate_series(
        state.first_position,state.last_position
      ) copy(position)
      CROSS JOIN mutations
      WHERE (SELECT value FROM changed)
      ORDER BY copy.position
      $statement$,
      source_name,filter_sql,result_name,quoted_outputs,expressions,
      topn_view.order_column,upper(topn_view.order_direction),
      CASE topn_view.nulls_first WHEN true THEN 'FIRST' ELSE 'LAST' END,
      topn_view.limit_offset,topn_view.limit_count
    )
    USING stream_view.result_oid,p_commit_lsn,stream_view.source_oid;
END;
$$;

-- Window state is grouped by the full canonical row and partition. All
-- affected partitions are rebuilt together from the snapshot plus this
-- commit's validated transition relation.
CREATE FUNCTION shiba._apply_window_batch(
    stream_view shiba_internal.stream_views,
    p_commit_lsn pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    window_view shiba_internal.window_views%ROWTYPE;
    source_name text;
    result_name text;
    filter_sql text;
    quoted_outputs text;
    expressions text;
    affected_row_upper_bound bigint;
    max_stage_rows bigint := coalesce(
      nullif(current_setting('shiba.max_stage_rows',true),'')::bigint,
      1000000
    );
BEGIN
    IF stream_view.view_kind<>'window' THEN
      RAISE EXCEPTION 'invalid Shiba window batch specialization for result %',
        stream_view.result_oid
        USING ERRCODE='P0S01';
    END IF;
    SELECT * INTO STRICT window_view
    FROM shiba_internal.window_views
    WHERE result_oid=stream_view.result_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.source_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT result_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=stream_view.result_oid;
    SELECT coalesce((
      SELECT predicate_sql
      FROM shiba_internal.stream_filters
      WHERE result_oid=stream_view.result_oid
        AND input_side='left'
        AND phase='pre'
    ),'true') INTO filter_sql;
    SELECT string_agg(format('%I',column_name),',' ORDER BY ordinal)
    INTO STRICT quoted_outputs
    FROM unnest(window_view.output_columns)
      WITH ORDINALITY output(column_name,ordinal);
    SELECT string_agg(expression,',' ORDER BY ordinal)
    INTO STRICT expressions
    FROM unnest(window_view.target_expressions)
      WITH ORDINALITY target(expression,ordinal);

    EXECUTE format(
      $bound$
      WITH typed_events AS MATERIALIZED (
        SELECT event.delta::bigint AS delta,
               coalesce(
                 to_jsonb((input.row).%3$I),'null'::jsonb
               ) AS partition_key
        FROM shiba_internal.all_change_log event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(NULL::%1$s,event.row_data) AS row
        ) input
        WHERE event.commit_lsn=$2
          AND event.source_oid=$3
          AND coalesce((%2$s),false)
      ),
      changed_partitions AS MATERIALIZED (
        SELECT DISTINCT partition_key FROM typed_events
      )
      SELECT
        coalesce((
          SELECT sum(state.multiplicity)
          FROM shiba_internal.window_rows state
          JOIN changed_partitions changed USING(partition_key)
          WHERE state.result_oid=$1
        ),0)
        +coalesce((
          SELECT sum(greatest(delta,0)) FROM typed_events
        ),0)
      $bound$,
      source_name,filter_sql,window_view.partition_column
    )
    INTO STRICT affected_row_upper_bound
    USING stream_view.result_oid,p_commit_lsn,stream_view.source_oid;
    IF affected_row_upper_bound>max_stage_rows THEN
      RAISE EXCEPTION
        'Shiba window commit % for result % may rebuild % rows, limit %',
        p_commit_lsn,stream_view.result_oid::regclass,
        affected_row_upper_bound,max_stage_rows
        USING ERRCODE='53400',
              HINT='Increase shiba.max_stage_rows or split the affected partition.';
    END IF;

    EXECUTE format(
      $statement$
      WITH typed_events AS MATERIALIZED (
        SELECT event.sequence AS ordinality,
               event.delta::bigint AS delta,input.row
        FROM shiba_internal.all_change_log event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(NULL::%1$s,event.row_data) AS row
        ) input
        WHERE event.commit_lsn=$2
          AND event.source_oid=$3
          AND coalesce((%2$s),false)
      ),
      canonical_events AS (
        SELECT event.ordinality,event.delta,
               coalesce(to_jsonb((event.row).%3$I),'null'::jsonb)
                 AS partition_key,
               canonical.row_data
        FROM typed_events event
        CROSS JOIN LATERAL (
          SELECT jsonb_object_agg(entry.key,entry.value) AS row_data
          FROM jsonb_each_text(to_jsonb(event.row)) entry
        ) canonical
      ),
      running AS (
        SELECT partition_key,row_data,delta,
               sum(delta) OVER (
                 PARTITION BY partition_key,row_data ORDER BY ordinality
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               )::bigint AS prefix
        FROM canonical_events
      ),
      contributions AS (
        SELECT partition_key,row_data,
               sum(delta)::bigint AS multiplicity_delta,
               min(prefix)::bigint AS minimum_prefix
        FROM running
        GROUP BY partition_key,row_data
      ),
      transitions AS MATERIALIZED (
        SELECT contribution.partition_key,contribution.row_data,
               contribution.multiplicity_delta,
               contribution.minimum_prefix,
               coalesce(state.multiplicity,0)::bigint AS old_multiplicity,
               (
                 coalesce(state.multiplicity,0)
                   + contribution.multiplicity_delta
               )::bigint AS new_multiplicity
        FROM contributions contribution
        LEFT JOIN shiba_internal.window_rows state
          ON state.result_oid=$1
         AND state.partition_key=contribution.partition_key
         AND state.row_data=contribution.row_data
      ),
      validated AS MATERIALIZED (
        SELECT transition.*
        FROM transitions transition
        WHERE shiba._assert_unary_batch_transition(
          transition.old_multiplicity+transition.minimum_prefix>=0
            AND transition.new_multiplicity>=0,
          'window'
        )
      ),
      changed_partitions AS MATERIALIZED (
        SELECT DISTINCT partition_key
        FROM validated
        WHERE multiplicity_delta<>0
      ),
      next_state AS MATERIALIZED (
        SELECT state.partition_key,state.row_data,state.multiplicity
        FROM shiba_internal.window_rows state
        JOIN changed_partitions changed
          ON changed.partition_key=state.partition_key
        WHERE state.result_oid=$1
          AND NOT EXISTS (
            SELECT 1
            FROM validated batch
            WHERE batch.partition_key=state.partition_key
              AND batch.row_data=state.row_data
              AND batch.multiplicity_delta<>0
          )
        UNION ALL
        SELECT batch.partition_key,batch.row_data,batch.new_multiplicity
        FROM validated batch
        WHERE batch.multiplicity_delta<>0
          AND batch.new_multiplicity>0
      ),
      state_upserts AS (
        INSERT INTO shiba_internal.window_rows
          (result_oid,partition_key,row_data,multiplicity)
        SELECT $1,batch.partition_key,batch.row_data,batch.new_multiplicity
        FROM validated batch
        WHERE batch.multiplicity_delta<>0
          AND batch.new_multiplicity>0
        ON CONFLICT(result_oid,partition_key,row_data) DO UPDATE
        SET multiplicity=EXCLUDED.multiplicity
        RETURNING partition_key,row_data
      ),
      state_deletes AS (
        DELETE FROM shiba_internal.window_rows state
        USING validated batch
        WHERE state.result_oid=$1
          AND state.partition_key=batch.partition_key
          AND state.row_data=batch.row_data
          AND batch.multiplicity_delta<>0
          AND batch.new_multiplicity=0
        RETURNING state.partition_key,state.row_data
      ),
      sink_deletes AS (
        DELETE FROM %4$s result
        USING changed_partitions changed
        WHERE coalesce(to_jsonb(result.%5$I),'null'::jsonb)
              =changed.partition_key
        RETURNING 1
      ),
      mutations AS MATERIALIZED (
        SELECT
          (SELECT count(*) FROM state_upserts)
          +(SELECT count(*) FROM state_deletes)
          +(SELECT count(*) FROM sink_deletes) AS mutation_count
      )
      INSERT INTO %4$s (%6$s)
      SELECT %7$s
      FROM next_state state
      CROSS JOIN LATERAL
        jsonb_populate_record(NULL::%1$s,state.row_data) input
      CROSS JOIN LATERAL generate_series(1,state.multiplicity) copy(n)
      CROSS JOIN mutations
      $statement$,
      source_name,filter_sql,window_view.partition_column,result_name,
      window_view.result_partition_column,quoted_outputs,expressions
    )
    USING stream_view.result_oid,p_commit_lsn,stream_view.source_oid;
END;
$$;
