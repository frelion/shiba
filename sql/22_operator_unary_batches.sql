-- DISTINCT is a threshold over projected keys. Decode and filter the source
-- commit once, combine collisions, update multiplicities once per key, and
-- touch the sink only when a key crosses the zero boundary.
CREATE FUNCTION shiba._apply_distinct_batch(
    stream_view shiba_internal.stream_views,
    events jsonb
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
    affected_keys jsonb[];
    multiplicity_deltas bigint[];
    minimum_prefixes bigint[];
    old_multiplicities bigint[];
    state_is_valid boolean;
    inserted_keys jsonb[];
    removed_keys jsonb[];
BEGIN
    IF stream_view.view_kind<>'distinct' THEN
      RAISE EXCEPTION 'invalid Shiba DISTINCT batch specialization for result %',
        stream_view.result_oid
        USING ERRCODE='data_corrupted';
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

    EXECUTE format(
      $statement$
      WITH typed_events AS MATERIALIZED (
        SELECT raw.ordinality,event.delta::bigint AS delta,input.row
        FROM jsonb_array_elements($2) WITH ORDINALITY raw(value,ordinality)
        CROSS JOIN LATERAL jsonb_populate_record(
          NULL::shiba_internal.delta_event,raw.value
        ) event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(NULL::%1$s,event.row_data) AS row
        ) input
        WHERE event.source_oid=$3
          AND coalesce((%2$s),false)
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
      contributions AS (
        SELECT row_key,sum(delta)::bigint AS multiplicity_delta,
               min(prefix)::bigint AS minimum_prefix
        FROM running
        GROUP BY row_key
      )
      SELECT array_agg(row_key ORDER BY row_key::text),
             array_agg(multiplicity_delta ORDER BY row_key::text),
             array_agg(minimum_prefix ORDER BY row_key::text)
      FROM contributions
      $statement$,
      source_name,filter_sql,key_arguments
    )
    USING stream_view.result_oid,events,stream_view.source_oid
    INTO affected_keys,multiplicity_deltas,minimum_prefixes;
    IF affected_keys IS NULL THEN
      RETURN;
    END IF;

    SELECT array_agg(coalesce(state.multiplicity,0) ORDER BY slot),
           coalesce(bool_and(
             coalesce(state.multiplicity,0)+minimum_prefixes[slot]>=0
           ),true)
    INTO old_multiplicities,state_is_valid
    FROM generate_subscripts(affected_keys,1) slot
    LEFT JOIN shiba_internal.projection_state state
      ON state.result_oid=stream_view.result_oid
     AND state.row_key=affected_keys[slot];
    IF NOT state_is_valid THEN
      RAISE EXCEPTION 'Shiba DISTINCT batch produced negative multiplicity'
        USING ERRCODE='data_corrupted';
    END IF;

    INSERT INTO shiba_internal.projection_state
      (result_oid,row_key,multiplicity)
    SELECT stream_view.result_oid,affected_keys[slot],
           old_multiplicities[slot]+multiplicity_deltas[slot]
    FROM generate_subscripts(affected_keys,1) slot
    WHERE multiplicity_deltas[slot]<>0
      AND old_multiplicities[slot]+multiplicity_deltas[slot]>0
    ON CONFLICT(result_oid,row_key) DO UPDATE
    SET multiplicity=EXCLUDED.multiplicity;
    DELETE FROM shiba_internal.projection_state state
    USING generate_subscripts(affected_keys,1) slot
    WHERE state.result_oid=stream_view.result_oid
      AND state.row_key=affected_keys[slot]
      AND multiplicity_deltas[slot]<>0
      AND old_multiplicities[slot]+multiplicity_deltas[slot]=0;

    SELECT array_agg(affected_keys[slot] ORDER BY slot)
    INTO inserted_keys
    FROM generate_subscripts(affected_keys,1) slot
    WHERE old_multiplicities[slot]=0
      AND multiplicity_deltas[slot]>0;
    SELECT array_agg(affected_keys[slot] ORDER BY slot)
    INTO removed_keys
    FROM generate_subscripts(affected_keys,1) slot
    WHERE old_multiplicities[slot]>0
      AND old_multiplicities[slot]+multiplicity_deltas[slot]=0;
    IF removed_keys IS NOT NULL THEN
      EXECUTE format(
        'DELETE FROM %s target WHERE to_jsonb(target)=ANY($1)',
        result_name
      ) USING removed_keys;
    END IF;
    IF inserted_keys IS NOT NULL THEN
      EXECUTE format(
        'INSERT INTO %s
         SELECT (jsonb_populate_record(NULL::%s,key_value)).*
         FROM unnest($1::jsonb[]) key_value',
        result_name,result_name
      ) USING inserted_keys;
    END IF;
END;
$$;

-- TopN keeps a full multiset. Apply the commit's net row changes together and
-- rebuild the bounded sink once, rather than once for every source delta.
CREATE FUNCTION shiba._apply_topn_batch(
    stream_view shiba_internal.stream_views,
    events jsonb
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
    affected_rows jsonb[];
    multiplicity_deltas bigint[];
    minimum_prefixes bigint[];
    old_multiplicities bigint[];
    state_is_valid boolean;
    quoted_outputs text;
    expressions text;
BEGIN
    IF stream_view.view_kind<>'topn' THEN
      RAISE EXCEPTION 'invalid Shiba TopN batch specialization for result %',
        stream_view.result_oid
        USING ERRCODE='data_corrupted';
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

    EXECUTE format(
      $statement$
      WITH typed_events AS MATERIALIZED (
        SELECT raw.ordinality,event.delta::bigint AS delta,input.row
        FROM jsonb_array_elements($2) WITH ORDINALITY raw(value,ordinality)
        CROSS JOIN LATERAL jsonb_populate_record(
          NULL::shiba_internal.delta_event,raw.value
        ) event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(NULL::%1$s,event.row_data) AS row
        ) input
        WHERE event.source_oid=$3
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
      )
      SELECT array_agg(row_data ORDER BY row_data::text),
             array_agg(multiplicity_delta ORDER BY row_data::text),
             array_agg(minimum_prefix ORDER BY row_data::text)
      FROM contributions
      $statement$,
      source_name,filter_sql
    )
    USING stream_view.result_oid,events,stream_view.source_oid
    INTO affected_rows,multiplicity_deltas,minimum_prefixes;
    IF affected_rows IS NULL THEN
      RETURN;
    END IF;
    SELECT array_agg(coalesce(state.multiplicity,0) ORDER BY slot),
           coalesce(bool_and(
             coalesce(state.multiplicity,0)+minimum_prefixes[slot]>=0
           ),true)
    INTO old_multiplicities,state_is_valid
    FROM generate_subscripts(affected_rows,1) slot
    LEFT JOIN shiba_internal.topn_rows state
      ON state.result_oid=stream_view.result_oid
     AND state.row_data=affected_rows[slot];
    IF NOT state_is_valid THEN
      RAISE EXCEPTION 'Shiba TopN batch produced negative multiplicity'
        USING ERRCODE='data_corrupted';
    END IF;

    INSERT INTO shiba_internal.topn_rows
      (result_oid,row_data,multiplicity)
    SELECT stream_view.result_oid,affected_rows[slot],
           old_multiplicities[slot]+multiplicity_deltas[slot]
    FROM generate_subscripts(affected_rows,1) slot
    WHERE multiplicity_deltas[slot]<>0
      AND old_multiplicities[slot]+multiplicity_deltas[slot]>0
    ON CONFLICT(result_oid,row_data) DO UPDATE
    SET multiplicity=EXCLUDED.multiplicity;
    DELETE FROM shiba_internal.topn_rows state
    USING generate_subscripts(affected_rows,1) slot
    WHERE state.result_oid=stream_view.result_oid
      AND state.row_data=affected_rows[slot]
      AND multiplicity_deltas[slot]<>0
      AND old_multiplicities[slot]+multiplicity_deltas[slot]=0;

    IF NOT EXISTS (
      SELECT 1 FROM unnest(multiplicity_deltas) delta(value)
      WHERE delta.value<>0
    ) THEN
      RETURN;
    END IF;

    EXECUTE format('DELETE FROM %s',result_name);
    SELECT string_agg(format('%I',output_column),',' ORDER BY ordinal),
           string_agg(format('input.%I',source_column),',' ORDER BY ordinal)
    INTO STRICT quoted_outputs,expressions
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

-- Window state is grouped by the full canonical row and partition. Apply all
-- multiplicity changes first and rebuild each changed partition exactly once.
CREATE FUNCTION shiba._apply_window_batch(
    stream_view shiba_internal.stream_views,
    events jsonb
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
    affected_partitions jsonb[];
    affected_rows jsonb[];
    multiplicity_deltas bigint[];
    minimum_prefixes bigint[];
    old_multiplicities bigint[];
    state_is_valid boolean;
    rebuild_partitions jsonb[];
    rebuild_partition jsonb;
    quoted_outputs text;
    expressions text;
BEGIN
    IF stream_view.view_kind<>'window' THEN
      RAISE EXCEPTION 'invalid Shiba window batch specialization for result %',
        stream_view.result_oid
        USING ERRCODE='data_corrupted';
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

    EXECUTE format(
      $statement$
      WITH typed_events AS MATERIALIZED (
        SELECT raw.ordinality,event.delta::bigint AS delta,input.row
        FROM jsonb_array_elements($2) WITH ORDINALITY raw(value,ordinality)
        CROSS JOIN LATERAL jsonb_populate_record(
          NULL::shiba_internal.delta_event,raw.value
        ) event
        CROSS JOIN LATERAL (
          SELECT jsonb_populate_record(NULL::%1$s,event.row_data) AS row
        ) input
        WHERE event.source_oid=$3
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
      )
      SELECT array_agg(partition_key ORDER BY partition_key::text,row_data::text),
             array_agg(row_data ORDER BY partition_key::text,row_data::text),
             array_agg(multiplicity_delta ORDER BY partition_key::text,row_data::text),
             array_agg(minimum_prefix ORDER BY partition_key::text,row_data::text)
      FROM contributions
      $statement$,
      source_name,filter_sql,window_view.partition_column
    )
    USING stream_view.result_oid,events,stream_view.source_oid
    INTO affected_partitions,affected_rows,multiplicity_deltas,minimum_prefixes;
    IF affected_rows IS NULL THEN
      RETURN;
    END IF;

    SELECT array_agg(coalesce(state.multiplicity,0) ORDER BY slot),
           coalesce(bool_and(
             coalesce(state.multiplicity,0)+minimum_prefixes[slot]>=0
           ),true)
    INTO old_multiplicities,state_is_valid
    FROM generate_subscripts(affected_rows,1) slot
    LEFT JOIN shiba_internal.window_rows state
      ON state.result_oid=stream_view.result_oid
     AND state.partition_key=affected_partitions[slot]
     AND state.row_data=affected_rows[slot];
    IF NOT state_is_valid THEN
      RAISE EXCEPTION 'Shiba window batch produced negative multiplicity'
        USING ERRCODE='data_corrupted';
    END IF;

    INSERT INTO shiba_internal.window_rows
      (result_oid,partition_key,row_data,multiplicity)
    SELECT stream_view.result_oid,affected_partitions[slot],
           affected_rows[slot],
           old_multiplicities[slot]+multiplicity_deltas[slot]
    FROM generate_subscripts(affected_rows,1) slot
    WHERE multiplicity_deltas[slot]<>0
      AND old_multiplicities[slot]+multiplicity_deltas[slot]>0
    ON CONFLICT(result_oid,partition_key,row_data) DO UPDATE
    SET multiplicity=EXCLUDED.multiplicity;
    DELETE FROM shiba_internal.window_rows state
    USING generate_subscripts(affected_rows,1) slot
    WHERE state.result_oid=stream_view.result_oid
      AND state.partition_key=affected_partitions[slot]
      AND state.row_data=affected_rows[slot]
      AND multiplicity_deltas[slot]<>0
      AND old_multiplicities[slot]+multiplicity_deltas[slot]=0;

    SELECT array_agg(partition_key ORDER BY partition_key::text)
    INTO rebuild_partitions
    FROM (
      SELECT DISTINCT affected_partitions[slot] AS partition_key
      FROM generate_subscripts(affected_partitions,1) slot
      WHERE multiplicity_deltas[slot]<>0
    ) changed;
    IF rebuild_partitions IS NULL THEN
      RETURN;
    END IF;
    SELECT string_agg(format('%I',column_name),',' ORDER BY ordinal)
    INTO STRICT quoted_outputs
    FROM unnest(window_view.output_columns)
      WITH ORDINALITY output(column_name,ordinal);
    SELECT string_agg(expression,',' ORDER BY ordinal)
    INTO STRICT expressions
    FROM unnest(window_view.target_expressions)
      WITH ORDINALITY target(expression,ordinal);
    FOREACH rebuild_partition IN ARRAY rebuild_partitions LOOP
      EXECUTE format(
        'DELETE FROM %s
         WHERE coalesce(to_jsonb(%I),''null''::jsonb)=$1',
        result_name,window_view.result_partition_column
      ) USING rebuild_partition;
      EXECUTE format(
        'INSERT INTO %s (%s)
         SELECT %s
         FROM shiba_internal.window_rows state
         CROSS JOIN LATERAL jsonb_populate_record(NULL::%s,state.row_data) input
         CROSS JOIN LATERAL generate_series(1,state.multiplicity) copy(n)
         WHERE state.result_oid=$1 AND state.partition_key=$2',
        result_name,quoted_outputs,expressions,source_name
      ) USING stream_view.result_oid,rebuild_partition;
    END LOOP;
END;
$$;
