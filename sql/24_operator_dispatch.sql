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
CREATE FUNCTION shiba._logical_execution_descriptor(result_relation oid)
RETURNS jsonb
LANGUAGE sql
STABLE
SET search_path = pg_catalog, shiba_internal
AS $$
    SELECT coalesce(
      (
        SELECT jsonb_strip_nulls(jsonb_build_object(
          'pipeline',CASE
            WHEN bool_or(node->>'operator' IN (
              'inner_join','left_join','right_join','full_join','semi_join',
              'anti_join','null_aware_anti_join'
            )) THEN 'join'
            WHEN bool_or(node->>'operator'='aggregate') THEN 'aggregate'
            WHEN bool_or(node->>'operator'='window') THEN 'window'
            WHEN bool_or(node->>'operator'='top_n') THEN 'topn'
            WHEN bool_or(node->>'operator'='distinct') THEN 'distinct'
          END,
          'left_source_oid',max((node->'config'->>'source_oid')::oid)
            FILTER (WHERE node->>'id'='scan_left'),
          'right_source_oid',max((node->'config'->>'source_oid')::oid)
            FILTER (WHERE node->>'id'='scan_right'),
          'join_type',max(CASE node->>'operator'
            WHEN 'inner_join' THEN 'inner'
            WHEN 'left_join' THEN 'left'
            WHEN 'right_join' THEN 'right'
            WHEN 'full_join' THEN 'full'
            WHEN 'semi_join' THEN 'semi'
            WHEN 'anti_join' THEN 'anti'
            WHEN 'null_aware_anti_join' THEN 'null_anti'
          END)
        ))
        FROM shiba_internal.stream_graphs graph
        CROSS JOIN LATERAL jsonb_array_elements(graph.logical_plan->'nodes') node
        WHERE graph.result_oid=result_relation
        HAVING count(*)>0
      ),
      -- Old direct-call tests and pre-plan catalogs can use this compatibility
      -- fallback. DagRuntime never reaches it.
      (
        SELECT jsonb_strip_nulls(jsonb_build_object(
          'pipeline',CASE WHEN join_view.result_oid IS NULL
            THEN stream_view.view_kind ELSE 'join' END,
          'left_source_oid',stream_view.source_oid,
          'right_source_oid',join_view.right_source_oid,
          'join_type',join_view.join_type
        ))
        FROM shiba_internal.stream_views stream_view
        LEFT JOIN shiba_internal.inner_join_views join_view USING(result_oid)
        WHERE stream_view.result_oid=result_relation
      )
    )
$$;

CREATE FUNCTION shiba._apply_dag_delta_state(
    result_relation oid,
    execution_descriptor jsonb,
    source_relation oid,
    row_data jsonb,
    delta integer,
    commit_lsn text,
    defer_join_sink boolean DEFAULT false
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    stream_view shiba_internal.stream_views%ROWTYPE;
    join_view shiba_internal.inner_join_views%ROWTYPE;
    execution_pipeline text := execution_descriptor->>'pipeline';
    execution_join_type text := execution_descriptor->>'join_type';
    left_source_oid oid := (execution_descriptor->>'left_source_oid')::oid;
    right_source_oid oid := (execution_descriptor->>'right_source_oid')::oid;
    input_side text;
BEGIN
    SELECT * INTO STRICT stream_view FROM shiba_internal.stream_views WHERE result_oid = result_relation;
    IF stream_view.source_oid<>left_source_oid THEN
      RAISE EXCEPTION 'logical plan left input disagrees with metadata for result %',result_relation
        USING ERRCODE='data_corrupted';
    END IF;
    row_data := shiba._canonicalize_row(source_relation,row_data);
    IF execution_pipeline='window' THEN
        IF stream_view.view_kind<>'window' THEN
          RAISE EXCEPTION 'logical plan pipeline disagrees with window metadata for result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
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
    IF execution_pipeline='distinct' THEN
        IF stream_view.view_kind<>'distinct' THEN
          RAISE EXCEPTION 'logical plan pipeline disagrees with DISTINCT metadata for result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
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
    IF execution_pipeline='topn' THEN
        IF stream_view.view_kind<>'topn' THEN
          RAISE EXCEPTION 'logical plan pipeline disagrees with TopN metadata for result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
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
    IF execution_pipeline='join' THEN
        IF stream_view.view_kind<>'aggregate' THEN
          RAISE EXCEPTION 'logical plan pipeline disagrees with join metadata for result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
        SELECT * INTO STRICT join_view
        FROM shiba_internal.inner_join_views WHERE result_oid = result_relation;
        IF join_view.right_source_oid<>right_source_oid
           OR join_view.join_type<>execution_join_type THEN
          RAISE EXCEPTION 'logical plan join descriptor disagrees with metadata for result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
        IF source_relation = left_source_oid THEN
            input_side := 'left';
        ELSIF source_relation = right_source_oid THEN
            input_side := 'right';
        ELSE
            RAISE EXCEPTION 'Shiba DAG inbox source does not belong to result %', result_relation
                USING ERRCODE = 'data_corrupted';
        END IF;
        IF NOT shiba._row_passes_filter(result_relation, input_side, row_data) THEN
            RETURN;
        END IF;
        PERFORM shiba._apply_inner_join_delta(
          result_relation,input_side,row_data,delta,commit_lsn,defer_join_sink
        );
    ELSIF execution_pipeline='aggregate' THEN
        IF stream_view.view_kind<>'aggregate'
           OR EXISTS (
             SELECT 1 FROM shiba_internal.inner_join_views
             WHERE result_oid=result_relation
           ) THEN
          RAISE EXCEPTION 'logical plan pipeline disagrees with aggregate metadata for result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
        IF source_relation <> stream_view.source_oid THEN
            RAISE EXCEPTION 'Shiba DAG inbox source does not belong to result %', result_relation
                USING ERRCODE = 'data_corrupted';
        END IF;
        IF NOT shiba._row_passes_filter(result_relation, 'left', row_data) THEN
            RETURN;
        END IF;
        PERFORM shiba._apply_logged_delta(stream_view, row_data, delta);
    ELSE
        RAISE EXCEPTION 'unsupported logical execution pipeline %',execution_pipeline
          USING ERRCODE='data_corrupted';
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
      result_relation,shiba._logical_execution_descriptor(result_relation),
      source_relation,row_data,delta,commit_lsn
    );
    PERFORM shiba._advance_dag_progress(result_relation,commit_lsn);
END;
$$;

-- Commit-level physical dispatch. JOIN and small Aggregate commits retain WAL
-- order; DISTINCT, TopN and Window consume their complete source commit so
-- projected collisions and affected partitions are coalesced before state is
-- changed.
CREATE FUNCTION shiba._apply_dag_delta_batch(
    result_relation oid,
    execution_descriptor jsonb,
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
    aggregate_only_insertions boolean := true;
    stream_view shiba_internal.stream_views%ROWTYPE;
    execution_pipeline text := execution_descriptor->>'pipeline';
    left_source_oid oid := (execution_descriptor->>'left_source_oid')::oid;
    use_aggregate_batch boolean := false;
    use_unary_batch boolean :=
      execution_pipeline IN ('window','distinct','topn');
BEGIN
    IF jsonb_typeof(execution_descriptor) IS DISTINCT FROM 'object'
       OR left_source_oid IS NULL
       OR execution_pipeline IS NULL
       OR execution_pipeline NOT IN ('aggregate','join','window','distinct','topn')
       OR (execution_pipeline='join' AND (
         (execution_descriptor->>'right_source_oid') IS NULL
         OR (execution_descriptor->>'join_type') NOT IN (
           'inner','left','right','full','semi','anti','null_anti'
         )
       )) THEN
      RAISE EXCEPTION 'invalid Shiba logical execution descriptor'
        USING ERRCODE='invalid_parameter_value';
    END IF;
    IF jsonb_typeof(events) IS DISTINCT FROM 'array' THEN
      RAISE EXCEPTION 'Shiba DAG delta batch must be a JSON array'
        USING ERRCODE='invalid_parameter_value';
    END IF;

    PERFORM pg_advisory_xact_lock(result_relation::bigint);
    -- Unary stateful operators avoid repeated sink/partition rebuilds even for
    -- small UPDATE commits. Aggregate keeps its measured crossover threshold.
    IF use_unary_batch OR jsonb_array_length(events)>=64 THEN
      SELECT * INTO STRICT stream_view
      FROM shiba_internal.stream_views
      WHERE result_oid=result_relation;
    END IF;
    IF use_unary_batch AND stream_view.source_oid<>left_source_oid THEN
      RAISE EXCEPTION
        'logical plan unary input disagrees with metadata for result %',
        result_relation
        USING ERRCODE='data_corrupted';
    END IF;
    IF jsonb_array_length(events)>=64 THEN
      use_aggregate_batch :=
        execution_pipeline='aggregate'
        AND stream_view.view_kind='aggregate'
        AND stream_view.source_oid=left_source_oid
        AND NOT EXISTS (
          SELECT 1 FROM shiba_internal.inner_join_views
          WHERE result_oid=result_relation
        );
    END IF;
    IF execution_pipeline='join' AND jsonb_array_length(events)>1 THEN
      PERFORM shiba._begin_join_batch(result_relation);
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
      IF use_aggregate_batch
         AND NOT stream_view.count_distinct
         AND (event.value->>'delta')::integer<>1 THEN
        aggregate_only_insertions := false;
      END IF;
      IF use_aggregate_batch OR use_unary_batch THEN
        IF (event.value->>'source_oid')::oid<>stream_view.source_oid THEN
          RAISE EXCEPTION 'Shiba DAG inbox source does not belong to result %',result_relation
            USING ERRCODE='data_corrupted';
        END IF;
      ELSE
        PERFORM shiba._apply_dag_delta_state(
          result_relation,
          execution_descriptor,
          (event.value->>'source_oid')::oid,
          event.value->'row_data',
          (event.value->>'delta')::integer,
          commit_lsn,
          execution_pipeline='join' AND jsonb_array_length(events)>1
        );
      END IF;
      event_count := event_count+1;
    END LOOP;

    IF event_count=0 THEN
      RAISE EXCEPTION 'Shiba DAG delta batch must not be empty'
        USING ERRCODE='invalid_parameter_value';
    END IF;
    IF use_aggregate_batch THEN
      PERFORM shiba._apply_single_source_aggregate_batch(
        stream_view,events,aggregate_only_insertions
      );
    ELSIF execution_pipeline='distinct' THEN
      PERFORM shiba._apply_distinct_batch(stream_view,events);
    ELSIF execution_pipeline='topn' THEN
      PERFORM shiba._apply_topn_batch(stream_view,events);
    ELSIF execution_pipeline='window' THEN
      PERFORM shiba._apply_window_batch(stream_view,events);
    END IF;
    IF execution_pipeline='join' AND jsonb_array_length(events)>1 THEN
      PERFORM shiba._finish_join_batch(result_relation);
    END IF;
    PERFORM shiba._advance_dag_progress(result_relation,commit_lsn);
END;
$$;

-- Compatibility entry point. The executor worker never uses this overload:
-- its route comes from the already validated in-memory LogicalPlan.
CREATE FUNCTION shiba._apply_dag_delta_batch(
    result_relation oid,
    events jsonb,
    commit_lsn text
)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
    SELECT shiba._apply_dag_delta_batch(
      result_relation,
      shiba._logical_execution_descriptor(result_relation),
      events,
      commit_lsn
    )
$$;
