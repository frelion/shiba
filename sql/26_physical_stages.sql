-- Resolve a compiler-created Stage relation through catalog identity rather
-- than reconstructing its name.  Only UNLOGGED relations in shiba_internal
-- are accepted as commit-scoped physical storage.
CREATE FUNCTION shiba._physical_stage_name(
    result_relation oid,
    p_stage_name text
)
RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
    SELECT format('%I.%I',namespace.nspname,relation.relname)
    FROM shiba_internal.physical_stages stage
    JOIN pg_class relation ON relation.oid=stage.relation_oid
    JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
    WHERE stage.result_oid=result_relation
      AND stage.stage_name=p_stage_name
      AND stage.storage='unlogged'
      AND relation.relpersistence='u'
      AND namespace.nspname='shiba_internal'
$$;

-- Validate the fixed v1 Stage program before a Runtime executes it. The JSON
-- plan is authoritative for which cross-statement Stages exist; catalog rows
-- and their physical relations must match it exactly.
CREATE FUNCTION shiba_internal._validate_physical_stages(
    result_relation oid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    mismatch_count bigint;
BEGIN
    WITH expected AS (
      SELECT (stage.value ->> 'stage_id')::integer AS stage_id,
             CASE stage.value ->> 'kernel'
               WHEN 'join' THEN 'join_delta'
               WHEN 'source' THEN CASE stage.value -> 'node_ids' ->> 0
                 WHEN 'scan_left' THEN 'left_input_delta'
                 WHEN 'scan_right' THEN 'right_input_delta'
                 ELSE NULL
               END
               ELSE NULL
             END AS stage_name
      FROM shiba_internal.physical_plans AS plan
      CROSS JOIN LATERAL jsonb_array_elements(plan.plan -> 'stages') AS stage(value)
      WHERE plan.result_oid=result_relation
        AND stage.value ->> 'storage'='unlogged'
    ),
    actual AS (
      SELECT physical_stage.stage_id,
             physical_stage.stage_name,
             physical_stage.plan_id,
             physical_plan.plan_id AS expected_plan_id,
             namespace.nspname,
             relation.relpersistence,
             relation.relkind
      FROM shiba_internal.physical_stages AS physical_stage
      JOIN shiba_internal.physical_plans AS physical_plan
        ON physical_plan.result_oid=physical_stage.result_oid
      LEFT JOIN pg_class AS relation
        ON relation.oid=physical_stage.relation_oid
      LEFT JOIN pg_namespace AS namespace
        ON namespace.oid=relation.relnamespace
      WHERE physical_stage.result_oid=result_relation
    )
    SELECT count(*) INTO mismatch_count
    FROM expected
    FULL JOIN actual USING (stage_id)
    WHERE expected.stage_id IS NULL
       OR actual.stage_id IS NULL
       OR expected.stage_name IS NULL
       OR actual.stage_name IS DISTINCT FROM expected.stage_name
       OR actual.plan_id IS DISTINCT FROM actual.expected_plan_id
       OR actual.nspname IS DISTINCT FROM 'shiba_internal'
       OR actual.relpersistence IS DISTINCT FROM 'u'
       OR actual.relkind IS DISTINCT FROM 'r';

    IF mismatch_count<>0 THEN
      RAISE EXCEPTION
        'physical Stage program for result % does not match its plan/catalog relations',
        result_relation
        USING ERRCODE='P0S01';
    END IF;

    WITH expected AS (
      SELECT stage.stage_id,
             column_spec.ordinality::smallint AS attnum,
             column_spec.value ->> 'name' AS attname,
             (column_spec.value ->> 'type_oid')::oid AS atttypid,
             coalesce(
               (column_spec.value ->> 'typmod')::integer,-1
             ) AS atttypmod,
             coalesce(
               (column_spec.value ->> 'collation_oid')::oid,0::oid
             ) AS attcollation,
             NOT coalesce(
               (column_spec.value ->> 'nullable')::boolean,true
             ) AS attnotnull
      FROM shiba_internal.physical_stages AS stage
      CROSS JOIN LATERAL jsonb_array_elements(stage.schema_spec)
        WITH ORDINALITY AS column_spec(value,ordinality)
      WHERE stage.result_oid=result_relation
    ),
    actual AS (
      SELECT stage.stage_id,
             attribute.attnum,
             attribute.attname::text AS attname,
             attribute.atttypid,
             attribute.atttypmod,
             attribute.attcollation,
             attribute.attnotnull
      FROM shiba_internal.physical_stages AS stage
      JOIN pg_attribute AS attribute
        ON attribute.attrelid=stage.relation_oid
       AND attribute.attnum>0
       AND NOT attribute.attisdropped
      WHERE stage.result_oid=result_relation
    )
    SELECT count(*) INTO mismatch_count
    FROM expected
    FULL JOIN actual USING(stage_id,attnum)
    WHERE expected.stage_id IS NULL
       OR actual.stage_id IS NULL
       OR actual.attname IS DISTINCT FROM expected.attname
       OR actual.atttypid IS DISTINCT FROM expected.atttypid
       OR actual.atttypmod IS DISTINCT FROM expected.atttypmod
       OR actual.attcollation IS DISTINCT FROM expected.attcollation
       OR actual.attnotnull IS DISTINCT FROM expected.attnotnull;

    IF mismatch_count<>0
       OR EXISTS (
         SELECT 1
         FROM shiba_internal.physical_stages AS stage
         WHERE stage.result_oid=result_relation
           AND stage.index_spec<>'[]'::jsonb
       )
       OR EXISTS (
         SELECT 1
         FROM shiba_internal.physical_stages AS stage
         JOIN pg_index AS stage_index
           ON stage_index.indrelid=stage.relation_oid
         WHERE stage.result_oid=result_relation
       ) THEN
      RAISE EXCEPTION
        'physical Stage relation shape for result % does not match the fixed v1 program',
        result_relation
        USING ERRCODE='P0S01';
    END IF;
END;
$$;

-- Seed planner statistics after registration without extending the user's
-- CTAS/backfill transaction. DagRuntime calls this once, immediately before
-- the first commit program for a physical-plan generation.
CREATE FUNCTION shiba_internal._analyze_dag_runtime_relations(
    result_relation oid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    registered_view_kind text;
BEGIN
    SELECT view_kind INTO STRICT registered_view_kind
    FROM shiba_internal.stream_views
    WHERE result_oid = result_relation;

    CASE registered_view_kind
        WHEN 'aggregate' THEN
            IF NOT EXISTS (
                SELECT 1 FROM pg_stat_user_tables
                WHERE relid='shiba_internal.aggregate_state'::regclass
                  AND (last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL)
            ) THEN
                ANALYZE shiba_internal.aggregate_state;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_stat_user_tables
                WHERE relid='shiba_internal.distinct_state'::regclass
                  AND (last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL)
            ) THEN
                ANALYZE shiba_internal.distinct_state;
            END IF;
        WHEN 'distinct' THEN
            IF NOT EXISTS (
                SELECT 1 FROM pg_stat_user_tables
                WHERE relid='shiba_internal.projection_state'::regclass
                  AND (last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL)
            ) THEN
                ANALYZE shiba_internal.projection_state;
            END IF;
        WHEN 'window' THEN
            IF NOT EXISTS (
                SELECT 1 FROM pg_stat_user_tables
                WHERE relid='shiba_internal.window_rows'::regclass
                  AND (last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL)
            ) THEN
                ANALYZE shiba_internal.window_rows;
            END IF;
        WHEN 'topn' THEN
            IF NOT EXISTS (
                SELECT 1 FROM pg_stat_user_tables
                WHERE relid='shiba_internal.topn_rows'::regclass
                  AND (last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL)
            ) THEN
                ANALYZE shiba_internal.topn_rows;
            END IF;
        ELSE
            NULL;
    END CASE;
    IF EXISTS (
        SELECT 1
        FROM shiba_internal.inner_join_views
        WHERE result_oid = result_relation
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_stat_user_tables
        WHERE relid='shiba_internal.join_arrangements'::regclass
          AND (last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL)
    ) THEN
        ANALYZE shiba_internal.join_arrangements;
    END IF;
END;
$$;

-- Keep PostgreSQL ERROR handling inside an intentional subtransaction.
-- Deterministic plan/Stage corruption quarantines one DAG; lock conflicts are
-- retried; process/resource failures still abort and restart the singleton
-- Runtime.
CREATE FUNCTION shiba_internal._load_dag_runtime_safely(
    result_relation oid
)
RETURNS TABLE (
    outcome text,
    plan_json text,
    plan_generation text,
    load_error text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    loaded_plan text;
    loaded_generation text;
    error_state text;
    error_message text;
    error_detail text;
    error_hint text;
BEGIN
    BEGIN
        IF NOT pg_try_advisory_xact_lock(
          shiba_internal.dag_lock_key(result_relation)
        ) THEN
          RETURN QUERY
            SELECT 'retry',NULL::text,NULL::text,NULL::text;
          RETURN;
        END IF;
        SELECT plan::text,plan_id::text
        INTO STRICT loaded_plan,loaded_generation
        FROM shiba_internal.physical_plans
        WHERE result_oid=result_relation;
        PERFORM shiba_internal._validate_physical_stages(result_relation);
        PERFORM shiba._truncate_physical_stages(result_relation);
        PERFORM shiba_internal._analyze_dag_runtime_relations(result_relation);
        RETURN QUERY
          SELECT 'loaded',loaded_plan,loaded_generation,NULL::text;
    EXCEPTION
      WHEN serialization_failure OR deadlock_detected OR lock_not_available THEN
        RETURN QUERY SELECT 'retry',NULL::text,NULL::text,NULL::text;
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
          error_state = RETURNED_SQLSTATE,
          error_message = MESSAGE_TEXT,
          error_detail = PG_EXCEPTION_DETAIL,
          error_hint = PG_EXCEPTION_HINT;
        IF left(error_state,2) IN ('40','53','54','57','58','XX') THEN
          RAISE;
        END IF;
        error_message := concat_ws(
          E'\n',
          format('[%s] %s',error_state,error_message),
          nullif(error_detail,''),
          nullif(error_hint,'')
        );
        UPDATE shiba_internal.dag_runtime_state
        SET active=false,
            last_error=error_message,
            failed_at=clock_timestamp()
        WHERE result_oid=result_relation;
        RETURN QUERY
          SELECT 'quarantined',NULL::text,NULL::text,error_message;
    END;
END;
$$;

-- Stage cleanup is a plan-level loop, never a data-row loop.  It is used when
-- a Runtime first loads a DAG and after every successful commit program.
CREATE FUNCTION shiba._truncate_physical_stages(result_relation oid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    stage record;
BEGIN
    -- Re-entrant when the apply protocol already owns this key; required when
    -- Runtime startup clears a Stage before claiming its first inbox row.
    PERFORM pg_advisory_xact_lock(
      shiba_internal.dag_lock_key(result_relation)
    );
    FOR stage IN
      SELECT stage_id,
             shiba._physical_stage_name(result_oid,stage_name) AS relation_name
      FROM shiba_internal.physical_stages
      WHERE result_oid=result_relation
      ORDER BY stage_id
    LOOP
      IF stage.relation_name IS NULL THEN
        RAISE EXCEPTION
          'Shiba physical Stage % for result % is missing or is not UNLOGGED',
          stage.stage_id,result_relation
          USING ERRCODE='P0S01';
      END IF;
      EXECUTE format('TRUNCATE TABLE %s',stage.relation_name);
    END LOOP;
END;
$$;

-- DELETE RETURNING keeps the hot path transactional but leaves dead heap
-- tuples. Compact only an empty Stage that has crossed a coarse size bound;
-- this keeps storage bounded without paying AccessExclusive TRUNCATE on every
-- commit. The DAG lock makes the emptiness check and TRUNCATE one lifecycle
-- operation with respect to apply and DROP.
CREATE FUNCTION shiba_internal._compact_physical_stages(
    result_relation oid,
    threshold_bytes bigint DEFAULT 67108864
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    stage record;
    stage_empty boolean;
    compacted integer := 0;
BEGIN
    IF result_relation IS NULL
       OR threshold_bytes IS NULL
       OR threshold_bytes<=0 THEN
      RAISE EXCEPTION 'invalid physical Stage compaction request'
        USING ERRCODE='invalid_parameter_value';
    END IF;

    PERFORM pg_advisory_xact_lock(
      shiba_internal.dag_lock_key(result_relation)
    );
    FOR stage IN
      SELECT relation_oid,
             shiba._physical_stage_name(result_oid,stage_name)
               AS relation_name
      FROM shiba_internal.physical_stages
      WHERE result_oid=result_relation
      ORDER BY stage_id
    LOOP
      IF stage.relation_name IS NULL THEN
        RAISE EXCEPTION
          'Shiba physical Stage for result % is missing or is not UNLOGGED',
          result_relation
          USING ERRCODE='P0S01';
      END IF;
      IF pg_relation_size(stage.relation_oid)>=threshold_bytes THEN
        EXECUTE format(
          'SELECT NOT EXISTS (SELECT 1 FROM %s LIMIT 1)',
          stage.relation_name
        ) INTO STRICT stage_empty;
        IF stage_empty THEN
          EXECUTE format('TRUNCATE TABLE %s',stage.relation_name);
          compacted := compacted+1;
        END IF;
      END IF;
    END LOOP;
    RETURN compacted;
END;
$$;

CREATE FUNCTION shiba.explain_physical(result_table regclass)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
    SELECT jsonb_build_object(
      'version',plan.version,
      'plan_id',plan.plan_id,
      'plan',plan.plan,
      'stages',coalesce((
        SELECT jsonb_agg(
          jsonb_build_object(
            'stage_id',stage.stage_id,
            'stage_name',stage.stage_name,
            'storage',stage.storage,
            'relation',stage.relation_oid::regclass::text,
            'schema',stage.schema_spec,
            'indexes',stage.index_spec
          )
          ORDER BY stage.stage_id
        )
        FROM shiba_internal.physical_stages stage
        WHERE stage.result_oid=plan.result_oid
      ),'[]'::jsonb)
    )
    FROM shiba_internal.physical_plans plan
    WHERE plan.result_oid=result_table::oid
$$;

REVOKE ALL ON FUNCTION shiba._physical_stage_name(oid,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION shiba_internal._validate_physical_stages(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION shiba._truncate_physical_stages(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION shiba_internal._compact_physical_stages(oid,bigint)
FROM PUBLIC;
