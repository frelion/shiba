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
        USING ERRCODE='P0S01';
    END IF;
    row_data := shiba._canonicalize_row(source_relation,row_data);
    IF execution_pipeline='window' THEN
        IF stream_view.view_kind<>'window' THEN
          RAISE EXCEPTION 'logical plan pipeline disagrees with window metadata for result %',result_relation
            USING ERRCODE='P0S01';
        END IF;
        IF source_relation<>stream_view.source_oid THEN
          RAISE EXCEPTION 'Shiba window DAG inbox source does not belong to result %',result_relation
            USING ERRCODE='P0S01';
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
            USING ERRCODE='P0S01';
        END IF;
        IF source_relation<>stream_view.source_oid THEN
          RAISE EXCEPTION 'Shiba DISTINCT DAG inbox source does not belong to result %',result_relation
            USING ERRCODE='P0S01';
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
            USING ERRCODE='P0S01';
        END IF;
        IF source_relation<>stream_view.source_oid THEN
          RAISE EXCEPTION 'Shiba TopN DAG inbox source does not belong to result %',result_relation
            USING ERRCODE='P0S01';
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
            USING ERRCODE='P0S01';
        END IF;
        SELECT * INTO STRICT join_view
        FROM shiba_internal.inner_join_views WHERE result_oid = result_relation;
        IF join_view.right_source_oid<>right_source_oid
           OR join_view.join_type<>execution_join_type THEN
          RAISE EXCEPTION 'logical plan join descriptor disagrees with metadata for result %',result_relation
            USING ERRCODE='P0S01';
        END IF;
        IF source_relation = left_source_oid THEN
            input_side := 'left';
        ELSIF source_relation = right_source_oid THEN
            input_side := 'right';
        ELSE
            RAISE EXCEPTION 'Shiba DAG inbox source does not belong to result %', result_relation
                USING ERRCODE='P0S01';
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
            USING ERRCODE='P0S01';
        END IF;
        IF source_relation <> stream_view.source_oid THEN
            RAISE EXCEPTION 'Shiba DAG inbox source does not belong to result %', result_relation
                USING ERRCODE='P0S01';
        END IF;
        IF NOT shiba._row_passes_filter(result_relation, 'left', row_data) THEN
            RETURN;
        END IF;
        PERFORM shiba._apply_logged_delta(stream_view, row_data, delta);
    ELSE
        RAISE EXCEPTION 'unsupported logical execution pipeline %',execution_pipeline
          USING ERRCODE='P0S01';
    END IF;
END;
$$;

CREATE FUNCTION shiba._advance_dag_progress(
    result_relation oid,
    commit_lsn text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    changed_rows bigint;
BEGIN
    INSERT INTO shiba_internal.view_progress AS progress
        (result_oid, applied_lsn, updated_at)
    VALUES (result_relation, commit_lsn::pg_lsn, clock_timestamp())
    ON CONFLICT (result_oid) DO UPDATE
    SET applied_lsn = EXCLUDED.applied_lsn,
        updated_at = EXCLUDED.updated_at
    WHERE progress.applied_lsn IS NULL
       OR progress.applied_lsn < EXCLUDED.applied_lsn;

    GET DIAGNOSTICS changed_rows = ROW_COUNT;
    IF changed_rows = 0 THEN
      RAISE EXCEPTION
        'Shiba DAG % progress must advance monotonically beyond %, requested %',
        result_relation,
        (
          SELECT progress.applied_lsn
          FROM shiba_internal.view_progress AS progress
          WHERE progress.result_oid=result_relation
        ),
        commit_lsn::pg_lsn
        USING ERRCODE='P0S01';
    END IF;
END;
$$;

-- Acquire the one canonical DAG apply lock sequence.  The advisory lock
-- serializes execution with DROP/lifecycle work, the runtime-state row
-- revalidates that the DAG is runnable, and only then may the oldest inbox row
-- be claimed.  All locks are transaction-scoped or row locks and therefore
-- remain held by callers after this function returns.
CREATE FUNCTION shiba._claim_dag_commit(
    result_relation oid,
    requested_commit_lsn pg_lsn DEFAULT NULL
)
RETURNS TABLE (
    claim_status text,
    claimed_ingress_txn_id bigint,
    claimed_commit_lsn pg_lsn
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    earliest_ingress_txn_id bigint;
    earliest_commit_lsn pg_lsn;
    runtime_is_active boolean;
BEGIN
    -- User-managed index DDL owns this lock while PostgreSQL builds or drops
    -- an index.  Never let the singleton Runtime block behind that work:
    -- report a retry so the scheduler can continue with other ready DAGs.
    IF NOT pg_try_advisory_xact_lock(
      shiba_internal.dag_lock_key(result_relation)
    ) THEN
      claim_status := 'retry';
      claimed_ingress_txn_id := NULL;
      claimed_commit_lsn := NULL;
      RETURN NEXT;
      RETURN;
    END IF;
    SELECT runtime.active INTO runtime_is_active
    FROM shiba_internal.dag_runtime_state AS runtime
    WHERE runtime.result_oid=result_relation
    FOR UPDATE;
    IF NOT FOUND OR NOT runtime_is_active THEN
      claim_status := 'inactive';
      claimed_ingress_txn_id := NULL;
      claimed_commit_lsn := NULL;
      RETURN NEXT;
      RETURN;
    END IF;

    SELECT inbox.ingress_txn_id,inbox.commit_lsn
    INTO earliest_ingress_txn_id,earliest_commit_lsn
    FROM shiba_internal.dag_inbox AS inbox
    WHERE inbox.result_oid=result_relation
    ORDER BY inbox.commit_lsn
    LIMIT 1
    FOR UPDATE;
    IF NOT FOUND THEN
      IF requested_commit_lsn IS NOT NULL THEN
        RAISE EXCEPTION 'Shiba DAG % has no inbox work for commit %',
          result_relation,requested_commit_lsn
          USING ERRCODE='P0S01';
      END IF;
      claim_status := 'idle';
      claimed_ingress_txn_id := NULL;
      claimed_commit_lsn := NULL;
      RETURN NEXT;
      RETURN;
    END IF;
    IF requested_commit_lsn IS NOT NULL
       AND earliest_commit_lsn<>requested_commit_lsn THEN
      RAISE EXCEPTION
        'Shiba DAG % must apply earliest inbox commit %, requested %',
        result_relation,earliest_commit_lsn,requested_commit_lsn
        USING ERRCODE='P0S01';
    END IF;

    claim_status := 'claimed';
    claimed_ingress_txn_id := earliest_ingress_txn_id;
    claimed_commit_lsn := earliest_commit_lsn;
    RETURN NEXT;
END;
$$;

-- Execute one already-claimed commit.  The caller must have used
-- _claim_dag_commit in this transaction; this function deliberately performs
-- no additional scheduler locks or inbox scans.
CREATE FUNCTION shiba._apply_claimed_dag_commit(
    result_relation oid,
    execution_descriptor jsonb,
    ingress_transaction_id bigint,
    commit_lsn pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    stream_view shiba_internal.stream_views%ROWTYPE;
    execution_pipeline text := execution_descriptor->>'pipeline';
    left_source_oid oid := (execution_descriptor->>'left_source_oid')::oid;
    source_commit_lsn pg_lsn := commit_lsn;
    use_unary_batch boolean :=
      execution_pipeline IN ('window','distinct','topn');
    commit_event_count bigint;
    commit_payload_bytes bigint;
    max_commit_rows bigint := coalesce(
      nullif(current_setting('shiba.max_commit_rows',true),'')::bigint,
      1000000
    );
    max_commit_bytes bigint := coalesce(
      nullif(current_setting('shiba.max_commit_bytes',true),'')::bigint,
      1073741824
    );
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

    SELECT * INTO STRICT stream_view
    FROM shiba_internal.stream_views AS metadata
    WHERE metadata.result_oid=result_relation;
    PERFORM set_config(
      'TimeZone',stream_view.execution_settings->>'TimeZone',true
    );
    PERFORM set_config(
      'DateStyle',stream_view.execution_settings->>'DateStyle',true
    );
    PERFORM set_config(
      'IntervalStyle',stream_view.execution_settings->>'IntervalStyle',true
    );
    PERFORM set_config(
      'extra_float_digits',
      stream_view.execution_settings->>'extra_float_digits',
      true
    );
    PERFORM set_config(
      'bytea_output',stream_view.execution_settings->>'bytea_output',true
    );
    IF use_unary_batch AND stream_view.source_oid<>left_source_oid THEN
      RAISE EXCEPTION
        'logical plan unary input disagrees with metadata for result %',
        result_relation
        USING ERRCODE='P0S01';
    END IF;
    IF stream_view.source_oid<>left_source_oid THEN
      RAISE EXCEPTION
        'logical plan left input disagrees with metadata for result %',
        result_relation
        USING ERRCODE='P0S01';
    END IF;

    SELECT txn.event_count,txn.payload_bytes
    INTO STRICT commit_event_count,commit_payload_bytes
    FROM shiba_internal.ingress_transactions txn
    WHERE txn.ingress_txn_id=ingress_transaction_id
      AND txn.commit_lsn=source_commit_lsn
      AND txn.status='committed';
    IF commit_event_count>max_commit_rows
       OR commit_payload_bytes>max_commit_bytes THEN
      RAISE EXCEPTION
        'Shiba source commit % exceeds Runtime admission: % rows/% bytes, limits %/%',
        source_commit_lsn,commit_event_count,commit_payload_bytes,
        max_commit_rows,max_commit_bytes
        USING ERRCODE='53400',
              HINT='Increase shiba.max_commit_rows/max_commit_bytes or split the source transaction.';
    END IF;

    IF execution_pipeline='join' THEN
      PERFORM shiba._apply_join_commit_temp_free(
        result_relation,execution_descriptor,source_commit_lsn
      );
    ELSIF execution_pipeline='aggregate' THEN
      PERFORM shiba._apply_single_source_aggregate_temp_free(
        stream_view,source_commit_lsn,false
      );
    ELSIF execution_pipeline='distinct' THEN
      PERFORM shiba._apply_distinct_batch(stream_view,source_commit_lsn);
    ELSIF execution_pipeline='topn' THEN
      PERFORM shiba._apply_topn_batch(stream_view,source_commit_lsn);
    ELSIF execution_pipeline='window' THEN
      PERFORM shiba._apply_window_batch(stream_view,source_commit_lsn);
    END IF;

    PERFORM shiba._advance_dag_progress(result_relation,commit_lsn::text);
END;
$$;

-- Compatibility entry point for callers that already selected a commit. It
-- now takes the canonical advisory -> runtime-state -> inbox lock sequence,
-- then executes the claimed payload without acknowledging it.
CREATE FUNCTION shiba._apply_dag_commit(
    result_relation oid,
    execution_descriptor jsonb,
    commit_lsn pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    claim_status_value text;
    claimed_ingress_txn_id bigint;
    claimed_lsn pg_lsn;
BEGIN
    SELECT claim.claim_status,
           claim.claimed_ingress_txn_id,
           claim.claimed_commit_lsn
    INTO STRICT claim_status_value,claimed_ingress_txn_id,claimed_lsn
    FROM shiba._claim_dag_commit(result_relation,commit_lsn) AS claim;
    IF claim_status_value<>'claimed' THEN
      RAISE EXCEPTION 'Shiba DAG % is not active',result_relation
        USING ERRCODE='object_not_in_prerequisite_state';
    END IF;
    PERFORM shiba._apply_claimed_dag_commit(
      result_relation,execution_descriptor,claimed_ingress_txn_id,claimed_lsn
    );
END;
$$;

-- Apply an already-claimed commit inside an intentional PL/pgSQL
-- subtransaction:
-- an operator/SPI error rolls back every state/result/progress mutation from
-- this source commit. Explicitly transient concurrency failures leave the DAG
-- active for a later transaction retry; deterministic failures quarantine it.
-- The normal entry point requests acknowledgement here so its affected-row
-- count is part of the same error/isolation protocol.
CREATE FUNCTION shiba._apply_claimed_dag_safely(
    result_relation oid,
    execution_descriptor jsonb,
    p_ingress_txn_id bigint,
    p_commit_lsn pg_lsn,
    acknowledge boolean
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    error_state text;
    error_message text;
    error_detail text;
    error_hint text;
    acknowledged_rows bigint;
BEGIN
    BEGIN
        PERFORM shiba._apply_claimed_dag_commit(
          result_relation,execution_descriptor,p_ingress_txn_id,p_commit_lsn
        );
        IF acknowledge THEN
          DELETE FROM shiba_internal.dag_inbox AS inbox
          WHERE inbox.result_oid=result_relation
            AND inbox.ingress_txn_id=p_ingress_txn_id;
          GET DIAGNOSTICS acknowledged_rows = ROW_COUNT;
          IF acknowledged_rows<>1 THEN
            RAISE EXCEPTION
              'Shiba DAG % acknowledgement for commit % affected % rows, expected 1',
              result_relation::regclass,p_commit_lsn,acknowledged_rows
              USING ERRCODE='P0S01';
          END IF;
        END IF;
        RETURN 'applied';
    EXCEPTION
      WHEN serialization_failure OR deadlock_detected OR lock_not_available THEN
        -- The exception block is a subtransaction, so all apply mutations have
        -- already rolled back. Keep the DAG and inbox eligible for retry in a
        -- fresh outer transaction. Do not include query_canceled here:
        -- cancellation and administrative termination must propagate.
        RETURN 'retry';
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
          error_state = RETURNED_SQLSTATE,
          error_message = MESSAGE_TEXT,
          error_detail = PG_EXCEPTION_DETAIL,
          error_hint = PG_EXCEPTION_HINT;

        -- A configured resource ceiling is a per-DAG pause, not a Runtime
        -- crash.  The exception block has rolled back every operator mutation,
        -- so retaining the inbox row and disabling this DAG is atomic.  An
        -- administrator can raise the ceiling and explicitly resume it.
        IF error_state='53400' THEN
          UPDATE shiba_internal.dag_runtime_state
          SET active = false,
              last_error = concat_ws(
                E'\n',
                format('[%s] %s', error_state, error_message),
                nullif(error_detail, ''),
                nullif(error_hint, '')
              ),
              failed_at = clock_timestamp()
          WHERE result_oid = result_relation;
          RETURN 'resource_blocked';
        END IF;

        -- Uncontrolled resource exhaustion, operator intervention, and system
        -- failures remain Runtime/backend failures. Propagate them so
        -- PostgreSQL can abort/restart the singleton Runtime.
        -- PL/pgSQL's OTHERS does not catch query_canceled, so 57014 already
        -- propagates without reaching this branch.
        IF left(error_state,2) IN ('40','53','54','57','58','XX') THEN
          RAISE;
        END IF;

        UPDATE shiba_internal.dag_runtime_state
        SET active = false,
            last_error = concat_ws(
              E'\n',
              format('[%s] %s', error_state, error_message),
              nullif(error_detail, ''),
              nullif(error_hint, '')
            ),
            failed_at = clock_timestamp()
        WHERE result_oid = result_relation;
        RETURN 'quarantined';
    END;
END;
$$;

-- Normal Runtime integration entry point. It owns claim, apply and exact
-- one-row acknowledgement, and reports both the outcome and the claimed LSN so
-- the caller can retain deterministic failpoint/logging behavior.
CREATE FUNCTION shiba._apply_next_dag_change_log(
    result_relation oid,
    execution_descriptor jsonb
)
RETURNS TABLE (
    outcome text,
    commit_lsn pg_lsn
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    claim_status_value text;
    claimed_ingress_txn_id bigint;
    claimed_lsn pg_lsn;
    apply_outcome text;
BEGIN
    SELECT claim.claim_status,
           claim.claimed_ingress_txn_id,
           claim.claimed_commit_lsn
    INTO STRICT claim_status_value,claimed_ingress_txn_id,claimed_lsn
    FROM shiba._claim_dag_commit(result_relation,NULL) AS claim;
    IF claim_status_value<>'claimed' THEN
      RETURN QUERY SELECT claim_status_value,NULL::pg_lsn;
      RETURN;
    END IF;

    apply_outcome := shiba._apply_claimed_dag_safely(
      result_relation,
      execution_descriptor,
      claimed_ingress_txn_id,
      claimed_lsn,
      true
    );
    RETURN QUERY SELECT apply_outcome,claimed_lsn;
END;
$$;

-- Compatibility entry point for the current Rust caller and direct SQL tests.
-- It uses the canonical lock sequence but intentionally leaves acknowledgement
-- to the caller. Runtime integration should move to _apply_next_dag_change_log
-- and remove its Rust-side inbox pre-lock and DELETE.
CREATE FUNCTION shiba._safe_apply_dag_change_log(
    result_relation oid,
    execution_descriptor jsonb,
    commit_lsn pg_lsn
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    claim_status_value text;
    claimed_ingress_txn_id bigint;
    claimed_lsn pg_lsn;
BEGIN
    SELECT claim.claim_status,
           claim.claimed_ingress_txn_id,
           claim.claimed_commit_lsn
    INTO STRICT claim_status_value,claimed_ingress_txn_id,claimed_lsn
    FROM shiba._claim_dag_commit(result_relation,commit_lsn) AS claim;
    IF claim_status_value<>'claimed' THEN
      RAISE EXCEPTION 'Shiba DAG % is not active',result_relation
        USING ERRCODE='object_not_in_prerequisite_state';
    END IF;
    RETURN shiba._apply_claimed_dag_safely(
      result_relation,
      execution_descriptor,
      claimed_ingress_txn_id,
      claimed_lsn,
      false
    );
END;
$$;

-- Catalog-plan compatibility entry point for SQL callers. The Runtime should
-- pass its already validated descriptor to the three-argument overload above.
CREATE FUNCTION shiba._safe_apply_dag_change_log(
    result_relation oid,
    commit_lsn pg_lsn
)
RETURNS text
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
    SELECT shiba._safe_apply_dag_change_log(
      result_relation,
      shiba._logical_execution_descriptor(result_relation),
      commit_lsn
    )
$$;
