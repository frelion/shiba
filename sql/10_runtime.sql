CREATE FUNCTION shiba_internal.slot_name()
RETURNS name
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT format('shiba_%s', oid)::name
    FROM pg_database
    WHERE datname = current_database()
$$;

CREATE FUNCTION shiba_internal.extension_owner()
RETURNS name
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT pg_get_userbyid(extowner)::name
    FROM pg_extension
    WHERE extname = 'shiba'
$$;

CREATE FUNCTION shiba._begin_route_transaction(commit_lsn pg_lsn)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
BEGIN
    INSERT INTO shiba_internal.routed_transactions (commit_lsn)
    VALUES (commit_lsn)
    ON CONFLICT DO NOTHING;
    RETURN FOUND;
END;
$$;

CREATE FUNCTION shiba._ensure_logical_slot()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = shiba_internal.slot_name()::text
    ) THEN
        PERFORM pg_create_logical_replication_slot(shiba_internal.slot_name(), 'pgoutput');
    END IF;
END;
$$;

CREATE FUNCTION shiba.activate()
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal, public
AS $$
BEGIN
    PERFORM shiba._ensure_logical_slot();
    UPDATE shiba_internal.worker_state
    SET active = true, last_heartbeat = NULL, last_requested_at = NULL
    WHERE singleton AND NOT active;
    UPDATE shiba_internal.dag_worker_state
    SET active = true, last_heartbeat = NULL, last_requested_at = NULL
    WHERE NOT active;
    PERFORM shiba._ensure_worker();
    PERFORM shiba._ensure_dag_workers();
    RETURN true;
END;
$$;

CREATE FUNCTION shiba._ensure_dag_worker(result_relation oid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    state shiba_internal.dag_worker_state%ROWTYPE;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM shiba_internal.worker_state WHERE singleton AND active) THEN
        RETURN;
    END IF;
    SELECT * INTO state FROM shiba_internal.dag_worker_state WHERE result_oid = result_relation;
    IF NOT FOUND OR NOT state.active THEN RETURN; END IF;
    IF (state.last_heartbeat IS NOT NULL AND state.last_heartbeat >= pg_postmaster_start_time()
        AND state.last_heartbeat >= clock_timestamp() - interval '5 seconds')
       OR (state.last_requested_at IS NOT NULL AND state.last_requested_at >= pg_postmaster_start_time()
        AND state.last_requested_at >= clock_timestamp() - interval '5 seconds') THEN
        RETURN;
    END IF;

    PERFORM pg_advisory_xact_lock(8485, result_relation::integer);
    SELECT * INTO state FROM shiba_internal.dag_worker_state WHERE result_oid = result_relation FOR UPDATE;
    IF NOT FOUND OR NOT state.active THEN RETURN; END IF;
    -- Recheck both lease fields after waiting for the lock: this makes worker
    -- creation single-owner even when several source sessions wake together.
    IF (state.last_heartbeat IS NOT NULL AND state.last_heartbeat >= pg_postmaster_start_time()
        AND state.last_heartbeat >= clock_timestamp() - interval '5 seconds')
       OR (state.last_requested_at IS NOT NULL AND state.last_requested_at >= pg_postmaster_start_time()
        AND state.last_requested_at >= clock_timestamp() - interval '5 seconds') THEN
        RETURN;
    END IF;
    IF NOT shiba.start_view_worker(result_relation::integer) THEN
        RAISE EXCEPTION 'Shiba could not start DAG worker for result %; increase max_worker_processes', result_relation
            USING ERRCODE = 'configuration_limit_exceeded';
    END IF;
    UPDATE shiba_internal.dag_worker_state
    SET last_requested_at = clock_timestamp()
    WHERE result_oid = result_relation;
END;
$$;

CREATE FUNCTION shiba._ensure_dag_workers()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE state_row record;
BEGIN
    FOR state_row IN SELECT result_oid FROM shiba_internal.dag_worker_state ORDER BY result_oid LOOP
        PERFORM shiba._ensure_dag_worker(state_row.result_oid);
    END LOOP;
END;
$$;

CREATE FUNCTION shiba._ensure_worker()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    state shiba_internal.worker_state%ROWTYPE;
BEGIN
    SELECT * INTO state FROM shiba_internal.worker_state WHERE singleton;
    IF NOT state.active THEN
        RETURN;
    END IF;
    IF state.last_heartbeat IS NOT NULL
       AND state.last_heartbeat >= pg_postmaster_start_time()
       AND state.last_heartbeat >= clock_timestamp() - interval '5 seconds' THEN
        RETURN;
    END IF;
    IF state.last_requested_at IS NOT NULL
       AND state.last_requested_at >= pg_postmaster_start_time()
       AND state.last_requested_at >= clock_timestamp() - interval '5 seconds' THEN
        RETURN;
    END IF;

    PERFORM pg_advisory_xact_lock(8484, (SELECT oid::integer FROM pg_database WHERE datname = current_database()));
    SELECT * INTO state FROM shiba_internal.worker_state WHERE singleton FOR UPDATE;
    IF NOT state.active THEN
        RETURN;
    END IF;
    IF (state.last_heartbeat IS NULL
        OR state.last_heartbeat < pg_postmaster_start_time()
        OR state.last_heartbeat < clock_timestamp() - interval '5 seconds')
       AND (state.last_requested_at IS NULL
        OR state.last_requested_at < pg_postmaster_start_time()
        OR state.last_requested_at < clock_timestamp() - interval '5 seconds') THEN
        IF NOT shiba.start_worker() THEN
            RAISE EXCEPTION 'Shiba could not restart its database worker; increase max_worker_processes'
                USING ERRCODE = 'configuration_limit_exceeded';
        END IF;
        UPDATE shiba_internal.worker_state
        SET last_requested_at = clock_timestamp()
        WHERE singleton;
    END IF;
END;
$$;

CREATE FUNCTION shiba._lock_sources_for_analysis(analysis jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal, public
AS $$
DECLARE
    source_oid oid;
    source_name text;
BEGIN
    FOR source_oid IN
      SELECT DISTINCT oid
      FROM (
        SELECT (source ->> 'oid')::oid AS oid
        FROM jsonb_array_elements(analysis -> 'sources') source
        UNION ALL
        SELECT (subquery ->> 'source_oid')::oid
        FROM jsonb_array_elements(coalesce(analysis -> 'subqueries','[]'::jsonb)) subquery
      ) sources
      ORDER BY oid
    LOOP
      SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid=source_oid;
      EXECUTE format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE',source_name);
    END LOOP;
END;
$$;

CREATE FUNCTION shiba._prepare_stream_drops(result_relations oid[])
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,shiba_internal
AS $$
DECLARE
    result_relation oid;
    source_oid oid;
    source_name text;
BEGIN
    -- DAG execution takes this transaction advisory lock before touching
    -- state. Acquire every result lock globally before every globally sorted
    -- source lock, so overlapping multi-result DROP statements cannot invert.
    FOR result_relation IN
      SELECT DISTINCT result_oid
      FROM shiba_internal.stream_views
      WHERE result_oid=ANY(result_relations)
      ORDER BY result_oid
    LOOP
      PERFORM pg_advisory_xact_lock(result_relation::bigint);
    END LOOP;
    UPDATE shiba_internal.dag_worker_state
    SET active=false
    WHERE result_oid=ANY(result_relations);
    FOR source_oid IN
      SELECT DISTINCT oid FROM (
        SELECT stream.source_oid AS oid
        FROM shiba_internal.stream_views stream
        WHERE stream.result_oid=ANY(result_relations)
        UNION
        SELECT joined.right_source_oid
        FROM shiba_internal.inner_join_views joined
        WHERE joined.result_oid=ANY(result_relations)
      ) sources
      ORDER BY oid
    LOOP
      SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid=source_oid;
      EXECUTE format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE',source_name);
    END LOOP;
END;
$$;

CREATE FUNCTION shiba._prepare_stream_drop(result_relation oid)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path=pg_catalog,shiba_internal
AS $$
  SELECT shiba._prepare_stream_drops(ARRAY[result_relation])
$$;

CREATE FUNCTION shiba._request_worker()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_replication_slots
        WHERE slot_name = shiba_internal.slot_name()::text
    ) THEN
        PERFORM shiba._ensure_worker();
    END IF;
    -- Wakeup triggers are statement-level and never carry row data. Logical
    -- decoding remains the only data path, so there is no tuple to return.
    RETURN NULL;
END;
$$;

CREATE FUNCTION shiba._reject_source_truncate()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'TRUNCATE is not supported for a Shiba source; delete rows or drop dependent Shiba tables first'
        USING ERRCODE = 'feature_not_supported';
END;
$$;

CREATE FUNCTION shiba._validate_source_table(source_relation oid)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    valid_source boolean;
BEGIN
    SELECT c.relkind = 'r' AND c.relpersistence = 'p' AND n.nspname <> 'shiba'
    INTO valid_source
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = source_relation;
    IF valid_source IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'Shiba sources must be persistent ordinary tables outside the shiba schema'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    -- The MVP stores complete tuples as JSON.  Reject sources that can emit
    -- unchanged-TOAST markers instead of silently deriving an incorrect row.
    IF EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = source_relation AND attnum > 0 AND NOT attisdropped
          AND attstorage <> 'p'
    ) THEN
        RAISE EXCEPTION 'Shiba MVP does not support TOASTable source columns'
            USING ERRCODE = 'feature_not_supported';
    END IF;
END;
$$;

CREATE FUNCTION shiba._ensure_replica_identity_full(source_relation oid)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    source_name text;
    replica_identity "char";
BEGIN
    SELECT format('%I.%I', n.nspname, c.relname), c.relreplident
    INTO source_name, replica_identity
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = source_relation;
    IF replica_identity IS DISTINCT FROM 'f' THEN
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', source_name);
    END IF;
END;
$$;
