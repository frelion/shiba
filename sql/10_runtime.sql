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

-- Keep Shiba's runtime identity and DAG locks out of the common small-key
-- advisory-lock space. Advisory locks remain cooperative by design.
CREATE FUNCTION shiba_internal.identity_lock_namespace()
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
    SELECT 1397246274
$$;

CREATE FUNCTION shiba_internal.dag_lock_key(result_relation oid)
RETURNS bigint
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
    SELECT (1397246274::bigint << 32) | result_relation::bigint
$$;

-- Registration, activation, and deactivation are database-wide lifecycle
-- transitions.  They all take this transaction lock before inspecting or
-- changing lifecycle state, so a completed registration can never cross a
-- concurrently committed deactivation.
CREATE FUNCTION shiba._lock_database_lifecycle()
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
    SELECT pg_advisory_xact_lock(
        shiba_internal.identity_lock_namespace(), 1
    )
$$;

CREATE FUNCTION shiba._begin_stream_registration()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
BEGIN
    PERFORM shiba._lock_database_lifecycle();
    IF NOT EXISTS (
        SELECT 1
        FROM shiba_internal.runtime_state
        WHERE singleton AND active
    )
       AND NOT EXISTS (
        SELECT 1
        FROM pg_replication_slots
        WHERE slot_name=shiba_internal.slot_name()::text
    ) THEN
        RAISE EXCEPTION
            'Shiba is deactivated; call shiba.activate() before registering a result table'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname='shiba_publication'
    ) THEN
        RAISE EXCEPTION
            'Shiba is deactivated; call shiba.activate() before registering a result table'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END;
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

-- Persist one decoded delta in the shared payload.  Production routing appends
-- every row first, then canonicalizes and fans out the complete commit once.
-- The caller must first claim commit_lsn with _begin_route_transaction in this
-- same transaction.
CREATE FUNCTION shiba._route_change_log_delta(
    source_relation oid,
    row_data jsonb,
    delta integer,
    commit_lsn text,
    event_sequence integer
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
BEGIN
    INSERT INTO shiba_internal.change_log (
        commit_lsn,
        sequence,
        source_oid,
        delta,
        row_data
    )
    VALUES (
        commit_lsn::pg_lsn,
        event_sequence,
        source_relation,
        delta,
        row_data
    );
END;
$$;

-- Compatibility helper for direct SQL callers that route one event at a time.
-- The Runtime does not call this per row; it fans out once in
-- _canonicalize_change_log_commit after the complete transaction is present.
CREATE FUNCTION shiba._enqueue_change_log_source(
    source_relation oid,
    routed_lsn pg_lsn
)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
    INSERT INTO shiba_internal.dag_inbox (result_oid, commit_lsn)
    SELECT DISTINCT stream_view.result_oid, routed_lsn
    FROM shiba_internal.stream_views AS stream_view
    JOIN shiba_internal.view_progress AS progress
      ON progress.result_oid = stream_view.result_oid
    LEFT JOIN shiba_internal.inner_join_views AS join_view
      ON join_view.result_oid = stream_view.result_oid
    WHERE stream_view.activation_lsn < routed_lsn
      AND (
        progress.applied_lsn IS NULL
        OR progress.applied_lsn < routed_lsn
      )
      AND (
        stream_view.source_oid = source_relation
        OR join_view.right_source_oid = source_relation
      )
    ON CONFLICT DO NOTHING;
$$;

-- pgoutput transports tuple values as text. Normalize the shared payload once
-- per source relation after the complete commit has been routed, rather than
-- making every downstream DAG and operator decode every row again.
CREATE FUNCTION shiba._canonicalize_change_log_commit(p_commit_lsn pg_lsn)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    source_record record;
    source_name text;
BEGIN
    FOR source_record IN
      SELECT DISTINCT source_oid
      FROM shiba_internal.change_log
      WHERE commit_lsn=p_commit_lsn
    LOOP
      SELECT format('%I.%I',namespace.nspname,relation.relname)
      INTO STRICT source_name
      FROM pg_class relation
      JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
      WHERE relation.oid=source_record.source_oid;

      EXECUTE format(
        'UPDATE shiba_internal.change_log event
         SET row_data=to_jsonb(
           jsonb_populate_record(NULL::%s,event.row_data)
         )
         WHERE event.commit_lsn=$1
           AND event.source_oid=$2',
        source_name
      ) USING p_commit_lsn,source_record.source_oid;
    END LOOP;

    UPDATE shiba_internal.routed_transactions routed
    SET event_count=payload.event_count,
        payload_bytes=payload.payload_bytes
    FROM (
      SELECT count(*)::bigint AS event_count,
             coalesce(sum(pg_column_size(event.row_data)),0)::bigint
               AS payload_bytes
      FROM shiba_internal.change_log event
      WHERE event.commit_lsn=p_commit_lsn
    ) payload
    WHERE routed.commit_lsn=p_commit_lsn;

    -- One transaction-level fan-out replaces a catalog lookup and conflicting
    -- inbox insert for every source row in a large commit.
    INSERT INTO shiba_internal.dag_inbox (result_oid,commit_lsn)
    SELECT DISTINCT stream_view.result_oid,p_commit_lsn
    FROM (
      SELECT DISTINCT source_oid
      FROM shiba_internal.change_log
      WHERE commit_lsn=p_commit_lsn
    ) changed_source
    JOIN shiba_internal.stream_views stream_view
      ON stream_view.source_oid=changed_source.source_oid
    JOIN shiba_internal.view_progress progress
      ON progress.result_oid=stream_view.result_oid
    LEFT JOIN shiba_internal.inner_join_views join_view
      ON join_view.result_oid=stream_view.result_oid
    WHERE stream_view.activation_lsn<p_commit_lsn
      AND (
        progress.applied_lsn IS NULL
        OR progress.applied_lsn<p_commit_lsn
      )
    UNION
    SELECT DISTINCT stream_view.result_oid,p_commit_lsn
    FROM (
      SELECT DISTINCT source_oid
      FROM shiba_internal.change_log
      WHERE commit_lsn=p_commit_lsn
    ) changed_source
    JOIN shiba_internal.inner_join_views join_view
      ON join_view.right_source_oid=changed_source.source_oid
    JOIN shiba_internal.stream_views stream_view
      ON stream_view.result_oid=join_view.result_oid
    JOIN shiba_internal.view_progress progress
      ON progress.result_oid=stream_view.result_oid
    WHERE stream_view.activation_lsn<p_commit_lsn
      AND (
        progress.applied_lsn IS NULL
        OR progress.applied_lsn<p_commit_lsn
      )
    ON CONFLICT DO NOTHING;
END;
$$;

-- Delete only complete routed transactions for which no DAG reference remains.
-- Keep a short, time-bounded grace period so monitoring can observe completed
-- routing and a just-restarted Runtime can cheaply recognize replayed WAL.
-- Deleting the transaction header cascades to its shared change-log payload.
CREATE FUNCTION shiba._gc_change_log(max_transactions integer)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    deleted_count bigint;
BEGIN
    IF max_transactions IS NULL OR max_transactions < 1 THEN
        RAISE EXCEPTION 'max_transactions must be at least 1'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    WITH garbage AS (
        SELECT routed.commit_lsn
        FROM shiba_internal.routed_transactions AS routed
        WHERE routed.routed_at < clock_timestamp() - interval '1 second'
          AND NOT EXISTS (
            SELECT 1
            FROM shiba_internal.dag_inbox AS inbox
            WHERE inbox.commit_lsn = routed.commit_lsn
        )
        ORDER BY routed.commit_lsn
        LIMIT max_transactions
        FOR UPDATE SKIP LOCKED
    ),
    deleted AS (
        DELETE FROM shiba_internal.routed_transactions AS routed
        USING garbage
        WHERE routed.commit_lsn = garbage.commit_lsn
        RETURNING routed.commit_lsn
    )
    SELECT count(*) INTO deleted_count FROM deleted;

    RETURN deleted_count;
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
    PERFORM shiba._lock_database_lifecycle();
    -- Logical slot creation must precede transactional catalog writes in this
    -- transaction.  Publication creation is safe after the slot exists.
    PERFORM shiba._ensure_logical_slot();
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'shiba_publication'
    ) THEN
        CREATE PUBLICATION shiba_publication;
    END IF;
    UPDATE shiba_internal.runtime_state
    SET active = true,
        last_heartbeat = NULL,
        last_requested_at = NULL,
        pending_launch_xid = NULL,
        pending_since = NULL
    WHERE singleton AND NOT active;
    UPDATE shiba_internal.dag_runtime_state
    SET active = true
    WHERE NOT active AND failed_at IS NULL;
    -- These queue relations are born empty.  Seed zero-row statistics before
    -- the first source commit so PostgreSQL does not plan the first handful
    -- of Runtime transactions with its default unknown-table cardinality.
    ANALYZE shiba_internal.routed_transactions;
    ANALYZE shiba_internal.change_log;
    ANALYZE shiba_internal.dag_inbox;
    PERFORM shiba._ensure_runtime();
    RETURN true;
END;
$$;

CREATE FUNCTION shiba._ensure_runtime()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    state shiba_internal.runtime_state%ROWTYPE;
    launch_xid xid8;
    next_generation bigint;
BEGIN
    -- The Runtime owns this key as a session lock for its process lifetime.
    -- If the xact try-lock fails, a live Runtime or a concurrent launch request
    -- already owns the identity. Heartbeats are diagnostic only.
    IF NOT pg_try_advisory_xact_lock(
        shiba_internal.identity_lock_namespace(), 0
    ) THEN
        RETURN;
    END IF;
    SELECT * INTO state
    FROM shiba_internal.runtime_state
    WHERE singleton
    FOR UPDATE;
    IF NOT state.active THEN
        RETURN;
    END IF;
    launch_xid := pg_current_xact_id();
    -- A committed pending generation closes both launch races: repeated
    -- ensure calls in one transaction see their own xid and register once,
    -- while later transactions leave a recent launch for its BGW to claim.
    IF state.pending_launch_xid IS NOT NULL
       AND (
         state.pending_launch_xid = launch_xid
         OR state.pending_since > clock_timestamp() - interval '5 seconds'
       ) THEN
        RETURN;
    END IF;
    next_generation := state.launch_generation + 1;
    UPDATE shiba_internal.runtime_state
    SET owner_pid = NULL,
        started_at = NULL,
        last_heartbeat = NULL,
        last_requested_at = clock_timestamp(),
        launch_generation = next_generation,
        pending_launch_xid = launch_xid,
        pending_since = clock_timestamp()
    WHERE singleton;
    IF NOT shiba.start_runtime(next_generation) THEN
        RAISE EXCEPTION 'Shiba could not start its Runtime background worker; increase max_worker_processes'
            USING ERRCODE = 'configuration_limit_exceeded';
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
      PERFORM pg_advisory_xact_lock(shiba_internal.dag_lock_key(result_relation));
    END LOOP;
    UPDATE shiba_internal.dag_runtime_state
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

-- DDL that can remove a result indirectly (for example DROP SCHEMA/OWNED
-- ... CASCADE) cannot enumerate its final relation targets before PostgreSQL
-- dependency expansion. Acquire every DAG lifecycle lock first, in canonical
-- order, so the later sql_drop cleanup never reverses apply's DAG -> relation
-- lock order. This intentionally does not deactivate unrelated DAGs.
CREATE FUNCTION shiba_internal._lock_all_dags_for_utility()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,shiba_internal
AS $$
DECLARE
    result_relation oid;
BEGIN
    FOR result_relation IN
      SELECT result_oid
      FROM shiba_internal.stream_views
      ORDER BY result_oid
    LOOP
      PERFORM pg_advisory_xact_lock(
        shiba_internal.dag_lock_key(result_relation)
      );
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

CREATE FUNCTION shiba._request_runtime()
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
        PERFORM shiba._ensure_runtime();
        -- The Rust callback sets the Runtime latch only after this source
        -- transaction commits. Logical decoding therefore sees committed WAL
        -- immediately without reducing the fallback poll or carrying rows.
        PERFORM shiba.wake_runtime_on_commit(state.owner_pid)
        FROM shiba_internal.runtime_state AS state
        WHERE state.singleton
          AND state.active
          AND state.owner_pid IS NOT NULL;
    END IF;
    -- Wakeup triggers are statement-level and never carry row data. Logical
    -- decoding remains the only data path, so there is no tuple to return.
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION shiba.wake_runtime_on_commit(integer) FROM PUBLIC;

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
    source_has_rls boolean;
BEGIN
    SELECT
        c.relkind = 'r' AND c.relpersistence = 'p' AND n.nspname <> 'shiba',
        c.relrowsecurity OR c.relforcerowsecurity
    INTO valid_source, source_has_rls
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = source_relation;
    IF valid_source IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'Shiba sources must be persistent ordinary tables outside the shiba schema'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    IF source_has_rls THEN
        RAISE EXCEPTION
            'Shiba MVP does not support source tables with row-level security enabled or forced'
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
    -- State identity round-trips through typed JSONB in a dedicated Runtime
    -- session. Keep the MVP to built-in types whose input/output settings are
    -- explicitly normalized; locale-sensitive money and arbitrary user base
    -- types could otherwise encode the same value differently across sessions.
    IF EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid=source_relation
          AND attnum>0
          AND NOT attisdropped
          AND atttypid<>ALL(ARRAY[
            'boolean'::regtype,
            'smallint'::regtype,
            'integer'::regtype,
            'bigint'::regtype,
            'real'::regtype,
            'double precision'::regtype,
            'numeric'::regtype,
            'date'::regtype,
            'time without time zone'::regtype,
            'time with time zone'::regtype,
            'timestamp without time zone'::regtype,
            'timestamp with time zone'::regtype,
            'interval'::regtype,
            'uuid'::regtype,
            'pg_lsn'::regtype,
            'oid'::regtype,
            'name'::regtype,
            'text'::regtype,
            'character varying'::regtype,
            'character'::regtype,
            'bytea'::regtype,
            'bit'::regtype,
            'bit varying'::regtype,
            'inet'::regtype,
            'cidr'::regtype,
            'macaddr'::regtype,
            'macaddr8'::regtype,
            'json'::regtype,
            'jsonb'::regtype
          ]::oid[])
    ) THEN
        RAISE EXCEPTION
          'Shiba MVP source columns must use supported built-in identity types'
          USING ERRCODE='feature_not_supported';
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
