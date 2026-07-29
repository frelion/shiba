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

-- Keep Shiba's runtime identity and dataflow locks out of the common small-key
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

CREATE FUNCTION shiba_internal.dataflow_lock_key(result_relation oid)
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

CREATE FUNCTION shiba._begin_dataflow_registration()
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

-- Delete only slot-confirmed ingress transactions whose source effects are
-- publication-safe.  Typed stream payload has its own consumer-driven GC;
-- deleting this staging copy cannot remove a published effect.
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
        SELECT txn.ingress_txn_id
        FROM shiba_internal.ingress_transactions AS txn
        JOIN shiba_internal.ingress_replay_state AS replay
          ON replay.slot_generation = txn.slot_generation
        WHERE txn.status = 'committed'
          AND txn.finalized_at < clock_timestamp()
              - pg_catalog.current_setting('shiba.ingress_retention')::interval
          AND replay.replay_safe_lsn >= txn.end_lsn
          AND replay.published_lsn >= txn.final_lsn
          AND txn.pending_publications = 0
        ORDER BY txn.commit_lsn
        LIMIT max_transactions
        FOR UPDATE SKIP LOCKED
    ),
    deleted AS (
        DELETE FROM shiba_internal.ingress_transactions AS txn
        USING garbage
        WHERE txn.ingress_txn_id = garbage.ingress_txn_id
        RETURNING txn.ingress_txn_id
    )
    SELECT count(*) INTO deleted_count FROM deleted;

    RETURN deleted_count;
END;
$$;

CREATE FUNCTION shiba._ensure_logical_slot()
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = shiba_internal.slot_name()::text
    ) THEN
        PERFORM pg_create_logical_replication_slot(shiba_internal.slot_name(), 'pgoutput');
        RETURN true;
    END IF;
    RETURN false;
END;
$$;

CREATE FUNCTION shiba.activate()
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    slot_created boolean;
BEGIN
    PERFORM shiba._lock_database_lifecycle();
    IF nullif(
        pg_catalog.current_setting('shiba.replication_conninfo', true),
        ''
    ) IS NULL THEN
        RAISE EXCEPTION
            'shiba.replication_conninfo must be configured before activation'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    -- The empty publication is created with the extension and survives
    -- deactivation. A slot baseline must never precede publication creation:
    -- pgoutput would decode that catalog transaction with a snapshot in which
    -- the publication does not yet exist.
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'shiba_publication'
    ) THEN
        RAISE EXCEPTION
            'publication shiba_publication is missing; recreate the extension-owned publication before activation'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    -- Logical slot creation must precede the transactional ingress catalog
    -- writes below.
    slot_created := shiba._ensure_logical_slot();
    PERFORM shiba_internal.ensure_ingress_generation(
        shiba_internal.slot_name(),
        slot_created
    );
    IF EXISTS (
        SELECT 1
        FROM pg_publication
        WHERE pubname = 'shiba_publication'
          AND pubtruncate
    ) THEN
        ALTER PUBLICATION shiba_publication
          SET (publish = 'insert, update, delete');
    END IF;
    UPDATE shiba_internal.runtime_state
    SET active = true,
        last_heartbeat = NULL,
        pending_launch_xid = NULL,
        pending_since = NULL
    WHERE singleton AND NOT active;
    UPDATE shiba_internal.dataflows
    SET active = true
    WHERE NOT active;
    -- These queue relations are born empty.  Seed zero-row statistics before
    -- the first source commit so PostgreSQL does not plan the first handful
    -- of Runtime transactions with its default unknown-table cardinality.
    ANALYZE shiba_internal.ingress_transactions;
    ANALYZE shiba_internal.change_log;
    ANALYZE shiba_internal.source_publications;
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

CREATE FUNCTION shiba._prepare_dataflow_drops(result_relations oid[])
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
    -- Dataflow execution takes this transaction advisory lock before touching
    -- state. Acquire every result lock globally before every globally sorted
    -- source lock, so overlapping multi-result DROP statements cannot invert.
    FOR result_relation IN
      SELECT DISTINCT result_oid
      FROM shiba_internal.dataflows
      WHERE result_oid=ANY(result_relations)
      ORDER BY result_oid
    LOOP
      PERFORM pg_advisory_xact_lock(
        shiba_internal.dataflow_lock_key(result_relation)
      );
    END LOOP;
    UPDATE shiba_internal.dataflows
    SET active=false
    WHERE result_oid=ANY(result_relations);
    FOR source_oid IN
      SELECT DISTINCT source.source_oid
      FROM shiba_internal.dataflow_sources AS source
      WHERE source.result_oid=ANY(result_relations)
      ORDER BY source.source_oid
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
-- dependency expansion. Acquire every dataflow lifecycle lock first, in
-- canonical order, so the later sql_drop cleanup never reverses apply's
-- dataflow -> relation lock order. This intentionally does not deactivate
-- unrelated dataflows.
CREATE FUNCTION shiba_internal._lock_all_dataflows_for_utility()
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
      FROM shiba_internal.dataflows
      ORDER BY result_oid
    LOOP
      PERFORM pg_advisory_xact_lock(
        shiba_internal.dataflow_lock_key(result_relation)
      );
    END LOOP;
END;
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
        -- immediately without reducing the idle poll or carrying rows.
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
            'Shiba sources cannot use row-level security'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    -- Ingress reconstructs unchanged TOAST columns from the UPDATE old tuple.
    -- Generated storage and scalar evaluation accept PostgreSQL's built-in
    -- types; user-defined type I/O is outside the trusted execution boundary.
    IF EXISTS (
        SELECT 1
        FROM pg_attribute AS attribute
        JOIN pg_type AS type_catalog
          ON type_catalog.oid = attribute.atttypid
        JOIN pg_namespace AS namespace
          ON namespace.oid = type_catalog.typnamespace
        WHERE attribute.attrelid = source_relation
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
          AND namespace.nspname <> 'pg_catalog'
    ) THEN
        RAISE EXCEPTION
          'Shiba source columns must use pg_catalog types'
          USING ERRCODE='feature_not_supported';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_attribute AS attribute
        JOIN pg_collation AS collation_catalog
          ON collation_catalog.oid = attribute.attcollation
        JOIN pg_namespace AS namespace
          ON namespace.oid = collation_catalog.collnamespace
        WHERE attribute.attrelid = source_relation
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
          AND attribute.attcollation <> 0::oid
          AND namespace.nspname <> 'pg_catalog'
    ) THEN
        RAISE EXCEPTION
          'Shiba source columns must use pg_catalog collations'
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
