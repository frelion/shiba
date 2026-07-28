-- Stage relations are owned by a physical plan rather than by a user session.
-- Drop them in canonical stage order before deleting their metadata.  Looking
-- the relation name up by OID prevents stale metadata from naming an unrelated
-- object after a failed or externally forced DDL operation.
CREATE FUNCTION shiba_internal._drop_physical_plan_stages(result_relation oid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    stage record;
    actual_schema name;
    actual_relation name;
    actual_persistence "char";
BEGIN
    IF result_relation IS NULL THEN
        RAISE EXCEPTION 'result relation cannot be NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM pg_advisory_xact_lock(
        shiba_internal.dag_lock_key(result_relation)
    );

    FOR stage IN
        SELECT physical_stage.plan_id,
               physical_stage.stage_id,
               physical_stage.relation_oid,
               physical_stage.relation_name
        FROM shiba_internal.physical_stages AS physical_stage
        JOIN shiba_internal.physical_plans AS physical_plan
          ON physical_plan.result_oid = physical_stage.result_oid
         AND physical_plan.plan_id = physical_stage.plan_id
        WHERE physical_stage.result_oid = result_relation
          AND physical_stage.storage = 'unlogged'
        ORDER BY physical_stage.stage_id
    LOOP
        SELECT namespace_catalog.nspname,
               class_catalog.relname,
               class_catalog.relpersistence
        INTO actual_schema, actual_relation, actual_persistence
        FROM pg_class AS class_catalog
        JOIN pg_namespace AS namespace_catalog
          ON namespace_catalog.oid = class_catalog.relnamespace
        WHERE class_catalog.oid = stage.relation_oid
          AND class_catalog.relkind = 'r';

        IF NOT FOUND THEN
            CONTINUE;
        END IF;
        IF actual_schema <> 'shiba_internal'::name
           OR actual_relation <> stage.relation_name
           OR actual_persistence <> 'u'::"char" THEN
            RAISE EXCEPTION
                'physical Stage metadata for result %, plan %, stage % does not identify its UNLOGGED relation',
                result_relation,
                stage.plan_id,
                stage.stage_id
                USING ERRCODE='P0S01';
        END IF;

        EXECUTE format(
            'DROP TABLE %I.%I',
            actual_schema,
            actual_relation
        );
    END LOOP;

    DELETE FROM shiba_internal.physical_plans
    WHERE result_oid = result_relation;
END;
$$;

CREATE FUNCTION shiba._cleanup_dropped_stream_table()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    dropped record;
    stream_view shiba_internal.stream_views%ROWTYPE;
    join_view shiba_internal.inner_join_views%ROWTYPE;
    source_name text;
    right_source_name text;
    right_oid_value oid;
    source_still_used boolean;
    right_source_still_used boolean;
BEGIN
    FOR dropped IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        -- ProcessUtility catches explicit DROP TABLE targets before locks are
        -- taken. This event-trigger check also catches tables deleted
        -- indirectly by DROP SCHEMA/OWNED/EXTENSION ... CASCADE.
        IF dropped.object_type = 'table'
           AND EXISTS (
             SELECT 1
             FROM shiba_internal.stream_views
             WHERE source_oid=dropped.objid
             UNION ALL
             SELECT 1
             FROM shiba_internal.inner_join_views
             WHERE right_source_oid=dropped.objid
           ) THEN
            RAISE EXCEPTION
              'cannot drop Shiba source % indirectly; drop dependent Shiba tables first',
              dropped.object_identity
              USING ERRCODE='object_not_in_prerequisite_state';
        END IF;
        IF dropped.object_type <> 'table' OR dropped.schema_name <> 'shiba' THEN
            CONTINUE;
        END IF;

        SELECT *
        INTO stream_view
        FROM shiba_internal.stream_views
        WHERE result_oid = dropped.objid;
        IF NOT FOUND THEN
            CONTINUE;
        END IF;

        SELECT format('%I.%I', source_namespace.nspname, source.relname)
        INTO source_name
        FROM pg_class AS source
        JOIN pg_namespace AS source_namespace ON source_namespace.oid = source.relnamespace
        WHERE source.oid = stream_view.source_oid;
        SELECT * INTO join_view FROM shiba_internal.inner_join_views WHERE result_oid = stream_view.result_oid;
        IF FOUND THEN
            right_oid_value := join_view.right_source_oid;
            SELECT format('%I.%I', source_namespace.nspname, source.relname)
            INTO right_source_name
            FROM pg_class AS source
            JOIN pg_namespace AS source_namespace ON source_namespace.oid = source.relnamespace
            WHERE source.oid = join_view.right_source_oid;
            IF source_name IS NOT NULL THEN
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', format('shiba_wakeup_%s_left', stream_view.result_oid), source_name);
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', format('shiba_no_truncate_%s_left', stream_view.result_oid), source_name);
            END IF;
            IF right_source_name IS NOT NULL THEN
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', format('shiba_wakeup_%s_right', stream_view.result_oid), right_source_name);
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', format('shiba_no_truncate_%s_right', stream_view.result_oid), right_source_name);
            END IF;
        ELSIF source_name IS NOT NULL THEN
            EXECUTE format(
                'DROP TRIGGER IF EXISTS %I ON %s',
                format('shiba_wakeup_%s', stream_view.result_oid),
                source_name
            );
            EXECUTE format(
                'DROP TRIGGER IF EXISTS %I ON %s',
                format('shiba_no_truncate_%s', stream_view.result_oid),
                source_name
            );
        END IF;

        -- Stage relations are not children of the user-visible result table.
        -- Remove them explicitly before result-scoped catalog rows cascade.
        PERFORM shiba_internal._drop_physical_plan_stages(
            stream_view.result_oid
        );
        DELETE FROM shiba_internal.stream_views
        WHERE result_oid = stream_view.result_oid;

        SELECT EXISTS (
            SELECT 1 FROM shiba_internal.stream_views WHERE source_oid = stream_view.source_oid
            UNION ALL
            SELECT 1 FROM shiba_internal.inner_join_views WHERE right_source_oid = stream_view.source_oid
        ) INTO source_still_used;
        IF NOT source_still_used AND source_name IS NOT NULL THEN
            EXECUTE format('ALTER PUBLICATION shiba_publication DROP TABLE %s', source_name);
        END IF;
        IF right_oid_value IS NOT NULL THEN
            SELECT EXISTS (
                SELECT 1 FROM shiba_internal.stream_views WHERE source_oid = right_oid_value
                UNION ALL
                SELECT 1 FROM shiba_internal.inner_join_views WHERE right_source_oid = right_oid_value
            ) INTO right_source_still_used;
            IF NOT right_source_still_used AND right_source_name IS NOT NULL THEN
                EXECUTE format('ALTER PUBLICATION shiba_publication DROP TABLE %s', right_source_name);
            END IF;
        END IF;
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER shiba_cleanup_dropped_stream_table
    ON sql_drop
    EXECUTE FUNCTION shiba._cleanup_dropped_stream_table();

CREATE FUNCTION shiba._guard_source_table_alter()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    command record;
BEGIN
    FOR command IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF command.object_type = 'table'
           AND EXISTS (
               SELECT 1
               FROM shiba_internal.stream_views
               WHERE source_oid = command.objid
               UNION ALL
               SELECT 1
               FROM shiba_internal.inner_join_views
               WHERE right_source_oid = command.objid
           ) THEN
            RAISE EXCEPTION
                'cannot ALTER TABLE % while it is a Shiba source; drop dependent Shiba tables first',
                command.object_identity
                USING ERRCODE = 'object_not_in_prerequisite_state';
        END IF;
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER shiba_guard_source_table_alter
    ON ddl_command_end
    WHEN TAG IN ('ALTER TABLE')
    EXECUTE FUNCTION shiba._guard_source_table_alter();

CREATE FUNCTION shiba.progress(result_table regclass)
RETURNS TABLE (
    applied_lsn pg_lsn,
    pending_wal_bytes bigint,
    updated_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
    SELECT
        progress.applied_lsn,
        GREATEST(
            pg_wal_lsn_diff(pg_current_wal_lsn(), slot.confirmed_flush_lsn)::bigint,
            0
        ) AS pending_wal_bytes,
        progress.updated_at
    FROM shiba_internal.view_progress AS progress
    JOIN pg_replication_slots AS slot
        ON slot.slot_name = shiba_internal.slot_name()::text
    WHERE progress.result_oid = result_table::oid
$$;

-- Resume only a DAG paused by a configured resource ceiling.  Deterministic
-- operator failures remain quarantined and require fixing/re-registering the
-- definition rather than blindly replaying the same poison commit.
CREATE FUNCTION shiba.resume(result_table regclass)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    resumed boolean;
    dag_creator_oid oid;
BEGIN
    SELECT creator_oid INTO dag_creator_oid
    FROM shiba_internal.stream_views
    WHERE result_oid=result_table::oid;
    IF dag_creator_oid IS NULL THEN
      RETURN false;
    END IF;
    IF session_user::regrole::oid<>dag_creator_oid
       AND NOT has_table_privilege(session_user,result_table,'UPDATE') THEN
      RAISE EXCEPTION 'permission denied to resume Shiba DAG %',result_table
        USING ERRCODE = 'insufficient_privilege';
    END IF;
    PERFORM pg_advisory_xact_lock(
      shiba_internal.dag_lock_key(result_table::oid)
    );
    UPDATE shiba_internal.dag_runtime_state
    SET active = true,
        last_error = NULL,
        failed_at = NULL
    WHERE result_oid = result_table::oid
      AND NOT active
      AND last_error LIKE '[53400] %'
    RETURNING true INTO resumed;
    IF coalesce(resumed,false) THEN
      PERFORM shiba._ensure_runtime();
    END IF;
    RETURN coalesce(resumed,false);
END;
$$;

-- Stop the current Runtime without waiting for it to observe transactional
-- catalog changes.  Once this function acquires the Runtime identity as an
-- xact lock, no existing or replacement Runtime can claim that identity before
-- the surrounding deactivation transaction commits.
CREATE FUNCTION shiba._stop_runtime_for_deactivation()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    graceful_deadline timestamptz := clock_timestamp() + interval '1 second';
    deadline timestamptz := clock_timestamp() + interval '15 seconds';
    owner_pid_value integer;
    slot_active_pid integer;
    candidate_pid integer;
    candidate_backend_type text;
    candidate_datid oid;
    escalated boolean := false;
BEGIN
    LOOP
        IF pg_try_advisory_xact_lock(
            shiba_internal.identity_lock_namespace(), 0
        ) THEN
            EXIT;
        END IF;

        SELECT owner_pid
        INTO owner_pid_value
        FROM shiba_internal.runtime_state
        WHERE singleton;

        SELECT active_pid
        INTO slot_active_pid
        FROM pg_replication_slots
        WHERE slot_name = shiba_internal.slot_name()::text;

        -- Prefer the slot's live owner over catalog state, which can lag a
        -- process exit.  Outside routing, owner_pid identifies the Runtime.
        candidate_pid := coalesce(slot_active_pid, owner_pid_value);
        IF candidate_pid IS NOT NULL THEN
            SELECT backend_type, datid
            INTO candidate_backend_type, candidate_datid
            FROM pg_stat_activity
            WHERE pid = candidate_pid;

            IF FOUND THEN
                IF candidate_pid = pg_backend_pid()
                   OR candidate_backend_type IS DISTINCT FROM 'shiba runtime'
                   OR candidate_datid IS DISTINCT FROM (
                       SELECT oid FROM pg_database WHERE datname = current_database()
                   ) THEN
                    RAISE EXCEPTION
                        'cannot deactivate Shiba while Runtime identity is owned by unexpected backend PID % (%)',
                        candidate_pid,
                        coalesce(candidate_backend_type, 'unknown')
                        USING ERRCODE = 'object_in_use';
                END IF;
                IF NOT escalated AND clock_timestamp() < graceful_deadline THEN
                    -- SIGINT is the Runtime's graceful lifecycle-stop signal.
                    -- It returns normally, so the dynamic BGW registration
                    -- does not schedule a crash replacement.
                    PERFORM pg_cancel_backend(candidate_pid);
                ELSE
                    -- A phase stuck inside PostgreSQL may not reach the outer
                    -- SIGINT check. Escalate to standard backend SIGTERM so
                    -- the active transaction aborts at an interrupt point.
                    -- Its crash replacement cannot claim the identity while
                    -- this deactivation transaction holds the xact lock.
                    escalated := true;
                    PERFORM pg_terminate_backend(candidate_pid);
                END IF;
            END IF;
        END IF;

        IF clock_timestamp() >= deadline THEN
            RAISE EXCEPTION
                'timed out stopping the Shiba Runtime during deactivation'
                USING ERRCODE = 'lock_not_available';
        END IF;
        PERFORM pg_sleep(0.01);
    END LOOP;

    -- The Runtime identity is now held by this transaction.  A slot user at
    -- this point cannot be the valid Shiba Runtime, so wait for termination
    -- cleanup and reject an unrelated logical-decoding client rather than
    -- signaling an arbitrary PostgreSQL backend.
    LOOP
        SELECT active_pid
        INTO slot_active_pid
        FROM pg_replication_slots
        WHERE slot_name = shiba_internal.slot_name()::text;
        EXIT WHEN slot_active_pid IS NULL;

        SELECT backend_type, datid
        INTO candidate_backend_type, candidate_datid
        FROM pg_stat_activity
        WHERE pid = slot_active_pid;
        IF FOUND
           AND (
               candidate_backend_type IS DISTINCT FROM 'shiba runtime'
               OR candidate_datid IS DISTINCT FROM (
                   SELECT oid FROM pg_database WHERE datname = current_database()
               )
           ) THEN
            RAISE EXCEPTION
                'cannot deactivate Shiba while logical slot % is used by backend PID % (%)',
                shiba_internal.slot_name(),
                slot_active_pid,
                coalesce(candidate_backend_type, 'unknown')
                USING ERRCODE = 'object_in_use';
        END IF;

        IF clock_timestamp() >= deadline THEN
            RAISE EXCEPTION
                'timed out waiting for Shiba logical slot % to become inactive',
                shiba_internal.slot_name()
                USING ERRCODE = 'lock_not_available';
        END IF;
        PERFORM pg_sleep(0.01);
    END LOOP;
END;
$$;

CREATE FUNCTION shiba.deactivate()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
BEGIN
    PERFORM shiba._lock_database_lifecycle();
    IF EXISTS (SELECT 1 FROM shiba_internal.stream_views) THEN
        RAISE EXCEPTION 'drop all Shiba result tables before deactivation'
            USING ERRCODE = 'object_in_use';
    END IF;
    PERFORM shiba._stop_runtime_for_deactivation();

    UPDATE shiba_internal.runtime_state
    SET active = false,
        owner_pid = NULL,
        started_at = NULL,
        last_heartbeat = NULL,
        pending_launch_xid = NULL,
        pending_since = NULL
    WHERE singleton;
    UPDATE shiba_internal.dag_runtime_state SET active = false;
    DELETE FROM shiba_internal.routed_transactions;
    DROP PUBLICATION IF EXISTS shiba_publication;
    IF EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = shiba_internal.slot_name()::text
    ) THEN
        PERFORM pg_drop_replication_slot(shiba_internal.slot_name());
    END IF;
END;
$$;

DO $$
BEGIN
    IF current_setting('wal_level') <> 'logical' THEN
        RAISE EXCEPTION 'Shiba requires wal_level=logical; set it in postgresql.conf and restart PostgreSQL'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF current_setting('max_replication_slots')::integer < 1 THEN
        RAISE EXCEPTION 'Shiba requires max_replication_slots >= 1'
            USING ERRCODE = 'configuration_limit_exceeded';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'shiba_publication') THEN
        RAISE EXCEPTION 'publication shiba_publication already exists; choose a clean database or remove the conflicting publication'
            USING ERRCODE = 'duplicate_object';
    END IF;
    CREATE PUBLICATION shiba_publication;
END;
$$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA shiba FROM PUBLIC;
GRANT EXECUTE ON FUNCTION shiba.progress(regclass) TO PUBLIC;
GRANT EXECUTE ON FUNCTION shiba.resume(regclass) TO PUBLIC;
