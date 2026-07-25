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

CREATE FUNCTION shiba.deactivate()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM shiba_internal.stream_views) THEN
        RAISE EXCEPTION 'drop all Shiba result tables before deactivation'
            USING ERRCODE = 'object_in_use';
    END IF;
    UPDATE shiba_internal.worker_state SET active = false WHERE singleton;
    UPDATE shiba_internal.dag_worker_state SET active = false;
    IF EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = shiba_internal.slot_name()::text
    ) THEN
        PERFORM pg_drop_replication_slot(shiba_internal.slot_name());
    END IF;
    DROP PUBLICATION IF EXISTS shiba_publication;
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
