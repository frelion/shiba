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

CREATE FUNCTION shiba._cleanup_dropped_managed_index()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    dropped record;
BEGIN
    FOR dropped IN
        SELECT *
        FROM pg_event_trigger_dropped_objects()
        WHERE object_type = 'index'
    LOOP
        DELETE FROM shiba_internal.managed_indexes
        WHERE index_oid = dropped.objid;
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER shiba_cleanup_dropped_managed_index
    ON sql_drop
    EXECUTE FUNCTION shiba._cleanup_dropped_managed_index();

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
    candidate_application_name text;
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

        -- In v2 the slot's active_pid is PostgreSQL's walsender, not Shiba's
        -- Runtime.  Signal only the Runtime identity owner; closing its libpq
        -- connection makes the paired walsender exit.
        candidate_pid := owner_pid_value;
        IF candidate_pid IS NOT NULL THEN
            SELECT backend_type, application_name, datid
            INTO candidate_backend_type, candidate_application_name, candidate_datid
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

        SELECT backend_type, application_name, datid
        INTO candidate_backend_type, candidate_application_name, candidate_datid
        FROM pg_stat_activity
        WHERE pid = slot_active_pid;
        IF FOUND
           AND (
               candidate_backend_type IS DISTINCT FROM 'walsender'
               OR candidate_application_name IS DISTINCT FROM 'shiba'
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
    DELETE FROM shiba_internal.ingress_transactions;
    DROP PUBLICATION IF EXISTS shiba_publication;
    PERFORM shiba_internal.retire_ingress_generation(
        shiba_internal.slot_name()
    );
    IF EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = shiba_internal.slot_name()::text
    ) THEN
        PERFORM pg_drop_replication_slot(shiba_internal.slot_name());
    END IF;
END;
$$;

-- Result relations are owned by the extension owner so that the Runtime can
-- update them through the protected DML path.  A separately authorized index
-- manager can add a bounded set of workload-specific access paths without
-- becoming the table owner.  Keep this API deliberately narrower than CREATE
-- INDEX: only fixed-width built-in types with a default B-tree operator class
-- are accepted, and the conservative total key-width bound guarantees that a
-- future value cannot exceed PostgreSQL's B-tree index-tuple size limit.
CREATE FUNCTION shiba.create_index(
    result_table regclass,
    index_name text,
    index_columns text[]
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    result_schema name;
    result_relation_name text;
    column_sql text;
    invoker_oid oid;
    supported_column_count integer;
    fixed_key_bytes integer;
    created_index_oid oid;
    maximum_identifier_length integer :=
        current_setting('max_identifier_length')::integer;
BEGIN
    -- Privileged DDL relation locks live until transaction end.  Requiring an
    -- autocommit top-level call prevents a caller from retaining them in an
    -- idle explicit transaction after this function returns.
    PERFORM shiba.require_index_ddl_top_level();
    invoker_oid := shiba.index_ddl_invoker();

    IF result_table IS NULL
       OR index_name IS NULL
       OR btrim(index_name) = ''
       OR octet_length(index_name) > maximum_identifier_length
       OR index_columns IS NULL
       OR cardinality(index_columns) = 0
       OR cardinality(index_columns) > 8
       OR array_ndims(index_columns) IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'invalid Shiba result index specification'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM shiba_internal.stream_views
        WHERE result_oid = result_table::oid
    ) THEN
        RAISE EXCEPTION 'relation % is not a Shiba result table', result_table
            USING ERRCODE = 'wrong_object_type';
    END IF;

    IF NOT has_table_privilege(invoker_oid, result_table, 'SELECT') THEN
        RAISE EXCEPTION
            'role % must have SELECT on Shiba result table % to create an index',
            pg_get_userbyid(invoker_oid),
            result_table
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM unnest(index_columns) AS requested(column_name)
        WHERE requested.column_name IS NULL
           OR btrim(requested.column_name) = ''
    ) OR EXISTS (
        SELECT 1
        FROM unnest(index_columns) AS requested(column_name)
        GROUP BY requested.column_name
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'Shiba result index columns must be non-empty and distinct'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM pg_advisory_xact_lock(
        shiba_internal.dag_lock_key(result_table::oid)
    );

    -- Recheck identity and authorization after taking the same DAG lock used
    -- by apply and result DROP.  Revoking SELECT while this call waits takes
    -- effect before privileged DDL begins.
    PERFORM 1
    FROM shiba_internal.stream_views
    WHERE result_oid = result_table::oid
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'relation % is not a Shiba result table', result_table
            USING ERRCODE = 'wrong_object_type';
    END IF;

    IF NOT has_table_privilege(invoker_oid, result_table, 'SELECT') THEN
        RAISE EXCEPTION
            'role % must have SELECT on Shiba result table % to create an index',
            pg_get_userbyid(invoker_oid),
            result_table
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    IF (
        SELECT count(*)
        FROM shiba_internal.managed_indexes
        WHERE result_oid = result_table::oid
    ) >= 8 THEN
        RAISE EXCEPTION
            'Shiba result table % already has the maximum of 8 managed indexes',
            result_table
            USING ERRCODE = 'configuration_limit_exceeded';
    END IF;

    SELECT namespace.nspname,
           format('%I.%I', namespace.nspname, relation.relname)
    INTO STRICT result_schema, result_relation_name
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace
      ON namespace.oid = relation.relnamespace
    WHERE relation.oid = result_table::oid
      AND relation.relkind = 'r'
      AND namespace.nspname = 'shiba';

    IF EXISTS (
        SELECT 1
        FROM unnest(index_columns) AS requested(column_name)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_attribute AS attribute
            WHERE attribute.attrelid = result_table::oid
              AND attribute.attname = requested.column_name
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
        )
    ) THEN
        RAISE EXCEPTION
            'one or more Shiba result index columns do not exist on %',
            result_table
            USING ERRCODE = 'undefined_column';
    END IF;

    SELECT count(*),
           coalesce(sum(type_catalog.typlen), 0)::integer
    INTO STRICT supported_column_count, fixed_key_bytes
    FROM unnest(index_columns) AS requested(column_name)
    JOIN pg_attribute AS attribute
      ON attribute.attrelid = result_table::oid
     AND attribute.attname = requested.column_name
     AND attribute.attnum > 0
     AND NOT attribute.attisdropped
    JOIN pg_type AS type_catalog
      ON type_catalog.oid = attribute.atttypid
    JOIN pg_namespace AS type_namespace
      ON type_namespace.oid = type_catalog.typnamespace
    WHERE type_namespace.nspname = 'pg_catalog'
      AND type_catalog.typlen > 0
      AND EXISTS (
          SELECT 1
          FROM pg_opclass AS operator_class
          JOIN pg_am AS access_method
            ON access_method.oid = operator_class.opcmethod
          WHERE access_method.amname = 'btree'
            AND operator_class.opcdefault
            AND operator_class.opcintype = attribute.atttypid
      );

    IF supported_column_count <> cardinality(index_columns)
       OR fixed_key_bytes > 1024 THEN
        RAISE EXCEPTION
            'Shiba managed indexes require at most 1024 bytes of fixed-width built-in B-tree columns'
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Index only fixed-width pg_catalog types; variable-width text, bytea, numeric, arrays, JSON, and user-defined types are not accepted.';
    END IF;

    SELECT string_agg(
               format('%I', requested.column_name),
               ', ' ORDER BY requested.ordinality
           )
    INTO STRICT column_sql
    FROM unnest(index_columns) WITH ORDINALITY
        AS requested(column_name, ordinality);

    -- PostgreSQL places an index in its parent table's namespace.  The index
    -- name is therefore intentionally not schema-qualified here.
    EXECUTE format(
        'CREATE INDEX %I ON %s USING btree (%s)',
        index_name,
        result_relation_name,
        column_sql
    );

    SELECT index_class.oid
    INTO STRICT created_index_oid
    FROM pg_class AS index_class
    JOIN pg_namespace AS index_namespace
      ON index_namespace.oid = index_class.relnamespace
    JOIN pg_index AS index_catalog
      ON index_catalog.indexrelid = index_class.oid
    WHERE index_namespace.nspname = result_schema
      AND index_class.relname = index_name
      AND index_class.relkind = 'i'
      AND index_catalog.indrelid = result_table::oid
      AND NOT index_catalog.indisunique;

    INSERT INTO shiba_internal.managed_indexes (
        index_oid,
        result_oid,
        index_name,
        index_columns,
        creator_oid
    )
    VALUES (
        created_index_oid,
        result_table::oid,
        index_name::name,
        index_columns::name[],
        invoker_oid
    );
END;
$$;

-- Drop only indexes previously created through the managed API, and only for
-- their creator (or the extension owner).  Lock the index by OID before
-- re-reading its live identity so a concurrent trusted DDL cannot substitute a
-- different same-name object between validation and DROP INDEX.
CREATE FUNCTION shiba.drop_index(index_relation regclass)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    parent_relation oid;
    index_schema name;
    index_name name;
    is_unique boolean;
    is_primary boolean;
    is_exclusion boolean;
    creator_oid oid;
    invoker_oid oid;
    extension_owner_oid oid;
BEGIN
    PERFORM shiba.require_index_ddl_top_level();
    invoker_oid := shiba.index_ddl_invoker();

    IF index_relation IS NULL THEN
        RAISE EXCEPTION 'index relation cannot be NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT managed.result_oid
    INTO parent_relation
    FROM shiba_internal.managed_indexes AS managed
    WHERE managed.index_oid = index_relation::oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'index % is not a Shiba-managed user index', index_relation
            USING ERRCODE = 'feature_not_supported';
    END IF;

    PERFORM pg_advisory_xact_lock(
        shiba_internal.dag_lock_key(parent_relation)
    );
    PERFORM shiba.lock_index_ddl_target(index_relation::oid);

    SELECT index_catalog.indrelid,
           index_namespace.nspname,
           index_class.relname,
           index_catalog.indisunique,
           index_catalog.indisprimary,
           index_catalog.indisexclusion,
           managed.creator_oid
    INTO
        parent_relation,
        index_schema,
        index_name,
        is_unique,
        is_primary,
        is_exclusion,
        creator_oid
    FROM shiba_internal.managed_indexes AS managed
    JOIN pg_class AS index_class
      ON index_class.oid = managed.index_oid
     AND index_class.relname = managed.index_name
    JOIN pg_namespace AS index_namespace
      ON index_namespace.oid = index_class.relnamespace
    JOIN pg_index AS index_catalog
      ON index_catalog.indexrelid = index_class.oid
     AND index_catalog.indrelid = managed.result_oid
    JOIN shiba_internal.stream_views AS stream_view
      ON stream_view.result_oid = managed.result_oid
    WHERE managed.index_oid = index_relation::oid
      AND index_class.relkind = 'i'
    FOR UPDATE OF managed;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'managed index % changed identity before it could be dropped', index_relation
            USING ERRCODE = 'wrong_object_type';
    END IF;

    IF NOT has_table_privilege(invoker_oid, parent_relation, 'SELECT') THEN
        RAISE EXCEPTION
            'role % must have SELECT on the Shiba result table to drop index %',
            pg_get_userbyid(invoker_oid),
            index_relation
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    SELECT extowner
    INTO STRICT extension_owner_oid
    FROM pg_extension
    WHERE extname = 'shiba';
    IF invoker_oid <> creator_oid
       AND invoker_oid <> extension_owner_oid THEN
        RAISE EXCEPTION
            'role % did not create Shiba managed index %',
            pg_get_userbyid(invoker_oid),
            index_relation
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    IF is_unique
       OR is_primary
       OR is_exclusion
       OR EXISTS (
           SELECT 1
           FROM pg_constraint AS constraint_catalog
           WHERE constraint_catalog.conindid = index_relation::oid
       ) THEN
        RAISE EXCEPTION
            'cannot drop constraint-backed or unique Shiba result index %',
            index_relation
            USING ERRCODE = 'feature_not_supported';
    END IF;

    EXECUTE format(
        'DROP INDEX %I.%I',
        index_schema,
        index_name
    );
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
    CREATE PUBLICATION shiba_publication
      WITH (publish = 'insert, update, delete');
END;
$$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA shiba FROM PUBLIC;
GRANT EXECUTE ON FUNCTION shiba.progress(regclass) TO PUBLIC;
GRANT EXECUTE ON FUNCTION shiba.resume(regclass) TO PUBLIC;
