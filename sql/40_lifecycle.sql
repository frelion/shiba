-- Generated object OIDs are authoritative. Resolve a live name only after
-- verifying that the OID still denotes a LOGGED table in shiba_internal.
CREATE FUNCTION shiba_internal.drop_cataloged_relation(relation_oid oid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    live_schema name;
    live_name name;
    live_kind "char";
    live_persistence "char";
BEGIN
    SELECT namespace.nspname,
           relation.relname,
           relation.relkind,
           relation.relpersistence
    INTO live_schema, live_name, live_kind, live_persistence
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace
      ON namespace.oid = relation.relnamespace
    WHERE relation.oid = relation_oid;

    IF NOT FOUND THEN
      RETURN;
    END IF;
    IF live_schema <> 'shiba_internal'::name
       OR live_kind <> 'r'::"char"
       OR live_persistence <> 'p'::"char" THEN
      RAISE EXCEPTION 'cataloged relation % changed identity', relation_oid
        USING ERRCODE = 'data_corrupted';
    END IF;

    EXECUTE format('DROP TABLE %I.%I', live_schema, live_name);
END;
$$;

-- A stream payload owns both its LOGGED table and generated composite. Kernel
-- state is dropped first by drop_dataflow_storage, so no live relation can
-- still depend on the row type.
CREATE FUNCTION shiba_internal.drop_effect_stream_payload(
    target_stream_id bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    payload shiba_internal.effect_stream_payloads%ROWTYPE;
    live_type_schema name;
    live_type_name name;
    live_type_kind "char";
    live_type_relation oid;
BEGIN
    SELECT *
    INTO payload
    FROM shiba_internal.effect_stream_payloads
    WHERE stream_id = target_stream_id
    FOR UPDATE;
    IF NOT FOUND THEN
      RETURN;
    END IF;

    PERFORM shiba_internal.drop_cataloged_relation(payload.relation_oid);

    SELECT namespace.nspname,
           type_catalog.typname,
           type_catalog.typtype,
           type_catalog.typrelid
    INTO live_type_schema,
         live_type_name,
         live_type_kind,
         live_type_relation
    FROM pg_type AS type_catalog
    JOIN pg_namespace AS namespace
      ON namespace.oid = type_catalog.typnamespace
    WHERE type_catalog.oid = payload.row_type_oid;
    IF FOUND THEN
      IF live_type_schema <> 'shiba_internal'::name
         OR live_type_kind <> 'c'::"char"
         OR live_type_relation = 0::oid THEN
        RAISE EXCEPTION 'effect stream % row type changed identity',
          target_stream_id
          USING ERRCODE = 'data_corrupted';
      END IF;
      EXECUTE format(
        'DROP TYPE %I.%I',
        live_type_schema,
        live_type_name
      );
    END IF;

    DELETE FROM shiba_internal.effect_stream_payloads
    WHERE stream_id = target_stream_id;
END;
$$;

-- Dynamic objects are not PostgreSQL children of the result table. Drop them
-- under the DAG lock before result-scoped catalog rows cascade.
CREATE FUNCTION shiba_internal.drop_dataflow_storage(
    result_relation oid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    storage record;
BEGIN
    IF result_relation IS NULL THEN
      RAISE EXCEPTION 'result relation cannot be NULL'
        USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM pg_advisory_xact_lock(
      shiba_internal.dataflow_lock_key(result_relation)
    );
    UPDATE shiba_internal.dataflows
    SET active = false
    WHERE result_oid = result_relation;

    FOR storage IN
      SELECT owned.relation_oid
      FROM (
        SELECT state.relation_oid, 0 AS kind,
               state.stage_id, state.state_slot AS ordinal
        FROM shiba_internal.operator_state_relations AS state
        WHERE state.result_oid = result_relation
        UNION ALL
        SELECT continuation.relation_oid,
               1 AS kind,
               continuation.stage_id,
               0 AS ordinal
        FROM shiba_internal.operator_continuation_relations AS continuation
        WHERE continuation.result_oid = result_relation
      ) AS owned
      ORDER BY owned.kind, owned.stage_id, owned.ordinal
    LOOP
      PERFORM shiba_internal.drop_cataloged_relation(storage.relation_oid);
    END LOOP;

    FOR storage IN
      SELECT stream.stream_id
      FROM shiba_internal.effect_streams AS stream
      WHERE stream.producer_kind = 'operator'
        AND stream.producer_result_oid = result_relation
      ORDER BY stream.producer_stage_id
    LOOP
      PERFORM shiba_internal.drop_effect_stream_payload(storage.stream_id);
    END LOOP;
END;
$$;

CREATE FUNCTION shiba_internal.detach_unused_source(source_relation oid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    source_name text;
    source_stream record;
BEGIN
    IF EXISTS (
      SELECT 1
      FROM shiba_internal.dataflow_sources
      WHERE source_oid = source_relation
    ) THEN
      RETURN;
    END IF;

    FOR source_stream IN
      SELECT stream.stream_id
      FROM shiba_internal.effect_streams AS stream
      JOIN shiba_internal.ingress_replay_state AS replay
        ON replay.slot_generation = stream.slot_generation
       AND replay.state = 'active'
      WHERE stream.producer_kind = 'source'
        AND stream.source_oid = source_relation
        AND NOT EXISTS (
          SELECT 1
          FROM shiba_internal.effect_stream_consumers AS consumer
          WHERE consumer.stream_id = stream.stream_id
        )
      ORDER BY stream.stream_id
      FOR UPDATE OF stream
    LOOP
      PERFORM shiba_internal.drop_effect_stream_payload(
        source_stream.stream_id
      );
      DELETE FROM shiba_internal.effect_streams
      WHERE stream_id = source_stream.stream_id;
    END LOOP;

    SELECT format('%I.%I', namespace.nspname, relation.relname)
    INTO source_name
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace
      ON namespace.oid = relation.relnamespace
    WHERE relation.oid = source_relation;
    IF source_name IS NULL THEN
      RETURN;
    END IF;

    EXECUTE format(
      'DROP TRIGGER IF EXISTS shiba_wakeup ON %s',
      source_name
    );
    EXECUTE format(
      'DROP TRIGGER IF EXISTS shiba_no_truncate ON %s',
      source_name
    );
    IF EXISTS (
      SELECT 1
      FROM pg_publication AS publication
      JOIN pg_publication_rel AS member
        ON member.prpubid = publication.oid
      WHERE publication.pubname = 'shiba_publication'
        AND member.prrelid = source_relation
    ) THEN
      EXECUTE format(
        'ALTER PUBLICATION shiba_publication DROP TABLE %s',
        source_name
      );
    END IF;
END;
$$;

CREATE FUNCTION shiba_internal.cleanup_dropped_dataflow()
RETURNS event_trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    dropped record;
    source_oids oid[];
    source_oid oid;
BEGIN
    FOR dropped IN
      SELECT *
      FROM pg_event_trigger_dropped_objects()
      WHERE object_type = 'table'
    LOOP
      IF EXISTS (
        SELECT 1
        FROM shiba_internal.dataflow_sources AS dependency
        WHERE dependency.source_oid = dropped.objid
      ) THEN
        RAISE EXCEPTION
          'cannot drop Shiba source %; drop dependent Shiba tables first',
          dropped.object_identity
          USING ERRCODE = 'object_not_in_prerequisite_state';
      END IF;
      IF dropped.schema_name <> 'shiba'
         OR NOT EXISTS (
           SELECT 1
           FROM shiba_internal.dataflows AS dataflow
           WHERE dataflow.result_oid = dropped.objid
         ) THEN
        CONTINUE;
      END IF;

      SELECT array_agg(source.source_oid ORDER BY source.source_oid)
      INTO source_oids
      FROM shiba_internal.dataflow_sources AS source
      WHERE source.result_oid = dropped.objid;

      PERFORM shiba_internal.drop_dataflow_storage(dropped.objid);
      DELETE FROM shiba_internal.dataflows AS dataflow
      WHERE dataflow.result_oid = dropped.objid;

      FOREACH source_oid IN ARRAY source_oids LOOP
        PERFORM shiba_internal.detach_unused_source(source_oid);
      END LOOP;
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER shiba_cleanup_dropped_dataflow
  ON sql_drop
  EXECUTE FUNCTION shiba_internal.cleanup_dropped_dataflow();

CREATE FUNCTION shiba_internal.cleanup_dropped_managed_index()
RETURNS event_trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba_internal
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
  EXECUTE FUNCTION shiba_internal.cleanup_dropped_managed_index();

CREATE FUNCTION shiba_internal.guard_source_alter()
RETURNS event_trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    command record;
BEGIN
    FOR command IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
      IF command.object_type = 'table'
         AND EXISTS (
           SELECT 1
           FROM shiba_internal.dataflow_sources
           WHERE source_oid = command.objid
         ) THEN
        RAISE EXCEPTION
          'cannot ALTER TABLE % while it is a Shiba source; drop dependent Shiba tables first',
          command.object_identity
          USING ERRCODE = 'object_not_in_prerequisite_state';
      END IF;
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER shiba_guard_source_alter
  ON ddl_command_end
  WHEN TAG IN ('ALTER TABLE')
  EXECUTE FUNCTION shiba_internal.guard_source_alter();

CREATE FUNCTION shiba.progress(result_table regclass)
RETURNS TABLE (
    applied_lsn pg_lsn,
    pending_wal_bytes bigint,
    updated_at timestamptz
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM shiba_internal.dataflows AS dataflow
    WHERE dataflow.result_oid = result_table::oid
  ) THEN
    RAISE EXCEPTION 'relation % is not a Shiba result table', result_table
      USING ERRCODE = 'wrong_object_type';
  END IF;
  IF NOT has_table_privilege(
    shiba.invoker_oid(), result_table, 'SELECT'
  ) THEN
    RAISE EXCEPTION 'SELECT privilege on % is required', result_table
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  RETURN QUERY
  SELECT consumer.consumed_frontier_lsn,
         greatest(
           pg_wal_lsn_diff(
             pg_current_wal_lsn(),
             slot.confirmed_flush_lsn
           )::bigint,
           0
         ),
         consumer.updated_at
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(
    dataflow.plan -> 'stages'
  ) WITH ORDINALITY AS sink(value, ordinality)
  JOIN shiba_internal.effect_stream_consumers AS consumer
    ON consumer.result_oid = dataflow.result_oid
   AND consumer.consumer_stage_id = sink.ordinality - 1
  JOIN pg_replication_slots AS slot
    ON slot.slot_name = shiba_internal.slot_name()::text
  WHERE dataflow.result_oid = result_table::oid
    AND sink.value -> 'spec' ->> 'operator' = 'sink';
END;
$$;

-- Stop the Runtime before deactivation. The identity advisory lock prevents a
-- replacement process from claiming the database while this transaction is
-- removing its slot and durable ingress generation.
CREATE FUNCTION shiba_internal.stop_runtime_for_deactivation()
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
    backend_kind text;
    backend_database oid;
BEGIN
    LOOP
      EXIT WHEN pg_try_advisory_xact_lock(
        shiba_internal.identity_lock_namespace(), 0
      );

      SELECT owner_pid
      INTO owner_pid_value
      FROM shiba_internal.runtime_state
      WHERE singleton;
      IF owner_pid_value IS NOT NULL THEN
        SELECT backend_type, datid
        INTO backend_kind, backend_database
        FROM pg_stat_activity
        WHERE pid = owner_pid_value;
        IF FOUND THEN
          IF owner_pid_value = pg_backend_pid()
             OR backend_kind IS DISTINCT FROM 'shiba runtime'
             OR backend_database IS DISTINCT FROM (
               SELECT oid
               FROM pg_database
               WHERE datname = current_database()
             ) THEN
            RAISE EXCEPTION
              'Runtime identity is owned by unexpected backend PID %',
              owner_pid_value
              USING ERRCODE = 'object_in_use';
          END IF;
          IF clock_timestamp() < graceful_deadline THEN
            PERFORM pg_cancel_backend(owner_pid_value);
          ELSE
            PERFORM pg_terminate_backend(owner_pid_value);
          END IF;
        END IF;
      END IF;

      IF clock_timestamp() >= deadline THEN
        RAISE EXCEPTION 'timed out stopping the Shiba Runtime'
          USING ERRCODE = 'lock_not_available';
      END IF;
      PERFORM pg_sleep(0.01);
    END LOOP;

    LOOP
      SELECT active_pid
      INTO slot_active_pid
      FROM pg_replication_slots
      WHERE slot_name = shiba_internal.slot_name()::text;
      EXIT WHEN slot_active_pid IS NULL;
      IF clock_timestamp() >= deadline THEN
        RAISE EXCEPTION 'timed out waiting for logical slot %',
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
DECLARE
    stream record;
    active_generation bigint;
BEGIN
    PERFORM shiba._lock_database_lifecycle();
    IF EXISTS (SELECT 1 FROM shiba_internal.dataflows) THEN
      RAISE EXCEPTION 'drop all Shiba result tables before deactivation'
        USING ERRCODE = 'object_in_use';
    END IF;
    PERFORM shiba_internal.stop_runtime_for_deactivation();

    SELECT slot_generation
    INTO active_generation
    FROM shiba_internal.ingress_replay_state
    WHERE database_oid = (
      SELECT oid
      FROM pg_database
      WHERE datname = current_database()
    )
      AND slot_name = shiba_internal.slot_name()
      AND state = 'active'
    FOR UPDATE;

    DELETE FROM shiba_internal.ingress_transactions
    WHERE slot_generation = active_generation;
    FOR stream IN
      SELECT stream_id
      FROM shiba_internal.effect_streams
      WHERE producer_kind = 'source'
        AND slot_generation = active_generation
      ORDER BY stream_id
    LOOP
      PERFORM shiba_internal.drop_effect_stream_payload(stream.stream_id);
      DELETE FROM shiba_internal.effect_streams
      WHERE stream_id = stream.stream_id;
    END LOOP;

    UPDATE shiba_internal.runtime_state
    SET active = false,
        owner_pid = NULL,
        started_at = NULL,
        last_heartbeat = NULL,
        pending_launch_xid = NULL,
        pending_since = NULL
    WHERE singleton;

    PERFORM shiba_internal.retire_ingress_generation(
      shiba_internal.slot_name()
    );
    IF EXISTS (
      SELECT 1
      FROM pg_replication_slots
      WHERE slot_name = shiba_internal.slot_name()::text
    ) THEN
      PERFORM pg_drop_replication_slot(shiba_internal.slot_name());
    END IF;
END;
$$;

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
    result_name text;
    result_schema name;
    column_sql text;
    invoker_oid oid;
    supported_columns integer;
    fixed_key_bytes integer;
    created_index_oid oid;
BEGIN
    PERFORM shiba.require_index_ddl_top_level();
    invoker_oid := shiba.invoker_oid();
    IF result_table IS NULL
       OR index_name IS NULL
       OR btrim(index_name) = ''
       OR octet_length(index_name)
            > current_setting('max_identifier_length')::integer
       OR index_columns IS NULL
       OR array_ndims(index_columns) IS DISTINCT FROM 1
       OR cardinality(index_columns) NOT BETWEEN 1 AND 8
       OR array_position(index_columns, NULL) IS NOT NULL
       OR cardinality(index_columns) IS DISTINCT FROM (
         SELECT count(DISTINCT requested.value)
         FROM unnest(index_columns) AS requested(value)
       ) THEN
      RAISE EXCEPTION 'invalid Shiba result index specification'
        USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF NOT EXISTS (
      SELECT 1
      FROM shiba_internal.dataflows
      WHERE result_oid = result_table::oid
    ) THEN
      RAISE EXCEPTION 'relation % is not a Shiba result table', result_table
        USING ERRCODE = 'wrong_object_type';
    END IF;
    IF NOT has_table_privilege(invoker_oid, result_table, 'SELECT') THEN
      RAISE EXCEPTION 'SELECT privilege on % is required', result_table
        USING ERRCODE = 'insufficient_privilege';
    END IF;

    PERFORM pg_advisory_xact_lock(
      shiba_internal.dataflow_lock_key(result_table::oid)
    );
    PERFORM 1
    FROM shiba_internal.dataflows
    WHERE result_oid = result_table::oid
    FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'relation % is not a Shiba result table', result_table
        USING ERRCODE = 'wrong_object_type';
    END IF;
    IF (
      SELECT count(*)
      FROM shiba_internal.managed_indexes
      WHERE result_oid = result_table::oid
    ) >= 8 THEN
      RAISE EXCEPTION 'result table % already has 8 managed indexes',
        result_table
        USING ERRCODE = 'configuration_limit_exceeded';
    END IF;

    SELECT namespace.nspname,
           format('%I.%I', namespace.nspname, relation.relname)
    INTO STRICT result_schema, result_name
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace
      ON namespace.oid = relation.relnamespace
    WHERE relation.oid = result_table::oid
      AND relation.relkind = 'r'
      AND namespace.nspname = 'shiba';

    SELECT count(*), coalesce(sum(type_catalog.typlen), 0)::integer
    INTO supported_columns, fixed_key_bytes
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
    IF supported_columns <> cardinality(index_columns)
       OR fixed_key_bytes > 1024 THEN
      RAISE EXCEPTION
        'managed indexes require at most 1024 bytes of fixed-width built-in B-tree columns'
        USING ERRCODE = 'feature_not_supported';
    END IF;

    SELECT string_agg(
      format('%I', requested.column_name),
      ', ' ORDER BY requested.ordinality
    )
    INTO column_sql
    FROM unnest(index_columns) WITH ORDINALITY
      AS requested(column_name, ordinality);

    EXECUTE format(
      'CREATE INDEX %I ON %s USING btree (%s)',
      index_name,
      result_name,
      column_sql
    );
    SELECT index_relation.oid
    INTO STRICT created_index_oid
    FROM pg_class AS index_relation
    JOIN pg_namespace AS namespace
      ON namespace.oid = index_relation.relnamespace
    JOIN pg_index AS index_catalog
      ON index_catalog.indexrelid = index_relation.oid
    WHERE namespace.nspname = result_schema
      AND index_relation.relname = index_name
      AND index_catalog.indrelid = result_table::oid;

    INSERT INTO shiba_internal.managed_indexes(
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

CREATE FUNCTION shiba.drop_index(index_relation regclass)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    parent_relation oid;
    live_schema name;
    live_name name;
    creator_oid oid;
    invoker_oid oid;
    extension_owner_oid oid;
BEGIN
    PERFORM shiba.require_index_ddl_top_level();
    invoker_oid := shiba.invoker_oid();
    SELECT managed.result_oid
    INTO parent_relation
    FROM shiba_internal.managed_indexes AS managed
    WHERE managed.index_oid = index_relation::oid;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'index % is not Shiba-managed', index_relation
        USING ERRCODE = 'wrong_object_type';
    END IF;

    PERFORM pg_advisory_xact_lock(
      shiba_internal.dataflow_lock_key(parent_relation)
    );
    PERFORM shiba.lock_index_ddl_target(index_relation::oid);

    SELECT namespace.nspname,
           relation.relname,
           managed.creator_oid
    INTO live_schema, live_name, creator_oid
    FROM shiba_internal.managed_indexes AS managed
    JOIN shiba_internal.dataflows AS dataflow
      ON dataflow.result_oid = managed.result_oid
    JOIN pg_class AS relation
      ON relation.oid = managed.index_oid
     AND relation.relname = managed.index_name
     AND relation.relkind = 'i'
    JOIN pg_namespace AS namespace
      ON namespace.oid = relation.relnamespace
    JOIN pg_index AS index_catalog
      ON index_catalog.indexrelid = relation.oid
     AND index_catalog.indrelid = managed.result_oid
     AND NOT index_catalog.indisunique
     AND NOT index_catalog.indisprimary
     AND NOT index_catalog.indisexclusion
    WHERE managed.index_oid = index_relation::oid
    FOR UPDATE OF managed;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'managed index % changed identity', index_relation
        USING ERRCODE = 'data_corrupted';
    END IF;
    IF NOT has_table_privilege(invoker_oid, parent_relation, 'SELECT') THEN
      RAISE EXCEPTION 'SELECT privilege on the result table is required'
        USING ERRCODE = 'insufficient_privilege';
    END IF;
    SELECT extowner
    INTO STRICT extension_owner_oid
    FROM pg_extension
    WHERE extname = 'shiba';
    IF invoker_oid <> creator_oid
       AND invoker_oid <> extension_owner_oid THEN
      RAISE EXCEPTION 'role % did not create index %',
        pg_get_userbyid(invoker_oid), index_relation
        USING ERRCODE = 'insufficient_privilege';
    END IF;

    EXECUTE format('DROP INDEX %I.%I', live_schema, live_name);
END;
$$;

DO $$
BEGIN
    IF current_setting('wal_level') <> 'logical' THEN
      RAISE EXCEPTION
        'Shiba requires wal_level=logical; set it in postgresql.conf and restart PostgreSQL'
        USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF current_setting('max_replication_slots')::integer < 1 THEN
      RAISE EXCEPTION 'Shiba requires max_replication_slots >= 1'
        USING ERRCODE = 'configuration_limit_exceeded';
    END IF;
    IF EXISTS (
      SELECT 1
      FROM pg_publication
      WHERE pubname = 'shiba_publication'
    ) THEN
      RAISE EXCEPTION 'publication shiba_publication already exists'
        USING ERRCODE = 'duplicate_object';
    END IF;
    CREATE PUBLICATION shiba_publication
      WITH (publish = 'insert, update, delete');
END;
$$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA shiba FROM PUBLIC;
GRANT EXECUTE ON FUNCTION shiba.progress(regclass) TO PUBLIC;
GRANT EXECUTE ON FUNCTION shiba.explain_dataflow(regclass) TO PUBLIC;
