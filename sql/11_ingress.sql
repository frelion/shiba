-- Durable ingress fixed-shape SQL API.
--
-- CALLING TRANSACTION BOUNDARIES
-- --------------------------------
-- These functions never commit and never perform replication I/O.  The
-- Runtime calls claim/create, bounded event admission, and an optional Commit
-- finalization inside one bounded SPI transaction, then commits before reading
-- or waiting on the replication socket. Every durable apply batch creates one
-- publication task per source. Open-transaction tasks are durable but gated
-- from the shared source stream. Prefix batches do not advance
-- durable replication progress; commit_ingress_transaction seals the header
-- and advances persisted_lsn in constant work.
-- Read feedback_upper_bound in a short SPI transaction, end that transaction,
-- and only then send Standby Status Update.  After a
-- successful send, record the confirmation intent in another short SPI
-- transaction with record_ingress_feedback.
--
-- GLOBAL ROW-LOCK ORDER
-- --------------------------------
-- 0. ingress_replay_state table lock (generation creation only)
-- 1. ingress_replay_state rows (ascending slot_generation)
-- 2. ingress_transactions rows (ascending ingress_txn_id)
-- 3. change_log stable identity
-- 4. ingress_apply_batches / source_publications
-- 5. effect_streams / effect_stream_chunks / typed payload relation
--
-- Every mutating function below follows the applicable prefix of this order.
-- PostgreSQL retains the row locks until the caller commits or rolls back the
-- surrounding transaction.  A retry after deadlock or serialization failure
-- must replay the complete bounded SPI transaction.
--
-- All names in SECURITY DEFINER bodies are schema-qualified and search_path is
-- restricted.  PUBLIC receives no EXECUTE privilege; the extension owner
-- invokes these functions from the Runtime backend.

CREATE FUNCTION shiba_internal.ensure_ingress_generation(
    p_slot_name name,
    p_force_new boolean DEFAULT false
)
RETURNS TABLE (
    slot_generation bigint,
    created boolean,
    persisted_lsn pg_lsn,
    confirmed_lsn pg_lsn,
    replay_safe_lsn pg_lsn
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_database_oid oid;
    v_slot_generation bigint;
    v_max_generation bigint;
    v_system_identifier text;
    v_slot_database name;
    v_slot_plugin name;
    v_slot_confirmed_lsn pg_lsn;
    v_persisted_lsn pg_lsn;
    v_baseline_lsn pg_lsn;
BEGIN
    IF p_slot_name IS NULL
       OR length(p_slot_name::text) = 0
       OR p_force_new IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'ingress slot name must not be NULL or empty';
    END IF;

    SELECT database.oid
      INTO STRICT v_database_oid
      FROM pg_catalog.pg_database AS database
     WHERE database.datname = pg_catalog.current_database();

    SELECT control.system_identifier::text
      INTO STRICT v_system_identifier
      FROM pg_catalog.pg_control_system() AS control;

    SELECT slot.database,
           slot.plugin,
           slot.confirmed_flush_lsn
      INTO v_slot_database,
           v_slot_plugin,
           v_slot_confirmed_lsn
      FROM pg_catalog.pg_replication_slots AS slot
     WHERE slot.slot_name = p_slot_name::text;

    IF NOT FOUND
       OR v_slot_database IS DISTINCT FROM pg_catalog.current_database()::name
       OR v_slot_plugin IS DISTINCT FROM 'pgoutput'::name
       OR v_slot_confirmed_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'logical slot %s is absent or is not this database''s pgoutput slot',
                p_slot_name
            );
    END IF;

    -- Lock-order level 0.  SHARE ROW EXCLUSIVE serializes generation
    -- allocation with concurrent lifecycle calls and ordinary table writers.
    -- Keep this lifecycle transaction short; the table lock is released by
    -- caller COMMIT.
    LOCK TABLE shiba_internal.ingress_replay_state
        IN SHARE ROW EXCLUSIVE MODE;

    SELECT replay.slot_generation
      INTO v_slot_generation
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.database_oid = v_database_oid
       AND replay.slot_name = p_slot_name
       AND replay.state = 'active'
     FOR UPDATE;

    IF FOUND AND p_force_new THEN
        UPDATE shiba_internal.ingress_replay_state AS replay
           SET state = 'retired',
               retired_at = clock_timestamp(),
               updated_at = clock_timestamp()
         WHERE replay.slot_generation = v_slot_generation;
        v_slot_generation := NULL;
    ELSIF FOUND THEN
        SELECT replay.persisted_lsn,
               replay.slot_baseline_lsn
          INTO v_persisted_lsn,
               v_baseline_lsn
          FROM shiba_internal.ingress_replay_state AS replay
         WHERE replay.slot_generation = v_slot_generation
           AND replay.system_identifier = v_system_identifier;

        IF NOT FOUND
           OR v_slot_confirmed_lsn < v_baseline_lsn
           OR (v_persisted_lsn IS NULL
               AND v_slot_confirmed_lsn <> v_baseline_lsn)
           OR (v_persisted_lsn IS NOT NULL
               AND v_slot_confirmed_lsn > v_persisted_lsn) THEN
            -- Fail closed without pretending to persist an `invalid` state:
            -- the exception rolls back this lifecycle transaction. Recovery
            -- requires repairing/replacing the mismatched slot explicitly.
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = format(
                    'logical slot %s no longer matches durable ingress generation %s',
                    p_slot_name,
                    v_slot_generation
                );
        END IF;
        created := false;
    END IF;

    IF v_slot_generation IS NULL THEN
        SELECT max(replay.slot_generation)
          INTO v_max_generation
          FROM shiba_internal.ingress_replay_state AS replay;

        IF v_max_generation = 9223372036854775807 THEN
            RAISE EXCEPTION USING
                ERRCODE = '22003',
                MESSAGE = 'ingress slot generation exhausted bigint range';
        END IF;

        v_slot_generation := coalesce(v_max_generation, 0) + 1;

        INSERT INTO shiba_internal.ingress_replay_state (
            slot_generation,
            slot_name,
            database_oid,
            system_identifier,
            slot_baseline_lsn
        )
        VALUES (
            v_slot_generation,
            p_slot_name,
            v_database_oid,
            v_system_identifier,
            v_slot_confirmed_lsn
        );
        created := true;
    END IF;

    RETURN QUERY
    SELECT replay.slot_generation,
           created,
           replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation;
END;
$$;

CREATE FUNCTION shiba_internal.retire_ingress_generation(
    p_slot_name name
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    UPDATE shiba_internal.ingress_replay_state AS replay
       SET state = 'retired',
           retired_at = clock_timestamp(),
           updated_at = clock_timestamp()
     WHERE replay.database_oid = (
               SELECT database.oid
                 FROM pg_catalog.pg_database AS database
                WHERE database.datname = pg_catalog.current_database()
           )
       AND replay.slot_name = p_slot_name
       AND replay.state = 'active';
    RETURN FOUND;
END;
$$;

CREATE FUNCTION shiba_internal.claim_ingress_transaction(
    p_slot_generation bigint,
    p_source_xid bigint,
    p_transaction_start_lsn pg_lsn
)
RETURNS TABLE (
    ingress_txn_id bigint,
    txn_status text,
    event_count bigint,
    payload_bytes bigint,
    created boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_ingress_txn_id bigint;
    v_created boolean;
BEGIN
    IF p_slot_generation IS NULL
       OR p_source_xid IS NULL
       OR p_transaction_start_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress transaction identity must not contain NULL';
    END IF;

    -- Lock-order level 1: generation/replay state.
    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.database_oid =
           (SELECT database.oid
              FROM pg_catalog.pg_database AS database
             WHERE database.datname = pg_catalog.current_database())
       AND replay.state = 'active'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'ingress slot generation %s is absent, inactive, or belongs to another database',
                p_slot_generation
            );
    END IF;

    INSERT INTO shiba_internal.ingress_transactions (
        slot_generation,
        source_xid,
        transaction_start_lsn
    )
    VALUES (
        p_slot_generation,
        p_source_xid,
        p_transaction_start_lsn
    )
    ON CONFLICT (slot_generation, source_xid, transaction_start_lsn)
        DO NOTHING
    RETURNING ingress_transactions.ingress_txn_id
         INTO v_ingress_txn_id;

    v_created := FOUND;

    IF NOT v_created THEN
        -- Lock-order level 2: transaction header.
        SELECT txn.ingress_txn_id
          INTO STRICT v_ingress_txn_id
         FROM shiba_internal.ingress_transactions AS txn
         WHERE txn.slot_generation = p_slot_generation
           AND txn.source_xid = p_source_xid
           AND txn.transaction_start_lsn = p_transaction_start_lsn
         FOR UPDATE;
    END IF;

    RETURN QUERY
    SELECT txn.ingress_txn_id,
           txn.status,
           txn.event_count,
           txn.payload_bytes,
           v_created
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = v_ingress_txn_id;
END;
$$;

-- One Runtime/SPI call admits a bounded array in wire order. Each element has
-- this exact JSONB shape:
-- {
--   "change_lsn": "0/16B6A20",
--   "change_ordinal": 0,
--   "image_ordinal": 0,
--   "source_subxid": 42,
--   "source_oid": 16384,
--   "weight": 1,
--   "payload": {...}
-- }
CREATE FUNCTION shiba_internal.insert_ingress_events(
    p_ingress_txn_id bigint,
    p_events jsonb
)
RETURNS TABLE (
    inserted_count bigint,
    replayed_count bigint,
    first_input_seq bigint,
    last_input_seq bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_slot_generation bigint;
    v_status text;
    v_existing_event_count bigint;
    v_existing_payload_bytes bigint;
    v_existing_batch_count bigint;
    v_existing_pending_publications bigint;
    v_batch_payload_bytes bigint;
    v_task_count bigint;
    v_total_count bigint;
    v_source_count bigint;
    v_last_replayed_ordinal bigint;
    v_first_new_ordinal bigint;
    v_identity_conflict boolean;
    v_first_inserted_input_seq bigint;
    v_last_inserted_input_seq bigint;
    v_batch_ordinal bigint;
    v_source_oid oid;
    v_source_name text;
BEGIN
    IF p_ingress_txn_id IS NULL OR p_events IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress event batch fields must not contain NULL';
    END IF;

    IF pg_catalog.jsonb_typeof(p_events) <> 'array' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'ingress event batch must be a JSONB array';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.jsonb_array_elements(p_events)
               WITH ORDINALITY AS item(value, ordinality)
         WHERE CASE
             WHEN pg_catalog.jsonb_typeof(item.value) <> 'object' THEN true
             ELSE NOT (
                 item.value ?& ARRAY[
                     'change_lsn',
                     'change_ordinal',
                     'image_ordinal',
                     'source_subxid',
                     'source_oid',
                     'weight',
                     'payload'
                 ]
             )
             OR EXISTS (
                 SELECT 1
                   FROM pg_catalog.jsonb_object_keys(item.value) AS member(key)
                  WHERE member.key <> ALL (ARRAY[
                      'change_lsn',
                      'change_ordinal',
                      'image_ordinal',
                      'source_subxid',
                      'source_oid',
                      'weight',
                      'payload'
                  ])
             )
         END
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'ingress event batch has an invalid element shape';
    END IF;

    BEGIN
        IF EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_array_elements(p_events)
                   WITH ORDINALITY AS item(value, ordinality)
             WHERE pg_catalog.jsonb_typeof(item.value -> 'change_lsn')
                       <> 'string'
                OR pg_catalog.jsonb_typeof(item.value -> 'change_ordinal')
                       <> 'number'
                OR pg_catalog.jsonb_typeof(item.value -> 'image_ordinal')
                       <> 'number'
                OR pg_catalog.jsonb_typeof(item.value -> 'source_subxid')
                       <> 'number'
                OR pg_catalog.jsonb_typeof(item.value -> 'source_oid')
                       <> 'number'
                OR pg_catalog.jsonb_typeof(item.value -> 'weight')
                       <> 'number'
                OR pg_catalog.jsonb_typeof(item.value -> 'payload')
                       <> 'object'
                OR (item.value ->> 'change_ordinal')::bigint < 0
                OR (item.value ->> 'image_ordinal')::integer < 0
                OR (item.value ->> 'source_subxid')::bigint
                       NOT BETWEEN 0 AND 4294967295
                OR (item.value ->> 'source_oid')::oid = 0::oid
                OR (item.value ->> 'weight')::bigint NOT IN (-1, 1)
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = 'ingress event batch contains an invalid value';
        END IF;

        IF EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_array_elements(p_events) AS item(value)
             GROUP BY (item.value ->> 'change_lsn')::pg_lsn,
                      (item.value ->> 'change_ordinal')::bigint,
                      (item.value ->> 'image_ordinal')::integer
            HAVING count(*) > 1
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = 'ingress event batch repeats a stable event identity';
        END IF;
    EXCEPTION
        WHEN invalid_text_representation OR numeric_value_out_of_range THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = 'ingress event batch contains an invalid typed value';
    END;

    SELECT txn.slot_generation
      INTO STRICT v_slot_generation
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    -- Lock once per bounded batch: generation, then transaction header.
    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'ingress slot generation %s is not active',
                v_slot_generation
            );
    END IF;

    SELECT txn.status,
           txn.event_count,
           txn.payload_bytes,
           txn.batch_count,
           txn.pending_publications
      INTO STRICT v_status,
                  v_existing_event_count,
                  v_existing_payload_bytes,
                  v_existing_batch_count,
                  v_existing_pending_publications
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;
    -- Validate one source at a time. jsonb_populate_record makes PostgreSQL
    -- cast every pgoutput text field to the source row type, but change_log
    -- retains the original per-column text JSON. A later publisher therefore
    -- reconstructs exactly the same typed values, including array dimensions.
    v_total_count := 0;
    FOR v_source_oid IN
        SELECT DISTINCT (item.value ->> 'source_oid')::oid
          FROM pg_catalog.jsonb_array_elements(p_events) AS item(value)
         ORDER BY 1
    LOOP
        SELECT pg_catalog.format(
                   '%I.%I',
                   namespace_catalog.nspname,
                   relation_catalog.relname
               )
          INTO STRICT v_source_name
          FROM pg_catalog.pg_class AS relation_catalog
          JOIN pg_catalog.pg_namespace AS namespace_catalog
            ON namespace_catalog.oid = relation_catalog.relnamespace
         WHERE relation_catalog.oid = v_source_oid
           AND relation_catalog.relkind IN ('r', 'p')
           AND relation_catalog.relpersistence = 'p';

        EXECUTE pg_catalog.format(
            'SELECT count(
                      pg_catalog.jsonb_populate_record(
                        NULL::%s,
                        item.value -> ''payload''
                      )
                    )
               FROM pg_catalog.jsonb_array_elements($1)
                    AS item(value)
              WHERE (item.value ->> ''source_oid'')::oid = $2',
            v_source_name
        )
        INTO STRICT v_source_count
        USING p_events, v_source_oid;

        v_total_count := v_total_count + v_source_count;
    END LOOP;

    IF v_total_count <> pg_catalog.jsonb_array_length(p_events) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = 'typed ingress validation lost an event';
    END IF;

    WITH incoming AS (
        SELECT item.ordinality::bigint AS ordinal,
               (item.value ->> 'change_lsn')::pg_lsn AS change_lsn,
               (item.value ->> 'change_ordinal')::bigint AS change_ordinal,
               (item.value ->> 'image_ordinal')::integer AS image_ordinal,
               (item.value ->> 'source_subxid')::bigint AS source_subxid,
               (item.value ->> 'source_oid')::oid AS source_oid,
               (item.value ->> 'weight')::bigint AS weight,
               item.value -> 'payload' AS payload
          FROM pg_catalog.jsonb_array_elements(p_events)
               WITH ORDINALITY AS item(value, ordinality)
    ),
    matched AS (
        SELECT incoming.*,
               existing.input_seq,
               existing.source_subxid AS existing_source_subxid,
               existing.source_oid AS existing_source_oid,
               existing.weight AS existing_weight,
               existing.payload AS existing_payload
          FROM incoming
          LEFT JOIN shiba_internal.change_log AS existing
            ON existing.ingress_txn_id = p_ingress_txn_id
           AND existing.change_lsn = incoming.change_lsn
           AND existing.change_ordinal = incoming.change_ordinal
           AND existing.image_ordinal = incoming.image_ordinal
    )
    SELECT count(*) FILTER (WHERE input_seq IS NULL),
           count(*) FILTER (WHERE input_seq IS NOT NULL),
           max(ordinal) FILTER (WHERE input_seq IS NOT NULL),
           min(ordinal) FILTER (WHERE input_seq IS NULL),
           min(input_seq),
           max(input_seq),
           coalesce(
             bool_or(
               input_seq IS NOT NULL
               AND (
                 existing_source_oid IS DISTINCT FROM source_oid
                 OR existing_source_subxid IS DISTINCT FROM source_subxid
                 OR existing_weight IS DISTINCT FROM weight
                 OR existing_payload IS DISTINCT FROM payload
               )
             ),
             false
           )
      INTO inserted_count,
           replayed_count,
           v_last_replayed_ordinal,
           v_first_new_ordinal,
           first_input_seq,
           last_input_seq,
           v_identity_conflict
      FROM matched;

    IF v_identity_conflict THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = format(
                'ingress event identity conflict for transaction %s',
                p_ingress_txn_id
            );
    END IF;

    IF v_first_new_ordinal IS NOT NULL
       AND v_last_replayed_ordinal IS NOT NULL
       AND v_first_new_ordinal < v_last_replayed_ordinal THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = format(
                'ingress replay for transaction %s is not an existing prefix',
                p_ingress_txn_id
            );
    END IF;

    IF v_status <> 'open' THEN
        IF inserted_count > 0 THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'replay added events to ingress transaction %s in terminal state %s',
                    p_ingress_txn_id,
                    v_status
                );
        END IF;
        RETURN NEXT;
        RETURN;
    END IF;

    IF inserted_count > 0 THEN
        IF v_existing_event_count > 9223372036854775807 - inserted_count THEN
            RAISE EXCEPTION USING
                ERRCODE = '22003',
                MESSAGE = 'ingress transaction event count exhausted bigint range';
        END IF;

        WITH incoming AS (
            SELECT item.ordinality::bigint AS ordinal,
                   (item.value ->> 'change_lsn')::pg_lsn AS change_lsn,
                   (item.value ->> 'change_ordinal')::bigint
                     AS change_ordinal,
                   (item.value ->> 'image_ordinal')::integer
                     AS image_ordinal,
                   (item.value ->> 'source_subxid')::bigint
                     AS source_subxid,
                   (item.value ->> 'source_oid')::oid AS source_oid,
                   (item.value ->> 'weight')::bigint AS weight,
                   item.value -> 'payload' AS payload
              FROM pg_catalog.jsonb_array_elements(p_events)
                   WITH ORDINALITY AS item(value, ordinality)
        ),
        new_events AS (
            SELECT incoming.*,
                   v_existing_event_count
                     + pg_catalog.row_number() OVER (
                       ORDER BY incoming.ordinal
                       ) AS input_seq
              FROM incoming
             WHERE incoming.ordinal > replayed_count
        ),
        inserted AS (
            INSERT INTO shiba_internal.change_log (
                ingress_txn_id,
                change_lsn,
                change_ordinal,
                image_ordinal,
                source_subxid,
                input_seq,
                source_oid,
                weight,
                payload
            )
            SELECT p_ingress_txn_id,
                   new_events.change_lsn,
                   new_events.change_ordinal,
                   new_events.image_ordinal,
                   new_events.source_subxid,
                   new_events.input_seq,
                   new_events.source_oid,
                   new_events.weight,
                   new_events.payload
              FROM new_events
             ORDER BY new_events.ordinal
            RETURNING input_seq, payload
        )
        SELECT min(input_seq),
               max(input_seq),
               coalesce(
                 sum(pg_catalog.octet_length(
                   pg_catalog.jsonb_send(payload)
                 )),
                 0
               )::bigint
          INTO STRICT v_first_inserted_input_seq,
                      v_last_inserted_input_seq,
                      v_batch_payload_bytes
          FROM inserted;

        first_input_seq := least(
            coalesce(first_input_seq, v_first_inserted_input_seq),
            v_first_inserted_input_seq
        );
        last_input_seq := greatest(
            coalesce(last_input_seq, v_last_inserted_input_seq),
            v_last_inserted_input_seq
        );

        IF v_existing_payload_bytes
             > 9223372036854775807 - v_batch_payload_bytes THEN
            RAISE EXCEPTION USING
                ERRCODE = '22003',
                MESSAGE = 'ingress transaction payload summary exhausted bigint range';
        END IF;

        UPDATE shiba_internal.ingress_replay_state AS replay
           SET open_payload_bytes =
                   replay.open_payload_bytes + v_batch_payload_bytes,
               updated_at = clock_timestamp()
         WHERE replay.slot_generation = v_slot_generation
           AND replay.open_payload_bytes
                 <= pg_catalog.pg_size_bytes(
                        pg_catalog.current_setting(
                          'shiba.ingress_staging_limit'
                        )
                    ) - v_batch_payload_bytes;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '54000',
                MESSAGE = format(
                    'open ingress payload exceeds shiba.ingress_staging_limit while staging transaction %s',
                    p_ingress_txn_id
                ),
                HINT = 'Increase shiba.ingress_staging_limit or commit smaller source transactions.';
        END IF;

    END IF;

    -- The transaction header lock serializes sequence and batch allocation.
    -- Replayed events create no second apply batch even when transport frames
    -- are regrouped after a Runtime restart.
    IF inserted_count > 0 THEN
        IF v_first_inserted_input_seq IS NULL
           OR v_last_inserted_input_seq IS NULL
           OR v_last_inserted_input_seq - v_first_inserted_input_seq + 1
                <> inserted_count THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'new ingress events for transaction %s are not contiguous',
                    p_ingress_txn_id
                );
        END IF;

        IF v_existing_batch_count = 9223372036854775807 THEN
            RAISE EXCEPTION USING
                ERRCODE = '22003',
                MESSAGE = 'ingress apply batch count exhausted bigint range';
        END IF;
        v_batch_ordinal := v_existing_batch_count + 1;

        IF v_first_inserted_input_seq
             <> v_existing_event_count + 1 THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'ingress apply batches for transaction %s are not adjacent',
                    p_ingress_txn_id
                );
        END IF;

        INSERT INTO shiba_internal.ingress_apply_batches (
            ingress_txn_id,
            batch_ordinal,
            first_input_seq,
            last_input_seq
        )
        VALUES (
            p_ingress_txn_id,
            v_batch_ordinal,
            v_first_inserted_input_seq,
            v_last_inserted_input_seq
        );

        INSERT INTO shiba_internal.source_publications (
            ingress_txn_id,
            batch_ordinal,
            source_oid,
            next_input_seq
        )
        SELECT p_ingress_txn_id,
               v_batch_ordinal,
               event.source_oid,
               min(event.input_seq)
          FROM shiba_internal.change_log AS event
         WHERE event.ingress_txn_id = p_ingress_txn_id
           AND event.input_seq BETWEEN
               v_first_inserted_input_seq AND v_last_inserted_input_seq
         GROUP BY event.source_oid;

        GET DIAGNOSTICS v_task_count = ROW_COUNT;
        IF v_task_count < 1
           OR v_existing_pending_publications
                > 9223372036854775807 - v_task_count THEN
            RAISE EXCEPTION USING
                ERRCODE = '22003',
                MESSAGE = 'ingress publication task count exhausted bigint range';
        END IF;

        -- Header summaries are the only transaction-wide authority. The
        -- bounded rows, batch range, source tasks, and these counters commit
        -- together; exact replay inserts none of them twice.
        UPDATE shiba_internal.ingress_transactions AS txn
           SET event_count = txn.event_count + inserted_count,
               payload_bytes = txn.payload_bytes + v_batch_payload_bytes,
               batch_count = txn.batch_count + 1,
               pending_publications =
                   txn.pending_publications + v_task_count
         WHERE txn.ingress_txn_id = p_ingress_txn_id;
    END IF;

    RETURN NEXT;
END;
$$;

CREATE FUNCTION shiba_internal.commit_ingress_transaction(
    p_ingress_txn_id bigint,
    p_commit_lsn pg_lsn,
    p_end_lsn pg_lsn
)
RETURNS TABLE (
    finalized boolean,
    event_count bigint,
    payload_bytes bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_slot_generation bigint;
    v_status text;
    v_final_lsn pg_lsn;
    v_existing_commit_lsn pg_lsn;
    v_existing_end_lsn pg_lsn;
BEGIN
    IF p_ingress_txn_id IS NULL
       OR p_commit_lsn IS NULL
       OR p_end_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress commit fields must not contain NULL';
    END IF;

    SELECT txn.slot_generation
      INTO STRICT v_slot_generation
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    -- Lock-order levels 1 then 2.
    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'ingress slot generation %s is not active',
                v_slot_generation
            );
    END IF;

    SELECT txn.status,
           txn.final_lsn,
           txn.commit_lsn,
           txn.end_lsn,
           txn.event_count,
           txn.payload_bytes
      INTO STRICT v_status,
                  v_final_lsn,
                  v_existing_commit_lsn,
                  v_existing_end_lsn,
                  event_count,
                  payload_bytes
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;

    IF p_end_lsn < p_commit_lsn THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = format(
                'invalid commit/end LSNs %s/%s',
                p_commit_lsn,
                p_end_lsn
            );
    END IF;

    IF v_status = 'committed' THEN
        IF v_existing_commit_lsn IS DISTINCT FROM p_commit_lsn
           OR v_existing_end_lsn IS DISTINCT FROM p_end_lsn THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'ingress commit identity conflict for transaction %s',
                    p_ingress_txn_id
                );
        END IF;
        finalized := false;
    ELSIF v_status = 'open' THEN
        -- Commit is deliberately header-only. Every bounded admission already
        -- created its source tasks and incremented pending_publications in the
        -- same transaction, so sealing a ten-million-row source transaction
        -- touches exactly these two small authority rows.
        UPDATE shiba_internal.ingress_transactions AS txn
           SET status = 'committed',
               final_lsn = p_commit_lsn,
               commit_lsn = p_commit_lsn,
               end_lsn = p_end_lsn,
               finalized_at = clock_timestamp()
         WHERE txn.ingress_txn_id = p_ingress_txn_id;
        finalized := true;

        UPDATE shiba_internal.ingress_replay_state AS replay
           SET open_payload_bytes =
                   replay.open_payload_bytes - payload_bytes
         WHERE replay.slot_generation = v_slot_generation
           AND replay.open_payload_bytes >= payload_bytes;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = 'open ingress payload counter is smaller than committed transaction';
        END IF;
    ELSE
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = format(
                'cannot commit ingress transaction %s in state %s',
                p_ingress_txn_id,
                v_status
            );
    END IF;

    -- This is the only API that advances durable replication progress.
    -- Header sealing and this watermark update commit or roll back together.
    UPDATE shiba_internal.ingress_replay_state AS replay
       SET persisted_lsn = CASE
               WHEN replay.persisted_lsn IS NULL
                 OR replay.persisted_lsn < p_end_lsn
               THEN p_end_lsn
               ELSE replay.persisted_lsn
           END,
           updated_at = clock_timestamp()
     WHERE replay.slot_generation = v_slot_generation;

    RETURN NEXT;
END;
$$;

CREATE FUNCTION shiba_internal.abort_ingress_transaction(
    p_ingress_txn_id bigint,
    p_abort_lsn pg_lsn
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_slot_generation bigint;
    v_status text;
    v_existing_abort_lsn pg_lsn;
    v_payload_bytes bigint;
BEGIN
    IF p_ingress_txn_id IS NULL OR p_abort_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress abort fields must not contain NULL';
    END IF;

    SELECT txn.slot_generation
      INTO STRICT v_slot_generation
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'ingress slot generation %s is not active',
                v_slot_generation
            );
    END IF;

    SELECT txn.status,
           txn.final_lsn,
           txn.payload_bytes
      INTO STRICT v_status,
                  v_existing_abort_lsn,
                  v_payload_bytes
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;

    IF v_status = 'aborted' THEN
        IF v_existing_abort_lsn IS DISTINCT FROM p_abort_lsn THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'ingress abort identity conflict for transaction %s',
                    p_ingress_txn_id
                );
        END IF;
        RETURN false;
    END IF;
    IF v_status <> 'open' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = format(
                'cannot abort ingress transaction %s in state %s',
                p_ingress_txn_id,
                v_status
            );
    END IF;

    UPDATE shiba_internal.ingress_transactions AS txn
       SET status = 'aborted',
           final_lsn = p_abort_lsn,
           end_lsn = p_abort_lsn,
           pending_publications = 0,
           finalized_at = clock_timestamp()
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    UPDATE shiba_internal.ingress_replay_state AS replay
       SET open_payload_bytes = replay.open_payload_bytes - v_payload_bytes,
           updated_at = clock_timestamp()
     WHERE replay.slot_generation = v_slot_generation
       AND replay.open_payload_bytes >= v_payload_bytes;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'XX001',
            MESSAGE = 'open ingress payload counter is smaller than aborted transaction';
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION shiba_internal.abort_ingress_subtransaction(
    p_ingress_txn_id bigint,
    p_source_subxid bigint
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_slot_generation bigint;
    v_status text;
BEGIN
    IF p_ingress_txn_id IS NULL OR p_source_subxid IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress subtransaction abort fields must not contain NULL';
    END IF;

    SELECT txn.slot_generation
      INTO STRICT v_slot_generation
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id;

    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = v_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'ingress slot generation %s is not active',
                v_slot_generation
            );
    END IF;

    SELECT txn.status
      INTO STRICT v_status
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;
    IF v_status <> 'open' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'cannot record a subtransaction abort for ingress transaction %s in state %s',
                p_ingress_txn_id,
                v_status
            );
    END IF;

    INSERT INTO shiba_internal.ingress_aborted_subtransactions (
        ingress_txn_id,
        source_subxid
    )
    VALUES (
        p_ingress_txn_id,
        p_source_subxid
    )
    ON CONFLICT DO NOTHING;
    RETURN FOUND;
END;
$$;

-- Advance only across a contiguous prefix of sealed transaction headers whose
-- source tasks all reached a durable terminal state.  This frontier is DAG
-- visibility, not logical-slot persistence, confirmation, or replay safety.
CREATE FUNCTION shiba_internal.advance_ingress_publication_frontier(
    p_slot_generation bigint
)
RETURNS pg_lsn
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_current_lsn pg_lsn;
    v_persisted_lsn pg_lsn;
    v_next_txn_id bigint;
    v_next_status text;
    v_next_lsn pg_lsn;
    v_next_pending_publications bigint;
BEGIN
    IF p_slot_generation IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress publication generation must not be NULL';
    END IF;

    SELECT replay.published_lsn,
           replay.persisted_lsn
      INTO STRICT v_current_lsn,
                  v_persisted_lsn
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    -- An open transaction cannot terminate before a commit/abort record that
    -- is already present in WAL, so only sealed headers define this order.
    -- Advancing one transaction per call keeps recovery work bounded.
    SELECT txn.ingress_txn_id,
           txn.status,
           txn.final_lsn,
           txn.pending_publications
      INTO v_next_txn_id,
           v_next_status,
           v_next_lsn,
           v_next_pending_publications
     FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.slot_generation = p_slot_generation
       AND txn.final_lsn IS NOT NULL
       AND (v_current_lsn IS NULL OR txn.final_lsn > v_current_lsn)
     ORDER BY txn.final_lsn, txn.ingress_txn_id
     LIMIT 1
     FOR UPDATE;

    IF v_next_txn_id IS NULL
       OR v_next_status NOT IN ('committed', 'aborted')
       OR v_persisted_lsn IS NULL
       OR v_next_lsn > v_persisted_lsn
       OR v_next_pending_publications <> 0 THEN
        RETURN v_current_lsn;
    END IF;

    UPDATE shiba_internal.ingress_replay_state AS replay
       SET published_lsn = v_next_lsn,
           updated_at = clock_timestamp()
     WHERE replay.slot_generation = p_slot_generation;
    v_current_lsn := v_next_lsn;

    RETURN v_current_lsn;
END;
$$;

-- Publish one bounded typed prefix of an already-durable, source-local task.
-- The immutable chunk, typed payload rows, and task cursor commit together.
-- A retry sees either the old cursor and stream sequence or both new values;
-- there is no intermediate durable state.
--
-- Only the head task for each source is eligible, preserving causal LSN order
-- within that shared stream.  Backpressured heads are skipped so other sources
-- can finish, but `has_pending` remains true and tells Runtime not to read more
-- replication input once no publishable head remains.
CREATE FUNCTION shiba_internal.publish_source_batch(
    p_slot_generation bigint
)
RETURNS TABLE (
    outcome text,
    ingress_txn_id bigint,
    batch_ordinal bigint,
    source_oid oid,
    final_lsn pg_lsn,
    chunk_seq bigint,
    has_pending boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_task shiba_internal.source_publications%ROWTYPE;
    v_candidate_txn_id bigint;
    v_candidate_batch_ordinal bigint;
    v_candidate_source_oid oid;
    v_final_lsn pg_lsn;
    v_first_input_seq bigint;
    v_last_input_seq bigint;
    v_current_input_seq bigint;
    v_selected_last_input_seq bigint;
    v_next_source_input_seq bigint;
    v_stream_id bigint;
    v_expected_chunk_seq bigint;
    v_stream_backpressured boolean;
    v_target_chunk_rows bigint;
    v_target_chunk_bytes bigint;
    v_append_outcome text;
    v_appended_chunk_seq bigint;
    v_payload_relation text;
    v_row_type text;
    v_payload_rows bigint;
    v_payload_bytes bigint;
    v_inserted_rows bigint;
    v_stored_rows bigint;
    v_stored_bytes bigint;
BEGIN
    IF p_slot_generation IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'source publication generation must not be NULL';
    END IF;

    -- Lock-order level 1.  Ingress, publication, and frontier advancement for
    -- one generation are serialized by this small authority row.
    PERFORM 1
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'ingress slot generation %s is not active',
                p_slot_generation
            );
    END IF;

    -- Candidate discovery is read-only.  Once selected, authority rows are
    -- locked explicitly in the global order below and the pending identity is
    -- revalidated before any stream is touched.
    SELECT publication.ingress_txn_id,
           publication.batch_ordinal,
           publication.source_oid
      INTO v_candidate_txn_id,
           v_candidate_batch_ordinal,
           v_candidate_source_oid
      FROM shiba_internal.source_publications AS publication
     JOIN shiba_internal.ingress_transactions AS txn
        ON txn.ingress_txn_id = publication.ingress_txn_id
     WHERE txn.slot_generation = p_slot_generation
       AND txn.status = 'committed'
       AND publication.next_input_seq IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
             FROM shiba_internal.source_publications AS earlier
             JOIN shiba_internal.ingress_transactions AS earlier_txn
              ON earlier_txn.ingress_txn_id = earlier.ingress_txn_id
            WHERE earlier_txn.slot_generation = txn.slot_generation
              AND earlier_txn.status = 'committed'
              AND earlier.source_oid = publication.source_oid
              AND earlier.next_input_seq IS NOT NULL
              AND (
                  earlier_txn.final_lsn,
                  earlier.ingress_txn_id,
                  earlier.batch_ordinal
              ) < (
                  txn.final_lsn,
                  publication.ingress_txn_id,
                  publication.batch_ordinal
              )
       )
       AND NOT EXISTS (
           SELECT 1
             FROM shiba_internal.effect_streams AS stream
            WHERE stream.producer_kind = 'source'
              AND stream.slot_generation = txn.slot_generation
              AND stream.source_oid = publication.source_oid
              AND stream.backpressured
              AND EXISTS (
                  SELECT 1
                    FROM shiba_internal.effect_stream_consumers AS consumer
                   WHERE consumer.stream_id = stream.stream_id
                     AND consumer.activation_lsn < txn.final_lsn
              )
       )
     ORDER BY txn.final_lsn,
              publication.ingress_txn_id,
              publication.batch_ordinal,
              publication.source_oid
     LIMIT 1;

    IF v_candidate_txn_id IS NULL THEN
        outcome := CASE
            WHEN EXISTS (
                SELECT 1
                  FROM shiba_internal.source_publications AS publication
                 JOIN shiba_internal.ingress_transactions AS txn
                    ON txn.ingress_txn_id = publication.ingress_txn_id
                 WHERE txn.slot_generation = p_slot_generation
                   AND txn.status = 'committed'
                   AND publication.next_input_seq IS NOT NULL
            )
            THEN 'blocked'
            ELSE 'idle'
        END;
        has_pending := outcome = 'blocked';
        RETURN NEXT;
        RETURN;
    END IF;

    -- Lock-order levels 2 through 4: transaction, apply batch, publication
    -- identity.  The generation row above prevents a new earlier task from
    -- appearing between read-only discovery and this recheck.
    SELECT txn.final_lsn
      INTO STRICT v_final_lsn
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = v_candidate_txn_id
       AND txn.slot_generation = p_slot_generation
     FOR UPDATE;

    SELECT batch.first_input_seq,
           batch.last_input_seq
      INTO STRICT v_first_input_seq,
                  v_last_input_seq
      FROM shiba_internal.ingress_apply_batches AS batch
     WHERE batch.ingress_txn_id = v_candidate_txn_id
       AND batch.batch_ordinal = v_candidate_batch_ordinal
     FOR UPDATE;

    SELECT publication.*
      INTO STRICT v_task
      FROM shiba_internal.source_publications AS publication
     WHERE publication.ingress_txn_id = v_candidate_txn_id
       AND publication.batch_ordinal = v_candidate_batch_ordinal
       AND publication.source_oid = v_candidate_source_oid
       AND publication.next_input_seq IS NOT NULL
     FOR UPDATE;

    v_current_input_seq := v_task.next_input_seq;
    ingress_txn_id := v_task.ingress_txn_id;
    batch_ordinal := v_task.batch_ordinal;
    source_oid := v_task.source_oid;
    final_lsn := v_final_lsn;

    -- A source stream is shared by every DAG in this generation.  If no
    -- eligible consumer existed at this causal LSN, the effect is explicitly
    -- discarded: a later registration starts after this LSN and must not see
    -- historical change-log rows.
    SELECT stream.stream_id,
           stream.next_chunk_seq,
           stream.backpressured,
           stream.target_chunk_rows,
           stream.target_chunk_bytes
      INTO v_stream_id,
           v_expected_chunk_seq,
           v_stream_backpressured,
           v_target_chunk_rows,
           v_target_chunk_bytes
      FROM shiba_internal.effect_streams AS stream
     WHERE stream.producer_kind = 'source'
       AND stream.slot_generation = p_slot_generation
       AND stream.source_oid = v_task.source_oid
     FOR UPDATE;

    IF NOT FOUND
       OR NOT EXISTS (
           SELECT 1
             FROM shiba_internal.effect_stream_consumers AS consumer
            WHERE consumer.stream_id = v_stream_id
              AND consumer.activation_lsn < v_final_lsn
       ) THEN
        outcome := 'discarded';
    ELSIF v_stream_backpressured THEN
        outcome := 'blocked';
    ELSE
        -- Resolve both relation and generated row type from authoritative OID
        -- metadata, then validate the complete fixed payload ABI.
        SELECT pg_catalog.format(
                   '%I.%I',
                   relation_namespace.nspname,
                   relation_catalog.relname
               ),
               pg_catalog.format(
                   '%I.%I',
                   type_namespace.nspname,
                   row_type.typname
               )
          INTO STRICT v_payload_relation,
                      v_row_type
          FROM shiba_internal.effect_stream_payloads AS payload
          JOIN pg_catalog.pg_class AS relation_catalog
            ON relation_catalog.oid = payload.relation_oid
           AND relation_catalog.relkind = 'r'
           AND relation_catalog.relpersistence = 'p'
          JOIN pg_catalog.pg_namespace AS relation_namespace
            ON relation_namespace.oid = relation_catalog.relnamespace
           AND relation_namespace.nspname = 'shiba_internal'
          JOIN pg_catalog.pg_type AS payload_record_type
            ON payload_record_type.oid = relation_catalog.reltype
           AND payload_record_type.typrelid = relation_catalog.oid
           AND payload_record_type.typnamespace =
                 relation_catalog.relnamespace
          JOIN pg_catalog.pg_type AS row_type
            ON row_type.oid = payload.row_type_oid
           AND row_type.typtype = 'c'
          JOIN pg_catalog.pg_namespace AS type_namespace
            ON type_namespace.oid = row_type.typnamespace
           AND type_namespace.nspname = 'shiba_internal'
          JOIN pg_catalog.pg_class AS row_type_relation
            ON row_type_relation.oid = row_type.typrelid
           AND row_type_relation.relkind = 'c'
           AND row_type_relation.relnamespace = type_namespace.oid
          JOIN pg_catalog.pg_attribute AS stream_id_attribute
            ON stream_id_attribute.attrelid = relation_catalog.oid
           AND stream_id_attribute.attnum = 1
           AND stream_id_attribute.attname = 'stream_id'
           AND stream_id_attribute.atttypid = 'bigint'::regtype
           AND stream_id_attribute.attnotnull
           AND NOT stream_id_attribute.attisdropped
          JOIN pg_catalog.pg_attribute AS chunk_seq_attribute
            ON chunk_seq_attribute.attrelid = relation_catalog.oid
           AND chunk_seq_attribute.attnum = 2
           AND chunk_seq_attribute.attname = 'chunk_seq'
           AND chunk_seq_attribute.atttypid = 'bigint'::regtype
           AND chunk_seq_attribute.attnotnull
           AND NOT chunk_seq_attribute.attisdropped
          JOIN pg_catalog.pg_attribute AS row_ordinal_attribute
            ON row_ordinal_attribute.attrelid = relation_catalog.oid
           AND row_ordinal_attribute.attnum = 3
           AND row_ordinal_attribute.attname = 'row_ordinal'
           AND row_ordinal_attribute.atttypid = 'bigint'::regtype
           AND row_ordinal_attribute.attnotnull
           AND NOT row_ordinal_attribute.attisdropped
          JOIN pg_catalog.pg_attribute AS weight_attribute
            ON weight_attribute.attrelid = relation_catalog.oid
           AND weight_attribute.attnum = 4
           AND weight_attribute.attname = 'weight'
           AND weight_attribute.atttypid = 'bigint'::regtype
           AND weight_attribute.attnotnull
           AND NOT weight_attribute.attisdropped
          JOIN pg_catalog.pg_attribute AS row_value_attribute
            ON row_value_attribute.attrelid = relation_catalog.oid
           AND row_value_attribute.attnum = 5
           AND row_value_attribute.attname = 'row_value'
           AND row_value_attribute.atttypid = row_type.oid
           AND row_value_attribute.attnotnull
           AND NOT row_value_attribute.attisdropped
         WHERE payload.stream_id = v_stream_id
           AND (
               SELECT count(*)
                 FROM pg_catalog.pg_attribute AS fixed_attribute
                WHERE fixed_attribute.attrelid = relation_catalog.oid
                  AND fixed_attribute.attnum > 0
                  AND NOT fixed_attribute.attisdropped
           ) = 5;

        -- Convert only this task's remaining source rows.  The running prefix
        -- is bounded by the stream's row and actual typed-byte targets; the
        -- first row may stand alone when its typed representation is larger
        -- than the byte target.
        EXECUTE pg_catalog.format(
            'WITH candidates AS MATERIALIZED (
               SELECT event.input_seq,
                      event.weight,
                      event.payload
                 FROM shiba_internal.change_log AS event
                WHERE event.ingress_txn_id = $3
                  AND event.source_oid = $4
                  AND event.input_seq BETWEEN $5 AND $6
                  AND NOT EXISTS (
                    SELECT 1
                      FROM shiba_internal.ingress_aborted_subtransactions
                           AS aborted
                     WHERE aborted.ingress_txn_id = event.ingress_txn_id
                       AND aborted.source_subxid = event.source_subxid
                  )
                ORDER BY event.input_seq
                LIMIT $7
             ),
             converted AS MATERIALIZED (
               SELECT candidate.input_seq,
                      candidate.weight,
                      pg_catalog.row_number() OVER (
                        ORDER BY candidate.input_seq
                      ) AS ordinal,
                      pg_catalog.jsonb_populate_record(
                        NULL::%s,
                        candidate.payload
                      ) AS row_value
                 FROM candidates AS candidate
             ),
             measured AS (
               SELECT converted.*,
                      shiba_internal.effect_row_bytes(
                        converted.row_value
                      ) AS row_bytes
                 FROM converted
             ),
             running AS (
               SELECT measured.*,
                      sum(measured.row_bytes) OVER (
                        ORDER BY measured.input_seq
                        ROWS UNBOUNDED PRECEDING
                      ) AS running_bytes
                 FROM measured
             ),
             selected AS (
               SELECT running.*
                 FROM running
                WHERE running.ordinal = 1
                   OR (
                     running.ordinal <= $7
                     AND running.running_bytes <= $8
                   )
             )
             SELECT count(*)::bigint,
                    coalesce(
                      sum(selected.row_bytes),
                      0
                    )::bigint,
                    max(selected.input_seq)
               FROM selected',
            v_row_type
        )
        INTO STRICT v_payload_rows,
                    v_payload_bytes,
                    v_selected_last_input_seq
        USING v_stream_id,
              v_expected_chunk_seq,
              v_task.ingress_txn_id,
              v_task.source_oid,
              greatest(v_current_input_seq, v_first_input_seq),
              v_last_input_seq,
              v_target_chunk_rows,
              v_target_chunk_bytes;

        IF v_payload_rows = 0 THEN
            outcome := 'completed';
        ELSIF v_payload_bytes < 1 THEN
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'source publication %s/%s/%s has no typed payload',
                    v_task.ingress_txn_id,
                    v_task.batch_ordinal,
                    v_task.source_oid
                );
        ELSE
        SELECT append.outcome,
               append.appended_chunk_seq
          INTO STRICT v_append_outcome,
                      v_appended_chunk_seq
          FROM shiba_internal.append_effect_stream_chunk(
                   v_stream_id,
                   v_expected_chunk_seq,
                   'data',
                   v_payload_rows,
                   v_payload_bytes,
                   v_final_lsn
               ) AS append;

        IF v_append_outcome = 'appended' THEN
            EXECUTE pg_catalog.format(
                'WITH candidates AS MATERIALIZED (
                   SELECT event.input_seq,
                          event.weight,
                          event.payload
                     FROM shiba_internal.change_log AS event
                    WHERE event.ingress_txn_id = $3
                      AND event.source_oid = $4
                      AND event.input_seq BETWEEN $5 AND $6
                      AND NOT EXISTS (
                        SELECT 1
                          FROM shiba_internal.ingress_aborted_subtransactions
                               AS aborted
                         WHERE aborted.ingress_txn_id = event.ingress_txn_id
                           AND aborted.source_subxid = event.source_subxid
                      )
                    ORDER BY event.input_seq
                    LIMIT $7
                 ),
                 converted AS MATERIALIZED (
                   SELECT candidate.input_seq,
                          candidate.weight,
                          pg_catalog.row_number() OVER (
                            ORDER BY candidate.input_seq
                          ) AS ordinal,
                          pg_catalog.jsonb_populate_record(
                            NULL::%s,
                            candidate.payload
                          ) AS row_value
                     FROM candidates AS candidate
                 ),
                 measured AS (
                   SELECT converted.*,
                          shiba_internal.effect_row_bytes(
                            converted.row_value
                          ) AS row_bytes
                     FROM converted
                 ),
                 running AS (
                   SELECT measured.*,
                          sum(measured.row_bytes) OVER (
                            ORDER BY measured.input_seq
                            ROWS UNBOUNDED PRECEDING
                          ) AS running_bytes
                     FROM measured
                 )
                 INSERT INTO %s (
                     stream_id,
                     chunk_seq,
                     row_ordinal,
                     weight,
                     row_value
                 )
                 SELECT $1,
                        $2,
                        (running.ordinal - 1)::bigint,
                        running.weight,
                        running.row_value
                   FROM running
                  WHERE running.ordinal = 1
                     OR (
                       running.ordinal <= $7
                       AND running.running_bytes <= $8
                     )
                  ORDER BY running.input_seq',
                v_row_type,
                v_payload_relation
            )
            USING v_stream_id,
                  v_appended_chunk_seq,
                  v_task.ingress_txn_id,
                  v_task.source_oid,
                  greatest(v_current_input_seq, v_first_input_seq),
                  v_last_input_seq,
                  v_target_chunk_rows,
                  v_target_chunk_bytes;

            GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;
            EXECUTE pg_catalog.format(
                'SELECT count(*)::bigint,
                        coalesce(
                          sum(shiba_internal.effect_row_bytes(
                            stored.row_value
                          )),
                          0
                        )::bigint
                   FROM %s AS stored
                  WHERE stored.stream_id = $1
                    AND stored.chunk_seq = $2',
                v_payload_relation
            )
            INTO STRICT v_stored_rows,
                        v_stored_bytes
            USING v_stream_id,
                  v_appended_chunk_seq;

            IF v_inserted_rows <> v_payload_rows
               OR v_stored_rows <> v_payload_rows
               OR v_stored_bytes <> v_payload_bytes THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'XX001',
                    MESSAGE = format(
                        'source publication %s/%s/%s typed payload measured %s/%s, inserted %s, and stored %s/%s',
                        v_task.ingress_txn_id,
                        v_task.batch_ordinal,
                        v_task.source_oid,
                        v_payload_rows,
                        v_payload_bytes,
                        v_inserted_rows,
                        v_stored_rows,
                        v_stored_bytes
                    );
            END IF;

            SELECT min(event.input_seq)
              INTO v_next_source_input_seq
              FROM shiba_internal.change_log AS event
             WHERE event.ingress_txn_id = v_task.ingress_txn_id
               AND event.source_oid = v_task.source_oid
               AND event.input_seq > v_selected_last_input_seq
               AND event.input_seq <= v_last_input_seq
               AND NOT EXISTS (
                   SELECT 1
                     FROM shiba_internal.ingress_aborted_subtransactions
                          AS aborted
                    WHERE aborted.ingress_txn_id = event.ingress_txn_id
                      AND aborted.source_subxid = event.source_subxid
               );

            outcome := CASE
                WHEN v_next_source_input_seq IS NULL THEN 'completed'
                ELSE 'appended'
            END;
            chunk_seq := v_appended_chunk_seq;
        ELSIF v_append_outcome = 'discarded' THEN
            -- The last consumer can disappear after the eligibility check.
            -- append_effect_stream_chunk resolves that race while holding the
            -- stream lock and deliberately creates no empty chunk.
            outcome := 'discarded';
        ELSIF v_append_outcome = 'blocked' THEN
            outcome := 'blocked';
        ELSE
            RAISE EXCEPTION USING
                ERRCODE = 'XX001',
                MESSAGE = format(
                    'unknown source stream append outcome %s',
                    v_append_outcome
                );
        END IF;
        END IF;
    END IF;

    IF outcome IN ('appended', 'completed', 'discarded') THEN
        UPDATE shiba_internal.source_publications AS publication
           SET next_input_seq = CASE
                   WHEN outcome = 'appended' THEN v_next_source_input_seq
                   ELSE NULL
               END
         WHERE publication.ingress_txn_id = v_task.ingress_txn_id
           AND publication.batch_ordinal = v_task.batch_ordinal
           AND publication.source_oid = v_task.source_oid
           AND publication.next_input_seq = v_current_input_seq;

        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                MESSAGE = 'source publication task changed during append';
        END IF;

        IF outcome IN ('completed', 'discarded') THEN
            UPDATE shiba_internal.ingress_transactions AS txn
               SET pending_publications = txn.pending_publications - 1
             WHERE txn.ingress_txn_id = v_task.ingress_txn_id
               AND txn.pending_publications > 0;

            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'XX001',
                    MESSAGE = 'source publication counter reached zero before its task';
            END IF;
        END IF;
    END IF;

    SELECT EXISTS (
        SELECT 1
         FROM shiba_internal.ingress_transactions AS txn
         WHERE txn.slot_generation = p_slot_generation
           AND txn.status = 'committed'
           AND txn.pending_publications > 0
    )
      INTO has_pending;
    RETURN NEXT;
END;
$$;

CREATE FUNCTION shiba_internal.ingress_feedback_upper_bound(
    p_slot_generation bigint
)
RETURNS TABLE (
    slot_generation bigint,
    persisted_lsn pg_lsn,
    confirmed_lsn pg_lsn,
    replay_safe_lsn pg_lsn
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT replay.slot_generation,
           replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.database_oid =
           (SELECT database.oid
              FROM pg_catalog.pg_database AS database
             WHERE database.datname = pg_catalog.current_database())
       AND replay.state = 'active'
$$;

-- Record feedback only after the replication feedback write succeeds.  This
-- is monotonic confirmation intent; it MUST NOT advance replay_safe_lsn.
CREATE FUNCTION shiba_internal.record_ingress_feedback(
    p_slot_generation bigint,
    p_confirmed_lsn pg_lsn
)
RETURNS TABLE (
    confirmed_lsn pg_lsn,
    persisted_lsn pg_lsn,
    replay_safe_lsn pg_lsn,
    advanced boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_persisted_lsn pg_lsn;
    v_confirmed_lsn pg_lsn;
    v_replay_safe_lsn pg_lsn;
BEGIN
    IF p_slot_generation IS NULL OR p_confirmed_lsn IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress feedback fields must not contain NULL';
    END IF;

    -- Lock-order level 1.  The row lock and update are transaction-scoped.
    SELECT replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      INTO STRICT v_persisted_lsn,
                  v_confirmed_lsn,
                  v_replay_safe_lsn
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.database_oid =
           (SELECT database.oid
              FROM pg_catalog.pg_database AS database
             WHERE database.datname = pg_catalog.current_database())
       AND replay.state = 'active'
     FOR UPDATE;

    IF v_persisted_lsn IS NULL OR p_confirmed_lsn > v_persisted_lsn THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = format(
                'feedback LSN %s exceeds persisted ingress LSN %s for generation %s',
                p_confirmed_lsn,
                coalesce(v_persisted_lsn::text, 'NULL'),
                p_slot_generation
            );
    END IF;

    advanced := v_confirmed_lsn IS NULL
                OR p_confirmed_lsn > v_confirmed_lsn;

    IF advanced THEN
        UPDATE shiba_internal.ingress_replay_state AS replay
           SET confirmed_lsn = p_confirmed_lsn,
               updated_at = clock_timestamp()
         WHERE replay.slot_generation = p_slot_generation;
        v_confirmed_lsn := p_confirmed_lsn;
    END IF;

    confirmed_lsn := v_confirmed_lsn;
    persisted_lsn := v_persisted_lsn;
    replay_safe_lsn := v_replay_safe_lsn;
    RETURN NEXT;
END;
$$;

-- Advance the GC-safe watermark only from PostgreSQL's observed slot state.
-- A successful feedback write is intent; confirmed_flush_lsn is proof.
CREATE FUNCTION shiba_internal.reconcile_ingress_replay_safe(
    p_slot_generation bigint
)
RETURNS pg_lsn
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_slot_name name;
    v_database_oid oid;
    v_baseline_lsn pg_lsn;
    v_persisted_lsn pg_lsn;
    v_confirmed_lsn pg_lsn;
    v_replay_safe_lsn pg_lsn;
    v_actual_lsn pg_lsn;
BEGIN
    SELECT replay.slot_name,
           replay.database_oid,
           replay.slot_baseline_lsn,
           replay.persisted_lsn,
           replay.confirmed_lsn,
           replay.replay_safe_lsn
      INTO STRICT v_slot_name,
                  v_database_oid,
                  v_baseline_lsn,
                  v_persisted_lsn,
                  v_confirmed_lsn,
                  v_replay_safe_lsn
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    SELECT slot.confirmed_flush_lsn
      INTO v_actual_lsn
      FROM pg_catalog.pg_replication_slots AS slot
     WHERE slot.slot_name = v_slot_name::text
       AND slot.database = pg_catalog.current_database()
       AND slot.plugin = 'pgoutput';

    IF NOT FOUND
       OR v_database_oid IS DISTINCT FROM (
           SELECT database.oid
             FROM pg_catalog.pg_database AS database
            WHERE database.datname = pg_catalog.current_database()
       )
       OR v_actual_lsn IS NULL
       OR v_actual_lsn < v_baseline_lsn
       OR (v_persisted_lsn IS NOT NULL
           AND v_actual_lsn > v_persisted_lsn)
       OR (v_persisted_lsn IS NULL
           AND v_actual_lsn <> v_baseline_lsn) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'logical slot %s cannot prove replay safety for generation %s',
                v_slot_name,
                p_slot_generation
            );
    END IF;

    IF v_persisted_lsn IS NULL THEN
        RETURN NULL;
    END IF;

    v_confirmed_lsn := greatest(
        coalesce(v_confirmed_lsn, v_actual_lsn),
        v_actual_lsn
    );
    v_replay_safe_lsn := greatest(
        coalesce(v_replay_safe_lsn, v_actual_lsn),
        v_actual_lsn
    );

    UPDATE shiba_internal.ingress_replay_state AS replay
       SET confirmed_lsn = v_confirmed_lsn,
           replay_safe_lsn = v_replay_safe_lsn,
           updated_at = clock_timestamp()
     WHERE replay.slot_generation = p_slot_generation
       AND (
           replay.confirmed_lsn IS DISTINCT FROM v_confirmed_lsn
           OR replay.replay_safe_lsn IS DISTINCT FROM v_replay_safe_lsn
       );

    RETURN v_replay_safe_lsn;
END;
$$;

REVOKE ALL ON FUNCTION
    shiba_internal.ensure_ingress_generation(name, boolean)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.retire_ingress_generation(name)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.claim_ingress_transaction(bigint, bigint, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.insert_ingress_events(bigint, jsonb)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.commit_ingress_transaction(bigint, pg_lsn, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.abort_ingress_transaction(bigint, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.abort_ingress_subtransaction(bigint, bigint)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.advance_ingress_publication_frontier(bigint)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.publish_source_batch(bigint)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.ingress_feedback_upper_bound(bigint)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.record_ingress_feedback(bigint, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.reconcile_ingress_replay_safe(bigint)
    FROM PUBLIC;
