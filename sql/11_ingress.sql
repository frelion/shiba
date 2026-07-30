-- Durable ingress SQL primitives.
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
-- 4. source_publications
-- 5. effect_streams / effect_stream_chunks / typed payload relation
--
-- Every mutating function below and the Rust admission/publication protocols
-- follow the applicable prefix of this order.
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

-- Bounded event admission control flow lives in Rust (src/admission.rs).
-- PostgreSQL retains the durable catalog written by its one atomic CTE.

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
    v_transaction_start_lsn pg_lsn;
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
           txn.transaction_start_lsn,
           txn.payload_bytes
      INTO STRICT v_status,
                  v_existing_abort_lsn,
                  v_transaction_start_lsn,
                  v_payload_bytes
      FROM shiba_internal.ingress_transactions AS txn
     WHERE txn.ingress_txn_id = p_ingress_txn_id
     FOR UPDATE;

    IF v_status = 'aborted' THEN
        -- Crash reconciliation uses transaction_start_lsn as a deterministic
        -- local identity because PostgreSQL may omit StreamAbort after a
        -- postmaster crash.  Some PostgreSQL versions nevertheless replay a
        -- matching StreamAbort while the slot catches up.  Treat that record
        -- as the same durable abort; an unrelated LSN remains corruption.
        IF v_existing_abort_lsn IS DISTINCT FROM p_abort_lsn
           AND v_existing_abort_lsn IS DISTINCT FROM v_transaction_start_lsn THEN
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

-- PostgreSQL does not promise a StreamAbort record after a postmaster crash:
-- logical decoding simply omits the source transaction that crash recovery
-- aborted.  Its already-durable streamed prefix must therefore be finalized
-- locally before it can retain admission budget forever.  A normal Runtime
-- restart keeps the same postmaster epoch and deliberately does nothing.
CREATE FUNCTION shiba_internal.reconcile_postmaster_restart(
    p_slot_generation bigint
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_recorded_started_at timestamptz;
    v_current_started_at timestamptz;
    v_aborted_count bigint;
BEGIN
    IF p_slot_generation IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22004',
            MESSAGE = 'ingress slot generation must not be NULL';
    END IF;

    v_current_started_at := pg_catalog.pg_postmaster_start_time();

    -- Lock-order level 1.  This shares the same generation-row serialization
    -- point as admission and explicit StreamAbort processing.
    SELECT replay.postmaster_started_at
      INTO STRICT v_recorded_started_at
      FROM shiba_internal.ingress_replay_state AS replay
     WHERE replay.slot_generation = p_slot_generation
       AND replay.state = 'active'
     FOR UPDATE;

    IF v_recorded_started_at = v_current_started_at THEN
        RETURN 0;
    END IF;

    UPDATE shiba_internal.ingress_transactions AS txn
       SET status = 'aborted',
           -- Crash recovery gives no protocol Abort LSN.  The immutable
           -- transaction-start LSN is a deterministic local final identity;
           -- aborted transactions never advance replication feedback.
           final_lsn = txn.transaction_start_lsn,
           end_lsn = txn.transaction_start_lsn,
           pending_publications = 0,
           finalized_at = clock_timestamp()
     WHERE txn.slot_generation = p_slot_generation
       AND txn.status = 'open';
    GET DIAGNOSTICS v_aborted_count = ROW_COUNT;

    UPDATE shiba_internal.ingress_replay_state AS replay
       SET postmaster_started_at = v_current_started_at,
           open_payload_bytes = 0,
           updated_at = clock_timestamp()
     WHERE replay.slot_generation = p_slot_generation;

    RETURN v_aborted_count;
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

-- Source publication control flow lives in Rust (src/publication.rs).
-- SQL retains only the durable catalog and typed append primitive.

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
    shiba_internal.commit_ingress_transaction(bigint, pg_lsn, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.abort_ingress_transaction(bigint, pg_lsn)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.reconcile_postmaster_restart(bigint)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.abort_ingress_subtransaction(bigint, bigint)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    shiba_internal.advance_ingress_publication_frontier(bigint)
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
