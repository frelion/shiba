CREATE SCHEMA IF NOT EXISTS shiba_internal;
REVOKE ALL ON SCHEMA shiba_internal FROM PUBLIC;

-- One row is one user-visible, continuously maintained result table.
CREATE TABLE shiba_internal.dataflows (
    result_oid oid PRIMARY KEY CHECK (result_oid <> 0::oid),
    plan jsonb NOT NULL CHECK (jsonb_typeof(plan) = 'object'),
    activation_lsn pg_lsn NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE shiba_internal.dataflow_sources (
    result_oid oid NOT NULL
      REFERENCES shiba_internal.dataflows(result_oid) ON DELETE CASCADE,
    source_oid oid NOT NULL CHECK (source_oid <> 0::oid),
    PRIMARY KEY (result_oid, source_oid)
);

CREATE INDEX dataflow_sources_source_idx
  ON shiba_internal.dataflow_sources(source_oid, result_oid);

-- Only indexes created through shiba.create_index are removable through the
-- user API. PostgreSQL catalog OIDs cannot be foreign-key targets, so every
-- lifecycle operation revalidates the live object identity.
CREATE TABLE shiba_internal.managed_indexes (
    index_oid oid PRIMARY KEY,
    result_oid oid NOT NULL
      REFERENCES shiba_internal.dataflows(result_oid) ON DELETE CASCADE,
    index_name name NOT NULL,
    index_columns name[] NOT NULL,
    creator_oid oid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (result_oid, index_name)
);

-- One row is one stage's durable CAS authority and runtime identity.
CREATE TABLE shiba_internal.operator_checkpoints (
    result_oid oid NOT NULL
      REFERENCES shiba_internal.dataflows(result_oid) ON DELETE CASCADE,
    stage_id integer NOT NULL CHECK (stage_id >= 0),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    has_continuation boolean NOT NULL DEFAULT false,
    admitted_rows bigint NOT NULL DEFAULT 0 CHECK (admitted_rows >= 0),
    admitted_bytes bigint NOT NULL DEFAULT 0 CHECK (admitted_bytes >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (result_oid, stage_id)
);

-- One database-scoped Runtime owns replication, publication, operator
-- scheduling, and bounded garbage collection.
CREATE TABLE shiba_internal.runtime_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    active boolean NOT NULL DEFAULT false,
    owner_pid integer CHECK (owner_pid > 0),
    started_at timestamptz,
    last_heartbeat timestamptz,
    launch_generation bigint NOT NULL DEFAULT 0 CHECK (launch_generation >= 0),
    pending_launch_xid xid8,
    pending_since timestamptz,
    CHECK ((owner_pid IS NULL) = (started_at IS NULL)),
    CHECK ((pending_launch_xid IS NULL) = (pending_since IS NULL)),
    CHECK (owner_pid IS NULL OR pending_launch_xid IS NULL)
);

-- A generation changes whenever the logical slot is recreated. persisted_lsn
-- is durable decode progress. published_lsn is the greatest contiguous source
-- transaction frontier whose effects have all reached source streams or were
-- intentionally discarded. Neither value is inferred from Runtime memory.
CREATE TABLE shiba_internal.ingress_replay_state (
    slot_generation bigint PRIMARY KEY CHECK (slot_generation > 0),
    slot_name name NOT NULL CHECK (length(slot_name::text) > 0),
    database_oid oid NOT NULL CHECK (database_oid <> 0::oid),
    plugin name NOT NULL DEFAULT 'pgoutput' CHECK (plugin = 'pgoutput'::name),
    system_identifier text NOT NULL CHECK (length(system_identifier) > 0),
    slot_baseline_lsn pg_lsn NOT NULL,
    state text NOT NULL DEFAULT 'active'
      CHECK (state IN ('active', 'retired')),
    persisted_lsn pg_lsn,
    published_lsn pg_lsn,
    confirmed_lsn pg_lsn,
    replay_safe_lsn pg_lsn,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz,
    CHECK (published_lsn IS NULL OR persisted_lsn IS NOT NULL),
    CHECK (published_lsn IS NULL OR published_lsn <= persisted_lsn),
    CHECK (confirmed_lsn IS NULL OR persisted_lsn IS NOT NULL),
    CHECK (confirmed_lsn IS NULL OR confirmed_lsn <= persisted_lsn),
    CHECK (replay_safe_lsn IS NULL OR confirmed_lsn IS NOT NULL),
    CHECK (replay_safe_lsn IS NULL OR replay_safe_lsn <= confirmed_lsn),
    CHECK ((state = 'active') = (retired_at IS NULL)),
    UNIQUE (slot_name, slot_generation)
);

CREATE UNIQUE INDEX ingress_active_slot_idx
  ON shiba_internal.ingress_replay_state(database_oid, slot_name)
  WHERE state = 'active';

-- With pgoutput streaming disabled, Begin.final_lsn is available before its
-- row messages and equals the later Commit.commit_lsn.
CREATE TABLE shiba_internal.ingress_transactions (
    ingress_txn_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slot_generation bigint NOT NULL
      REFERENCES shiba_internal.ingress_replay_state(slot_generation)
      ON DELETE RESTRICT,
    source_xid bigint NOT NULL CHECK (source_xid BETWEEN 0 AND 4294967295),
    final_lsn pg_lsn NOT NULL,
    status text NOT NULL DEFAULT 'open'
      CHECK (status IN ('open', 'committed')),
    commit_lsn pg_lsn,
    end_lsn pg_lsn,
    event_count bigint NOT NULL DEFAULT 0 CHECK (event_count >= 0),
    payload_bytes bigint NOT NULL DEFAULT 0 CHECK (payload_bytes >= 0),
    batch_count bigint NOT NULL DEFAULT 0 CHECK (batch_count >= 0),
    pending_publications bigint NOT NULL DEFAULT 0
      CHECK (pending_publications >= 0),
    opened_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finalized_at timestamptz,
    UNIQUE (slot_generation, source_xid, final_lsn),
    UNIQUE (ingress_txn_id, final_lsn),
    CHECK (
      (
        status = 'open'
        AND commit_lsn IS NULL
        AND end_lsn IS NULL
        AND finalized_at IS NULL
      )
      OR
      (
        status = 'committed'
        AND commit_lsn IS NOT NULL
        AND end_lsn IS NOT NULL
        AND finalized_at IS NOT NULL
        AND final_lsn = commit_lsn
        AND commit_lsn <= end_lsn
      )
    )
);

CREATE UNIQUE INDEX ingress_commit_lsn_idx
  ON shiba_internal.ingress_transactions(commit_lsn)
  WHERE status = 'committed';

CREATE INDEX ingress_open_txn_idx
  ON shiba_internal.ingress_transactions(slot_generation, source_xid, final_lsn)
  WHERE status = 'open';

CREATE INDEX ingress_publication_order_idx
  ON shiba_internal.ingress_transactions(
    slot_generation, final_lsn, ingress_txn_id
  );

CREATE INDEX ingress_pending_publication_idx
  ON shiba_internal.ingress_transactions(slot_generation)
  WHERE pending_publications > 0;

-- A source row image is stored once. input_seq is stable across replay even
-- when the replication transport regroups CopyData frames.
CREATE TABLE shiba_internal.change_log (
    ingress_txn_id bigint NOT NULL
      REFERENCES shiba_internal.ingress_transactions(ingress_txn_id)
      ON DELETE CASCADE,
    change_lsn pg_lsn NOT NULL,
    change_ordinal bigint NOT NULL CHECK (change_ordinal >= 0),
    image_ordinal integer NOT NULL CHECK (image_ordinal >= 0),
    input_seq bigint NOT NULL CHECK (input_seq > 0),
    source_oid oid NOT NULL CHECK (source_oid <> 0::oid),
    weight bigint NOT NULL CHECK (weight IN (-1, 1)),
    payload jsonb NOT NULL,
    persisted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
      ingress_txn_id, change_lsn, change_ordinal, image_ordinal
    ),
    UNIQUE (ingress_txn_id, input_seq)
);

CREATE INDEX change_log_source_batch_idx
  ON shiba_internal.change_log(ingress_txn_id, source_oid, input_seq);

-- Each bounded prefix admitted by ingress is independently publishable.
CREATE TABLE shiba_internal.ingress_apply_batches (
    ingress_txn_id bigint NOT NULL
      REFERENCES shiba_internal.ingress_transactions(ingress_txn_id)
      ON DELETE CASCADE,
    batch_ordinal bigint NOT NULL CHECK (batch_ordinal > 0),
    first_input_seq bigint NOT NULL CHECK (first_input_seq > 0),
    last_input_seq bigint NOT NULL CHECK (last_input_seq >= first_input_seq),
    persisted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (ingress_txn_id, batch_ordinal),
    UNIQUE (ingress_txn_id, first_input_seq, last_input_seq)
);

-- Publication work is per source, not per subscribing dataflow. A published
-- chunk is shared by every Scan consumer of that source stream.
CREATE TABLE shiba_internal.source_publications (
    ingress_txn_id bigint NOT NULL,
    batch_ordinal bigint NOT NULL,
    source_oid oid NOT NULL CHECK (source_oid <> 0::oid),
    next_input_seq bigint CHECK (next_input_seq > 0),
    PRIMARY KEY (ingress_txn_id, batch_ordinal, source_oid),
    FOREIGN KEY (ingress_txn_id, batch_ordinal)
      REFERENCES shiba_internal.ingress_apply_batches(
        ingress_txn_id, batch_ordinal
      )
      ON DELETE CASCADE
);

CREATE INDEX source_publications_ready_idx
  ON shiba_internal.source_publications(
    source_oid, ingress_txn_id, batch_ordinal
  )
  WHERE next_input_seq IS NOT NULL;

-- One producer output is one durable stream. Each downstream edge adds a
-- consumer cursor; fanout never copies the payload.
CREATE TABLE shiba_internal.effect_streams (
    stream_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    producer_kind text NOT NULL CHECK (producer_kind IN ('source', 'operator')),
    slot_generation bigint,
    source_oid oid,
    producer_result_oid oid,
    producer_stage_id integer,
    next_chunk_seq bigint NOT NULL DEFAULT 1 CHECK (next_chunk_seq >= 1),
    first_retained_chunk_seq bigint NOT NULL DEFAULT 1
      CHECK (first_retained_chunk_seq >= 1),
    latest_data_lsn pg_lsn,
    published_frontier_lsn pg_lsn,
    buffered_chunks bigint NOT NULL DEFAULT 0 CHECK (buffered_chunks >= 0),
    buffered_rows numeric NOT NULL DEFAULT 0 CHECK (buffered_rows >= 0),
    buffered_bytes numeric NOT NULL DEFAULT 0 CHECK (buffered_bytes >= 0),
    target_chunk_rows bigint NOT NULL CHECK (target_chunk_rows > 0),
    target_chunk_bytes bigint NOT NULL CHECK (target_chunk_bytes > 0),
    high_chunks bigint NOT NULL CHECK (high_chunks > 0),
    high_rows bigint NOT NULL CHECK (high_rows > 0),
    high_bytes bigint NOT NULL CHECK (high_bytes > 0),
    low_chunks bigint NOT NULL CHECK (low_chunks >= 0),
    low_rows bigint NOT NULL CHECK (low_rows >= 0),
    low_bytes bigint NOT NULL CHECK (low_bytes >= 0),
    backpressured boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (slot_generation)
      REFERENCES shiba_internal.ingress_replay_state(slot_generation)
      ON DELETE RESTRICT,
    FOREIGN KEY (producer_result_oid, producer_stage_id)
      REFERENCES shiba_internal.operator_checkpoints(result_oid, stage_id)
      ON DELETE CASCADE,
    CHECK (
      (
        producer_kind = 'source'
        AND slot_generation IS NOT NULL
        AND source_oid IS NOT NULL
        AND source_oid <> 0::oid
        AND producer_result_oid IS NULL
        AND producer_stage_id IS NULL
        AND published_frontier_lsn IS NULL
      )
      OR
      (
        producer_kind = 'operator'
        AND slot_generation IS NULL
        AND source_oid IS NULL
        AND producer_result_oid IS NOT NULL
        AND producer_stage_id IS NOT NULL
        AND producer_stage_id >= 0
      )
    ),
    CHECK (first_retained_chunk_seq <= next_chunk_seq),
    CHECK (
      buffered_chunks::numeric
        = next_chunk_seq::numeric - first_retained_chunk_seq::numeric
    ),
    CHECK (target_chunk_rows <= high_rows),
    CHECK (target_chunk_bytes <= high_bytes),
    CHECK (low_chunks < high_chunks),
    CHECK (low_rows < high_rows),
    CHECK (low_bytes < high_bytes)
);

CREATE UNIQUE INDEX effect_stream_source_producer_idx
  ON shiba_internal.effect_streams(slot_generation, source_oid)
  WHERE producer_kind = 'source';

CREATE UNIQUE INDEX effect_stream_operator_producer_idx
  ON shiba_internal.effect_streams(
    producer_result_oid, producer_stage_id
  )
  WHERE producer_kind = 'operator';

-- payload_bytes is the logical typed-effect size: sum(effect_row_bytes(
-- row_value)). The helper materializes the typed composite before measuring it
-- and adds eight bytes for weight, so TOAST does not change accounting.
-- stream_id, chunk_seq, and row_ordinal are bounded independently by row_count.
CREATE TABLE shiba_internal.effect_stream_chunks (
    stream_id bigint NOT NULL
      REFERENCES shiba_internal.effect_streams(stream_id) ON DELETE CASCADE,
    chunk_seq bigint NOT NULL CHECK (chunk_seq >= 1),
    chunk_kind text NOT NULL CHECK (chunk_kind IN ('data', 'frontier')),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    payload_bytes bigint NOT NULL CHECK (payload_bytes >= 0),
    chunk_lsn pg_lsn NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (stream_id, chunk_seq),
    CHECK (
      (
        chunk_kind = 'data'
        AND row_count > 0
        AND payload_bytes > 0
      )
      OR
      (
        chunk_kind = 'frontier'
        AND row_count = 0
        AND payload_bytes = 0
      )
    )
);

-- Source consumers keep their dataflow activation boundary. Operator
-- consumers start at 0/0 so the producer's activation SnapshotFrontier is a
-- real frontier advance.
CREATE TABLE shiba_internal.effect_stream_consumers (
    stream_id bigint NOT NULL
      REFERENCES shiba_internal.effect_streams(stream_id) ON DELETE CASCADE,
    result_oid oid NOT NULL,
    consumer_stage_id integer NOT NULL CHECK (consumer_stage_id >= 0),
    input_port integer NOT NULL CHECK (input_port >= 0),
    next_chunk_seq bigint NOT NULL CHECK (next_chunk_seq >= 1),
    activation_lsn pg_lsn NOT NULL,
    consumed_frontier_lsn pg_lsn NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (stream_id, result_oid, consumer_stage_id, input_port),
    UNIQUE (result_oid, consumer_stage_id, input_port),
    FOREIGN KEY (result_oid, consumer_stage_id)
      REFERENCES shiba_internal.operator_checkpoints(result_oid, stage_id)
      ON DELETE CASCADE,
    CHECK (consumed_frontier_lsn >= activation_lsn)
);

-- Every stream owns one generated composite and one LOGGED payload relation.
-- relation_oid and row_type_oid are the authority; runtime code must recheck
-- their live namespace/name identity and never guess an object name.
CREATE TABLE shiba_internal.effect_stream_payloads (
    stream_id bigint PRIMARY KEY
      REFERENCES shiba_internal.effect_streams(stream_id) ON DELETE CASCADE,
    relation_oid oid NOT NULL UNIQUE,
    row_type_oid oid NOT NULL UNIQUE
);

-- Each stateful kernel owns typed, LOGGED relations and records them here.
-- The common catalog knows only object identity and typed schema, never the
-- operator-specific row layout.
CREATE TABLE shiba_internal.operator_state_relations (
    result_oid oid NOT NULL,
    stage_id integer NOT NULL,
    state_slot integer NOT NULL CHECK (state_slot >= 0),
    relation_oid oid NOT NULL UNIQUE,
    PRIMARY KEY (result_oid, stage_id, state_slot),
    FOREIGN KEY (result_oid, stage_id)
      REFERENCES shiba_internal.operator_checkpoints(result_oid, stage_id)
      ON DELETE CASCADE
);

-- Continuations are also kernel-owned typed relations. A checkpoint's
-- has_continuation flag is the small CAS authority; this row identifies the
-- typed durable object that holds its resumable cursor.
CREATE TABLE shiba_internal.operator_continuation_relations (
    result_oid oid NOT NULL,
    stage_id integer NOT NULL,
    relation_oid oid NOT NULL UNIQUE,
    PRIMARY KEY (result_oid, stage_id),
    FOREIGN KEY (result_oid, stage_id)
      REFERENCES shiba_internal.operator_checkpoints(result_oid, stage_id)
      ON DELETE CASCADE
);

INSERT INTO shiba_internal.runtime_state(singleton) VALUES (true);
