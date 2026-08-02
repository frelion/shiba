-- M2's four durable facts. The runtime is their only logical writer and owns
-- one PostgreSQL transaction spanning every change for a source transaction.

CREATE TABLE shiba_internal.applied_insert (
    source_id bigint NOT NULL CHECK (source_id > 0),
    slot_generation bigint NOT NULL CHECK (slot_generation > 0),
    commit_lsn pg_lsn NOT NULL CHECK (commit_lsn > '0/0'::pg_lsn),
    ingress_transaction_id bigint NOT NULL CHECK (ingress_transaction_id > 0),
    input_sequence bigint NOT NULL CHECK (input_sequence > 0),
    source_row_id bigint NOT NULL,
    CONSTRAINT applied_insert_cause_primary PRIMARY KEY (
        source_id,
        slot_generation,
        commit_lsn,
        ingress_transaction_id,
        input_sequence
    ),
    CONSTRAINT applied_insert_source_row_unique UNIQUE (source_id, source_row_id)
);

CREATE TABLE shiba_internal.count_state (
    singleton smallint PRIMARY KEY CHECK (singleton = 1),
    row_count bigint NOT NULL CHECK (row_count >= 0)
);

INSERT INTO shiba_internal.count_state (singleton, row_count) VALUES (1, 0);

CREATE TABLE shiba_internal.source_continuation (
    source_id bigint NOT NULL CHECK (source_id > 0),
    slot_generation bigint NOT NULL CHECK (slot_generation > 0),
    commit_lsn pg_lsn NOT NULL CHECK (commit_lsn > '0/0'::pg_lsn),
    ingress_transaction_id bigint NOT NULL CHECK (ingress_transaction_id > 0),
    CONSTRAINT source_continuation_coordinate_primary PRIMARY KEY (
        source_id,
        slot_generation,
        commit_lsn
    )
);

CREATE TABLE shiba.count_result (
    singleton smallint PRIMARY KEY CHECK (singleton = 1),
    row_count bigint NOT NULL CHECK (row_count >= 0)
);

INSERT INTO shiba.count_result (singleton, row_count) VALUES (1, 0);

REVOKE ALL ON TABLE shiba_internal.applied_insert FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.count_state FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.source_continuation FROM PUBLIC;
REVOKE ALL ON TABLE shiba.count_result FROM PUBLIC;
GRANT SELECT ON TABLE shiba.count_result TO PUBLIC;

COMMENT ON TABLE shiba_internal.applied_insert IS
    'M2 durable Source Apply facts, keyed by stable source cause identity';
COMMENT ON TABLE shiba_internal.count_state IS
    'M2 private deterministic count operator state';
COMMENT ON TABLE shiba_internal.source_continuation IS
    'M2 committed transaction history and replay authority';
COMMENT ON TABLE shiba.count_result IS
    'M2 SQL-queryable count Result Sink projection';
