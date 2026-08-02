-- M9.1 durable Source Apply, operator, result, and replay authorities. The
-- registration/runtime writer owns each transaction; installation seeds none.

CREATE TABLE shiba_internal.applied_insert (
    source_id bigint NOT NULL CHECK (source_id > 0),
    slot_generation bigint NOT NULL CHECK (slot_generation > 0),
    commit_lsn pg_lsn NOT NULL CHECK (commit_lsn > '0/0'::pg_lsn),
    ingress_transaction_id bigint NOT NULL CHECK (ingress_transaction_id > 0),
    input_sequence bigint NOT NULL CHECK (input_sequence > 0),
    source_row_id bigint NOT NULL,
    CONSTRAINT applied_insert_cause_primary PRIMARY KEY (
        source_id, slot_generation, commit_lsn,
        ingress_transaction_id, input_sequence
    ),
    CONSTRAINT applied_insert_source_row_unique UNIQUE (source_id, source_row_id)
);

CREATE TABLE shiba_internal.source_continuation (
    source_id bigint NOT NULL CHECK (source_id > 0),
    slot_generation bigint NOT NULL CHECK (slot_generation > 0),
    commit_lsn pg_lsn NOT NULL CHECK (commit_lsn > '0/0'::pg_lsn),
    ingress_transaction_id bigint NOT NULL CHECK (ingress_transaction_id > 0),
    CONSTRAINT source_continuation_coordinate_primary PRIMARY KEY (
        source_id, slot_generation, commit_lsn
    )
);

CREATE TABLE shiba_internal.operator_definition (
    operator_id bigint PRIMARY KEY CHECK (operator_id > 0),
    source_id bigint NOT NULL CHECK (source_id > 0),
    compiler_version integer NOT NULL CHECK (compiler_version = 1),
    operator_kind text NOT NULL CHECK (
        operator_kind IN ('count_rows', 'sum_int8')
    ),
    input_classid oid,
    input_objid oid,
    input_objsubid integer,
    CONSTRAINT operator_definition_input_shape CHECK (
        (
            operator_kind = 'count_rows'
            AND input_classid IS NULL
            AND input_objid IS NULL
            AND input_objsubid IS NULL
        ) OR (
            operator_kind = 'sum_int8'
            AND input_classid IS NOT NULL
            AND input_classid = 'pg_class'::regclass
            AND input_objid IS NOT NULL
            AND input_objsubid IS NOT NULL
            AND input_objsubid > 0
        )
    ),
    CONSTRAINT operator_definition_sink_identity UNIQUE (
        operator_id, operator_kind
    )
);

CREATE TABLE shiba_internal.operator_state (
    operator_id bigint PRIMARY KEY REFERENCES shiba_internal.operator_definition,
    value_bigint bigint NOT NULL
);

CREATE TABLE shiba.operator_result (
    operator_id bigint PRIMARY KEY,
    operator_kind text NOT NULL,
    value_bigint bigint NOT NULL,
    CONSTRAINT operator_result_definition FOREIGN KEY (
        operator_id, operator_kind
    ) REFERENCES shiba_internal.operator_definition (
        operator_id, operator_kind
    )
);

REVOKE ALL ON TABLE shiba_internal.applied_insert FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.source_continuation FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.operator_definition FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.operator_state FROM PUBLIC;
REVOKE ALL ON TABLE shiba.operator_result FROM PUBLIC;
GRANT SELECT ON TABLE shiba.operator_result TO PUBLIC;

COMMENT ON TABLE shiba_internal.applied_insert IS
    'Current source-row state and stable applied-cause authority';
COMMENT ON TABLE shiba_internal.source_continuation IS
    'Committed source transaction history and exact replay authority';
COMMENT ON TABLE shiba_internal.operator_definition IS
    'Compiled operator authority; its sole registration writer verifies source existence';
COMMENT ON TABLE shiba_internal.operator_state IS
    'Private bigint state for one registered deterministic operator';
COMMENT ON TABLE shiba.operator_result IS
    'Read-only SQL result projection for one registered operator';
