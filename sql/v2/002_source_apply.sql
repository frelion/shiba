-- M9.1 durable Source Apply, operator, result, and replay authorities. The
-- registration/runtime writer owns each transaction; installation seeds none.

CREATE TABLE shiba_internal.source_row_state (
    row_state_id bigint GENERATED ALWAYS AS IDENTITY,
    source_id bigint NOT NULL CHECK (source_id > 0),
    source_row_id bigint NOT NULL,
    CONSTRAINT source_row_state_primary PRIMARY KEY (row_state_id),
    CONSTRAINT source_row_state_source_row_unique UNIQUE (source_id, source_row_id)
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
    spec_payload bytea NOT NULL CHECK (pg_catalog.octet_length(spec_payload) > 0),
    plan_format_version integer NOT NULL CHECK (plan_format_version = 1),
    plan_payload bytea NOT NULL CHECK (pg_catalog.octet_length(plan_payload) > 0),
    plan_digest bytea NOT NULL CHECK (pg_catalog.octet_length(plan_digest) = 32),
    state_codec_version integer NOT NULL CHECK (state_codec_version = 1),
    output_shape text NOT NULL CHECK (output_shape IN ('scalar', 'keyed')),
    output_value_type text NOT NULL CHECK (output_value_type = 'int8'),
    output_key_type text,
    output_value_nullable boolean NOT NULL,
    CONSTRAINT operator_definition_output_contract CHECK (
        (output_shape = 'scalar'
         AND output_key_type IS NULL
         AND NOT output_value_nullable)
        OR (output_shape = 'keyed'
            AND output_key_type = 'int8')
    ),
    CONSTRAINT operator_definition_sink_identity UNIQUE (operator_id, output_shape),
    CONSTRAINT operator_definition_state_identity UNIQUE (
        operator_id, state_codec_version
    )
);

CREATE TABLE shiba_internal.operator_state (
    operator_id bigint PRIMARY KEY,
    codec_version integer NOT NULL CHECK (codec_version > 0),
    state_payload bytea NOT NULL,
    CONSTRAINT operator_state_definition FOREIGN KEY (
        operator_id, codec_version
    ) REFERENCES shiba_internal.operator_definition (
        operator_id, state_codec_version
    )
);

CREATE TABLE shiba.operator_result (
    operator_id bigint PRIMARY KEY,
    output_shape text NOT NULL CHECK (output_shape IN ('scalar', 'keyed')),
    result_status text NOT NULL DEFAULT 'active'
        CHECK (result_status IN ('building', 'active')),
    value_bigint bigint,
    CONSTRAINT operator_result_visibility CHECK (
        (result_status = 'building' AND value_bigint IS NULL)
        OR (result_status = 'active' AND (
            (output_shape = 'scalar' AND value_bigint IS NOT NULL)
            OR (output_shape = 'keyed' AND value_bigint IS NULL)
        ))
    ),
    CONSTRAINT operator_result_definition FOREIGN KEY (
        operator_id, output_shape
    ) REFERENCES shiba_internal.operator_definition (
        operator_id, output_shape
    ),
    CONSTRAINT operator_result_sink_identity UNIQUE (operator_id, output_shape)
);

CREATE TABLE shiba_internal.operator_result_row (
    operator_id bigint NOT NULL,
    output_shape text NOT NULL DEFAULT 'keyed' CHECK (output_shape = 'keyed'),
    result_key_bigint bigint NOT NULL,
    result_value_bigint bigint,
    CONSTRAINT operator_result_row_primary PRIMARY KEY (
        operator_id, result_key_bigint
    ),
    CONSTRAINT operator_result_row_keyed_definition FOREIGN KEY (
        operator_id, output_shape
    ) REFERENCES shiba.operator_result (operator_id, output_shape)
);

CREATE VIEW shiba.operator_result_rows AS
SELECT result_row.operator_id,
       result_row.result_key_bigint,
       result_row.result_value_bigint
FROM shiba_internal.operator_result_row AS result_row
JOIN shiba.operator_result AS result USING (operator_id)
WHERE result.result_status = 'active'
  AND result.output_shape = 'keyed';

REVOKE ALL ON TABLE shiba_internal.source_row_state FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.source_continuation FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.operator_definition FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.operator_state FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.operator_result_row FROM PUBLIC;
REVOKE ALL ON TABLE shiba.operator_result FROM PUBLIC;
REVOKE ALL ON TABLE shiba.operator_result_rows FROM PUBLIC;
GRANT SELECT ON TABLE shiba.operator_result TO PUBLIC;
GRANT SELECT ON TABLE shiba.operator_result_rows TO PUBLIC;

COMMENT ON TABLE shiba_internal.source_row_state IS
    'Sole key-owned current source-row state; WAL and bootstrap causes are not stored here';
COMMENT ON TABLE shiba_internal.source_continuation IS
    'Committed source transaction history and exact replay authority';
COMMENT ON TABLE shiba_internal.operator_definition IS
    'Strict spec and canonical compiled-plan authority owned by registration';
COMMENT ON TABLE shiba_internal.operator_state IS
    'Private opaque versioned state owned only by Runtime';
COMMENT ON TABLE shiba.operator_result IS
    'Read-only generic result header; building rows expose no partial value';
COMMENT ON TABLE shiba_internal.operator_result_row IS
    'Private keyed result sink rows owned only by Runtime';
COMMENT ON VIEW shiba.operator_result_rows IS
    'Read-only keyed results for active generic result headers';
