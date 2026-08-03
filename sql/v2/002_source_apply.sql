-- M14.6 final clean-room graph runtime authority. Source rows remain keyed by
-- SourceId; definitions, progress, state and results belong only to GraphId.

CREATE TABLE shiba_internal.source_row_state (
    row_state_id bigint GENERATED ALWAYS AS IDENTITY,
    source_id bigint NOT NULL CHECK (source_id > 0),
    source_row_id bigint NOT NULL,
    CONSTRAINT source_row_state_primary PRIMARY KEY (row_state_id),
    CONSTRAINT source_row_state_source_row_unique UNIQUE (source_id, source_row_id)
);

CREATE TABLE shiba_internal.graph_definition (
    graph_id bigint PRIMARY KEY CHECK (graph_id > 0),
    source_count smallint NOT NULL CHECK (source_count IN (1, 2)),
    compiler_version integer NOT NULL CHECK (compiler_version = 1),
    spec_payload bytea NOT NULL CHECK (pg_catalog.octet_length(spec_payload) > 0),
    graph_format_version integer NOT NULL CHECK (graph_format_version = 1),
    graph_payload bytea NOT NULL CHECK (pg_catalog.octet_length(graph_payload) > 0),
    graph_digest bytea NOT NULL CHECK (pg_catalog.octet_length(graph_digest) = 32),
    state_codec_version integer NOT NULL CHECK (state_codec_version = 1),
    CONSTRAINT graph_definition_digest_identity UNIQUE (graph_id, graph_digest),
    CONSTRAINT graph_definition_state_identity UNIQUE (graph_id, state_codec_version)
);

CREATE TABLE shiba_internal.graph_continuation (
    graph_id bigint NOT NULL CHECK (graph_id > 0),
    slot_generation bigint NOT NULL CHECK (slot_generation > 0),
    commit_lsn pg_lsn NOT NULL CHECK (commit_lsn > '0/0'::pg_lsn),
    ingress_transaction_id bigint NOT NULL CHECK (ingress_transaction_id > 0),
    graph_digest bytea NOT NULL CHECK (pg_catalog.octet_length(graph_digest) = 32),
    CONSTRAINT graph_continuation_coordinate_primary PRIMARY KEY (
        graph_id, slot_generation, commit_lsn
    ),
    CONSTRAINT graph_continuation_exact_definition FOREIGN KEY (
        graph_id, graph_digest
    ) REFERENCES shiba_internal.graph_definition (graph_id, graph_digest)
);

CREATE TABLE shiba.graph_result (
    graph_id bigint NOT NULL CHECK (graph_id > 0),
    result_id bigint NOT NULL CHECK (result_id > 0),
    output_shape text NOT NULL CHECK (output_shape IN ('scalar', 'keyed')),
    output_key_type text,
    output_key_nullable boolean NOT NULL,
    output_value_type text NOT NULL CHECK (output_value_type = 'int8'),
    output_value_nullable boolean NOT NULL,
    result_status text NOT NULL DEFAULT 'active'
        CHECK (result_status IN ('building', 'active')),
    value_payload bytea,
    value_bigint bigint,
    CONSTRAINT graph_result_primary PRIMARY KEY (graph_id, result_id),
    CONSTRAINT graph_result_sink_identity UNIQUE (graph_id, result_id, output_shape),
    CONSTRAINT graph_result_output_contract CHECK (
        (output_shape = 'scalar' AND output_key_type IS NULL
         AND NOT output_key_nullable AND NOT output_value_nullable)
        OR (output_shape = 'keyed' AND output_key_type = 'int8')
    ),
    CONSTRAINT graph_result_visibility CHECK (
        (result_status = 'building' AND value_payload IS NULL AND value_bigint IS NULL)
        OR (result_status = 'active' AND (
            (output_shape = 'scalar' AND value_payload IS NOT NULL
             AND value_bigint IS NOT NULL)
            OR (output_shape = 'keyed' AND value_payload IS NULL
                AND value_bigint IS NULL)
        ))
    ),
    CONSTRAINT graph_result_definition FOREIGN KEY (graph_id)
        REFERENCES shiba_internal.graph_definition (graph_id)
);

REVOKE ALL ON TABLE shiba_internal.source_row_state FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.graph_definition FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.graph_continuation FROM PUBLIC;
REVOKE ALL ON TABLE shiba.graph_result FROM PUBLIC;
GRANT SELECT ON TABLE shiba.graph_result TO PUBLIC;

COMMENT ON TABLE shiba_internal.source_row_state IS
    'Sole key-owned current source-row state for a uniquely owned graph member';
COMMENT ON TABLE shiba_internal.graph_definition IS
    'Sole strict spec and canonical compiled graph authority';
COMMENT ON TABLE shiba_internal.graph_continuation IS
    'Exact committed graph-generation replay authority';
COMMENT ON TABLE shiba.graph_result IS
    'Read-only generic graph result header; building rows expose no partial value';
