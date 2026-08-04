-- One Runtime-owned graph node state table stores scalar unit keys and keyed
-- partitions alike. Result rows are keyed by terminal node/result identity.

CREATE TABLE shiba_internal.graph_node_state (
    graph_id bigint NOT NULL,
    node_id bigint NOT NULL CHECK (node_id > 0),
    namespace integer NOT NULL CHECK (namespace BETWEEN 0 AND 65535),
    partition_key_payload bytea NOT NULL CHECK (
        pg_catalog.octet_length(partition_key_payload) > 0
    ),
    item_key_payload bytea NOT NULL CHECK (
        pg_catalog.octet_length(item_key_payload) > 0
    ),
    item_order_key bytea CHECK (
        (item_key_payload = pg_catalog.convert_to('null', 'UTF8')
            AND item_order_key IS NULL)
        OR (item_key_payload <> pg_catalog.convert_to('null', 'UTF8')
            AND pg_catalog.octet_length(item_order_key) = 8)
    ),
    codec_version integer NOT NULL CHECK (codec_version > 0),
    state_payload bytea NOT NULL,
    CONSTRAINT graph_node_state_primary PRIMARY KEY (
        graph_id, node_id, namespace, partition_key_payload, item_key_payload
    ),
    CONSTRAINT graph_node_state_definition FOREIGN KEY (graph_id, codec_version)
        REFERENCES shiba_internal.graph_definition (graph_id, state_codec_version)
);

CREATE INDEX graph_node_state_ordered_item
    ON shiba_internal.graph_node_state (
        graph_id, node_id, namespace, partition_key_payload, item_order_key
    )
    WHERE item_order_key IS NOT NULL;

CREATE TABLE shiba_internal.graph_result_row (
    graph_id bigint NOT NULL,
    result_id bigint NOT NULL,
    schema_digest bytea NOT NULL CHECK (
        pg_catalog.octet_length(schema_digest) = 32
    ),
    row_identity bytea NOT NULL CHECK (
        pg_catalog.octet_length(row_identity) BETWEEN 1 AND 4096
    ),
    row_payload bytea NOT NULL CHECK (
        pg_catalog.octet_length(row_payload) BETWEEN 1 AND 4096
    ),
    CONSTRAINT graph_result_row_primary PRIMARY KEY (
        graph_id, result_id, row_identity
    ),
    CONSTRAINT graph_result_row_exact_schema FOREIGN KEY (
        graph_id, result_id, schema_digest
    ) REFERENCES shiba.graph_result (graph_id, result_id, schema_digest)
);

CREATE VIEW shiba.graph_result_rows AS
SELECT result_row.graph_id, result_row.result_id,
       result.schema_payload, result_row.schema_digest,
       result_row.row_identity, result_row.row_payload
FROM shiba_internal.graph_result_row AS result_row
JOIN shiba.graph_result AS result
  ON result.graph_id = result_row.graph_id
 AND result.result_id = result_row.result_id
 AND result.schema_digest = result_row.schema_digest
WHERE result.result_status = 'active';

REVOKE ALL ON TABLE shiba_internal.graph_node_state FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.graph_result_row FROM PUBLIC;
REVOKE ALL ON TABLE shiba.graph_result_rows FROM PUBLIC;
GRANT SELECT ON TABLE shiba.graph_result_rows TO PUBLIC;

COMMENT ON TABLE shiba_internal.graph_node_state IS
    'Private canonical scalar/keyed node state owned only by Runtime';
COMMENT ON TABLE shiba_internal.graph_result_row IS
    'Private canonical scalar or keyed rows tied to one exact result schema';
COMMENT ON VIEW shiba.graph_result_rows IS
    'Read-only canonical schema and complete typed rows for active graph results';
