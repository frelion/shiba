-- Generic keyed node state and keyed result storage extend the sole operator
-- authority. Canonical payloads, never decoded SQL values, own identity.

CREATE TABLE shiba_internal.operator_node_state (
    operator_id bigint NOT NULL,
    node_id bigint NOT NULL CHECK (node_id > 0),
    namespace integer NOT NULL CHECK (namespace BETWEEN 0 AND 65535),
    partition_key_payload bytea NOT NULL CHECK (
        pg_catalog.octet_length(partition_key_payload) > 0
    ),
    item_key_payload bytea NOT NULL CHECK (
        pg_catalog.octet_length(item_key_payload) > 0
    ),
    codec_version integer NOT NULL CHECK (codec_version > 0),
    state_payload bytea NOT NULL,
    CONSTRAINT operator_node_state_primary PRIMARY KEY (
        operator_id, node_id, namespace,
        partition_key_payload, item_key_payload
    ),
    CONSTRAINT operator_node_state_definition FOREIGN KEY (
        operator_id, codec_version
    ) REFERENCES shiba_internal.operator_definition (
        operator_id, state_codec_version
    )
);

CREATE TABLE shiba_internal.operator_result_row (
    operator_id bigint NOT NULL,
    output_shape text NOT NULL DEFAULT 'keyed' CHECK (output_shape = 'keyed'),
    key_payload bytea NOT NULL CHECK (pg_catalog.octet_length(key_payload) > 0),
    result_key_is_null boolean NOT NULL,
    result_key_bigint bigint,
    result_value_is_null boolean NOT NULL,
    result_value_bigint bigint,
    CONSTRAINT operator_result_row_key_contract CHECK (
        (result_key_is_null AND result_key_bigint IS NULL)
        OR (NOT result_key_is_null AND result_key_bigint IS NOT NULL)
    ),
    CONSTRAINT operator_result_row_value_contract CHECK (
        (result_value_is_null AND result_value_bigint IS NULL)
        OR (NOT result_value_is_null AND result_value_bigint IS NOT NULL)
    ),
    CONSTRAINT operator_result_row_primary PRIMARY KEY (operator_id, key_payload),
    CONSTRAINT operator_result_row_keyed_definition FOREIGN KEY (
        operator_id, output_shape
    ) REFERENCES shiba.operator_result (operator_id, output_shape)
);

CREATE VIEW shiba.operator_result_rows AS
SELECT result_row.operator_id,
       result_row.result_key_bigint,
       result_row.result_value_bigint,
       result_row.result_key_is_null,
       result_row.result_value_is_null
FROM shiba_internal.operator_result_row AS result_row
JOIN shiba.operator_result AS result USING (operator_id)
WHERE result.result_status = 'active'
  AND result.output_shape = 'keyed';

REVOKE ALL ON TABLE shiba_internal.operator_node_state FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.operator_result_row FROM PUBLIC;
REVOKE ALL ON TABLE shiba.operator_result_rows FROM PUBLIC;
GRANT SELECT ON TABLE shiba.operator_result_rows TO PUBLIC;

COMMENT ON TABLE shiba_internal.operator_node_state IS
    'Private canonical keyed state owned only by Runtime for one compiled plan';
COMMENT ON TABLE shiba_internal.operator_result_row IS
    'Private canonical keyed result rows owned only by Runtime';
COMMENT ON VIEW shiba.operator_result_rows IS
    'Read-only typed keyed results for active generic result headers';
