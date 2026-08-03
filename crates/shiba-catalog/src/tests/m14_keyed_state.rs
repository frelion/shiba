const AUTHORITY_SQL: &str = include_str!("../../../../sql/v2/002_source_apply.sql");
const KEYED_SQL: &str = include_str!("../../../../sql/v2/018_operator_keyed_state.sql");
const RESERVATION_SQL: &str =
    include_str!("../../../../sql/v2/012_source_bootstrap_reservation.sql");
const REPLACEMENT_SQL: &str =
    include_str!("../../../../sql/v2/013_source_bootstrap_replacement.sql");
const PREPARE_SQL: &str = include_str!("../../../../sql/v2/017_source_rebuild_prepare.sql");

#[test]
fn keyed_node_state_has_one_private_canonical_authority() {
    let sql = KEYED_SQL.to_ascii_lowercase();
    for required in [
        "create table shiba_internal.operator_node_state",
        "node_id bigint not null check (node_id > 0)",
        "namespace integer not null check (namespace between 0 and 65535)",
        "partition_key_payload bytea not null",
        "item_key_payload bytea not null",
        "pg_catalog.octet_length(partition_key_payload) > 0",
        "pg_catalog.octet_length(item_key_payload) > 0",
        "state_payload bytea not null",
        "primary key (\n        operator_id, node_id, namespace,\n        partition_key_payload, item_key_payload",
        "foreign key (\n        operator_id, codec_version",
        "revoke all on table shiba_internal.operator_node_state from public",
    ] {
        assert!(
            sql.contains(required),
            "missing keyed state contract: {required}"
        );
    }
    for forbidden in ["sentinel", "fallback", "alias", "create trigger"] {
        assert!(
            !sql.contains(forbidden),
            "forbidden keyed state surface: {forbidden}"
        );
    }
}

#[test]
fn keyed_result_identity_is_canonical_and_typed_null_is_explicit() {
    let sql = KEYED_SQL.to_ascii_lowercase();
    let authority = AUTHORITY_SQL.to_ascii_lowercase();
    for required in [
        "create table shiba_internal.operator_result_row",
        "key_payload bytea not null",
        "pg_catalog.octet_length(key_payload) > 0",
        "result_key_is_null boolean not null",
        "result_key_bigint bigint",
        "result_value_is_null boolean not null",
        "result_value_bigint bigint",
        "primary key (operator_id, key_payload)",
        "result_key_is_null and result_key_bigint is null",
        "not result_key_is_null and result_key_bigint is not null",
        "result_value_is_null and result_value_bigint is null",
        "not result_value_is_null and result_value_bigint is not null",
        "result_row.result_key_is_null",
        "result_row.result_value_is_null",
    ] {
        assert!(
            sql.contains(required),
            "missing typed keyed result: {required}"
        );
    }
    for required in [
        "output_key_nullable boolean not null",
        "output_key_type is null\n         and not output_key_nullable",
        "output_shape = 'keyed'\n            and output_key_type = 'int8'",
    ] {
        assert!(
            authority.contains(required),
            "missing key-null metadata: {required}"
        );
    }
    assert!(!authority.contains("result_key_bigint bigint not null"));
}

#[test]
fn bootstrap_and_rebuild_reset_generic_keyed_state() {
    let reservation = RESERVATION_SQL.to_ascii_lowercase();
    let replacement = REPLACEMENT_SQL.to_ascii_lowercase();
    let prepare = PREPARE_SQL.to_ascii_lowercase();
    assert!(reservation.contains("from shiba_internal.operator_node_state"));
    assert!(replacement.contains("from shiba_internal.operator_node_state"));
    assert!(prepare.contains("delete from shiba_internal.operator_node_state"));
    assert!(prepare.contains("definition.source_id = requested_source_id"));
}
