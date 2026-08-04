const STATE: &str = include_str!("../../../../sql/v2/018_operator_keyed_state.sql");

#[test]
fn scalar_and_keyed_results_share_one_wide_row_authority() {
    let sql = format!("{}\n{STATE}", super::RUNTIME).to_ascii_lowercase();
    for required in [
        "create table shiba_internal.graph_node_state",
        "graph_id, node_id, namespace, partition_key_payload, item_key_payload",
        "graph_node_state_definition foreign key",
        "create table shiba.graph_result",
        "result_status text not null default 'active'",
        "schema_payload bytea not null",
        "schema_digest bytea not null",
        "octet_length(schema_payload) between 1 and 16384",
        "octet_length(schema_digest) = 32",
        "graph_result_schema_identity unique",
        "create table shiba_internal.graph_result_row",
        "row_identity bytea not null",
        "row_payload bytea not null",
        "octet_length(row_identity) between 1 and 4096",
        "octet_length(row_payload) between 1 and 4096",
        "graph_result_row_primary primary key",
        "graph_id, result_id, row_identity",
        "graph_result_row_exact_schema foreign key",
        "create view shiba.graph_result_rows",
        "result.schema_payload, result_row.schema_digest",
        "result_row.row_identity, result_row.row_payload",
        "where result.result_status = 'active'",
        "revoke all on table shiba_internal.graph_result_row from public",
        "grant select on table shiba.graph_result_rows to public",
    ] {
        assert!(
            sql.contains(required),
            "missing generic state/sink: {required}"
        );
    }
    for forbidden in [
        "operator_node_state",
        "operator_result_row",
        "operator_id",
        "output_shape",
        "output_key_type",
        "output_value_type",
        "value_bigint",
        "value_payload",
        "result_key_bigint",
        "result_value_bigint",
        "aggregate_function",
        "aggregate_kind",
        "function_tag",
    ] {
        assert!(
            !sql.contains(forbidden),
            "obsolete state identity: {forbidden}"
        );
    }
    assert_eq!(sql.matches("create table shiba.graph_result (").count(), 1);
    assert_eq!(
        sql.matches("create table shiba_internal.graph_result_row (")
            .count(),
        1
    );
}
