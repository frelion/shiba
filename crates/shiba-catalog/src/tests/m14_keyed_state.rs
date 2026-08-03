const STATE: &str = include_str!("../../../../sql/v2/018_operator_keyed_state.sql");

#[test]
fn scalar_and_keyed_state_share_one_graph_node_authority() {
    let sql = STATE.to_ascii_lowercase();
    for required in [
        "create table shiba_internal.graph_node_state",
        "graph_id, node_id, namespace, partition_key_payload, item_key_payload",
        "graph_node_state_definition foreign key",
        "create table shiba_internal.graph_result_row",
        "primary key (graph_id, result_id, key_payload)",
        "create view shiba.graph_result_rows",
        "where result.result_status = 'active'",
    ] {
        assert!(
            sql.contains(required),
            "missing generic state/sink: {required}"
        );
    }
    for forbidden in ["operator_node_state", "operator_result_row", "operator_id"] {
        assert!(
            !sql.contains(forbidden),
            "obsolete state identity: {forbidden}"
        );
    }
}
