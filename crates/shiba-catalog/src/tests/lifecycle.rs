const BOOTSTRAP: &str = include_str!("../../../../sql/v2/011_source_bootstrap.sql");
const RESERVE: &str = include_str!("../../../../sql/v2/012_source_bootstrap_reservation.sql");
const REPLACE: &str = include_str!("../../../../sql/v2/013_source_bootstrap_replacement.sql");
const SLOT: &str = include_str!("../../../../sql/v2/014_source_rebuild.sql");
const TARGET: &str = include_str!("../../../../sql/v2/015_source_rebuild_preflight.sql");
const CURRENT: &str = include_str!("../../../../sql/v2/016_source_rebuild_current.sql");
const PREPARE: &str = include_str!("../../../../sql/v2/017_source_rebuild_prepare.sql");

#[test]
fn bootstrap_is_one_graph_lifecycle_with_member_checkpoints() {
    let sql = BOOTSTRAP.to_ascii_lowercase();
    for required in [
        "create table shiba_internal.graph_bootstrap",
        "create table shiba_internal.graph_bootstrap_checkpoint",
        "graph_bootstrap_checkpoint_member foreign key",
        "graph_bootstrap_exact_ingress foreign key",
        "graph_bootstrap_exact_definition foreign key",
        "'rebuild_prepared'",
        "activation_end_lsn >= catchup_fence_lsn",
        "slot_generation = retired_slot_generation + 1",
    ] {
        assert!(sql.contains(required), "missing lifecycle rule: {required}");
    }
    for forbidden in ["wal_payload", "effect_batch", "confirmed_flush_lsn"] {
        assert!(!sql.contains(forbidden));
    }
}

#[test]
fn reservation_and_replacement_are_graph_wide_and_forward_only() {
    let sql = format!("{RESERVE}\n{REPLACE}").to_ascii_lowercase();
    for required in [
        "reserve_graph_bootstrap",
        "graph_source_member",
        "graph_bootstrap_checkpoint",
        "delete from shiba_internal.graph_result_row",
        "update shiba.graph_result set result_status = 'building'",
        "publication member set does not match graph",
        "replace_pristine_graph_bootstrap",
        "new_generation <> old_generation + 1",
        "retired_slot_generation = old_generation",
    ] {
        assert!(
            sql.contains(required),
            "missing recovery boundary: {required}"
        );
    }
}

#[test]
fn rebuild_is_exact_graph_digest_and_generation_cas() {
    let sql = format!("{SLOT}\n{TARGET}\n{CURRENT}\n{PREPARE}").to_ascii_lowercase();
    for required in [
        "graph_rebuild_slot_is_exact",
        "validate_graph_rebuild_target",
        "validate_graph_rebuild_current",
        "stale graph definition digest",
        "target_source_ids <> array(",
        "prepare_graph_rebuild",
        "set constraints all deferred",
        "delete from shiba_internal.graph_continuation",
        "delete from shiba_internal.graph_node_state",
        "update shiba_internal.graph_definition",
        "result_status, schema_payload, schema_digest",
        "target_schema_payloads bytea[]",
        "target_schema_digests bytea[]",
        "phase = 'rebuild_prepared'",
    ] {
        assert!(sql.contains(required), "missing rebuild rule: {required}");
    }
    for forbidden in ["candidate", "fallback", "source_continuation"] {
        assert!(
            !sql.contains(forbidden),
            "forbidden rebuild path: {forbidden}"
        );
    }
}
