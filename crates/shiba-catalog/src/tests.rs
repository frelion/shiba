const IDENTITY: &str = include_str!("../../../sql/v2/001_catalog_identity.sql");
const RUNTIME: &str = include_str!("../../../sql/v2/002_source_apply.sql");
const BINDING: &str = include_str!("../../../sql/v2/007_source_invalidation.sql");
const INGRESS: &str = include_str!("../../../sql/v2/008_source_ingress.sql");
const CONFIGURE: &str = include_str!("../../../sql/v2/009_source_ingress_registration.sql");
const INVALIDATION: &str = include_str!("../../../sql/v2/010_source_ingress_invalidation.sql");

fn graph_sql() -> String {
    [RUNTIME, BINDING, INGRESS, CONFIGURE, INVALIDATION]
        .join("\n")
        .to_ascii_lowercase()
}

#[test]
fn catalog_identity_remains_single_database_local_authority() {
    let sql = IDENTITY.to_ascii_lowercase();
    assert!(sql.contains("create table shiba_internal.catalog_identity"));
    assert!(sql.contains("singleton = 1"));
    assert!(sql.contains("revoke all on table shiba_internal.catalog_identity from public"));
}

#[test]
fn graph_replaces_all_source_scoped_execution_authorities() {
    let sql = graph_sql();
    for required in [
        "create table shiba_internal.graph_definition",
        "create table shiba_internal.graph_source_member",
        "create table shiba_internal.graph_continuation",
        "create table shiba_internal.graph_ingress_config",
        "create table shiba_internal.graph_ingress_source",
        "create table shiba_internal.graph_ingress_invalidation",
        "create table shiba.graph_result",
    ] {
        assert!(
            sql.contains(required),
            "missing graph authority: {required}"
        );
    }
    for forbidden in [
        "create table shiba_internal.operator_definition",
        "create table shiba_internal.operator_state",
        "create table shiba_internal.source_continuation",
        "create table shiba_internal.source_ingress_config",
        "create table shiba_internal.source_ingress_invalidation",
        "create table shiba.operator_result",
        "configure_source_ingress",
        "rotate_source_ingress_slot",
    ] {
        assert!(
            !sql.contains(forbidden),
            "obsolete authority survived: {forbidden}"
        );
    }
}

#[test]
fn graph_membership_is_ordered_complete_and_exclusive() {
    let sql = BINDING.to_ascii_lowercase();
    for required in [
        "source_count smallint not null check (source_count in (1, 2))",
        "input_ordinal smallint not null check (input_ordinal in (0, 1))",
        "graph_source_member_ordinal unique (graph_id, input_ordinal)",
        "graph_source_member_one_graph unique (source_id)",
        "deferrable initially deferred",
        "graph source membership is incomplete",
    ] {
        assert!(
            graph_sql().contains(required),
            "missing membership rule: {required}"
        );
    }
    assert!(sql.contains("graph_source_member_exact_relation foreign key"));
}

#[test]
fn source_registration_persists_the_exact_effective_identity_index() {
    let sql = BINDING.to_ascii_lowercase();
    for required in [
        "relation.relreplident = 'd' and identity.indisprimary",
        "relation.relreplident = 'i' and identity.indisreplident",
        "cardinality(effective_identity_indexes) is distinct from 1",
        "source relation requires exactly one effective identity index",
        "requested_source_id, 'identity_index'",
    ] {
        assert!(
            sql.contains(required),
            "missing durable identity-index registration rule: {required}"
        );
    }
}

#[test]
fn ingress_owns_one_exact_publication_set_and_transport_generation() {
    let sql = format!("{INGRESS}\n{CONFIGURE}").to_ascii_lowercase();
    for required in [
        "graph_ingress_slot_generation_unique unique",
        "graph_ingress_source_member foreign key",
        "slot_name name not null unique",
        "publication member set does not match graph",
        "slot.plugin = 'pgoutput'",
        "not slot.two_phase and not slot.failover and not slot.synced",
        "create function shiba_internal.configure_graph_ingress",
    ] {
        assert!(sql.contains(required), "missing ingress rule: {required}");
    }
    assert!(!sql.contains("confirmed_flush_lsn"));
}

#[test]
fn ddl_invalidation_promotes_member_and_publication_drift() {
    let sql = INVALIDATION.to_ascii_lowercase();
    for required in [
        "create function shiba_internal.invalidate_graph_object()",
        "insert into shiba_internal.source_invalidation",
        "insert into shiba_internal.graph_ingress_invalidation",
        "graph_source_member",
        "pg_publication_rel",
        "on conflict (graph_id) do nothing",
    ] {
        assert!(
            sql.contains(required),
            "missing graph invalidation: {required}"
        );
    }
}

mod lifecycle;
mod m14_keyed_state;
