const AUTHORITY_SQL: &str = include_str!("../../../../sql/v2/002_source_apply.sql");
const KEYED_SQL: &str = include_str!("../../../../sql/v2/018_operator_keyed_state.sql");
const BOOTSTRAP_SQL: &str = include_str!("../../../../sql/v2/011_source_bootstrap.sql");
const RESERVATION_SQL: &str =
    include_str!("../../../../sql/v2/012_source_bootstrap_reservation.sql");
const REPLACEMENT_SQL: &str =
    include_str!("../../../../sql/v2/013_source_bootstrap_replacement.sql");
const REBUILD_SQL: &str = include_str!("../../../../sql/v2/014_source_rebuild.sql");
const TARGET_SQL: &str = include_str!("../../../../sql/v2/015_source_rebuild_preflight.sql");
const CURRENT_SQL: &str = include_str!("../../../../sql/v2/016_source_rebuild_current.sql");
const PREPARE_SQL: &str = include_str!("../../../../sql/v2/017_source_rebuild_prepare.sql");

#[test]
fn bootstrap_replaces_wal_causes_with_one_key_owned_row_state() {
    let sql = AUTHORITY_SQL.to_ascii_lowercase();
    let row_state = sql
        .split("create table shiba_internal.source_continuation")
        .next()
        .expect("source row state precedes continuation");
    for required in [
        "create table shiba_internal.source_row_state",
        "row_state_id bigint generated always as identity",
        "primary key (row_state_id)",
        "unique (source_id, source_row_id)",
    ] {
        assert!(
            row_state.contains(required),
            "missing row-state contract: {required}"
        );
    }
    for forbidden in [
        "applied_insert",
        "commit_lsn pg_lsn not null",
        "ingress_transaction_id bigint not null",
        "input_sequence bigint not null",
    ] {
        assert!(
            !row_state.contains(forbidden),
            "obsolete row cause: {forbidden}"
        );
    }
}

#[test]
fn result_visibility_is_closed_and_never_partial() {
    let sql = format!(
        "{}\n{}",
        AUTHORITY_SQL.to_ascii_lowercase(),
        KEYED_SQL.to_ascii_lowercase()
    );
    for required in [
        "result_status text not null default 'active'",
        "result_status in ('building', 'active')",
        "result_status = 'building' and value_bigint is null",
        "output_shape = 'scalar' and value_bigint is not null",
        "output_shape = 'keyed' and value_bigint is null",
        "where result.result_status = 'active'",
    ] {
        assert!(
            sql.contains(required),
            "missing result visibility: {required}"
        );
    }
}

#[test]
fn bootstrap_is_one_private_checkpoint_authority() {
    let sql = BOOTSTRAP_SQL.to_ascii_lowercase();
    assert_eq!(sql.matches("create table ").count(), 1);
    for required in [
        "create table shiba_internal.source_bootstrap",
        "bootstrap_id bigint not null unique check (bootstrap_id > 0)",
        "'creating', 'scanning', 'scan_complete', 'catching_up'",
        "'active', 'cleanup_pending', 'failed'",
        "last_batch_ordinal bigint not null default 0",
        "pg_catalog.octet_length(last_batch_digest) = 32",
        "fence_token uuid not null unique default pg_catalog.gen_random_uuid()",
        "activation_end_lsn pg_lsn",
        "phase = 'active'\n         and activation_end_lsn is not null",
        "activation_end_lsn >= catchup_fence_lsn",
        "phase <> 'active' and activation_end_lsn is null",
        "source_bootstrap_exact_ingress foreign key",
        "revoke all on table shiba_internal.source_bootstrap from public",
    ] {
        assert!(
            sql.contains(required),
            "missing bootstrap authority: {required}"
        );
    }
    for forbidden in [
        "confirmed_flush_lsn",
        "effect_batch",
        "wal_payload",
        "create table shiba_internal.bootstrap_batch",
    ] {
        assert!(
            !sql.contains(forbidden),
            "forbidden bootstrap state: {forbidden}"
        );
    }
}

#[test]
fn reservation_is_pristine_atomic_and_slot_absent() {
    let sql = RESERVATION_SQL.to_ascii_lowercase();
    assert_eq!(sql.matches("create table ").count(), 0);
    assert_eq!(sql.matches("create function ").count(), 1);
    for required in [
        "create function shiba_internal.reserve_source_bootstrap",
        "from shiba_internal.source_row_state",
        "from shiba_internal.source_continuation",
        "member.prqual is null",
        "pubinsert and pubupdate and pubdelete and not pubviaroot",
        "from pg_catalog.pg_replication_slots",
        "bootstrap slot must not exist before reservation",
        "insert into shiba_internal.source_ingress_config",
        "insert into shiba_internal.source_bootstrap",
        "set result_status = 'building', value_bigint = null",
    ] {
        assert!(
            sql.contains(required),
            "missing reservation contract: {required}"
        );
    }
    assert!(!sql.contains("pg_create_logical_replication_slot"));
    assert!(!sql.contains("pg_drop_replication_slot"));
}

#[test]
fn cleanup_constraints_admit_pre_boundary_failure_only_without_fence() {
    let sql = BOOTSTRAP_SQL.to_ascii_lowercase();
    for required in [
        "phase in ('cleanup_pending', 'failed') and (\n            consistent_point is null",
        "phase in ('cleanup_pending', 'failed') and (\n            catchup_fence_lsn is null",
        "consistent_point is not null\n                and catchup_fence_lsn >= consistent_point",
    ] {
        assert!(
            sql.contains(required),
            "missing recovery constraint: {required}"
        );
    }
}

#[test]
fn replacement_is_exact_pre_active_and_reuses_reservation() {
    let sql = REPLACEMENT_SQL.to_ascii_lowercase();
    assert_eq!(sql.matches("create table ").count(), 0);
    assert_eq!(sql.matches("create function ").count(), 1);
    for required in [
        "create function shiba_internal.replace_pristine_source_bootstrap",
        "new_generation <= old_generation",
        "for update of binding, bootstrap, config",
        "'creating', 'scanning', 'cleanup_pending', 'failed'",
        "replacement requires absent old and new slots",
        "from shiba_internal.source_continuation",
        "result.result_status <> 'building'",
        "delete from shiba_internal.source_row_state",
        "delete from shiba_internal.source_bootstrap",
        "delete from shiba_internal.source_ingress_config",
        "perform shiba_internal.reserve_source_bootstrap",
        "bootstrap.retired_bootstrap_id is not null",
        "new_generation <> old_generation + 1 or new_slot = old_slot",
        "m12 replacement binding identity drifted",
        "retired_bootstrap_id = old_bootstrap_id",
        "retired_slot_name = old_slot",
        "retired_slot_generation = old_generation",
        "m12 replacement lost successor ownership",
        "revoke all on function shiba_internal.replace_pristine_source_bootstrap",
    ] {
        assert!(
            sql.contains(required),
            "missing replacement boundary: {required}"
        );
    }
    for forbidden in [
        "pg_create_logical_replication_slot",
        "pg_drop_replication_slot",
        "confirmed_flush_lsn",
        "create table shiba_internal.bootstrap_batch",
        "delete from shiba_internal.operator_result_row",
    ] {
        assert!(
            !sql.contains(forbidden),
            "forbidden recovery authority: {forbidden}"
        );
    }
}

#[test]
fn rebuild_reuses_one_bootstrap_authority_with_generation_cas() {
    let sql = REBUILD_SQL.to_ascii_lowercase();
    assert_eq!(sql.matches("create table ").count(), 0);
    for required in [
        "'rebuild_prepared'",
        "retired_bootstrap_id bigint",
        "retired_slot_name name",
        "retired_slot_generation bigint",
        "retired_bootstrap_id is null",
        "retired_slot_name is null",
        "retired_slot_generation is null",
        "phase <> 'rebuild_prepared'",
        "bootstrap_id <> retired_bootstrap_id",
        "slot_name <> retired_slot_name",
        "slot_generation = retired_slot_generation + 1",
        "deferrable initially immediate",
    ] {
        assert!(sql.contains(required), "missing rebuild schema: {required}");
    }
    for forbidden in ["create table", "slot_birth", "candidate", "fallback"] {
        assert!(
            !sql.contains(forbidden),
            "forbidden rebuild authority: {forbidden}"
        );
    }
}

#[test]
fn rebuild_prepare_is_exact_and_destructive_only_after_cas() {
    let target = TARGET_SQL.to_ascii_lowercase();
    let current = CURRENT_SQL.to_ascii_lowercase();
    let prepare = PREPARE_SQL.to_ascii_lowercase();
    for required in [
        "has_table_privilege",
        "relation.relreplident = 'd'",
        "identity.indisprimary and identity.indisunique",
        "target rebuild slot must be absent",
        "publication.pubinsert and publication.pubupdate",
        "not publication.pubtruncate",
        "return next",
    ] {
        assert!(
            target.contains(required),
            "missing target preflight: {required}"
        );
    }
    for required in [
        "bootstrap.phase = 'active'",
        "target_generation <> expected_old_generation + 1",
        "'-9223372036854775808'::bigint + requested_source_id",
        "not slot.active and not slot.two_phase",
        "not slot.failover and not slot.synced",
        "bootstrap.retired_bootstrap_id is not null",
        "(case when m12_identity then 4 else 3 end) <>",
        "binding_kind = 'identity_index'",
        "stale source binding identity",
        "old_identity.indisprimary and old_identity.indisunique",
        "stale operator plan identity",
        "order by definition.operator_id",
        "state.codec_version <> definition.state_codec_version",
        "return next",
    ] {
        assert!(current.contains(required), "missing old CAS: {required}");
    }
    for required in [
        "set constraints shiba_internal.source_ingress_bound_source",
        "shiba_internal.source_bootstrap_exact_ingress deferred",
        "delete from shiba_internal.source_continuation",
        "delete from shiba_internal.source_row_state",
        "delete from shiba_internal.source_binding",
        "(requested_source_id, 'identity_index', 'pg_class'::regclass",
        "set result_status = 'building', value_bigint = null",
        "phase = 'rebuild_prepared'",
        "retired_bootstrap_id = expected_old_bootstrap_id",
    ] {
        assert!(
            prepare.contains(required),
            "missing prepare mutation: {required}"
        );
    }
    let all = format!("{target}{current}{prepare}");
    for forbidden in [
        "pg_drop_replication_slot",
        "pg_create_logical_replication_slot",
        "operator_kind",
        "count_operator_id",
        "sum_operator_id",
        "expected_sum_input_subid",
        "set value_bigint = 0",
    ] {
        assert!(
            !all.contains(forbidden),
            "specialized rebuild SQL leaked: {forbidden}"
        );
    }
}
