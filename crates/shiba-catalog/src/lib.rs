//! Database-local catalog and durable authority schema for clean-room V2.

::pgrx::pg_module_magic!(name, version);

::pgrx::extension_sql_file!(
    "../../../sql/v2/001_catalog_identity.sql",
    name = "catalog_identity"
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/002_source_apply.sql",
    name = "source_apply",
    requires = ["catalog_identity"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/003_nullable_insert.sql",
    name = "nullable_insert",
    requires = ["source_apply"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/004_empty_insert.sql",
    name = "empty_insert",
    requires = ["nullable_insert"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/005_composite_insert.sql",
    name = "composite_insert",
    requires = ["empty_insert"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/006_text_payload.sql",
    name = "text_payload",
    requires = ["composite_insert"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/007_source_invalidation.sql",
    name = "source_invalidation",
    requires = ["text_payload"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/008_source_ingress.sql",
    name = "source_ingress",
    requires = ["source_invalidation"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/009_source_ingress_registration.sql",
    name = "source_ingress_registration",
    requires = ["source_ingress"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/010_source_ingress_invalidation.sql",
    name = "source_ingress_invalidation",
    requires = ["source_ingress_registration"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/011_source_bootstrap.sql",
    name = "source_bootstrap",
    requires = ["source_ingress_invalidation"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/012_source_bootstrap_reservation.sql",
    name = "source_bootstrap_reservation",
    requires = ["source_bootstrap"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/013_source_bootstrap_replacement.sql",
    name = "source_bootstrap_replacement",
    requires = ["source_bootstrap_reservation"]
);

#[cfg(test)]
mod tests {
    const CATALOG_SQL: &str = include_str!("../../../sql/v2/001_catalog_identity.sql");
    const M9_AUTHORITY_SQL: &str = include_str!("../../../sql/v2/002_source_apply.sql");
    const M4_SQL: &str = include_str!("../../../sql/v2/003_nullable_insert.sql");
    const M4_EMPTY_SQL: &str = include_str!("../../../sql/v2/004_empty_insert.sql");
    const M4_COMPOSITE_SQL: &str = include_str!("../../../sql/v2/005_composite_insert.sql");
    const M5_TEXT_SQL: &str = include_str!("../../../sql/v2/006_text_payload.sql");
    const M7_SOURCE_SQL: &str = include_str!("../../../sql/v2/007_source_invalidation.sql");
    const M10_INGRESS_SQL: &str = include_str!("../../../sql/v2/008_source_ingress.sql");
    const M10_REGISTRATION_SQL: &str =
        include_str!("../../../sql/v2/009_source_ingress_registration.sql");
    const M10_INVALIDATION_SQL: &str =
        include_str!("../../../sql/v2/010_source_ingress_invalidation.sql");
    const M11_BOOTSTRAP_SQL: &str = include_str!("../../../sql/v2/011_source_bootstrap.sql");
    const M11_RESERVATION_SQL: &str =
        include_str!("../../../sql/v2/012_source_bootstrap_reservation.sql");
    const M11_REPLACEMENT_SQL: &str =
        include_str!("../../../sql/v2/013_source_bootstrap_replacement.sql");

    fn normalized_sql() -> String {
        CATALOG_SQL.to_ascii_lowercase()
    }

    #[test]
    fn installation_identity_owns_exactly_one_table() {
        let sql = normalized_sql();
        assert_eq!(sql.matches("create table ").count(), 1);
        assert!(sql.contains("create table shiba_internal.catalog_identity"));
    }

    #[test]
    fn m9_owns_only_its_five_required_tables() {
        let sql = M9_AUTHORITY_SQL.to_ascii_lowercase();
        assert_eq!(sql.matches("create table ").count(), 5);
        for table in [
            "shiba_internal.source_row_state",
            "shiba_internal.source_continuation",
            "shiba_internal.operator_definition",
            "shiba_internal.operator_state",
            "shiba.operator_result",
        ] {
            assert!(sql.contains(&format!("create table {table}")));
        }
        assert!(!sql.contains("count_state"));
        assert!(!sql.contains("count_result"));
    }

    #[test]
    fn m9_operator_authority_has_frozen_shapes() {
        let sql = M9_AUTHORITY_SQL.to_ascii_lowercase();
        for required in [
            "check (compiler_version = 1)",
            "operator_kind in ('count_rows', 'sum_int8')",
            "operator_kind = 'count_rows'",
            "input_classid is null",
            "input_objid is null",
            "input_objsubid is null",
            "operator_kind = 'sum_int8'",
            "input_classid is not null",
            "input_classid = 'pg_class'::regclass",
            "input_objid is not null",
            "input_objsubid is not null",
            "input_objsubid > 0",
            "unique (\n        operator_id, operator_kind",
            "value_bigint bigint not null",
            "foreign key (\n        operator_id, operator_kind",
        ] {
            assert!(
                sql.contains(required),
                "missing authority shape: {required}"
            );
        }
        assert!(!sql.contains("value_bigint >= 0"));
        assert!(!sql.contains("on delete"));
        assert_eq!(sql.matches("insert into ").count(), 0);
        assert_eq!(sql.matches("comment on table ").count(), 5);
        assert!(!sql.contains("create trigger"));
    }

    #[test]
    fn m9_operator_permissions_are_private_state_and_public_read_only_result() {
        let sql = M9_AUTHORITY_SQL.to_ascii_lowercase();
        for table in [
            "source_row_state",
            "source_continuation",
            "operator_definition",
            "operator_state",
        ] {
            assert!(sql.contains(&format!(
                "revoke all on table shiba_internal.{table} from public"
            )));
        }
        assert!(sql.contains("revoke all on table shiba.operator_result from public"));
        assert!(sql.contains("grant select on table shiba.operator_result to public"));
        assert!(!sql.contains("grant insert"));
        assert!(!sql.contains("grant update"));
    }

    #[test]
    fn identity_is_singleton_and_versions_are_frozen() {
        let sql = normalized_sql();
        assert!(sql.contains("primary key (singleton)"));
        assert!(sql.contains("check (singleton = 1)"));
        assert!(sql.contains("check (catalog_version = 1)"));
        assert!(sql.contains("check (protocol_version = 1)"));
        assert!(sql.contains("values (1, 1, 1)"));
    }

    #[test]
    fn public_surface_is_read_only_and_internal_state_is_private() {
        let sql = normalized_sql();
        assert!(sql.contains("revoke all on schema shiba_internal from public"));
        assert!(sql.contains("revoke all on table shiba_internal.catalog_identity from public"));
        assert!(sql.contains("security definer"));
        assert!(sql.contains("grant execute on function shiba.versions() to public"));
        assert!(!sql.contains("grant select on shiba_internal.catalog_identity"));
    }

    #[test]
    fn installation_has_no_dynamic_or_compatibility_mechanism() {
        let sql = format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}",
            normalized_sql(),
            M9_AUTHORITY_SQL.to_ascii_lowercase(),
            M4_SQL.to_ascii_lowercase(),
            M4_EMPTY_SQL.to_ascii_lowercase(),
            M4_COMPOSITE_SQL.to_ascii_lowercase(),
            M5_TEXT_SQL.to_ascii_lowercase(),
            M7_SOURCE_SQL.to_ascii_lowercase()
        );
        for forbidden in [
            "create trigger",
            "execute format(",
            "create table shiba_internal.source (",
            "create table shiba_internal.effect (",
            "count_state",
            "count_result",
            "compatibility",
            "fallback",
            "alias",
        ] {
            assert!(
                !sql.contains(forbidden),
                "forbidden SQL surface: {forbidden}"
            );
        }
    }

    #[test]
    fn text_payload_extends_only_current_row_state() {
        let sql = M5_TEXT_SQL.to_ascii_lowercase();
        assert_eq!(sql.matches("create table ").count(), 0);
        assert!(sql.contains("alter table shiba_internal.source_row_state"));
        assert!(sql.contains("add column payload_text text"));
        assert!(sql.contains("payload_int8 is null or payload_text is null"));
    }

    #[test]
    fn source_invalidation_uses_exact_object_addresses() {
        let sql = M7_SOURCE_SQL.to_ascii_lowercase();
        assert_eq!(sql.matches("create table ").count(), 2);
        for field in ["address_classid", "address_objid", "address_objsubid"] {
            assert!(sql.contains(field));
        }
        assert!(sql.contains("pg_event_trigger_ddl_commands()"));
        assert!(sql.contains("pg_event_trigger_dropped_objects()"));
        assert!(sql.contains("pg_catalog.pg_attribute"));
        assert!(sql.contains("attribute.attnum > 0"));
        assert!(sql.contains("pg_catalog.pg_index"));
        assert!(sql.contains("identity.indisreplident"));
        for kind in ["relation", "column", "identity_index"] {
            assert!(sql.contains(kind));
        }
        assert!(!sql.contains("object_identity"));
    }

    #[test]
    fn ingress_authority_has_one_config_and_one_exact_invalidation() {
        let sql = M10_INGRESS_SQL.to_ascii_lowercase();
        assert_eq!(sql.matches("create table ").count(), 2);
        for required in [
            "source_ingress_config",
            "source_ingress_invalidation",
            "publication_classid = 'pg_publication'::regclass",
            "slot_name name not null unique",
            "slot_generation bigint not null check (slot_generation > 0)",
            "publication_name name not null",
            "publication_attnums smallint[] not null",
            "source_ingress_bound_source foreign key",
            "source_ingress_invalidation_exact_config foreign key",
            "slot_generation = expected_generation + 1",
        ] {
            assert!(
                sql.contains(required),
                "missing ingress contract: {required}"
            );
        }
        for forbidden in [
            "confirmed_flush_lsn",
            "active_pid",
            "create_replication_slot",
        ] {
            assert!(
                !sql.contains(forbidden),
                "forbidden ingress state: {forbidden}"
            );
        }
    }

    #[test]
    fn ingress_registration_freezes_exact_publication_semantics() {
        let sql = M10_REGISTRATION_SQL.to_ascii_lowercase();
        for required in [
            "for update",
            "member.prqual is null",
            "member.prattrs::smallint[]",
            "attribute.attnum > 0 and not attribute.attisdropped",
            "pubinsert and pubupdate and pubdelete and not pubviaroot",
            "slot.plugin = 'pgoutput'",
            "not slot.temporary and not slot.active",
        ] {
            assert!(
                sql.contains(required),
                "missing registration contract: {required}"
            );
        }
    }

    #[test]
    fn ingress_event_writer_detects_snapshot_drift_without_name_identity() {
        let sql = M10_INVALIDATION_SQL.to_ascii_lowercase();
        for required in [
            "create or replace function shiba_internal.invalidate_source_object()",
            "source_ingress_invalidation",
            "publication.oid = config.publication_objid",
            "publication.pubname = config.publication_name",
            "config.publication_attnums = case",
            "member.prqual is null",
            "on conflict (source_id) do nothing",
        ] {
            assert!(
                sql.contains(required),
                "missing invalidation contract: {required}"
            );
        }
        let name_only_lookup = "where publication.pubname = config.publication_name";
        assert!(
            !sql.contains(name_only_lookup),
            "publication name became standalone identity: {name_only_lookup}"
        );
    }

    #[test]
    fn m11_replaces_wal_causes_with_one_key_owned_row_state() {
        let sql = M9_AUTHORITY_SQL.to_ascii_lowercase();
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
    fn m11_result_visibility_is_closed_and_never_partial() {
        let sql = M9_AUTHORITY_SQL.to_ascii_lowercase();
        for required in [
            "result_status text not null default 'active'",
            "result_status in ('building', 'active')",
            "result_status = 'building' and value_bigint is null",
            "result_status = 'active' and value_bigint is not null",
        ] {
            assert!(
                sql.contains(required),
                "missing result visibility: {required}"
            );
        }
    }

    #[test]
    fn m11_bootstrap_is_one_private_checkpoint_authority() {
        let sql = M11_BOOTSTRAP_SQL.to_ascii_lowercase();
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
    fn m11_reservation_is_pristine_atomic_and_slot_absent() {
        let sql = M11_RESERVATION_SQL.to_ascii_lowercase();
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
    fn m11_cleanup_constraints_admit_pre_boundary_failure_only_without_fence() {
        let sql = M11_BOOTSTRAP_SQL.to_ascii_lowercase();
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
    fn m11_replacement_is_exact_pre_active_and_reuses_reservation() {
        let sql = M11_REPLACEMENT_SQL.to_ascii_lowercase();
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
            "set value_bigint = 0",
            "normalization is transaction-local",
            "delete from shiba_internal.source_bootstrap",
            "delete from shiba_internal.source_ingress_config",
            "perform shiba_internal.reserve_source_bootstrap",
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
        ] {
            assert!(
                !sql.contains(forbidden),
                "forbidden recovery authority: {forbidden}"
            );
        }
    }
}
