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
fn operator_authority_owns_generic_state_and_two_result_shapes() {
    let sql = M9_AUTHORITY_SQL.to_ascii_lowercase();
    assert_eq!(sql.matches("create table ").count(), 6);
    for table in [
        "shiba_internal.source_row_state",
        "shiba_internal.source_continuation",
        "shiba_internal.operator_definition",
        "shiba_internal.operator_state",
        "shiba.operator_result",
        "shiba_internal.operator_result_row",
    ] {
        assert!(sql.contains(&format!("create table {table}")));
    }
    assert!(!sql.contains("count_state"));
    assert!(!sql.contains("count_result"));
}

#[test]
fn operator_authority_has_strict_generic_codecs_and_shapes() {
    let sql = M9_AUTHORITY_SQL.to_ascii_lowercase();
    for required in [
        "check (compiler_version = 1)",
        "spec_payload bytea not null",
        "plan_format_version integer not null",
        "plan_payload bytea not null",
        "pg_catalog.octet_length(plan_digest) = 32",
        "state_codec_version integer not null",
        "output_shape in ('scalar', 'keyed')",
        "output_value_type = 'int8'",
        "unique (operator_id, output_shape)",
        "codec_version integer not null",
        "state_payload bytea not null",
        "foreign key (\n        operator_id, output_shape",
        "create view shiba.operator_result_rows",
    ] {
        assert!(
            sql.contains(required),
            "missing authority shape: {required}"
        );
    }
    assert!(!sql.contains("value_bigint >= 0"));
    assert!(!sql.contains("on delete"));
    assert_eq!(sql.matches("insert into ").count(), 0);
    for forbidden in ["operator_kind", "count_rows", "sum_int8", "project_rows"] {
        assert!(
            !sql.contains(forbidden),
            "concrete operator leaked: {forbidden}"
        );
    }
    assert_eq!(sql.matches("comment on table ").count(), 6);
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
        "operator_result_row",
    ] {
        assert!(sql.contains(&format!(
            "revoke all on table shiba_internal.{table} from public"
        )));
    }
    assert!(sql.contains("revoke all on table shiba.operator_result from public"));
    assert!(sql.contains("grant select on table shiba.operator_result to public"));
    assert!(sql.contains("grant select on table shiba.operator_result_rows to public"));
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

mod lifecycle;
