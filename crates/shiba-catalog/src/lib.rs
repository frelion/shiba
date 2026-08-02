//! Minimal database-local catalog identity for the clean-room V2.
//!
//! This crate owns installation metadata only. Source, effect, runtime,
//! operator, result, registration, and compatibility state are intentionally
//! outside its Phase-1 authority.

::pgrx::pg_module_magic!(name, version);

::pgrx::extension_sql_file!(
    "../../../sql/v2/001_catalog_identity.sql",
    name = "catalog_identity"
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/002_insert_count.sql",
    name = "insert_count",
    requires = ["catalog_identity"]
);

::pgrx::extension_sql_file!(
    "../../../sql/v2/003_nullable_insert.sql",
    name = "nullable_insert",
    requires = ["insert_count"]
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

#[cfg(test)]
mod tests {
    const CATALOG_SQL: &str = include_str!("../../../sql/v2/001_catalog_identity.sql");
    const M2_SQL: &str = include_str!("../../../sql/v2/002_insert_count.sql");
    const M4_SQL: &str = include_str!("../../../sql/v2/003_nullable_insert.sql");
    const M4_EMPTY_SQL: &str = include_str!("../../../sql/v2/004_empty_insert.sql");
    const M4_COMPOSITE_SQL: &str = include_str!("../../../sql/v2/005_composite_insert.sql");
    const M5_TEXT_SQL: &str = include_str!("../../../sql/v2/006_text_payload.sql");
    const M7_SOURCE_SQL: &str = include_str!("../../../sql/v2/007_source_invalidation.sql");

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
    fn m2_owns_only_its_four_required_tables() {
        let sql = M2_SQL.to_ascii_lowercase();
        assert_eq!(sql.matches("create table ").count(), 4);
        for table in [
            "shiba_internal.applied_insert",
            "shiba_internal.count_state",
            "shiba_internal.source_continuation",
            "shiba.count_result",
        ] {
            assert!(sql.contains(&format!("create table {table}")));
        }
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
            M2_SQL.to_ascii_lowercase(),
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
        assert!(sql.contains("alter table shiba_internal.applied_insert"));
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
        assert!(!sql.contains("object_identity"));
    }
}
