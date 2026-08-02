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

#[cfg(test)]
mod tests {
    const CATALOG_SQL: &str = include_str!("../../../sql/v2/001_catalog_identity.sql");
    const M2_SQL: &str = include_str!("../../../sql/v2/002_insert_count.sql");

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
        let sql = format!("{}\n{}", normalized_sql(), M2_SQL.to_ascii_lowercase());
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
}
