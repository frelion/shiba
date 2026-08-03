//! Database-local catalog and durable authority schema for clean-room V2.

::pgrx::pg_module_magic!(name, version);
mod keyed_state;
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
    requires = ["operator_keyed_state"]
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
::pgrx::extension_sql_file!(
    "../../../sql/v2/014_source_rebuild.sql",
    name = "source_rebuild",
    requires = ["source_bootstrap_replacement"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/015_source_rebuild_preflight.sql",
    name = "source_rebuild_preflight",
    requires = ["source_rebuild"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/016_source_rebuild_current.sql",
    name = "source_rebuild_current",
    requires = ["source_rebuild_preflight"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/017_source_rebuild_prepare.sql",
    name = "source_rebuild_prepare",
    requires = ["source_rebuild_current"]
);

#[cfg(test)]
mod tests;
