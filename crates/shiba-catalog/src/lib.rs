//! Database-local catalog and durable authority schema for clean-room V2.

::pgrx::pg_module_magic!(name, version);
mod graph_state;
::pgrx::extension_sql_file!(
    "../../../sql/v2/001_catalog_identity.sql",
    name = "catalog_identity"
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/002_source_apply.sql",
    name = "graph_runtime",
    requires = ["catalog_identity"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/003_nullable_insert.sql",
    name = "nullable_insert",
    requires = ["graph_state"]
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
    name = "graph_binding",
    requires = ["text_payload"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/008_source_ingress.sql",
    name = "graph_ingress",
    requires = ["graph_binding"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/009_source_ingress_registration.sql",
    name = "graph_ingress_registration",
    requires = ["graph_ingress"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/010_source_ingress_invalidation.sql",
    name = "graph_ingress_invalidation",
    requires = ["graph_ingress_registration"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/011_source_bootstrap.sql",
    name = "graph_bootstrap",
    requires = ["graph_ingress_invalidation"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/012_source_bootstrap_reservation.sql",
    name = "graph_bootstrap_reservation",
    requires = ["graph_bootstrap"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/013_source_bootstrap_replacement.sql",
    name = "graph_bootstrap_replacement",
    requires = ["graph_bootstrap_reservation"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/014_source_rebuild.sql",
    name = "graph_rebuild_lifecycle",
    requires = ["graph_bootstrap_replacement"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/015_source_rebuild_preflight.sql",
    name = "graph_rebuild_preflight",
    requires = ["graph_rebuild_lifecycle"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/016_source_rebuild_current.sql",
    name = "graph_rebuild_current",
    requires = ["graph_rebuild_preflight"]
);
::pgrx::extension_sql_file!(
    "../../../sql/v2/017_source_rebuild_prepare.sql",
    name = "graph_rebuild_prepare",
    requires = ["graph_rebuild_current"]
);

#[cfg(test)]
mod tests;
