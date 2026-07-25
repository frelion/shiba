use pgrx::prelude::*;

mod ddl;
mod filter;
mod logical;
mod pgoutput;
mod query_tree;
mod worker;

::pgrx::pg_module_magic!();

pgrx::extension_sql_file!(
    "../sql/00_catalog.sql",
    name = "shiba_catalog",
    requires = [worker::start_worker],
);

pgrx::extension_sql_file!(
    "../sql/10_runtime.sql",
    name = "shiba_runtime",
    requires = ["shiba_catalog"],
);

pgrx::extension_sql_file!(
    "../sql/20_operators.sql",
    name = "shiba_operators",
    requires = ["shiba_runtime"],
);

pgrx::extension_sql_file!(
    "../sql/30_registration.sql",
    name = "shiba_registration",
    requires = ["shiba_operators"],
);

pgrx::extension_sql_file!(
    "../sql/40_lifecycle.sql",
    name = "shiba_lifecycle",
    requires = ["shiba_registration"],
    finalize,
);

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    unsafe {
        ddl::install_process_utility_hook();
    }
}

#[pg_extern]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(any(test, feature = "pg_test"))]
mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["wal_level = logical", "max_replication_slots = 4"]
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn version_is_available() {
        assert_eq!(version(), "0.1.0");
    }
}
