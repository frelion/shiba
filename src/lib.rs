use pgrx::prelude::*;

mod config;
mod ddl;
mod filter;
mod index_management;
mod ingress;
mod logical;
mod pgoutput;
pub mod query_analysis;
mod query_tree;
pub mod replication;
mod worker;

::pgrx::pg_module_magic!();

pgrx::extension_sql_file!(
    "../sql/00_catalog.sql",
    name = "shiba_catalog",
    requires = [worker::start_runtime],
);

pgrx::extension_sql_file!(
    "../sql/10_runtime.sql",
    name = "shiba_runtime",
    requires = ["shiba_catalog", worker::wake_runtime_on_commit],
);

pgrx::extension_sql_file!(
    "../sql/11_ingress.sql",
    name = "shiba_ingress",
    requires = ["shiba_runtime"],
);

pgrx::extension_sql_file!(
    "../sql/20_operator_filters.sql",
    name = "shiba_operator_filters",
    requires = ["shiba_ingress"],
);

pgrx::extension_sql_file!(
    "../sql/21_operator_aggregate.sql",
    name = "shiba_operator_aggregate",
    requires = ["shiba_operator_filters"],
);

pgrx::extension_sql_file!(
    "../sql/22_operator_unary_batches.sql",
    name = "shiba_operator_unary_batches",
    requires = ["shiba_operator_aggregate"],
);

pgrx::extension_sql_file!(
    "../sql/23_operator_join_batch.sql",
    name = "shiba_operator_join_batch",
    requires = ["shiba_operator_unary_batches"],
);

pgrx::extension_sql_file!(
    "../sql/24_operator_dispatch.sql",
    name = "shiba_operator_dispatch",
    requires = ["shiba_operator_join_batch"],
);

pgrx::extension_sql_file!(
    "../sql/25_operator_compat.sql",
    name = "shiba_operator_compat",
    requires = ["shiba_operator_dispatch"],
);

pgrx::extension_sql_file!(
    "../sql/26_physical_stages.sql",
    name = "shiba_physical_stages",
    requires = ["shiba_operator_compat"],
);

pgrx::extension_sql_file!(
    "../sql/30_registration.sql",
    name = "shiba_registration",
    requires = ["shiba_physical_stages"],
);

pgrx::extension_sql_file!(
    "../sql/40_lifecycle.sql",
    name = "shiba_lifecycle",
    requires = [
        "shiba_registration",
        index_management::index_ddl_invoker,
        index_management::lock_index_ddl_target,
        index_management::require_index_ddl_top_level
    ],
    finalize,
);

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    config::init();
    unsafe {
        ddl::install_process_utility_hook();
        worker::install_runtime_wakeup_callback();
    }
}

#[pg_extern]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(any(test, feature = "pg_test"))]
#[allow(dead_code)]
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

    #[pg_test]
    fn runtime_resource_gucs_are_registered_and_apply_to_the_session() {
        assert_eq!(
            Spi::get_one::<i32>("SELECT current_setting('shiba.stage_chunk_rows', true)::integer")
                .expect("stage_chunk_rows should be readable"),
            Some(2_048)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT current_setting('shiba.max_stage_rows', true)::integer")
                .expect("max_stage_rows should be readable"),
            Some(1_000_000)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT current_setting('shiba.max_cached_dags', true)::integer")
                .expect("max_cached_dags should be readable"),
            Some(128)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT current_setting('shiba.max_commit_rows', true)::integer")
                .expect("max_commit_rows should be readable"),
            Some(1_000_000)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT current_setting('shiba.max_commit_bytes', true)::integer")
                .expect("max_commit_bytes should be readable"),
            Some(1_073_741_824)
        );

        worker::configure_runtime_session();

        let settings = Spi::get_three::<String, String, String>(
            "SELECT current_setting('work_mem'),
                    current_setting('temp_file_limit'),
                    current_setting('plan_cache_mode')",
        )
        .expect("Runtime session settings should be readable");
        assert_eq!(settings.0.as_deref(), Some("16MB"));
        assert_eq!(settings.1.as_deref(), Some("1GB"));
        assert_eq!(settings.2.as_deref(), Some("force_generic_plan"));
        assert_eq!(
            Spi::get_one::<f64>("SELECT current_setting('hash_mem_multiplier')::double precision")
                .expect("hash_mem_multiplier should be readable"),
            Some(1.0)
        );
    }

    #[pg_test]
    fn ingress_transaction_claim_is_idempotent() {
        Spi::run(
            r#"
            INSERT INTO shiba_internal.ingress_replay_state (
                slot_generation,
                slot_name,
                database_oid,
                system_identifier,
                slot_baseline_lsn
            )
            SELECT 1,
                   'shiba_pg_test',
                   oid,
                   (SELECT system_identifier::text
                      FROM pg_catalog.pg_control_system()),
                   '0/0'
              FROM pg_catalog.pg_database
             WHERE datname = current_database()
            "#,
        )
        .expect("test ingress generation should be created");

        let first = Spi::get_one::<bool>(
            "SELECT created
               FROM shiba_internal.claim_ingress_transaction(
                   1, 42, false, '0/123456'
               )",
        )
        .expect("first ingress claim should execute");
        let second = Spi::get_one::<bool>(
            "SELECT created
               FROM shiba_internal.claim_ingress_transaction(
                   1, 42, false, '0/123456'
               )",
        )
        .expect("duplicate ingress claim should execute");
        let claims = Spi::get_one::<i64>(
            "SELECT count(*)
               FROM shiba_internal.ingress_transactions
              WHERE slot_generation=1
                AND source_xid=42
                AND identity_lsn='0/123456'",
        )
        .expect("ingress claims should be queryable");

        assert_eq!(first, Some(true));
        assert_eq!(second, Some(false));
        assert_eq!(claims, Some(1));
    }

    #[pg_test]
    fn ctas_statement_offsets_register_and_drop_all_metadata() {
        Spi::run(
            r#"
            UPDATE shiba_internal.runtime_state SET active=true WHERE singleton;
            CREATE TABLE tests.pg_boundary_source (
                group_id integer NOT NULL,
                amount integer NOT NULL
            );
            INSERT INTO tests.pg_boundary_source VALUES (1, 10), (1, 20);
            CREATE TABLE SHIBA.pg_boundary_result AS
              SELECT group_id, count(*) AS row_count, sum(amount) AS total
              FROM tests.pg_boundary_source
              GROUP BY group_id;
            "#,
        )
        .expect("the hook should isolate and register the final statement");

        let result_oid =
            Spi::get_one::<i32>("SELECT 'shiba.pg_boundary_result'::regclass::oid::integer")
                .expect("result oid should be queryable")
                .expect("result table should exist");
        let metadata_is_complete = Spi::get_one::<bool>(
            r#"
            SELECT EXISTS (
              SELECT 1
              FROM shiba_internal.stream_views stream
              JOIN shiba_internal.stream_graphs graph USING (result_oid)
              JOIN shiba_internal.view_progress progress USING (result_oid)
              JOIN shiba_internal.dag_runtime_state runtime USING (result_oid)
              WHERE stream.result_oid='shiba.pg_boundary_result'::regclass
                AND graph.analyzed_query->>'version'='1'
                AND jsonb_array_length(graph.analyzed_query->'sources')=1
                AND jsonb_array_length(graph.analyzed_query->'targets')=3
            )
            "#,
        )
        .expect("registration metadata should be queryable")
        .expect("metadata check should return a value");
        let source_is_prepared = Spi::get_one::<bool>(
            r#"
            SELECT source.relreplident='f'
              AND EXISTS (
                SELECT 1
                FROM pg_publication publication
                JOIN pg_publication_rel member
                  ON member.prpubid=publication.oid
                WHERE publication.pubname='shiba_publication'
                  AND member.prrelid=source.oid
              )
              AND (
                SELECT count(*)
                FROM pg_trigger trigger
                WHERE trigger.tgrelid=source.oid
                  AND NOT trigger.tgisinternal
                  AND trigger.tgname LIKE 'shiba_%'
              )=2
            FROM pg_class source
            WHERE source.oid='tests.pg_boundary_source'::regclass
            "#,
        )
        .expect("source preparation should be queryable")
        .expect("source table should exist");

        assert!(metadata_is_complete);
        assert!(source_is_prepared);

        Spi::run("DROP TABLE shiba.pg_boundary_result")
            .expect("dropping a result should clean its registration");

        let metadata_was_removed = Spi::get_one::<bool>(&format!(
            "SELECT NOT EXISTS (
               SELECT 1 FROM shiba_internal.stream_views
               WHERE result_oid={result_oid}::oid
             )
             AND NOT EXISTS (
               SELECT 1 FROM shiba_internal.stream_graphs
               WHERE result_oid={result_oid}::oid
             )"
        ))
        .expect("cleaned metadata should be queryable")
        .expect("cleanup check should return a value");
        let source_was_detached = Spi::get_one::<bool>(
            r#"
            SELECT NOT EXISTS (
                     SELECT 1
                     FROM pg_publication publication
                     JOIN pg_publication_rel member
                       ON member.prpubid=publication.oid
                     WHERE publication.pubname='shiba_publication'
                       AND member.prrelid='tests.pg_boundary_source'::regclass
                   )
              AND NOT EXISTS (
                    SELECT 1 FROM pg_trigger trigger
                    WHERE trigger.tgrelid='tests.pg_boundary_source'::regclass
                      AND NOT trigger.tgisinternal
                      AND trigger.tgname LIKE 'shiba_%'
                  )
            "#,
        )
        .expect("detached source should be queryable")
        .expect("source cleanup check should return a value");

        assert!(metadata_was_removed);
        assert!(source_was_detached);
    }

    #[pg_test(error = "Shiba sources must be persistent ordinary tables outside the shiba schema")]
    fn temporary_tables_cannot_be_stream_sources() {
        Spi::run(
            r#"
            UPDATE shiba_internal.runtime_state SET active=true WHERE singleton;
            CREATE TEMP TABLE pg_temp.pg_boundary_source (
                group_id integer NOT NULL,
                amount integer NOT NULL
            );
            CREATE TABLE shiba.pg_boundary_result AS
              SELECT group_id, count(*) AS row_count, sum(amount) AS total
              FROM pg_temp.pg_boundary_source
              GROUP BY group_id
            "#,
        )
        .expect("the expected PostgreSQL error is handled by the pg_test harness");
    }

    #[pg_test(error = "Shiba MVP does not support TOASTable source columns")]
    fn toastable_columns_cannot_be_stream_sources() {
        Spi::run(
            r#"
            UPDATE shiba_internal.runtime_state SET active=true WHERE singleton;
            CREATE TABLE tests.pg_toastable_source (
                group_id integer NOT NULL,
                amount integer NOT NULL,
                description text
            );
            CREATE TABLE shiba.pg_boundary_result AS
              SELECT group_id, count(*) AS row_count, sum(amount) AS total
              FROM tests.pg_toastable_source
              GROUP BY group_id
            "#,
        )
        .expect("the expected PostgreSQL error is handled by the pg_test harness");
    }

    #[pg_test(
        error = "the shiba schema only accepts CREATE TABLE shiba.name AS SELECT ... stream declarations"
    )]
    fn quoted_schema_name_cannot_bypass_reserved_schema() {
        Spi::run(r#"CREATE TABLE "shiba".pg_boundary_plain_table (id integer)"#)
            .expect("the expected PostgreSQL error is handled by the pg_test harness");
    }

    #[pg_test(
        error = "the shiba schema only accepts CREATE TABLE shiba.name AS SELECT ... stream declarations"
    )]
    fn search_path_cannot_bypass_reserved_schema() {
        Spi::run(
            r#"
            SET LOCAL search_path=shiba,pg_catalog;
            CREATE TABLE pg_boundary_plain_table (id integer)
            "#,
        )
        .expect("the expected PostgreSQL error is handled by the pg_test harness");
    }
}
