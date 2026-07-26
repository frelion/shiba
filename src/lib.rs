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

    #[pg_test]
    fn routed_transaction_claim_is_idempotent() {
        let commit_lsn = "0/123456";
        let first = Spi::get_one::<bool>(&format!(
            "SELECT shiba._begin_route_transaction('{commit_lsn}'::pg_lsn)"
        ))
        .expect("first route claim should execute")
        .expect("route claim should return a value");
        let second = Spi::get_one::<bool>(&format!(
            "SELECT shiba._begin_route_transaction('{commit_lsn}'::pg_lsn)"
        ))
        .expect("duplicate route claim should execute")
        .expect("route claim should return a value");
        let claims = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM shiba_internal.routed_transactions \
             WHERE commit_lsn='{commit_lsn}'::pg_lsn"
        ))
        .expect("route claims should be queryable")
        .expect("count should return a value");

        assert!(first);
        assert!(!second);
        assert_eq!(claims, 1);
    }

    #[pg_test]
    fn catalog_rejects_invalid_durable_state() {
        Spi::run(
            r#"
            INSERT INTO shiba_internal.stream_views
                (result_oid, view_kind, source_oid, activation_lsn)
            VALUES (4000000000::oid, 'aggregate', 4000000001::oid, '0/0');

            DO $block$
            BEGIN
              BEGIN
                INSERT INTO shiba_internal.stream_views
                    (result_oid, view_kind, source_oid, activation_lsn)
                VALUES (4000000002::oid, 'unknown', 4000000001::oid, '0/0');
                RAISE EXCEPTION 'stream_views accepted an invalid view_kind';
              EXCEPTION WHEN check_violation THEN NULL;
              END;

              BEGIN
                INSERT INTO shiba_internal.aggregate_state
                    (result_oid, group_key, row_count, count_value,
                     sum_nonnull_count, sum_value)
                VALUES (4000000000::oid, 'null'::jsonb, -1, 0, 0, 0);
                RAISE EXCEPTION 'aggregate_state accepted a negative row_count';
              EXCEPTION WHEN check_violation THEN NULL;
              END;

              BEGIN
                INSERT INTO shiba_internal.dag_inbox
                    (result_oid, commit_lsn, sequence, source_oid, delta, row_data)
                VALUES (4000000000::oid, '0/1', 0, 4000000001::oid, 1, '{}'::jsonb);
                RAISE EXCEPTION 'dag_inbox accepted a nonpositive sequence';
              EXCEPTION WHEN check_violation THEN NULL;
              END;

              BEGIN
                INSERT INTO shiba_internal.dag_inbox
                    (result_oid, commit_lsn, sequence, source_oid, delta, row_data)
                VALUES (4000000000::oid, '0/1', 1, 4000000001::oid, 0, '{}'::jsonb);
                RAISE EXCEPTION 'dag_inbox accepted an invalid delta';
              EXCEPTION WHEN check_violation THEN NULL;
              END;

              BEGIN
                INSERT INTO shiba_internal.worker_state (singleton)
                VALUES (false);
                RAISE EXCEPTION 'worker_state accepted a false singleton key';
              EXCEPTION WHEN check_violation THEN NULL;
              END;
            END
            $block$;

            DELETE FROM shiba_internal.stream_views
            WHERE result_oid=4000000000::oid;
            "#,
        )
        .expect("catalog constraints should reject invalid durable state");
    }

    #[pg_test]
    fn dag_batch_applies_ordered_rows_and_advances_progress_once() {
        Spi::run(
            r#"
            CREATE TABLE tests.batch_source (
                group_id integer NOT NULL,
                amount integer NOT NULL
            );
            INSERT INTO tests.batch_source VALUES (1, 5);
            CREATE TABLE tests.batch_result (
                group_id integer UNIQUE NULLS NOT DISTINCT,
                row_count bigint NOT NULL,
                total bigint
            );
            INSERT INTO tests.batch_result VALUES (1,1,5);
            INSERT INTO shiba_internal.stream_views (
                result_oid,source_oid,group_column,result_group_column,
                count_column,sum_input_column,sum_column,activation_lsn
            ) VALUES (
                'tests.batch_result'::regclass,
                'tests.batch_source'::regclass,
                'group_id','group_id','row_count','amount','total','0/0'
            );
            INSERT INTO shiba_internal.aggregate_state (
                result_oid,group_key,row_count,count_value,
                sum_nonnull_count,sum_value
            ) VALUES (
                'tests.batch_result'::regclass,'1'::jsonb,1,1,1,5
            );
            INSERT INTO shiba_internal.view_progress(result_oid)
            VALUES ('tests.batch_result'::regclass);

            CREATE TEMP TABLE batch_progress_writes (marker boolean);
            CREATE FUNCTION pg_temp.count_batch_progress_write()
            RETURNS trigger LANGUAGE plpgsql AS $trigger$
            BEGIN
              INSERT INTO batch_progress_writes VALUES (true);
              RETURN NEW;
            END
            $trigger$;
            CREATE TRIGGER count_batch_progress_write
              AFTER INSERT OR UPDATE ON shiba_internal.view_progress
              FOR EACH ROW EXECUTE FUNCTION pg_temp.count_batch_progress_write();

            SELECT shiba._apply_dag_delta_batch(
              'tests.batch_result'::regclass,
              jsonb_build_array(
                jsonb_build_object(
                  'source_oid','tests.batch_source'::regclass::oid,
                  'row_data',jsonb_build_object('group_id',1,'amount',10),
                  'delta',1
                ),
                jsonb_build_object(
                  'source_oid','tests.batch_source'::regclass::oid,
                  'row_data',jsonb_build_object('group_id',1,'amount',20),
                  'delta',1
                )
              ),
              '0/ABC'
            );
            "#,
        )
        .expect("ordered DAG batch should execute");

        let result = Spi::get_two::<i64, i64>(
            "SELECT row_count,total::bigint FROM tests.batch_result WHERE group_id=1",
        )
        .expect("batch result should be queryable");
        let progress = Spi::get_two::<String, i64>(
            "SELECT applied_lsn::text,
                    (SELECT count(*) FROM batch_progress_writes)
             FROM shiba_internal.view_progress
             WHERE result_oid='tests.batch_result'::regclass",
        )
        .expect("batch progress should be queryable");

        assert_eq!(result, (Some(3), Some(35)));
        assert_eq!(progress, (Some("0/ABC".into()), Some(1)));
    }

    #[pg_test]
    fn ctas_statement_offsets_register_and_drop_all_metadata() {
        Spi::run(
            r#"
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
              JOIN shiba_internal.dag_worker_state worker USING (result_oid)
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
