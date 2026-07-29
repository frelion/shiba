use pgrx::prelude::*;

mod config;
mod ddl;
mod filter;
mod index_management;
mod logical;
mod pgoutput;
pub mod query_analysis;
mod query_tree;
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
    "../sql/20_operator_filters.sql",
    name = "shiba_operator_filters",
    requires = ["shiba_runtime"],
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
                INSERT INTO shiba_internal.routed_transactions(commit_lsn)
                VALUES ('0/1');
                INSERT INTO shiba_internal.change_log
                    (commit_lsn, sequence, source_oid, delta, row_data)
                VALUES ('0/1', 0, 4000000001::oid, 1, '{}'::jsonb);
                RAISE EXCEPTION 'change_log accepted a nonpositive sequence';
              EXCEPTION WHEN check_violation THEN NULL;
              END;

              BEGIN
                INSERT INTO shiba_internal.routed_transactions(commit_lsn)
                VALUES ('0/2');
                INSERT INTO shiba_internal.change_log
                    (commit_lsn, sequence, source_oid, delta, row_data)
                VALUES ('0/2', 1, 4000000001::oid, 0, '{}'::jsonb);
                RAISE EXCEPTION 'change_log accepted an invalid delta';
              EXCEPTION WHEN check_violation THEN NULL;
              END;

              BEGIN
                INSERT INTO shiba_internal.runtime_state (singleton)
                VALUES (false);
                RAISE EXCEPTION 'runtime_state accepted a false singleton key';
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
            INSERT INTO shiba_internal.dag_runtime_state(result_oid)
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

            CREATE TEMP TABLE batch_sink_writes (marker boolean);
            CREATE FUNCTION pg_temp.count_batch_sink_write()
            RETURNS trigger LANGUAGE plpgsql AS $trigger$
            BEGIN
              INSERT INTO batch_sink_writes VALUES (true);
              RETURN NEW;
            END
            $trigger$;
            CREATE TRIGGER count_batch_sink_write
              AFTER INSERT OR UPDATE OR DELETE ON tests.batch_result
              FOR EACH ROW EXECUTE FUNCTION pg_temp.count_batch_sink_write();

            CREATE TEMP TABLE batch_apply_outcomes(value text);
            SELECT shiba._begin_route_transaction('0/ABC');
            SELECT shiba._route_wal_delta(
              'tests.batch_source'::regclass,
              jsonb_build_object('group_id',1,'amount',10),
              1,'0/ABC',value
            )
            FROM generate_series(1,64) value;
            INSERT INTO batch_apply_outcomes
            SELECT shiba._safe_apply_dag_change_log(
              'tests.batch_result'::regclass,'0/ABC'
            );
            DELETE FROM shiba_internal.dag_inbox
            WHERE result_oid='tests.batch_result'::regclass
              AND commit_lsn='0/ABC'::pg_lsn;

            SELECT shiba._begin_route_transaction('0/ABD');
            SELECT shiba._route_wal_delta(
              'tests.batch_source'::regclass,
              jsonb_build_object(
                'group_id',CASE WHEN value<=32 THEN 1 ELSE 2 END,
                'amount',10
              ),
              CASE WHEN value<=32 THEN -1 ELSE 1 END,
              '0/ABD',value
            )
            FROM generate_series(1,64) value;
            INSERT INTO batch_apply_outcomes
            SELECT shiba._safe_apply_dag_change_log(
              'tests.batch_result'::regclass,'0/ABD'
            );
            "#,
        )
        .expect("ordered DAG batch should execute");

        let first_group = Spi::get_two::<i64, i64>(
            "SELECT row_count,total::bigint FROM tests.batch_result WHERE group_id=1",
        )
        .expect("batch result should be queryable");
        let second_group = Spi::get_two::<i64, i64>(
            "SELECT row_count,total::bigint FROM tests.batch_result WHERE group_id=2",
        )
        .expect("moved batch result should be queryable");
        let progress = Spi::get_two::<String, i64>(
            "SELECT applied_lsn::text,
                    (SELECT count(*) FROM batch_progress_writes)
             FROM shiba_internal.view_progress
             WHERE result_oid='tests.batch_result'::regclass",
        )
        .expect("batch progress should be queryable");
        let sink_writes = Spi::get_one::<i64>("SELECT count(*) FROM batch_sink_writes")
            .expect("batch sink writes should be queryable")
            .expect("batch sink write count should be available");
        let outcomes = Spi::get_one::<String>(
            "SELECT string_agg(value,',' ORDER BY ctid) FROM batch_apply_outcomes",
        )
        .expect("batch outcomes should be queryable");

        assert_eq!(outcomes.as_deref(), Some("applied,applied"));
        assert_eq!(first_group, (Some(33), Some(325)));
        assert_eq!(second_group, (Some(32), Some(320)));
        assert_eq!(progress, (Some("0/ABD".into()), Some(2)));
        assert_eq!(sink_writes, 3);
    }

    #[pg_test]
    fn safe_dag_batch_rolls_back_and_quarantines_only_the_failed_dag() {
        Spi::run(
            r#"
            CREATE TABLE tests.quarantine_source (
                group_id integer NOT NULL,
                amount integer NOT NULL
            );
            CREATE TABLE tests.quarantine_result (
                group_id integer UNIQUE NULLS NOT DISTINCT,
                row_count bigint NOT NULL,
                total bigint
            );
            INSERT INTO tests.quarantine_result VALUES (1,1,5);
            INSERT INTO shiba_internal.stream_views (
                result_oid,source_oid,group_column,result_group_column,
                count_column,sum_input_column,sum_column,activation_lsn
            ) VALUES (
                'tests.quarantine_result'::regclass,
                'tests.quarantine_source'::regclass,
                'group_id','group_id','row_count','amount','total','0/0'
            );
            INSERT INTO shiba_internal.aggregate_state (
                result_oid,group_key,row_count,count_value,
                sum_nonnull_count,sum_value
            ) VALUES (
                'tests.quarantine_result'::regclass,'1'::jsonb,1,1,1,5
            );
            INSERT INTO shiba_internal.view_progress(result_oid)
            VALUES ('tests.quarantine_result'::regclass);
            INSERT INTO shiba_internal.dag_runtime_state(result_oid)
            VALUES ('tests.quarantine_result'::regclass);
            SELECT shiba._begin_route_transaction('0/AFE');
            SELECT shiba._route_wal_delta(
              'tests.quarantine_source'::regclass,
              '{"group_id":1,"amount":10}'::jsonb,1,'0/AFE',1
            );
            SELECT shiba._route_wal_delta(
              'tests.quarantine_source'::regclass,
              '{"group_id":1,"amount":"not-a-number"}'::jsonb,1,'0/AFE',2
            );

            SELECT shiba._safe_apply_dag_change_log(
              'tests.quarantine_result'::regclass,
              shiba._logical_execution_descriptor(
                'tests.quarantine_result'::regclass
              ),
              '0/AFE'::pg_lsn
            );
            "#,
        )
        .expect("safe DAG apply should catch and quarantine an operator error");

        let result = Spi::get_two::<i64, i64>(
            "SELECT row_count,total::bigint
             FROM tests.quarantine_result
             WHERE group_id=1",
        )
        .expect("quarantined result should be queryable");
        let state = Spi::get_two::<i64, i64>(
            "SELECT row_count,sum_value::bigint
             FROM shiba_internal.aggregate_state
             WHERE result_oid='tests.quarantine_result'::regclass",
        )
        .expect("quarantined operator state should be queryable");
        let isolated = Spi::get_one::<bool>(
            "SELECT progress.applied_lsn IS NULL
                AND NOT runtime.active
                AND runtime.last_error LIKE '[22P02] %'
                AND runtime.failed_at IS NOT NULL
                AND (SELECT count(*) FROM shiba_internal.dag_inbox
                     WHERE result_oid=runtime.result_oid)=1
             FROM shiba_internal.view_progress progress
             JOIN shiba_internal.dag_runtime_state runtime USING(result_oid)
             WHERE progress.result_oid='tests.quarantine_result'::regclass",
        )
        .expect("quarantine state should be queryable")
        .expect("quarantine state should exist");
        let quarantine_error = Spi::get_one::<String>(
            "SELECT last_error
             FROM shiba_internal.dag_runtime_state
             WHERE result_oid='tests.quarantine_result'::regclass",
        )
        .expect("quarantine error should be queryable");

        assert_eq!(result, (Some(1), Some(5)));
        assert_eq!(state, (Some(1), Some(5)));
        assert!(
            isolated,
            "unexpected quarantine error: {quarantine_error:?}"
        );
    }

    #[pg_test]
    fn safe_dag_batch_retries_transient_errors_without_quarantine_or_ack() {
        Spi::run(
            r#"
            CREATE TABLE tests.retry_source (
                group_id integer NOT NULL,
                amount integer NOT NULL
            );
            CREATE TABLE tests.retry_result (
                group_id integer UNIQUE NULLS NOT DISTINCT,
                row_count bigint NOT NULL,
                total bigint
            );
            INSERT INTO tests.retry_result VALUES (1,1,5);
            INSERT INTO shiba_internal.stream_views (
                result_oid,source_oid,group_column,result_group_column,
                count_column,sum_input_column,sum_column,activation_lsn
            ) VALUES (
                'tests.retry_result'::regclass,
                'tests.retry_source'::regclass,
                'group_id','group_id','row_count','amount','total','0/0'
            );
            INSERT INTO shiba_internal.aggregate_state (
                result_oid,group_key,row_count,count_value,
                sum_nonnull_count,sum_value
            ) VALUES (
                'tests.retry_result'::regclass,'1'::jsonb,1,1,1,5
            );
            INSERT INTO shiba_internal.view_progress(result_oid)
            VALUES ('tests.retry_result'::regclass);
            INSERT INTO shiba_internal.dag_runtime_state(result_oid)
            VALUES ('tests.retry_result'::regclass);
            SELECT shiba._begin_route_transaction('0/AFF');
            SELECT shiba._route_wal_delta(
              'tests.retry_source'::regclass,
              '{"group_id":1,"amount":10}'::jsonb,1,'0/AFF',1
            );

            CREATE FUNCTION tests.raise_serialization_failure()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $trigger$
            BEGIN
              RAISE serialization_failure
                USING MESSAGE = 'injected transient serialization failure';
            END;
            $trigger$;
            CREATE TRIGGER retry_result_transient_failure
            BEFORE INSERT OR UPDATE OR DELETE ON tests.retry_result
            FOR EACH STATEMENT
            EXECUTE FUNCTION tests.raise_serialization_failure();

            CREATE TEMP TABLE retry_outcome(value text);
            INSERT INTO retry_outcome
            SELECT shiba._safe_apply_dag_change_log(
              'tests.retry_result'::regclass,
              shiba._logical_execution_descriptor(
                'tests.retry_result'::regclass
              ),
              '0/AFF'::pg_lsn
            );
            "#,
        )
        .expect("safe DAG apply should classify serialization failure as retryable");

        let outcome = Spi::get_one::<String>("SELECT value FROM retry_outcome")
            .expect("retry outcome should be queryable");
        let result = Spi::get_two::<i64, i64>(
            "SELECT row_count,total::bigint
             FROM tests.retry_result
             WHERE group_id=1",
        )
        .expect("retry result should be queryable");
        let state = Spi::get_two::<i64, i64>(
            "SELECT row_count,sum_value::bigint
             FROM shiba_internal.aggregate_state
             WHERE result_oid='tests.retry_result'::regclass",
        )
        .expect("retry operator state should be queryable");
        let retryable = Spi::get_one::<bool>(
            "SELECT progress.applied_lsn IS NULL
                AND runtime.active
                AND runtime.last_error IS NULL
                AND runtime.failed_at IS NULL
                AND (SELECT count(*) FROM shiba_internal.dag_inbox
                     WHERE result_oid=runtime.result_oid)=1
             FROM shiba_internal.view_progress progress
             JOIN shiba_internal.dag_runtime_state runtime USING(result_oid)
             WHERE progress.result_oid='tests.retry_result'::regclass",
        )
        .expect("retry state should be queryable")
        .expect("retry state should exist");
        let retry_error = Spi::get_one::<String>(
            "SELECT last_error
             FROM shiba_internal.dag_runtime_state
             WHERE result_oid='tests.retry_result'::regclass",
        )
        .expect("retry error should be queryable");

        assert_eq!(
            outcome.as_deref(),
            Some("retry"),
            "unexpected retry error: {retry_error:?}"
        );
        assert_eq!(result, (Some(1), Some(5)));
        assert_eq!(state, (Some(1), Some(5)));
        assert!(retryable);
    }

    #[pg_test]
    fn dag_apply_rejects_out_of_order_work_and_non_monotonic_progress() {
        Spi::run(
            r#"
            CREATE TABLE tests.ordered_source (
                group_id integer NOT NULL,
                amount integer NOT NULL
            );
            CREATE TABLE tests.ordered_result (
                group_id integer UNIQUE NULLS NOT DISTINCT,
                row_count bigint NOT NULL,
                total bigint
            );
            INSERT INTO shiba_internal.stream_views (
                result_oid,source_oid,group_column,result_group_column,
                count_column,sum_input_column,sum_column,activation_lsn
            ) VALUES (
                'tests.ordered_result'::regclass,
                'tests.ordered_source'::regclass,
                'group_id','group_id','row_count','amount','total','0/0'
            );
            INSERT INTO shiba_internal.view_progress(result_oid)
            VALUES ('tests.ordered_result'::regclass);
            INSERT INTO shiba_internal.dag_runtime_state(result_oid)
            VALUES ('tests.ordered_result'::regclass);

            SELECT shiba._begin_route_transaction('0/B10');
            SELECT shiba._route_wal_delta(
              'tests.ordered_source'::regclass,
              '{"group_id":1,"amount":10}'::jsonb,1,'0/B10',1
            );
            SELECT shiba._begin_route_transaction('0/B11');
            SELECT shiba._route_wal_delta(
              'tests.ordered_source'::regclass,
              '{"group_id":1,"amount":20}'::jsonb,1,'0/B11',1
            );

            CREATE TEMP TABLE ordering_errors(kind text, sqlstate text);
            DO $block$
            BEGIN
              BEGIN
                PERFORM shiba._apply_dag_commit(
                  'tests.ordered_result'::regclass,
                  shiba._logical_execution_descriptor(
                    'tests.ordered_result'::regclass
                  ),
                  '0/B11'::pg_lsn
                );
                RAISE EXCEPTION 'out-of-order apply unexpectedly succeeded';
              EXCEPTION WHEN SQLSTATE 'P0S01' THEN
                INSERT INTO ordering_errors VALUES ('out_of_order',SQLSTATE);
              END;
            END
            $block$;

            SELECT shiba._safe_apply_dag_change_log(
              'tests.ordered_result'::regclass,'0/B10'
            );

            DO $block$
            BEGIN
              BEGIN
                PERFORM shiba._advance_dag_progress(
                  'tests.ordered_result'::regclass,'0/B10'
                );
                RAISE EXCEPTION 'replayed progress unexpectedly succeeded';
              EXCEPTION WHEN SQLSTATE 'P0S01' THEN
                INSERT INTO ordering_errors VALUES ('replay',SQLSTATE);
              END;
              BEGIN
                PERFORM shiba._advance_dag_progress(
                  'tests.ordered_result'::regclass,'0/B0F'
                );
                RAISE EXCEPTION 'regressed progress unexpectedly succeeded';
              EXCEPTION WHEN SQLSTATE 'P0S01' THEN
                INSERT INTO ordering_errors VALUES ('regression',SQLSTATE);
              END;
            END
            $block$;
            "#,
        )
        .expect("ordering boundary checks should execute");

        let ordering_is_enforced = Spi::get_one::<bool>(
            "SELECT
               (SELECT count(*) FROM ordering_errors
                WHERE sqlstate='P0S01')=3
               AND (SELECT count(*) FROM tests.ordered_result)=1
               AND (SELECT row_count=1 AND total=10
                    FROM tests.ordered_result WHERE group_id=1)
               AND (SELECT applied_lsn='0/B10'::pg_lsn
                    FROM shiba_internal.view_progress
                    WHERE result_oid='tests.ordered_result'::regclass)
               AND (SELECT count(*) FROM shiba_internal.dag_inbox
                    WHERE result_oid='tests.ordered_result'::regclass)=2",
        )
        .expect("ordering state should be queryable")
        .expect("ordering state should exist");

        assert!(ordering_is_enforced);
    }

    #[pg_test]
    fn safe_dag_apply_propagates_infrastructure_failures_without_quarantine() {
        Spi::run(
            r#"
            CREATE TABLE tests.infrastructure_source (
                group_id integer NOT NULL,
                amount integer NOT NULL
            );
            CREATE TABLE tests.infrastructure_result (
                group_id integer UNIQUE NULLS NOT DISTINCT,
                row_count bigint NOT NULL,
                total bigint
            );
            INSERT INTO tests.infrastructure_result VALUES (1,1,5);
            INSERT INTO shiba_internal.stream_views (
                result_oid,source_oid,group_column,result_group_column,
                count_column,sum_input_column,sum_column,activation_lsn
            ) VALUES (
                'tests.infrastructure_result'::regclass,
                'tests.infrastructure_source'::regclass,
                'group_id','group_id','row_count','amount','total','0/0'
            );
            INSERT INTO shiba_internal.aggregate_state (
                result_oid,group_key,row_count,count_value,
                sum_nonnull_count,sum_value
            ) VALUES (
                'tests.infrastructure_result'::regclass,
                '1'::jsonb,1,1,1,5
            );
            INSERT INTO shiba_internal.view_progress(result_oid)
            VALUES ('tests.infrastructure_result'::regclass);
            INSERT INTO shiba_internal.dag_runtime_state(result_oid)
            VALUES ('tests.infrastructure_result'::regclass);
            SELECT shiba._begin_route_transaction('0/B20');
            SELECT shiba._route_wal_delta(
              'tests.infrastructure_source'::regclass,
              '{"group_id":1,"amount":10}'::jsonb,1,'0/B20',1
            );

            CREATE TEMP TABLE injected_failure(code text NOT NULL);
            INSERT INTO injected_failure VALUES ('53100');
            CREATE TEMP TABLE propagated_failure(code text NOT NULL);
            CREATE FUNCTION pg_temp.raise_injected_failure()
            RETURNS trigger LANGUAGE plpgsql AS $trigger$
            DECLARE
              injected_code text;
            BEGIN
              SELECT code INTO STRICT injected_code FROM injected_failure;
              RAISE EXCEPTION USING
                ERRCODE=injected_code,
                MESSAGE='injected infrastructure failure';
            END
            $trigger$;
            CREATE TRIGGER infrastructure_failure
              BEFORE INSERT OR UPDATE OR DELETE ON tests.infrastructure_result
              FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.raise_injected_failure();

            DO $block$
            DECLARE
              expected_code text;
              caught_code text;
            BEGIN
              FOREACH expected_code IN ARRAY ARRAY[
                '53100','57P01','58000','57014'
              ]
              LOOP
                UPDATE injected_failure SET code=expected_code;
                caught_code := NULL;
                BEGIN
                  PERFORM shiba._safe_apply_dag_change_log(
                    'tests.infrastructure_result'::regclass,'0/B20'
                  );
                EXCEPTION
                  WHEN query_canceled THEN
                    GET STACKED DIAGNOSTICS caught_code=RETURNED_SQLSTATE;
                  WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS caught_code=RETURNED_SQLSTATE;
                END;
                IF caught_code IS DISTINCT FROM expected_code THEN
                  RAISE EXCEPTION
                    'expected SQLSTATE %, caught %',expected_code,caught_code;
                END IF;
                INSERT INTO propagated_failure VALUES (caught_code);
              END LOOP;
            END
            $block$;
            "#,
        )
        .expect("infrastructure failures should propagate to the Runtime boundary");

        let failures_propagated = Spi::get_one::<bool>(
            "SELECT
               (SELECT array_agg(code ORDER BY code)
                FROM propagated_failure)
                 = ARRAY['53100','57014','57P01','58000']
               AND runtime.active
               AND runtime.last_error IS NULL
               AND runtime.failed_at IS NULL
               AND progress.applied_lsn IS NULL
               AND (SELECT row_count=1 AND sum_value=5
                    FROM shiba_internal.aggregate_state
                    WHERE result_oid=runtime.result_oid)
               AND (SELECT row_count=1 AND total=5
                    FROM tests.infrastructure_result WHERE group_id=1)
               AND (SELECT count(*) FROM shiba_internal.dag_inbox
                    WHERE result_oid=runtime.result_oid)=1
             FROM shiba_internal.dag_runtime_state runtime
             JOIN shiba_internal.view_progress progress USING(result_oid)
             WHERE runtime.result_oid='tests.infrastructure_result'::regclass",
        )
        .expect("infrastructure failure state should be queryable")
        .expect("infrastructure failure state should exist");

        assert!(failures_propagated);
    }

    #[pg_test]
    fn aggregate_batch_deletes_non_finite_zero_row_groups() {
        Spi::run(
            r#"
            CREATE TABLE tests.nonfinite_source (
              group_id integer NOT NULL,
              amount numeric NOT NULL
            );
            INSERT INTO tests.nonfinite_source
            VALUES (1,'NaN'::numeric),(2,'Infinity'::numeric);
            CREATE TABLE tests.nonfinite_result (
              group_id integer UNIQUE NULLS NOT DISTINCT,
              row_count bigint NOT NULL,
              total numeric
            );
            INSERT INTO tests.nonfinite_result
            VALUES (1,1,'NaN'::numeric),(2,1,'Infinity'::numeric);
            INSERT INTO shiba_internal.stream_views (
              result_oid,source_oid,group_column,result_group_column,
              count_column,sum_input_column,sum_column,activation_lsn
            ) VALUES (
              'tests.nonfinite_result'::regclass,
              'tests.nonfinite_source'::regclass,
              'group_id','group_id','row_count','amount','total','0/0'
            );
            INSERT INTO shiba_internal.aggregate_state (
              result_oid,group_key,row_count,count_value,
              sum_nonnull_count,sum_value
            ) VALUES
              ('tests.nonfinite_result'::regclass,'1'::jsonb,1,1,1,'NaN'),
              ('tests.nonfinite_result'::regclass,'2'::jsonb,1,1,1,'Infinity');
            INSERT INTO shiba_internal.view_progress(result_oid)
            VALUES ('tests.nonfinite_result'::regclass);
            INSERT INTO shiba_internal.dag_runtime_state(result_oid)
            VALUES ('tests.nonfinite_result'::regclass);

            CREATE TEMP TABLE nonfinite_apply_outcome(value text);
            SELECT shiba._begin_route_transaction('0/AC0');
            SELECT shiba._route_wal_delta(
              'tests.nonfinite_source'::regclass,
              jsonb_build_object(
                'group_id',value,
                'amount',CASE value
                  WHEN 1 THEN 'NaN'
                  WHEN 2 THEN 'Infinity'
                  ELSE value::text
                END
              ),
              CASE WHEN value<=2 THEN -1 ELSE 1 END,
              '0/AC0',value
            )
            FROM generate_series(1,64) value;
            INSERT INTO nonfinite_apply_outcome
            SELECT shiba._safe_apply_dag_change_log(
              'tests.nonfinite_result'::regclass,'0/AC0'
            );
            "#,
        )
        .expect("batch should delete zero-row non-finite aggregate groups");

        let result = Spi::get_two::<i64, i64>(
            "SELECT count(*),sum(row_count)::bigint
             FROM tests.nonfinite_result",
        )
        .expect("non-finite batch result should be queryable");
        let state = Spi::get_two::<i64, String>(
            "SELECT count(*),max(applied_lsn)::text
             FROM shiba_internal.aggregate_state
             CROSS JOIN shiba_internal.view_progress
             WHERE aggregate_state.result_oid='tests.nonfinite_result'::regclass
               AND view_progress.result_oid=aggregate_state.result_oid",
        )
        .expect("non-finite batch state should be queryable");
        let outcome = Spi::get_one::<String>("SELECT value FROM nonfinite_apply_outcome")
            .expect("non-finite outcome should be queryable");

        assert_eq!(outcome.as_deref(), Some("applied"));
        assert_eq!(result, (Some(62), Some(62)));
        assert_eq!(state, (Some(62), Some("0/AC0".into())));
    }

    #[pg_test]
    fn canonical_apply_claims_earliest_commit_and_acknowledges_once() {
        Spi::run(
            r#"
            CREATE TABLE tests.claim_source (
              group_id integer NOT NULL,
              amount integer NOT NULL
            );
            CREATE TABLE tests.claim_result (
              group_id integer UNIQUE NULLS NOT DISTINCT,
              row_count bigint NOT NULL,
              total bigint
            );
            INSERT INTO shiba_internal.stream_views (
              result_oid,source_oid,group_column,result_group_column,
              count_column,sum_input_column,sum_column,activation_lsn
            ) VALUES (
              'tests.claim_result'::regclass,
              'tests.claim_source'::regclass,
              'group_id','group_id','row_count','amount','total','0/0'
            );
            INSERT INTO shiba_internal.view_progress(result_oid)
            VALUES ('tests.claim_result'::regclass);
            INSERT INTO shiba_internal.dag_runtime_state(result_oid)
            VALUES ('tests.claim_result'::regclass);

            SELECT shiba._begin_route_transaction('0/B30');
            SELECT shiba._route_wal_delta(
              'tests.claim_source'::regclass,
              '{"group_id":1,"amount":10}'::jsonb,1,'0/B30',1
            );
            SELECT shiba._begin_route_transaction('0/B31');
            SELECT shiba._route_wal_delta(
              'tests.claim_source'::regclass,
              '{"group_id":1,"amount":20}'::jsonb,1,'0/B31',1
            );

            CREATE TEMP TABLE claim_outcomes(
              ordinal integer GENERATED ALWAYS AS IDENTITY,
              outcome text,
              commit_lsn pg_lsn
            );
            INSERT INTO claim_outcomes(outcome,commit_lsn)
            SELECT * FROM shiba._apply_next_dag_change_log(
              'tests.claim_result'::regclass,
              shiba._logical_execution_descriptor(
                'tests.claim_result'::regclass
              )
            );
            INSERT INTO claim_outcomes(outcome,commit_lsn)
            SELECT * FROM shiba._apply_next_dag_change_log(
              'tests.claim_result'::regclass,
              shiba._logical_execution_descriptor(
                'tests.claim_result'::regclass
              )
            );
            INSERT INTO claim_outcomes(outcome,commit_lsn)
            SELECT * FROM shiba._apply_next_dag_change_log(
              'tests.claim_result'::regclass,
              shiba._logical_execution_descriptor(
                'tests.claim_result'::regclass
              )
            );
            "#,
        )
        .expect("canonical claim/apply/ack entry point should execute");

        let protocol_state = Spi::get_one::<String>(
            r#"
            SELECT json_build_object(
              'outcomes',(SELECT string_agg(
                outcome || ':' || coalesce(commit_lsn::text,'NULL'),
                ',' ORDER BY ordinal
              ) FROM claim_outcomes),
              'inbox',(SELECT count(*) FROM shiba_internal.dag_inbox
                WHERE result_oid='tests.claim_result'::regclass),
              'progress',(SELECT applied_lsn::text
                FROM shiba_internal.view_progress
                WHERE result_oid='tests.claim_result'::regclass),
              'result',(SELECT row_count::text || ':' || total::text
                FROM tests.claim_result WHERE group_id=1),
              'advisory',EXISTS (
                SELECT 1 FROM pg_locks
                WHERE pid=pg_backend_pid()
                  AND locktype='advisory'
                  AND granted
              )
            )::text
            "#,
        )
        .expect("canonical apply state should be queryable")
        .expect("canonical apply state should exist");

        assert_eq!(
            protocol_state,
            r#"{"outcomes" : "applied:0/B30,applied:0/B31,idle:NULL", "inbox" : 0, "progress" : "0/B31", "result" : "2:30", "advisory" : true}"#
        );
    }

    #[pg_test]
    fn canonical_apply_quarantines_when_ack_does_not_delete_exactly_one_row() {
        Spi::run(
            r#"
            CREATE TABLE tests.ack_source (
              group_id integer NOT NULL,
              amount integer NOT NULL
            );
            CREATE TABLE tests.ack_result (
              group_id integer UNIQUE NULLS NOT DISTINCT,
              row_count bigint NOT NULL,
              total bigint
            );
            INSERT INTO shiba_internal.stream_views (
              result_oid,source_oid,group_column,result_group_column,
              count_column,sum_input_column,sum_column,activation_lsn
            ) VALUES (
              'tests.ack_result'::regclass,
              'tests.ack_source'::regclass,
              'group_id','group_id','row_count','amount','total','0/0'
            );
            INSERT INTO shiba_internal.view_progress(result_oid)
            VALUES ('tests.ack_result'::regclass);
            INSERT INTO shiba_internal.dag_runtime_state(result_oid)
            VALUES ('tests.ack_result'::regclass);
            SELECT shiba._begin_route_transaction('0/B40');
            SELECT shiba._route_wal_delta(
              'tests.ack_source'::regclass,
              '{"group_id":1,"amount":10}'::jsonb,1,'0/B40',1
            );

            CREATE FUNCTION pg_temp.suppress_dag_ack()
            RETURNS trigger LANGUAGE plpgsql AS $trigger$
            BEGIN
              RETURN NULL;
            END
            $trigger$;
            CREATE TRIGGER suppress_dag_ack
              BEFORE DELETE ON shiba_internal.dag_inbox
              FOR EACH ROW EXECUTE FUNCTION pg_temp.suppress_dag_ack();

            CREATE TEMP TABLE ack_outcome(outcome text,commit_lsn pg_lsn);
            INSERT INTO ack_outcome
            SELECT * FROM shiba._apply_next_dag_change_log(
              'tests.ack_result'::regclass,
              shiba._logical_execution_descriptor(
                'tests.ack_result'::regclass
              )
            );
            "#,
        )
        .expect("zero-row acknowledgement should be isolated as a DAG failure");

        let failed_ack_state = Spi::get_one::<String>(
            r#"
            SELECT json_build_object(
              'outcome',(SELECT outcome || ':' || commit_lsn::text
                FROM ack_outcome),
              'progress',coalesce(progress.applied_lsn::text,'NULL'),
              'active',runtime.active,
              'error',runtime.last_error,
              'failed',runtime.failed_at IS NOT NULL,
              'result_rows',(SELECT count(*) FROM tests.ack_result),
              'state_rows',(SELECT count(*) FROM shiba_internal.aggregate_state
                WHERE result_oid=runtime.result_oid),
              'inbox_rows',(SELECT count(*) FROM shiba_internal.dag_inbox
                WHERE result_oid=runtime.result_oid
                  AND commit_lsn='0/B40'::pg_lsn)
            )::text
            FROM shiba_internal.dag_runtime_state runtime
            JOIN shiba_internal.view_progress progress USING(result_oid)
            WHERE runtime.result_oid='tests.ack_result'::regclass
            "#,
        )
        .expect("failed acknowledgement state should be queryable")
        .expect("failed acknowledgement state should exist");

        assert_eq!(
            failed_ack_state,
            r#"{"outcome" : "quarantined:0/B40", "progress" : "NULL", "active" : false, "error" : "[P0S01] Shiba DAG tests.ack_result acknowledgement for commit 0/B40 affected 0 rows, expected 1", "failed" : true, "result_rows" : 0, "state_rows" : 0, "inbox_rows" : 1}"#
        );
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
