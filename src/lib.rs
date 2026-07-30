//! A focused PostgreSQL streaming engine written in Rust.
//!
//! The shortest reading path through the code is:
//!
//! 1. `postgres` contains tiny, round-tripped PostgreSQL text encodings.
//! 2. `pgoutput` decodes PostgreSQL WAL messages.
//! 3. `ingress` turns WAL transaction fragments into bounded source chunks.
//! 4. `query_lowering` builds the one dataflow plan; `logical` validates it.
//! 5. `worker` runs the single database-scoped event loop.
//!
//! Operator state machines live in `kernel`; SQL files define the durable
//! catalog and shared transactional primitives. Start with `README.md`, then
//! follow `docs/LEARNING_RUST.md` for a guided code tour.

use pgrx::prelude::*;

// A plain Rust unit-test binary is not loaded by PostgreSQL. Keep the
// extension-only runtime graph unexported there so Linux can discard its
// unresolved PostgreSQL server symbols at link time.
#[cfg_attr(test, allow(dead_code))]
mod config;
mod ddl;
mod index_management;
#[cfg_attr(test, allow(dead_code))]
mod ingress;
mod kernel;
#[cfg_attr(test, allow(dead_code))]
mod logical;
mod pgoutput;
mod postgres;
mod query_lowering;
#[cfg_attr(test, allow(dead_code))]
mod replication;
mod scalar_sql;
#[cfg_attr(test, allow(dead_code))]
mod worker;

::pgrx::pg_module_magic!();

// pgrx needs explicitly named schemas in its entity graph. Public Rust
// functions use the extension's fixed `shiba` schema from shiba.control;
// implementation functions name this internal schema directly.
#[pg_schema]
mod shiba_internal {}

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
    "../sql/12_effect_stream.sql",
    name = "shiba_effect_stream",
    requires = ["shiba_ingress"],
);

pgrx::extension_sql_file!(
    "../sql/25_introspection.sql",
    name = "shiba_introspection",
    requires = ["shiba_effect_stream", index_management::invoker_oid],
);

pgrx::extension_sql_file!(
    "../sql/30_registration.sql",
    name = "shiba_registration",
    requires = [
        "shiba_introspection",
        kernel::register::create_effect_stream_payload,
        kernel::register::lock_dataflow_sources,
        kernel::register::prepare_dataflow_source,
        kernel::register::register_dataflow,
        kernel::register::validate_effect_stream_payload
    ],
);

pgrx::extension_sql_file!(
    "../sql/40_lifecycle.sql",
    name = "shiba_lifecycle",
    requires = [
        "shiba_registration",
        index_management::invoker_oid,
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

    fn install_test_ingress_generation() {
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
                   shiba_internal.slot_name(),
                   database.oid,
                   control.system_identifier::text,
                   '0/0'
            FROM pg_catalog.pg_database AS database
            CROSS JOIN pg_catalog.pg_control_system() AS control
            WHERE database.datname = current_database()
            "#,
        )
        .expect("test ingress generation should be created");
    }

    #[pg_test]
    fn version_is_available() {
        assert_eq!(version(), "0.1.0");
    }

    #[pg_test]
    fn lowers_multijoin_group_having_to_typed_ports_and_slots() {
        Spi::run(
            r#"
            CREATE TABLE tests.lowering_accounts (
                id integer PRIMARY KEY,
                region integer NOT NULL,
                active boolean NOT NULL
            );
            CREATE TABLE tests.lowering_orders (
                id integer PRIMARY KEY,
                account_id integer NOT NULL,
                product_id integer NOT NULL
            );
            CREATE TABLE tests.lowering_products (
                id integer PRIMARY KEY,
                amount bigint NOT NULL
            )
            "#,
        )
        .unwrap();

        let plan = unsafe {
            crate::query_lowering::lower_select_for_test(
                r#"
                SELECT a.region, count(*) AS row_count, sum(p.amount) AS total
                FROM tests.lowering_accounts a
                JOIN tests.lowering_orders o ON a.id = o.account_id
                LEFT JOIN tests.lowering_products p ON o.product_id = p.id
                WHERE a.active AND p.amount > 0
                GROUP BY a.region
                HAVING sum(p.amount) > 10
                "#,
            )
        }
        .unwrap();

        use crate::logical::model::{OperatorKind, ScalarExpr};
        let kinds = plan
            .stages
            .iter()
            .map(|stage| stage.spec.kind())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds
                .iter()
                .filter(|kind| **kind == OperatorKind::Scan)
                .count(),
            3
        );
        assert_eq!(
            kinds
                .iter()
                .filter(|kind| **kind == OperatorKind::Join)
                .count(),
            2
        );
        assert_eq!(
            kinds
                .iter()
                .filter(|kind| **kind == OperatorKind::Aggregate)
                .count(),
            1
        );
        assert!(
            kinds
                .iter()
                .filter(|kind| **kind == OperatorKind::Filter)
                .count()
                >= 3
        );
        assert_eq!(kinds.last(), Some(&OperatorKind::Sink));

        for (stage_id, stage) in plan.stages.iter().enumerate() {
            let inputs = &stage.schema.inputs;
            let outputs = &stage.schema.outputs;
            let bindings = inputs
                .iter()
                .map(|input| input.binding)
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(bindings.len(), inputs.len());
            let slots = outputs
                .iter()
                .map(|output| output.slot)
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(slots.len(), outputs.len());
            for expression in stage.spec.expressions() {
                expression.visit(&mut |part| {
                    if let ScalarExpr::Input { binding } = part {
                        assert!(
                            bindings.contains(binding),
                            "stage {stage_id} references a binding outside its input ports",
                        );
                    }
                });
            }
            if stage.spec.kind() == OperatorKind::Join {
                let ports = inputs
                    .iter()
                    .map(|input| input.input)
                    .collect::<std::collections::BTreeSet<_>>();
                assert_eq!(ports, std::collections::BTreeSet::from([0, 1]));
            }
        }
    }

    #[pg_test]
    fn lowers_nested_from_subquery_through_its_output_binding() {
        Spi::run(
            r#"
            CREATE TABLE tests.lowering_nested_source (
                id integer NOT NULL,
                active boolean NOT NULL
            )
            "#,
        )
        .unwrap();
        let plan = unsafe {
            crate::query_lowering::lower_select_for_test(
                r#"
                SELECT q.id
                FROM (
                    SELECT source.id
                    FROM tests.lowering_nested_source source
                    WHERE source.active
                ) q
                WHERE q.id > 0
                "#,
            )
        }
        .unwrap();
        use crate::logical::model::OperatorKind;
        let kinds = plan
            .stages
            .iter()
            .map(|stage| stage.spec.kind())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds
                .iter()
                .filter(|kind| **kind == OperatorKind::Scan)
                .count(),
            1
        );
        assert_eq!(
            kinds
                .iter()
                .filter(|kind| **kind == OperatorKind::Filter)
                .count(),
            2
        );
        assert_eq!(
            kinds
                .iter()
                .filter(|kind| **kind == OperatorKind::Project)
                .count(),
            2
        );
        assert_eq!(kinds.last(), Some(&OperatorKind::Sink));
    }

    #[pg_test]
    fn aggregate_order_by_junk_is_not_a_transition_argument() {
        Spi::run(
            r#"
            CREATE TABLE tests.lowering_aggregate_order (
                group_id integer NOT NULL,
                amount integer NOT NULL
            )
            "#,
        )
        .unwrap();
        let plan = unsafe {
            crate::query_lowering::lower_select_for_test(
                r#"
                SELECT max(amount ORDER BY group_id)
                FROM tests.lowering_aggregate_order
                "#,
            )
        }
        .unwrap();
        let aggregate = plan
            .stages
            .iter()
            .find_map(|stage| match &stage.spec {
                crate::logical::model::OperatorSpec::Aggregate(spec) => Some(&spec.aggregates[0]),
                _ => None,
            })
            .expect("query should contain an Aggregate stage");

        assert_eq!(aggregate.args.len(), 1);
        assert_eq!(aggregate.order_by.len(), 1);
    }

    #[pg_test]
    fn sort_group_contract_preserves_analyzed_equality() {
        Spi::run(
            r#"
            CREATE TABLE tests.lowering_sort_group (
                group_id integer NOT NULL,
                amount integer NOT NULL
            )
            "#,
        )
        .unwrap();

        let aggregate_plan = unsafe {
            crate::query_lowering::lower_select_for_test(
                r#"
                SELECT group_id, count(DISTINCT amount ORDER BY amount)
                FROM tests.lowering_sort_group
                GROUP BY group_id
                "#,
            )
        }
        .unwrap();
        let aggregate = aggregate_plan
            .stages
            .iter()
            .find_map(|stage| match &stage.spec {
                crate::logical::model::OperatorSpec::Aggregate(spec) => Some(spec),
                _ => None,
            })
            .expect("query should contain an Aggregate stage");
        assert_ne!(aggregate.groups[0].key.equality_operator_oid, 0);
        assert_ne!(aggregate.groups[0].key.sort_operator_oid, 0);
        assert_ne!(aggregate.aggregates[0].distinct[0].equality_operator_oid, 0);
        assert_ne!(aggregate.aggregates[0].order_by[0].sort_operator_oid, 0);

        let window_plan = unsafe {
            crate::query_lowering::lower_select_for_test(
                r#"
                SELECT sum(amount) OVER (
                         PARTITION BY group_id
                         ORDER BY amount
                         RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
                       )
                FROM tests.lowering_sort_group
                "#,
            )
        }
        .unwrap();
        let window = window_plan
            .stages
            .iter()
            .find_map(|stage| match &stage.spec {
                crate::logical::model::OperatorSpec::Window(spec) => Some(spec),
                _ => None,
            })
            .expect("query should contain a Window stage");
        assert_ne!(window.partition_by[0].equality_operator_oid, 0);
        assert_ne!(window.order_by[0].equality_operator_oid, 0);
        assert!(window.frame.start_in_range_function_oid.is_some());

        let topn_plan = unsafe {
            crate::query_lowering::lower_select_for_test(
                r#"
                SELECT group_id, amount
                FROM tests.lowering_sort_group
                ORDER BY amount NULLS FIRST
                FETCH FIRST 3 ROWS WITH TIES
                "#,
            )
        }
        .unwrap();
        let topn = topn_plan
            .stages
            .iter()
            .find_map(|stage| match &stage.spec {
                crate::logical::model::OperatorSpec::TopN(spec) => Some(spec),
                _ => None,
            })
            .expect("query should contain a TopN stage");
        assert!(topn.with_ties);
        assert!(topn.order_by[0].nulls_first);
        assert_ne!(topn.order_by[0].equality_operator_oid, 0);
    }

    #[pg_test]
    fn lowers_window_functions_as_appended_outputs() {
        Spi::run(
            r#"
            CREATE TABLE tests.lowering_window_source (
                id integer NOT NULL,
                val integer NOT NULL,
                group_id integer NOT NULL,
                payload text NOT NULL
            )
            "#,
        )
        .unwrap();

        let one = unsafe {
            crate::query_lowering::lower_select_for_test(
                r#"
                SELECT id, row_number() OVER (ORDER BY val, id) AS rn
                FROM tests.lowering_window_source
                "#,
            )
        }
        .unwrap();
        one.validate().unwrap();
        let one_window = one
            .stages
            .iter()
            .find_map(|stage| match &stage.spec {
                crate::logical::model::OperatorSpec::Window(spec) => Some((stage, spec)),
                _ => None,
            })
            .expect("query should contain one Window stage");
        assert_eq!(one_window.0.schema.inputs.len(), 4);
        assert_eq!(one_window.1.outputs.len(), 4);
        assert_eq!(one_window.1.functions.len(), 1);
        assert_eq!(one_window.0.schema.outputs.len(), 5);
        assert_eq!(
            one.stages
                .last()
                .expect("query should contain a Sink")
                .schema
                .inputs
                .len(),
            2
        );

        let three = unsafe {
            crate::query_lowering::lower_select_for_test(
                r#"
                SELECT id,
                       row_number() OVER ordered AS rn,
                       rank() OVER ordered AS rank,
                       dense_rank() OVER ordered AS dense_rank
                FROM tests.lowering_window_source
                WINDOW ordered AS (ORDER BY val, id)
                "#,
            )
        }
        .unwrap();
        three.validate().unwrap();
        let three_window = three
            .stages
            .iter()
            .find_map(|stage| match &stage.spec {
                crate::logical::model::OperatorSpec::Window(spec) => Some((stage, spec)),
                _ => None,
            })
            .expect("query should contain one Window stage");
        assert_eq!(three_window.0.schema.inputs.len(), 4);
        assert_eq!(three_window.1.outputs.len(), 4);
        assert_eq!(three_window.1.functions.len(), 3);
        assert_eq!(three_window.0.schema.outputs.len(), 7);
        assert_eq!(
            three
                .stages
                .last()
                .expect("query should contain a Sink")
                .schema
                .inputs
                .len(),
            4
        );
    }

    #[pg_test]
    fn reports_the_specific_unsupported_scalar_capability() {
        Spi::run("CREATE TABLE tests.lowering_array_source (id integer NOT NULL)").unwrap();
        let error = unsafe {
            crate::query_lowering::lower_select_for_test(
                "SELECT ARRAY[source.id] FROM tests.lowering_array_source source",
            )
        }
        .unwrap_err()
        .to_string();
        assert!(error.contains("project.expression"), "{error}");
        assert!(error.contains("T_ArrayExpr"), "{error}");
    }

    #[pg_test]
    fn runtime_resource_gucs_are_registered_and_apply_to_the_session() {
        assert_eq!(
            Spi::get_one::<i32>("SELECT current_setting('shiba.stage_chunk_rows', true)::integer")
                .expect("stage_chunk_rows should be readable"),
            Some(16 * 1024)
        );
        assert_eq!(
            Spi::get_one::<i64>(
                "SELECT pg_size_bytes(
                    current_setting('shiba.stage_chunk_bytes', true)
                 )",
            )
            .expect("stage_chunk_bytes should be readable"),
            Some(16 * 1024 * 1024)
        );
        assert_eq!(
            Spi::get_one::<i32>(
                "SELECT current_setting('shiba.max_cached_dataflows', true)::integer",
            )
            .expect("max_cached_dataflows should be readable"),
            Some(128)
        );
        assert_eq!(
            Spi::get_one::<i64>(
                "SELECT pg_size_bytes(
                    current_setting('shiba.ingress_staging_limit', true)
                 )",
            )
            .expect("ingress_staging_limit should be readable"),
            Some(64_i64 * 1024 * 1024 * 1024)
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
                   1, 42, '0/123456'
               )",
        )
        .expect("first ingress claim should execute");
        let second = Spi::get_one::<bool>(
            "SELECT created
               FROM shiba_internal.claim_ingress_transaction(
                   1, 42, '0/123456'
               )",
        )
        .expect("duplicate ingress claim should execute");
        let claims = Spi::get_one::<i64>(
            "SELECT count(*)
              FROM shiba_internal.ingress_transactions
              WHERE slot_generation=1
                AND source_xid=42
                AND transaction_start_lsn='0/123456'
                AND final_lsn IS NULL",
        )
        .expect("ingress claims should be queryable");

        assert_eq!(first, Some(true));
        assert_eq!(second, Some(false));
        assert_eq!(claims, Some(1));
    }

    #[pg_test]
    fn streamed_ingress_is_hidden_until_commit_and_abort_is_durable() {
        install_test_ingress_generation();
        Spi::run(
            r#"
            CREATE TABLE tests.pg_streamed_ingress_source (
                id integer NOT NULL
            );

            WITH claimed AS (
                SELECT ingress_txn_id
                  FROM shiba_internal.claim_ingress_transaction(
                       1, 42, '0/100'
                  )
            )
            SELECT shiba_internal.insert_ingress_events(
                claimed.ingress_txn_id,
                jsonb_build_array(
                    jsonb_build_object(
                        'change_lsn', '0/110',
                        'change_ordinal', 0,
                        'image_ordinal', 0,
                        'source_subxid', 42,
                        'source_oid',
                            'tests.pg_streamed_ingress_source'::regclass::oid::bigint,
                        'weight', 1,
                        'payload', jsonb_build_object('id', '1')
                    ),
                    jsonb_build_object(
                        'change_lsn', '0/120',
                        'change_ordinal', 0,
                        'image_ordinal', 0,
                        'source_subxid', 43,
                        'source_oid',
                            'tests.pg_streamed_ingress_source'::regclass::oid::bigint,
                        'weight', 1,
                        'payload', jsonb_build_object('id', '2')
                    )
                )
            )
            FROM claimed;
            "#,
        )
        .expect("streamed ingress rows should stage");

        assert_eq!(
            Spi::get_one::<String>(
                "SELECT outcome
                   FROM shiba_internal.publish_source_batch(1)"
            )
            .expect("open publication check should execute")
            .as_deref(),
            Some("idle")
        );
        assert!(
            Spi::get_one::<i64>(
                "SELECT open_payload_bytes
                   FROM shiba_internal.ingress_replay_state
                  WHERE slot_generation = 1"
            )
            .expect("open payload counter should be readable")
            .unwrap_or(0)
                > 0
        );

        Spi::run(
            r#"
            SELECT shiba_internal.abort_ingress_subtransaction(
                ingress_txn_id, 43
            )
            FROM shiba_internal.ingress_transactions
            WHERE slot_generation = 1
              AND transaction_start_lsn = '0/100';

            SELECT shiba_internal.commit_ingress_transaction(
                ingress_txn_id, '0/130', '0/140'
            )
            FROM shiba_internal.ingress_transactions
            WHERE slot_generation = 1
              AND transaction_start_lsn = '0/100';
            "#,
        )
        .expect("streamed transaction should finalize");

        let (status, open_payload_bytes) = Spi::get_two::<String, i64>(
            "SELECT txn.status, replay.open_payload_bytes
               FROM shiba_internal.ingress_transactions AS txn
               JOIN shiba_internal.ingress_replay_state AS replay
                 USING (slot_generation)
              WHERE txn.transaction_start_lsn = '0/100'",
        )
        .expect("committed streamed transaction should be readable");
        assert_eq!(status.as_deref(), Some("committed"));
        assert_eq!(open_payload_bytes, Some(0));
        assert_eq!(
            Spi::get_one::<i64>(
                "SELECT count(*)
                   FROM shiba_internal.change_log AS event
                  WHERE event.source_subxid = 43
                    AND EXISTS (
                        SELECT 1
                          FROM shiba_internal.ingress_aborted_subtransactions
                               AS aborted
                         WHERE aborted.ingress_txn_id = event.ingress_txn_id
                           AND aborted.source_subxid = event.source_subxid
                    )"
            )
            .expect("aborted subtransaction marker should be queryable"),
            Some(1)
        );

        Spi::run(
            r#"
            WITH claimed AS (
                SELECT ingress_txn_id
                  FROM shiba_internal.claim_ingress_transaction(
                       1, 42, '0/200'
                  )
            )
            SELECT shiba_internal.abort_ingress_transaction(
                claimed.ingress_txn_id, '0/210'
            )
            FROM claimed
            "#,
        )
        .expect("top-level stream abort should finalize");
        assert_eq!(
            Spi::get_one::<String>(
                "SELECT status
                   FROM shiba_internal.ingress_transactions
                  WHERE transaction_start_lsn = '0/200'"
            )
            .expect("aborted transaction should be readable")
            .as_deref(),
            Some("aborted")
        );
        assert_eq!(
            Spi::get_one::<String>(
                "SELECT coalesce(persisted_lsn::text, 'none')
                   FROM shiba_internal.ingress_replay_state
                  WHERE slot_generation = 1"
            )
            .expect("abort feedback position should be readable")
            .as_deref(),
            Some("0/140")
        );
    }

    #[pg_test]
    fn ctas_statement_offsets_register_and_drop_all_metadata() {
        install_test_ingress_generation();
        Spi::run(
            r#"
            UPDATE shiba_internal.runtime_state SET active=true WHERE singleton;
            CREATE TABLE tests.pg_boundary_source (
                group_id integer NOT NULL,
                amount integer NOT NULL
            );
            INSERT INTO tests.pg_boundary_source VALUES (1, 10), (1, 20);
            CREATE TABLE SHIBA.pg_boundary_result AS
              SELECT group_id, amount
              FROM tests.pg_boundary_source
              WHERE amount > 0;
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
              FROM shiba_internal.dataflows dataflow
              WHERE dataflow.result_oid='shiba.pg_boundary_result'::regclass
                AND dataflow.active
                AND jsonb_array_length(dataflow.plan->'stages')=4
                AND (
                  SELECT count(*)
                  FROM shiba_internal.operator_checkpoints checkpoint
                  WHERE checkpoint.result_oid=dataflow.result_oid
                )=4
                AND (
                  SELECT count(*)
                  FROM shiba_internal.dataflow_sources source
                  WHERE source.result_oid=dataflow.result_oid
                )=1
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
               SELECT 1 FROM shiba_internal.dataflows
               WHERE result_oid={result_oid}::oid
             )
             AND NOT EXISTS (
               SELECT 1 FROM shiba_internal.dataflow_sources
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

    #[pg_test]
    fn toastable_columns_are_valid_stream_sources() {
        install_test_ingress_generation();
        Spi::run(
            r#"
            UPDATE shiba_internal.runtime_state SET active=true WHERE singleton;
            CREATE TABLE tests.pg_toastable_source (
                group_id integer NOT NULL,
                amount integer NOT NULL,
                description text
            );
            CREATE TABLE shiba.pg_boundary_result AS
              SELECT group_id, amount, description
              FROM tests.pg_toastable_source
            "#,
        )
        .expect("TOASTable built-in source types should register");
    }

    #[pg_test(
        error = "the shiba schema only accepts CREATE TABLE shiba.name AS SELECT ... dataflow declarations"
    )]
    fn quoted_schema_name_cannot_bypass_reserved_schema() {
        Spi::run(r#"CREATE TABLE "shiba".pg_boundary_plain_table (id integer)"#)
            .expect("the expected PostgreSQL error is handled by the pg_test harness");
    }

    #[pg_test(
        error = "the shiba schema only accepts CREATE TABLE shiba.name AS SELECT ... dataflow declarations"
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
