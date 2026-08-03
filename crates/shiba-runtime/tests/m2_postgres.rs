use postgres::{Client, NoTls};
use shiba_protocol::{
    GraphId, GraphTransactionId, IngressTransactionId, InputSequence, PostgresLsn, SlotGeneration,
    SourceId,
};
use shiba_runtime::{
    GraphSourceChange, GraphTransaction, M2Error, ProcessOutcome, SourceChange, SourceInsert,
    process,
};

mod support;

fn input(lsn: u64, ingress: u64, rows: &[(u64, i64)]) -> GraphTransaction {
    let identity = GraphTransactionId::new(
        GraphId::new(1).expect("non-zero graph"),
        SlotGeneration::new(1).expect("non-zero generation"),
        PostgresLsn::from_u64(lsn),
        IngressTransactionId::new(ingress).expect("non-zero ingress transaction"),
    )
    .expect("non-zero commit LSN");
    let changes = rows
        .iter()
        .map(|&(sequence, row_id)| GraphSourceChange {
            source_id: SourceId::new(1).expect("non-zero source"),
            change: SourceChange::Insert(SourceInsert::new(
                InputSequence::new(sequence).expect("non-zero input sequence"),
                row_id,
            )),
        })
        .collect();
    GraphTransaction::new(identity, changes).expect("valid M2 test input")
}

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.graph_result WHERE graph_id = 1 AND result_id = 2),
                (SELECT state_payload FROM shiba_internal.graph_node_state
                 WHERE graph_id = 1 AND node_id = 1 AND namespace = 0),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.graph_continuation)",
            &[],
        )
        .expect("query durable M2 state");
    (
        row.get(0),
        support::decode_optional_scalar_state(row.get::<_, Option<Vec<u8>>>(1).as_deref()),
        row.get(2),
        row.get(3),
    )
}

fn prove_operator_rollback(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m2_test;
             CREATE FUNCTION m2_test.fail_operator() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 RAISE EXCEPTION 'injected operator failure';
             END
             $$;
             CREATE TRIGGER m2_fail_operator
             BEFORE UPDATE ON shiba_internal.graph_node_state
             FOR EACH ROW EXECUTE FUNCTION m2_test.fail_operator();",
        )
        .expect("install test-only operator failure trigger");
    let second = input(0x65, 9, &[(1, 103)]);
    assert!(matches!(
        process(client, &second),
        Err(M2Error::Postgres(_))
    ));
    assert_eq!(durable_state(client), (2, 2, 2, 1));
    client
        .batch_execute("DROP TRIGGER m2_fail_operator ON shiba_internal.graph_node_state")
        .expect("remove operator failure trigger");
    assert_eq!(
        process(client, &second).expect("retry rolled-back transaction"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(client), (3, 3, 3, 2));
}

fn prove_crash_rollback(connection: &str, mut client: Client) -> Client {
    client
        .batch_execute(
            "CREATE FUNCTION m2_test.crash_after_continuation() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m2_crash_after_continuation
             AFTER INSERT ON shiba_internal.graph_continuation
             FOR EACH ROW EXECUTE FUNCTION m2_test.crash_after_continuation();",
        )
        .expect("install test-only crash trigger");
    let third = input(0x66, 10, &[(1, 104)]);
    assert!(matches!(
        process(&mut client, &third),
        Err(M2Error::Postgres(_))
    ));

    let mut client = Client::connect(connection, NoTls).expect("reconnect after backend crash");
    assert_eq!(durable_state(&mut client), (3, 3, 3, 2));
    client
        .batch_execute("DROP SCHEMA m2_test CASCADE")
        .expect("remove test-only crash objects");
    assert_eq!(
        process(&mut client, &third).expect("retry transaction lost to backend crash"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (4, 4, 4, 3));
    client
}

fn prove_permissions(client: &mut Client) {
    client
        .batch_execute("CREATE ROLE shiba_m2_reader NOLOGIN; SET ROLE shiba_m2_reader")
        .expect("assume ordinary role");
    let visible_count: i64 = client
        .query_one(
            "SELECT value_bigint FROM shiba.graph_result WHERE graph_id = 1 AND result_id = 2",
            &[],
        )
        .expect("ordinary role can query result")
        .get(0);
    assert_eq!(visible_count, 4);
    assert!(
        client
            .execute(
                "UPDATE shiba.graph_result SET value_bigint = 0 WHERE graph_id = 1 AND result_id = 2",
                &[],
            )
            .is_err()
    );
    assert!(
        client
            .query("SELECT * FROM shiba_internal.graph_continuation", &[])
            .is_err()
    );
    client
        .batch_execute("RESET ROLE")
        .expect("restore test owner");
    assert_eq!(durable_state(client), (4, 4, 4, 3));
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster owned by scripts/test-m2.sh"]
fn m2_transaction_replay_failure_crash_and_permissions() {
    let connection = std::env::var("SHIBA_M2_DATABASE_URL")
        .expect("scripts/test-m2.sh must provide SHIBA_M2_DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute("CREATE EXTENSION shiba_catalog")
        .expect("install packaged catalog and M2 SQL");

    client
        .batch_execute(
            "CREATE SCHEMA source_a;
             CREATE TABLE source_a.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m2_pub FOR TABLE source_a.events;
             BEGIN;
             INSERT INTO source_a.events (id) VALUES (101), (102);
             COMMIT;",
        )
        .expect("commit source fixture in its own schema");
    let source_count: i64 = client
        .query_one("SELECT count(*) FROM source_a.events", &[])
        .expect("query committed source fixture")
        .get(0);
    assert_eq!(source_count, 2);
    client
        .query_one(
            "SELECT shiba_internal.register_source(1, 'source_a.events'::regclass)",
            &[],
        )
        .expect("register M2 source relation");
    support::register_count_operator(&mut client, 1, 1);
    client
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot(
                'shiba_m2_slot', 'pgoutput')",
            &[],
        )
        .expect("create exact M2 graph slot");
    support::configure_graph_ingress(&mut client, 1, "shiba_m2_pub", "shiba_m2_slot");

    let first = input(0x64, 7, &[(1, 101), (2, 102)]);
    assert_eq!(
        process(&mut client, &first).expect("apply committed source transaction"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    assert_eq!(
        process(&mut client, &first).expect("replay exact transaction"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    let conflicting = input(0x64, 8, &[(1, 999)]);
    assert!(matches!(
        process(&mut client, &conflicting),
        Err(M2Error::IdentityConflict)
    ));
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    prove_operator_rollback(&mut client);
    let mut client = prove_crash_rollback(&connection, client);
    prove_permissions(&mut client);
}
