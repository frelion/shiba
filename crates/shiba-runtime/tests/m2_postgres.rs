use postgres::{Client, NoTls};
use shiba_protocol::{
    IngressTransactionId, InputSequence, PostgresLsn, SlotGeneration, SourceId, SourceTransactionId,
};
use shiba_runtime::{M2Error, ProcessOutcome, SourceInsert, SourceTransaction, process};

fn input(lsn: u64, ingress: u64, rows: &[(u64, i64)]) -> SourceTransaction {
    let identity = SourceTransactionId::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        PostgresLsn::from_u64(lsn),
        IngressTransactionId::new(ingress).expect("non-zero ingress transaction"),
    )
    .expect("non-zero commit LSN");
    let inserts = rows
        .iter()
        .map(|&(sequence, row_id)| {
            SourceInsert::new(
                InputSequence::new(sequence).expect("non-zero input sequence"),
                row_id,
            )
        })
        .collect();
    SourceTransaction::new(identity, inserts).expect("valid M2 test input")
}

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT row_count FROM shiba.count_result WHERE singleton = 1),
                (SELECT row_count FROM shiba_internal.count_state WHERE singleton = 1),
                (SELECT count(*) FROM shiba_internal.applied_insert),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable M2 state");
    (row.get(0), row.get(1), row.get(2), row.get(3))
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
             BEFORE UPDATE ON shiba_internal.count_state
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
        .batch_execute("DROP TRIGGER m2_fail_operator ON shiba_internal.count_state")
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
             AFTER INSERT ON shiba_internal.source_continuation
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
        .query_one("SELECT row_count FROM shiba.count_result", &[])
        .expect("ordinary role can query result")
        .get(0);
    assert_eq!(visible_count, 4);
    assert!(
        client
            .execute("UPDATE shiba.count_result SET row_count = 0", &[])
            .is_err()
    );
    assert!(
        client
            .query("SELECT * FROM shiba_internal.source_continuation", &[])
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
