use std::{num::NonZeroU64, sync::mpsc, thread, time::Duration};

use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_ingress::SourceReceiver;
use shiba_operator::OperatorId;
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, compile_and_register};

const SLOT: &str = "shiba_m10_committed_slot";
const PUBLICATION: &str = "shiba_m10_committed_pub";

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m10-committed-ingress.sh must set {name}"))
}

fn spec(operator_id: u64, operation: OperatorOperationV1) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(operator_id).expect("non-zero operator")),
        source_id: SourceId::new(1).expect("non-zero source"),
        operation,
    }
}

#[test]
#[ignore = "requires scripts/test-m10-committed-ingress.sh"]
fn production_copy_both_drives_count_and_sum() {
    let database_url = required("SHIBA_M10_DATABASE_URL");
    let replication_url = required("SHIBA_M10_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect admin/apply database");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events;
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);"
        ))
        .expect("install source and binding");
    compile_and_register(&mut admin, &spec(1, OperatorOperationV1::CountRows))
        .expect("register CountRows");
    compile_and_register(
        &mut admin,
        &spec(
            2,
            OperatorOperationV1::SumInt8 {
                input_column: "payload".to_owned(),
            },
        ),
    )
    .expect("register SumInt8");
    let relation_oid = admin
        .query_one("SELECT 'source.events'::regclass::oid::bigint", &[])
        .expect("read relation OID")
        .get::<_, i64>(0);
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .expect("create test-owned slot");

    let source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("source ID"),
        SlotGeneration::new(1).expect("slot generation"),
        u32::try_from(relation_oid).expect("OID fits u32"),
    );
    let (ready_tx, ready_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let receiver_thread = thread::spawn(move || {
        let mut receiver = SourceReceiver::connect(&replication_url, SLOT, PUBLICATION, 0)
            .expect("connect production replication receiver");
        ready_tx.send(()).expect("signal receiver ready");
        let mut apply =
            Client::connect(&database_url, NoTls).expect("connect separate Apply client");
        result_tx
            .send(receiver.receive_and_apply_one(&mut apply, source))
            .expect("return receiver result");
    });
    ready_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("receiver enters COPY BOTH");

    admin
        .batch_execute("INSERT INTO source.events VALUES (1, 10), (2, NULL)")
        .expect("commit source transaction");
    let applied = result_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("receiver applies committed transaction")
        .expect("production ingress succeeds");
    receiver_thread.join().expect("receiver thread exits");
    assert_eq!(applied.outcome, ProcessOutcome::Applied);
    assert!(applied.end_lsn > 0);

    let values = admin
        .query(
            "SELECT operator_id, value_bigint FROM shiba.operator_result ORDER BY operator_id",
            &[],
        )
        .expect("query public operator results")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, i64>(1)))
        .collect::<Vec<_>>();
    assert_eq!(values, vec![(1, 2), (2, 10)]);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_continuation",
                &[]
            )
            .expect("query continuation")
            .get::<_, i64>(0),
        1
    );
}
