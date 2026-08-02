use std::{num::NonZeroU64, sync::mpsc, thread, time::Duration};

use libpq::{Connection, Status};
use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_ingress::{AttachOptions, GovernedSourceSession, ReplicationMode, StreamedInput};
use shiba_operator::OperatorId;
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{ProcessOutcome, compile_and_register};

#[allow(dead_code)]
mod support;

use support::{slot_lsn, wait_for_slot_lsn};

const SLOT: &str = "shiba_m10_governed_slot";
const PUBLICATION: &str = "shiba_m10_governed_pub";
const REPLICATION_APPLICATION: &str = "shiba_m10_governed_receiver";
const APPLY_ROLE: &str = "shiba_m10_apply";
const RECEIVER_ROLE: &str = "shiba_m10_receiver_role";

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m10-governed-ingress.sh must set {name}"))
}

fn as_role(conninfo: &str, role: &str) -> String {
    format!("{conninfo} user={role}")
}

fn options() -> AttachOptions {
    AttachOptions::new(ReplicationMode::Streamed, Duration::from_secs(5))
        .expect("governed attach options")
}

fn attach(
    apply: &str,
    replication: &str,
) -> Result<GovernedSourceSession, shiba_ingress::IngressError> {
    GovernedSourceSession::attach(
        apply,
        replication,
        SourceId::new(1).expect("source ID"),
        SlotGeneration::new(1).expect("generation"),
        options(),
    )
}

fn activate_slot(conninfo: &str) -> Connection {
    let connection = Connection::new(conninfo).expect("connect raw active-slot fixture");
    let result = connection.exec(&format!(
        "START_REPLICATION SLOT \"{SLOT}\" LOGICAL 0/0
         (proto_version '2', streaming 'on', publication_names '{PUBLICATION}')"
    ));
    assert_eq!(result.status(), Status::CopyBoth);
    connection
}

#[test]
#[ignore = "requires scripts/test-m10-governed-ingress.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one governed ownership and privilege proof"
)]
fn governed_session_uses_split_roles_and_revalidates_before_empty_ack() {
    let database_url = required("SHIBA_M10_GOVERNED_DATABASE_URL");
    let replication_url = required("SHIBA_M10_GOVERNED_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect admin database");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events (id)
                 WITH (publish = 'insert, update, delete, truncate');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);
             CREATE ROLE {APPLY_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
             CREATE ROLE {RECEIVER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION;
             GRANT USAGE ON SCHEMA shiba_internal, shiba, source TO {APPLY_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.source_binding TO {APPLY_ROLE};
             GRANT SELECT ON shiba_internal.source_invalidation,
                 shiba_internal.source_ingress_config,
                 shiba_internal.source_ingress_invalidation,
                 shiba_internal.operator_definition TO {APPLY_ROLE};
             GRANT SELECT, INSERT, UPDATE
                 ON shiba_internal.source_continuation TO {APPLY_ROLE};
             GRANT SELECT, INSERT, UPDATE, DELETE
                 ON shiba_internal.applied_insert TO {APPLY_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.operator_state TO {APPLY_ROLE};
             GRANT USAGE ON SCHEMA shiba TO {APPLY_ROLE};
             GRANT SELECT, UPDATE ON shiba.operator_result TO {APPLY_ROLE};
             GRANT SELECT ON source.events TO {APPLY_ROLE};
             GRANT USAGE ON SCHEMA source TO {RECEIVER_ROLE};
             GRANT SELECT ON source.events TO {RECEIVER_ROLE};"
        ))
        .expect("install governed fixture and split roles");
    compile_and_register(
        &mut admin,
        &OperatorSpecV1 {
            version: OPERATOR_SPEC_VERSION,
            operator_id: OperatorId::new(NonZeroU64::new(1).expect("operator ID")),
            source_id: SourceId::new(1).expect("source ID"),
            operation: OperatorOperationV1::CountRows,
        },
    )
    .expect("register CountRows");
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .expect("create configured slot");
    let publication_oid: u32 = admin
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("read publication OID")
        .get(0);
    admin
        .execute(
            "SELECT shiba_internal.configure_source_ingress(1, $1, $2, 1)",
            &[&publication_oid, &SLOT],
        )
        .expect("configure governed source");

    let apply_url = as_role(&database_url, APPLY_ROLE);
    let receiver_url = as_role(&replication_url, RECEIVER_ROLE);
    assert!(attach(&receiver_url, &receiver_url).is_err());
    assert!(attach(&apply_url, &as_role(&replication_url, APPLY_ROLE)).is_err());
    assert!(
        GovernedSourceSession::attach(
            &apply_url,
            &receiver_url,
            SourceId::new(1).expect("source ID"),
            SlotGeneration::new(2).expect("wrong generation"),
            options(),
        )
        .is_err()
    );

    let active = activate_slot(&receiver_url);
    assert!(attach(&apply_url, &receiver_url).is_err());
    drop(active);
    let session = attach(&apply_url, &receiver_url).expect("attach split-role session");
    assert!(
        attach(&apply_url, &receiver_url).is_err(),
        "advisory ownership is exclusive"
    );
    let apply_connections: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_stat_activity
             WHERE application_name = 'shiba-governed-apply' AND usename = $1",
            &[&APPLY_ROLE],
        )
        .expect("count governed Apply connection")
        .get(0);
    let replication_connections: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_stat_replication
             WHERE application_name = $1 AND usename = $2",
            &[&REPLICATION_APPLICATION, &RECEIVER_ROLE],
        )
        .expect("count governed replication connection")
        .get(0);
    assert_eq!((apply_connections, replication_connections), (1, 1));
    session.detach().expect("detach releases ownership");

    let mut session = attach(&apply_url, &receiver_url).expect("reattach after detach");
    let initial_lsn = slot_lsn(&mut admin, SLOT);
    let (input_tx, input_rx) = mpsc::channel();
    let receive_thread = thread::spawn(move || {
        let input = session.receive_streamed_one();
        input_tx
            .send((session, input))
            .expect("return source terminal");
    });
    admin
        .batch_execute(
            "INSERT INTO source.events
             SELECT generate_series(1::bigint, 10000::bigint)",
        )
        .expect("commit least-privilege streamed source transaction");
    let (mut session, input) = input_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("receive source terminal");
    receive_thread.join().expect("join source receive");
    let StreamedInput::Transaction(input) = input.expect("decode source terminal") else {
        panic!("source transaction returned non-transaction terminal");
    };
    let applied = session
        .apply_received(&input)
        .expect("least-privilege Apply");
    assert_eq!(applied.outcome(), ProcessOutcome::Applied);
    session.acknowledge(&applied).expect("least-privilege ACK");
    wait_for_slot_lsn(&mut admin, SLOT, applied.end_lsn());
    assert!(applied.end_lsn() > initial_lsn);

    let durable_lsn = applied.end_lsn();
    let StreamedInput::EmptyCommitted(empty) = session
        .receive_streamed_one()
        .expect("receive Runtime Apply empty commit")
    else {
        panic!("Runtime Apply returned non-empty terminal");
    };
    admin
        .batch_execute(&format!(
            "ALTER PUBLICATION {PUBLICATION} DROP TABLE source.events;
             ALTER PUBLICATION {PUBLICATION} ADD TABLE source.events (id);"
        ))
        .expect("persist publication invalidation then re-add membership");
    assert!(session.acknowledge_empty(&empty).is_err());
    assert_eq!(slot_lsn(&mut admin, SLOT), durable_lsn);
    assert_eq!(
        admin
            .query_one(
                "SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1",
                &[]
            )
            .expect("query unchanged CountRows")
            .get::<_, i64>(0),
        10_000
    );
}
