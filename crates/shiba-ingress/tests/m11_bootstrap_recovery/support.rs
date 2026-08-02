use std::{num::NonZeroU64, process::Command};

use postgres::Client;
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_operator::OperatorId;
use shiba_protocol::SourceId;
use shiba_runtime::compile_and_register;

pub(crate) use crate::pg_support::slot_lsn;

pub(crate) const SLOT: &str = "shiba_m11_recovery_slot";
pub(crate) const OLD_SLOT: &str = "shiba_m11_abandoned_slot";
pub(crate) const FAILED_SLOT: &str = "shiba_m11_failed_create_slot";
pub(crate) const FOREIGN_SLOT: &str = "shiba_m11_foreign_slot";
pub(crate) const PUBLICATION: &str = "shiba_m11_recovery_pub";
const APPLICATION: &str = "shiba_m11_recovery_receiver";

pub(crate) fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("scripts/test-m11-recovery.sh must set {name}"))
}

pub(crate) fn restart_postgres(mode: &str) {
    let pg_ctl = required("SHIBA_TEST_PG_CTL");
    let data = required("SHIBA_TEST_PG_DATA");
    let socket = required("SHIBA_TEST_PG_SOCKET");
    let port = required("SHIBA_TEST_PG_PORT");
    let stopped = Command::new(&pg_ctl)
        .args(["-D", &data, "-m", mode, "-w", "stop"])
        .status()
        .expect("execute pg_ctl stop");
    assert!(stopped.success(), "pg_ctl immediate stop failed");
    let started = Command::new(pg_ctl)
        .args(["-D", &data, "-o"])
        .arg(format!("-k {socket} -p {port}"))
        .args(["-w", "start"])
        .status()
        .expect("execute pg_ctl start");
    assert!(started.success(), "pg_ctl restart failed");
}

pub(crate) fn operator_spec(operator_id: u64, operation: OperatorOperationV1) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(operator_id).expect("operator ID")),
        source_id: SourceId::new(1).expect("source ID"),
        operation,
    }
}

pub(crate) fn install_source(client: &mut Client) -> u32 {
    client
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events
               WITH (publish = 'insert, update, delete');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);
             INSERT INTO source.events VALUES (1, 10), (2, 20), (3, 10);"
        ))
        .expect("install recovery source");
    compile_and_register(client, &operator_spec(1, OperatorOperationV1::CountRows))
        .expect("register CountRows");
    compile_and_register(
        client,
        &operator_spec(
            2,
            OperatorOperationV1::SumInt8 {
                input_column: "payload".to_owned(),
            },
        ),
    )
    .expect("register SumInt8");
    client
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("query publication OID")
        .get(0)
}

pub(crate) fn states(client: &mut Client) -> Vec<(i64, i64)> {
    client
        .query(
            "SELECT operator_id, value_bigint
             FROM shiba_internal.operator_state ORDER BY operator_id",
            &[],
        )
        .expect("query operator states")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

pub(crate) fn rows(client: &mut Client) -> Vec<(i64, Option<i64>)> {
    client
        .query(
            "SELECT source_row_id, payload_int8
             FROM shiba_internal.source_row_state ORDER BY source_row_id",
            &[],
        )
        .expect("query source state")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

pub(crate) fn checkpoint(client: &mut Client) -> (String, i64, Option<i64>) {
    let row = client
        .query_one(
            "SELECT phase, last_batch_ordinal, last_source_row_id
             FROM shiba_internal.source_bootstrap WHERE source_id = 1",
            &[],
        )
        .expect("query bootstrap checkpoint");
    (row.get(0), row.get(1), row.get(2))
}

pub(crate) fn install_receiver_kill_trigger(client: &mut Client, target: &str, event: &str) {
    client
        .batch_execute(&format!(
            "CREATE FUNCTION public.kill_m11_receiver() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
               PERFORM pg_catalog.pg_terminate_backend(pid)
               FROM pg_catalog.pg_stat_replication
               WHERE application_name = '{APPLICATION}';
               RETURN NEW;
             END
             $body$;
             CREATE TRIGGER kill_m11_receiver {event} ON {target}
             FOR EACH ROW EXECUTE FUNCTION public.kill_m11_receiver();"
        ))
        .expect("install receiver failure injection");
}

pub(crate) fn remove_receiver_kill_trigger(client: &mut Client, target: &str) {
    client
        .batch_execute(&format!(
            "DROP TRIGGER kill_m11_receiver ON {target};
             DROP FUNCTION public.kill_m11_receiver();"
        ))
        .expect("remove receiver failure injection");
}
