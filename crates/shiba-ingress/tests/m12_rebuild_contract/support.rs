use std::{num::NonZeroU64, time::Duration};

use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_ingress::{
    BootstrapCatchupProgress, BootstrapOptions, BootstrapSession, BootstrapSpec, SnapshotProgress,
};
use shiba_operator::OperatorId;
use shiba_protocol::{BootstrapId, SlotGeneration, SourceId};
use shiba_runtime::{ProcessOutcome, compile_and_register};

pub(crate) const OLD_SLOT: &str = "shiba_m12_contract_old";
pub(crate) const FOREIGN_SLOT: &str = "shiba_m12_contract_foreign";
pub(crate) const NEW_SLOT: &str = "shiba_m12_contract_new";
pub(crate) const PUBLICATION: &str = "shiba_m12_contract_pub";

pub(crate) fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m12-rebuild-contract.sh must set {name}"))
}

fn operator_spec(operator_id: u64, operation: OperatorOperationV1) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(operator_id).expect("operator ID")),
        source_id: SourceId::new(1).expect("source ID"),
        operation,
    }
}

pub(crate) fn establish_active_source(
    database_url: &str,
    replication_url: &str,
) -> (Client, BootstrapSpec) {
    let mut admin = Client::connect(database_url, NoTls).expect("connect admin database");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events
               WITH (publish = 'insert, update, delete');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);
             INSERT INTO source.events VALUES (1, 10), (2, NULL), (3, 30);"
        ))
        .expect("install nonempty source");
    compile_and_register(
        &mut admin,
        &operator_spec(1, OperatorOperationV1::CountRows),
    )
    .expect("register CountRows");
    compile_and_register(
        &mut admin,
        &operator_spec(
            2,
            OperatorOperationV1::SumInt8 {
                input_column: "payload".to_owned(),
            },
        ),
    )
    .expect("register SumInt8");
    let publication_oid = admin
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("read publication OID")
        .get(0);
    let spec = BootstrapSpec {
        source_id: SourceId::new(1).expect("source ID"),
        bootstrap_id: BootstrapId::new(1).expect("bootstrap ID"),
        publication_oid,
        slot_name: OLD_SLOT.to_owned(),
        slot_generation: SlotGeneration::new(2).expect("slot generation"),
    };
    let options = BootstrapOptions::new(2, Duration::from_secs(5)).expect("bootstrap options");
    let mut bootstrap =
        BootstrapSession::begin(database_url, replication_url, spec.clone(), options)
            .expect("begin real exported-snapshot bootstrap");
    admin
        .batch_execute(
            "BEGIN;
             UPDATE source.events SET payload = 20 WHERE id = 1;
             DELETE FROM source.events WHERE id = 3;
             INSERT INTO source.events VALUES (4, 5);
             COMMIT;",
        )
        .expect("commit snapshot-concurrent WAL");
    loop {
        if bootstrap.scan_next().expect("scan snapshot") == SnapshotProgress::ScanComplete {
            break;
        }
    }
    let mut catchup = bootstrap.into_catchup().expect("enter catch-up");
    assert_eq!(
        catchup.catch_up_next().expect("apply catch-up transaction"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        catchup.catch_up_next().expect("activate bootstrap"),
        BootstrapCatchupProgress::Active
    );
    let mut live = catchup.into_live().expect("enter ordinary live ingress");
    admin
        .batch_execute("INSERT INTO source.events VALUES (5, 7)")
        .expect("commit post-activation WAL");
    let durable = live
        .receive_and_apply_one()
        .expect("apply post-activation transaction");
    assert_eq!(durable.outcome(), ProcessOutcome::Applied);
    live.acknowledge(&durable).expect("ack durable transaction");
    live.detach().expect("leave old slot inactive");
    (admin, spec)
}

pub(crate) fn shiba_authority_snapshot(client: &mut Client) -> Vec<Vec<String>> {
    [
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_binding ORDER BY address_objsubid) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_ingress_config ORDER BY source_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_bootstrap ORDER BY source_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT source_id, source_row_id, source_row_sub_id, payload_int8, payload_text FROM shiba_internal.source_row_state ORDER BY source_row_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.operator_state ORDER BY operator_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba.operator_result ORDER BY operator_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT source_id, slot_generation, commit_lsn::text, ingress_transaction_id FROM shiba_internal.source_continuation ORDER BY slot_generation, commit_lsn) x",
    ]
    .into_iter()
    .map(|query| {
        client
            .query(query, &[])
            .expect("snapshot durable authority")
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    })
    .collect()
}

pub(crate) fn authority_snapshot(client: &mut Client) -> Vec<Vec<String>> {
    let mut snapshot = shiba_authority_snapshot(client);
    snapshot.push(
        client
            .query(
                "SELECT row_to_json(x)::text FROM (
                    SELECT slot_name, slot_type, plugin, database, temporary, active,
                           two_phase, failover, synced, restart_lsn::text,
                           confirmed_flush_lsn::text
                    FROM pg_catalog.pg_replication_slots ORDER BY slot_name
                 ) x",
                &[],
            )
            .expect("snapshot physical slot authority")
            .into_iter()
            .map(|row| row.get(0))
            .collect(),
    );
    snapshot
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ObservableSlotIdentity(String);

fn observable_slot_identity(client: &mut Client, slot: &str) -> ObservableSlotIdentity {
    ObservableSlotIdentity(
        client
            .query_one(
                "SELECT row_to_json(identity)::text FROM (
                SELECT slot_name, plugin, slot_type, datoid, database, temporary,
                       two_phase, failover, synced
                FROM pg_catalog.pg_replication_slots WHERE slot_name = $1
             ) identity",
                &[&slot],
            )
            .expect("read stable observable slot identity")
            .get(0),
    )
}

pub(crate) fn recreate_foreign_slot(
    client: &mut Client,
) -> (ObservableSlotIdentity, ObservableSlotIdentity) {
    let before = observable_slot_identity(client, FOREIGN_SLOT);
    client
        .execute(
            "SELECT pg_catalog.pg_drop_replication_slot($1)",
            &[&FOREIGN_SLOT],
        )
        .expect("drop test-owned foreign slot");
    client
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&FOREIGN_SLOT],
        )
        .expect("recreate same-name test-owned foreign slot");
    let after = observable_slot_identity(client, FOREIGN_SLOT);
    (before, after)
}
