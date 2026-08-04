use postgres::Client;
use shiba_ingress::{BootstrapSession, SnapshotProgress};
use shiba_operator::TypedValue;
use shiba_protocol::{BootstrapBatchId, SourceId};
use shiba_runtime::{
    BootstrapBatch, BootstrapProcessOutcome, SnapshotRow, process_bootstrap_batch,
};

use crate::support::{self, Attempt};

#[allow(
    clippy::too_many_lines,
    reason = "ordered first/middle/last scan crash windows"
)]
pub(crate) fn prove_snapshot_restarts(
    database_url: &str,
    replication_url: &str,
    admin: &mut Client,
    initial: BootstrapSession,
    publication: u32,
) -> Attempt {
    drop(initial); // exported snapshot is now irrecoverably lost
    support::assert_building(admin);
    let mut failed = Attempt {
        bootstrap: 3,
        generation: 4,
        slot: support::RECOVERY_SLOTS[0],
        publication,
    };

    let foreign = Attempt {
        bootstrap: 4,
        generation: 5,
        slot: support::RECOVERY_SLOTS[1],
        publication,
    };
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_physical_replication_slot($1)",
            &[&foreign.slot],
        )
        .expect("preoccupy recovery slot with foreign shape");
    let before = support::evidence(admin);
    assert!(
        BootstrapSession::restart_abandoned(
            database_url,
            replication_url,
            &failed.spec(),
            foreign.spec(),
            support::options()
        )
        .is_err()
    );
    assert_eq!(support::evidence(admin), before);
    admin
        .execute("SELECT pg_drop_replication_slot($1)", &[&foreign.slot])
        .expect("drop foreign recovery slot");

    let mut bootstrap = BootstrapSession::restart_abandoned(
        database_url,
        replication_url,
        &failed.spec(),
        foreign.spec(),
        support::options(),
    )
    .expect("fresh attempt after lost initial exporter");
    assert_batch(&mut bootstrap, 1);
    drop(bootstrap); // crash after first committed batch
    failed = foreign;

    let middle = Attempt {
        bootstrap: 5,
        generation: 6,
        slot: support::RECOVERY_SLOTS[2],
        publication,
    };
    let mut bootstrap = BootstrapSession::restart_abandoned(
        database_url,
        replication_url,
        &failed.spec(),
        middle.spec(),
        support::options(),
    )
    .expect("restart after first-batch crash");
    assert_reset(admin);
    assert_batch(&mut bootstrap, 1);
    assert_batch(&mut bootstrap, 2);
    drop(bootstrap); // crash after middle committed batch
    failed = middle;

    let last = Attempt {
        bootstrap: 6,
        generation: 7,
        slot: support::RECOVERY_SLOTS[3],
        publication,
    };
    let mut bootstrap = BootstrapSession::restart_abandoned(
        database_url,
        replication_url,
        &failed.spec(),
        last.spec(),
        support::options(),
    )
    .expect("restart after middle-batch crash");
    assert_reset(admin);
    assert_batch(&mut bootstrap, 1);
    assert_batch(&mut bootstrap, 2);
    assert_batch(&mut bootstrap, 3);
    drop(bootstrap); // crash after last batch, before scan_complete

    let final_attempt = Attempt {
        bootstrap: 7,
        generation: 8,
        slot: "shiba_m12_recovery_final",
        publication,
    };
    let mut bootstrap = BootstrapSession::restart_abandoned(
        database_url,
        replication_url,
        &last.spec(),
        final_attempt.spec(),
        support::options(),
    )
    .expect("restart after last-batch crash");
    assert_reset(admin);
    admin
        .batch_execute(
            "BEGIN;
         UPDATE target.events SET payload = 110 WHERE id = 10;
         DELETE FROM target.events WHERE id = 12;
         INSERT INTO target.events VALUES (16, 8);
         COMMIT;",
        )
        .expect("commit final-attempt catch-up WAL");

    admin
        .execute(
            "INSERT INTO shiba_internal.graph_node_state (
                 graph_id,node_id,namespace,partition_key_payload,
                 item_key_payload,codec_version,state_payload
             ) VALUES (1,2,0,$1,$2,1,$3)",
            &[
                &TypedValue::Bool(true)
                    .to_canonical_json()
                    .expect("canonical scalar partition"),
                &b"null".as_slice(),
                &b"\0".as_slice(),
            ],
        )
        .expect("inject first-batch corrupt operator state");
    assert!(bootstrap.scan_next().is_err());
    admin
        .execute(
            "UPDATE shiba_internal.graph_node_state SET state_payload = $1
             WHERE graph_id = 1 AND node_id = 2",
            &[&0_i64.to_be_bytes().as_slice()],
        )
        .expect("restore failed batch state");
    assert_batch(&mut bootstrap, 1);
    let exact = BootstrapBatch::new(
        shiba_protocol::GraphId::new(1).unwrap(),
        SourceId::new(1).unwrap(),
        BootstrapBatchId::new(shiba_protocol::BootstrapId::new(7).unwrap(), 1).unwrap(),
        vec![
            SnapshotRow {
                source_row_id: 10,
                payload: Some(100),
            },
            SnapshotRow {
                source_row_id: 11,
                payload: None,
            },
        ],
    )
    .expect("exact first snapshot batch");
    assert_eq!(
        process_bootstrap_batch(admin, &exact).expect("exact batch retry"),
        BootstrapProcessOutcome::AlreadyApplied
    );
    assert_batch(&mut bootstrap, 2);
    assert_batch(&mut bootstrap, 3);
    assert_eq!(
        bootstrap.scan_next().expect("commit scan_complete"),
        SnapshotProgress::ScanComplete
    );
    drop(bootstrap);
    final_attempt
}

fn assert_batch(bootstrap: &mut BootstrapSession, ordinal: u64) {
    assert!(matches!(
        bootstrap.scan_next().expect("apply bounded snapshot batch"),
        SnapshotProgress::BatchApplied { ordinal: value, rows: 2 } if value == ordinal
    ));
}

fn assert_reset(client: &mut Client) {
    support::assert_building(client);
    let row = client
        .query_one(
            "SELECT checkpoint.last_batch_ordinal,
                (SELECT count(*) FROM shiba_internal.source_row_state WHERE source_id=1),
                (SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id=1)
         FROM shiba_internal.graph_bootstrap AS bootstrap
         JOIN shiba_internal.graph_bootstrap_checkpoint AS checkpoint USING (graph_id)
         WHERE bootstrap.graph_id=1 AND checkpoint.source_id=1",
            &[],
        )
        .expect("prove fresh attempt reset");
    assert_eq!(
        (
            row.get::<_, i64>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2)
        ),
        (0, 0, 0)
    );
}
