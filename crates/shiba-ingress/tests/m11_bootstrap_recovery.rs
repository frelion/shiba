use std::time::Duration;

use postgres::{Client, NoTls};
use shiba_ingress::{BootstrapOptions, BootstrapSession, BootstrapSpec, SnapshotProgress};
use shiba_protocol::{BootstrapBatchId, BootstrapId, GraphId, SlotGeneration, SourceId};
use shiba_runtime::{
    BootstrapBatch, BootstrapProcessOutcome, SnapshotRow, process_bootstrap_batch,
};

#[allow(dead_code)]
#[path = "support/mod.rs"]
mod pg_support;
#[path = "m11_bootstrap_recovery/catchup.rs"]
mod recovery_catchup;
#[path = "m11_bootstrap_recovery/support.rs"]
mod recovery_support;

use recovery_support::*;

#[test]
#[ignore = "requires scripts/test-m11-recovery.sh"]
#[allow(clippy::too_many_lines, reason = "ordered pre-catch-up crash proof")]
fn bootstrap_batches_workers_restart_feedback_and_cutover_recover() {
    let database_url = required("SHIBA_M11_RECOVERY_DATABASE_URL");
    let replication_url = required("SHIBA_M11_RECOVERY_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect test database");
    let publication_oid = install_source(&mut admin);
    let source_id = SourceId::new(1).expect("source ID");
    let graph_id = GraphId::new(1).expect("graph ID");
    let options = BootstrapOptions::new(2, Duration::from_secs(5)).expect("bootstrap options");

    let failed_spec = BootstrapSpec {
        graph_id,
        bootstrap_id: BootstrapId::new(10).expect("failed bootstrap ID"),
        publication_oid,
        slot_name: FAILED_SLOT.to_owned(),
        slot_generation: SlotGeneration::new(1).expect("failed generation"),
    };
    admin
        .query_one(
            "SELECT shiba_internal.reserve_graph_bootstrap(
                 $1, $2, $3::bigint::oid, $4::text::name, $5
             )",
            &[
                &i64::try_from(failed_spec.bootstrap_id.get()).expect("bootstrap bigint"),
                &i64::try_from(graph_id.get()).expect("graph bigint"),
                &i64::from(publication_oid),
                &FAILED_SLOT,
                &i64::try_from(failed_spec.slot_generation.get()).expect("generation bigint"),
            ],
        )
        .expect("persist exact crash-after-reservation window");
    assert_eq!(checkpoint(&mut admin), ("creating".to_owned(), 0, None));
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&FAILED_SLOT],
            )
            .expect("reservation must not imply physical slot")
            .get::<_, i64>(0),
        0
    );

    let abandoned_spec = BootstrapSpec {
        graph_id,
        bootstrap_id: BootstrapId::new(11).expect("abandoned bootstrap ID"),
        publication_oid,
        slot_name: OLD_SLOT.to_owned(),
        slot_generation: SlotGeneration::new(2).expect("abandoned generation"),
    };
    let mut bootstrap = BootstrapSession::restart_abandoned(
        &database_url,
        &replication_url,
        &failed_spec,
        abandoned_spec.clone(),
        options,
    )
    .expect("replace cleanup-pending attempt");
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 0, None));
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&FAILED_SLOT],
            )
            .expect("verify failed slot removal")
            .get::<_, i64>(0),
        0
    );

    let Err(competing) = BootstrapSession::begin(
        &database_url,
        &replication_url,
        abandoned_spec.clone(),
        options,
    ) else {
        panic!("a second worker must not own the same source");
    };
    assert!(
        competing
            .to_string()
            .contains("graph already has an active session"),
        "unexpected competing-worker failure: {competing}"
    );

    assert_eq!(
        bootstrap.scan_next().expect("apply first batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 1,
            rows: 2
        }
    );
    let exact = BootstrapBatch::new(
        graph_id,
        source_id,
        BootstrapBatchId::new(abandoned_spec.bootstrap_id, 1).expect("batch ID"),
        vec![
            SnapshotRow {
                source_row_id: 1,
                payload: Some(10),
            },
            SnapshotRow {
                source_row_id: 2,
                payload: Some(20),
            },
        ],
    )
    .expect("exact replay batch");
    assert_eq!(
        process_bootstrap_batch(&mut admin, &exact).expect("replay exact batch"),
        BootstrapProcessOutcome::AlreadyApplied
    );
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 1, Some(2)));
    assert_eq!(states(&mut admin), vec![(1, 2), (2, 30)]);

    drop(bootstrap);
    let bootstrap_id = BootstrapId::new(12).expect("replacement bootstrap ID");
    let generation = SlotGeneration::new(3).expect("replacement generation");
    let stale_replacement = BootstrapSpec {
        graph_id,
        bootstrap_id,
        publication_oid,
        slot_name: SLOT.to_owned(),
        slot_generation: abandoned_spec.slot_generation,
    };
    assert!(
        BootstrapSession::restart_abandoned(
            &database_url,
            &replication_url,
            &abandoned_spec,
            stale_replacement,
            options,
        )
        .is_err(),
        "replacement generation must strictly advance"
    );
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&FOREIGN_SLOT],
        )
        .expect("create foreign replacement slot");
    let foreign_replacement = BootstrapSpec {
        graph_id,
        bootstrap_id,
        publication_oid,
        slot_name: FOREIGN_SLOT.to_owned(),
        slot_generation: generation,
    };
    assert!(
        BootstrapSession::restart_abandoned(
            &database_url,
            &replication_url,
            &abandoned_spec,
            foreign_replacement,
            options,
        )
        .is_err(),
        "an existing replacement slot must never be adopted"
    );
    admin
        .query_one("SELECT pg_drop_replication_slot($1)", &[&FOREIGN_SLOT])
        .expect("drop test-owned foreign slot");
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 1, Some(2)));
    assert_eq!(states(&mut admin), vec![(1, 2), (2, 30)]);
    assert_eq!(rows(&mut admin), vec![(1, Some(10)), (2, Some(20))]);

    let spec = BootstrapSpec {
        graph_id,
        bootstrap_id,
        publication_oid,
        slot_name: SLOT.to_owned(),
        slot_generation: generation,
    };
    let mut bootstrap = BootstrapSession::restart_abandoned(
        &database_url,
        &replication_url,
        &abandoned_spec,
        spec,
        options,
    )
    .expect("restart abandoned partial scan");
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 0, None));
    assert!(states(&mut admin).is_empty());
    assert!(rows(&mut admin).is_empty());
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba.graph_result
                 WHERE result_status = 'building'",
                &[],
            )
            .expect("replacement results remain unavailable")
            .get::<_, i64>(0),
        2
    );
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&OLD_SLOT],
            )
            .expect("verify abandoned slot removal")
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        bootstrap
            .scan_next()
            .expect("apply replacement first batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 1,
            rows: 2
        }
    );

    admin
        .execute(
            "UPDATE shiba_internal.graph_node_state SET state_payload = $1
             WHERE graph_id = 1 AND node_id = 2 AND namespace = 1
               AND partition_key_payload = $2 AND item_key_payload = $3",
            &[
                &[2_i64.to_be_bytes(), (i64::MAX - 5).to_be_bytes()]
                    .concat()
                    .as_slice(),
                &scalar_state_partition(),
                &scalar_state_item(),
            ],
        )
        .expect("inject bounded operator overflow");
    let overflowing = BootstrapBatch::new(
        graph_id,
        source_id,
        BootstrapBatchId::new(bootstrap_id, 2).expect("batch ID"),
        vec![SnapshotRow {
            source_row_id: 3,
            payload: Some(10),
        }],
    )
    .expect("overflow batch");
    assert!(process_bootstrap_batch(&mut admin, &overflowing).is_err());
    assert_eq!(rows(&mut admin), vec![(1, Some(10)), (2, Some(20))]);
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 1, Some(2)));
    assert_eq!(states(&mut admin), vec![(1, 2), (2, i64::MAX - 5)]);
    admin
        .execute(
            "UPDATE shiba_internal.graph_node_state SET state_payload = $1
             WHERE graph_id = 1 AND node_id = 2 AND namespace = 1
               AND partition_key_payload = $2 AND item_key_payload = $3",
            &[
                &[2_i64.to_be_bytes(), 30_i64.to_be_bytes()]
                    .concat()
                    .as_slice(),
                &scalar_state_partition(),
                &scalar_state_item(),
            ],
        )
        .expect("restore operator state after failure injection");
    assert_eq!(
        bootstrap.scan_next().expect("retry second batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 2,
            rows: 1
        }
    );
    assert_eq!(
        bootstrap.scan_next().expect("finish scan"),
        SnapshotProgress::ScanComplete
    );

    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO source.events VALUES (4, 5);
             UPDATE source.events SET payload = 15 WHERE id = 1;
             COMMIT;",
        )
        .expect("commit catch-up transaction");
    drop(bootstrap);
    drop(admin);
    restart_postgres("immediate");
    recovery_catchup::prove_feedback_and_cutover_recovery(
        &database_url,
        &replication_url,
        graph_id,
        bootstrap_id,
        generation,
        options,
    );
}
