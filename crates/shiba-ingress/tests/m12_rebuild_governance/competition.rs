use std::{
    sync::{Arc, Barrier},
    thread,
};

use shiba_ingress::{AttachOptions, PreparedRebuild, ReplicationMode};
use shiba_protocol::{GraphId, SlotGeneration};
use shiba_runtime::ProcessOutcome;

use crate::support::{
    RebuildFixture, assert_building, establish_active_source, install_second_active_source, options,
};

pub(crate) fn prove_same_source_exclusion_and_other_source_progress(
    database_url: &str,
    replication_url: &str,
) {
    let (mut admin, active) = establish_active_source(database_url, replication_url);
    let fixture = RebuildFixture::install(&mut admin, active.publication_oid);
    let barrier = Arc::new(Barrier::new(3));
    let workers = (0..2)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let apply = database_url.to_owned();
            let replication = replication_url.to_owned();
            let spec = fixture.spec();
            thread::spawn(move || {
                barrier.wait();
                PreparedRebuild::prepare(&apply, &replication, &spec, options()).is_ok_and(
                    |prepared| {
                        prepared.detach().expect("release winning rebuild owner");
                        true
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let winners = workers
        .into_iter()
        .map(|worker| worker.join().expect("rebuild worker did not panic"))
        .filter(|winner| *winner)
        .count();
    assert_eq!(
        winners, 1,
        "one source has exactly one rebuild lifecycle writer"
    );
    assert_building(&mut admin);

    let mut source_two = install_second_active_source(&mut admin, database_url, replication_url);
    admin
        .batch_execute("INSERT INTO source_two.events VALUES (202, 12)")
        .expect("commit source-two live work while source one is building");
    let durable = source_two
        .receive_and_apply_one()
        .expect("Apply independent source work");
    assert_eq!(durable.outcome(), ProcessOutcome::Applied);
    source_two
        .acknowledge(&durable)
        .expect("ACK source-two work");
    let second = admin
        .query_one(
            "SELECT count(*), COALESCE(sum(payload), 0)::bigint FROM source_two.events",
            &[],
        )
        .expect("query source-two SQL oracle");
    let result = admin
        .query(
            "SELECT value_bigint FROM shiba.graph_result
             WHERE graph_id = 2 AND result_id IN (2, 4) ORDER BY result_id",
            &[],
        )
        .expect("read independent public result")
        .into_iter()
        .map(|row| row.get::<_, Option<i64>>(0))
        .collect::<Vec<_>>();
    assert_eq!(result, vec![Some(second.get(0)), Some(second.get(1))]);
    assert!(
        shiba_ingress::GovernedGraphSession::attach(
            database_url,
            replication_url,
            GraphId::new(1).expect("graph ID"),
            SlotGeneration::new(2).expect("retired generation"),
            AttachOptions::new(
                ReplicationMode::Committed,
                std::time::Duration::from_secs(5)
            )
            .expect("bounded attach options"),
        )
        .is_err(),
        "source-one building lifecycle never reopens old live ingress"
    );
    source_two
        .detach()
        .expect("detach independent live ingress");
}
