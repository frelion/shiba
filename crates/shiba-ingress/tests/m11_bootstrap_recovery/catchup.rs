use postgres::{Client, NoTls};
use shiba_ingress::{BootstrapCatchupProgress, BootstrapCatchupSession, BootstrapOptions};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};

use crate::recovery_support::{
    SLOT, checkpoint, install_receiver_kill_trigger, remove_receiver_kill_trigger,
    restart_postgres, rows, slot_lsn, states,
};

#[allow(
    clippy::too_many_lines,
    reason = "ordered Apply/feedback/cutover crash proof"
)]
pub(crate) fn prove_feedback_and_cutover_recovery(
    database_url: &str,
    replication_url: &str,
    graph_id: GraphId,
    bootstrap_id: BootstrapId,
    generation: SlotGeneration,
    options: BootstrapOptions,
) {
    let mut admin = Client::connect(database_url, NoTls).expect("reconnect after restart");
    assert_eq!(checkpoint(&mut admin).0, "scan_complete");
    let mut catchup = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        graph_id,
        bootstrap_id,
        generation,
        options,
    )
    .expect("resume scan-complete attempt after PostgreSQL restart");
    assert_eq!(checkpoint(&mut admin).0, "catching_up");
    install_receiver_kill_trigger(
        &mut admin,
        "shiba_internal.graph_continuation",
        "AFTER INSERT",
    );
    assert!(
        catchup.catch_up_next().is_err(),
        "feedback transport must fail after durable Apply"
    );
    assert_eq!(states(&mut admin), vec![(1, 4), (3, 50)]);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation",
                &[]
            )
            .expect("query continuation after failed feedback")
            .get::<_, i64>(0),
        1
    );
    assert_eq!(checkpoint(&mut admin).0, "catching_up");
    drop(catchup);
    remove_receiver_kill_trigger(&mut admin, "shiba_internal.graph_continuation");

    let mut catchup = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        graph_id,
        bootstrap_id,
        generation,
        options,
    )
    .expect("resume after durable Apply before feedback");
    assert_eq!(
        catchup.catch_up_next().expect("ack exact Apply replay"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(states(&mut admin), vec![(1, 4), (3, 50)]);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation",
                &[]
            )
            .expect("query replay continuation")
            .get::<_, i64>(0),
        1
    );

    install_receiver_kill_trigger(
        &mut admin,
        "shiba_internal.graph_bootstrap",
        "AFTER UPDATE OF phase",
    );
    assert!(
        catchup.catch_up_next().is_err(),
        "feedback transport must fail after durable activation"
    );
    let activation_end: String = admin
        .query_one(
            "SELECT activation_end_lsn::text FROM shiba_internal.graph_bootstrap
             WHERE graph_id = 1 AND phase = 'active'",
            &[],
        )
        .expect("activation committed before feedback failure")
        .get(0);
    assert_eq!(
        admin
            .query(
                "SELECT result_id, result_status, value_bigint
                 FROM shiba.graph_result WHERE graph_id = 1 ORDER BY result_id",
                &[],
            )
            .expect("query active results")
            .into_iter()
            .map(|row| (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, i64>(2)
            ))
            .collect::<Vec<_>>(),
        vec![(2, "active".to_owned(), 4), (4, "active".to_owned(), 50)]
    );
    drop(catchup);
    remove_receiver_kill_trigger(&mut admin, "shiba_internal.graph_bootstrap");

    let mut resumed = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        graph_id,
        bootstrap_id,
        generation,
        options,
    )
    .expect("resume active cutover before feedback");
    assert_eq!(
        resumed
            .catch_up_next()
            .expect("replay and acknowledge fence"),
        BootstrapCatchupProgress::Active
    );
    drop(resumed);
    drop(admin);
    restart_postgres("fast");
    let mut admin = Client::connect(database_url, NoTls).expect("reconnect after active restart");
    let mut resumed = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        graph_id,
        bootstrap_id,
        generation,
        options,
    )
    .expect("resume after feedback covered active cutover");
    assert_eq!(
        resumed.catch_up_next().expect("active restart is a no-op"),
        BootstrapCatchupProgress::Active
    );
    let activation_lsn = {
        let (high, low) = activation_end
            .split_once('/')
            .expect("activation LSN shape");
        (u64::from_str_radix(high, 16).expect("activation high") << 32)
            | u64::from_str_radix(low, 16).expect("activation low")
    };
    assert!(slot_lsn(&mut admin, SLOT) >= activation_lsn);
    assert_eq!(
        rows(&mut admin),
        vec![(1, Some(15)), (2, Some(20)), (3, Some(10)), (4, Some(5))]
    );
    let oracle = admin
        .query_one(
            "SELECT count(*), COALESCE(sum(payload), 0)::bigint FROM source.events",
            &[],
        )
        .expect("query final SQL oracle");
    assert_eq!((oracle.get::<_, i64>(0), oracle.get::<_, i64>(1)), (4, 50));
}
