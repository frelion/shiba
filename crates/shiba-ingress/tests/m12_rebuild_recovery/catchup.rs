use postgres::Client;
use shiba_ingress::{BootstrapCatchupProgress, BootstrapCatchupSession};

use crate::{
    pg_support::slot_lsn,
    support::{self, Attempt},
};

#[allow(
    clippy::too_many_lines,
    reason = "ordered Apply/ACK/activation crash windows"
)]
pub(crate) fn prove_catchup_activation_feedback(
    database_url: &str,
    replication_url: &str,
    admin: &mut Client,
    attempt: Attempt,
) {
    admin
        .batch_execute(
            "CREATE FUNCTION public.reject_m12_fence() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'm12 fail before fence commit'; END $$;
         CREATE TRIGGER reject_m12_fence BEFORE UPDATE OF phase
         ON shiba_internal.graph_bootstrap FOR EACH ROW
         WHEN (NEW.phase = 'catching_up') EXECUTE FUNCTION public.reject_m12_fence();",
        )
        .expect("install pre-fence failure");
    assert!(
        BootstrapCatchupSession::resume(
            database_url,
            replication_url,
            shiba_protocol::GraphId::new(1).unwrap(),
            shiba_protocol::BootstrapId::new(attempt.bootstrap).unwrap(),
            shiba_protocol::SlotGeneration::new(attempt.generation).unwrap(),
            support::options(),
        )
        .is_err()
    );
    let fence_state = admin
        .query_one(
            "SELECT phase, catchup_fence_lsn IS NULL
             FROM shiba_internal.graph_bootstrap WHERE graph_id=1",
            &[],
        )
        .expect("query rolled-back fence");
    assert_eq!(fence_state.get::<_, &str>(0), "scan_complete");
    assert!(fence_state.get::<_, bool>(1));
    admin
        .batch_execute(
            "DROP TRIGGER reject_m12_fence ON shiba_internal.graph_bootstrap;
         DROP FUNCTION public.reject_m12_fence();",
        )
        .expect("remove pre-fence failure");

    let catchup = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        shiba_protocol::GraphId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(attempt.bootstrap).unwrap(),
        shiba_protocol::SlotGeneration::new(attempt.generation).unwrap(),
        support::options(),
    )
    .expect("resume scan_complete after PostgreSQL restart");
    drop(catchup); // crash after durable fence/phase commit
    let mut catchup = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        shiba_protocol::GraphId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(attempt.bootstrap).unwrap(),
        shiba_protocol::SlotGeneration::new(attempt.generation).unwrap(),
        support::options(),
    )
    .expect("resume durable catch-up fence");
    support::assert_building(admin);

    admin
        .batch_execute(
            "CREATE FUNCTION public.reject_m12_apply() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'm12 fail before Apply commit'; END $$;
         CREATE TRIGGER reject_m12_apply BEFORE INSERT
         ON shiba_internal.graph_continuation FOR EACH ROW
         EXECUTE FUNCTION public.reject_m12_apply();",
        )
        .expect("install deterministic pre-Apply failure");
    let before_apply = support::evidence(admin);
    assert!(catchup.catch_up_next().is_err());
    assert_eq!(support::evidence(admin), before_apply);
    admin
        .batch_execute(
            "DROP TRIGGER reject_m12_apply ON shiba_internal.graph_continuation;
         DROP FUNCTION public.reject_m12_apply();",
        )
        .expect("remove pre-Apply failure");
    drop(catchup);

    let mut catchup = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        shiba_protocol::GraphId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(attempt.bootstrap).unwrap(),
        shiba_protocol::SlotGeneration::new(attempt.generation).unwrap(),
        support::options(),
    )
    .expect("replay failed Apply from the unacknowledged slot position");

    support::install_receiver_kill_trigger(
        admin,
        "shiba_internal.graph_continuation",
        "AFTER INSERT",
    );
    assert!(
        catchup.catch_up_next().is_err(),
        "Apply commit before ACK must surface failure"
    );
    let applied = support::evidence(admin);
    support::assert_building(admin);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation WHERE slot_generation=$1",
                &[&i64::try_from(attempt.generation).expect("generation bigint")],
            )
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    drop(catchup);
    support::remove_kill_trigger(admin, "shiba_internal.graph_continuation");

    let mut catchup = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        shiba_protocol::GraphId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(attempt.bootstrap).unwrap(),
        shiba_protocol::SlotGeneration::new(attempt.generation).unwrap(),
        support::options(),
    )
    .expect("resume durable Apply before ACK");
    assert_eq!(
        catchup.catch_up_next().expect("ACK exact Apply replay"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(support::evidence(admin)[0..7], applied[0..7]);

    admin
        .batch_execute(
            "CREATE FUNCTION public.reject_m12_activation() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'm12 fail before activation commit'; END $$;
         CREATE TRIGGER reject_m12_activation BEFORE UPDATE OF phase
         ON shiba_internal.graph_bootstrap FOR EACH ROW
         WHEN (NEW.phase = 'active') EXECUTE FUNCTION public.reject_m12_activation();",
        )
        .expect("install pre-activation failure");
    let before_activation = support::evidence(admin);
    assert!(catchup.catch_up_next().is_err());
    assert_eq!(support::evidence(admin), before_activation);
    support::assert_building(admin);
    admin
        .batch_execute(
            "DROP TRIGGER reject_m12_activation ON shiba_internal.graph_bootstrap;
         DROP FUNCTION public.reject_m12_activation();",
        )
        .expect("remove pre-activation failure");
    drop(catchup);

    let mut catchup = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        shiba_protocol::GraphId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(attempt.bootstrap).unwrap(),
        shiba_protocol::SlotGeneration::new(attempt.generation).unwrap(),
        support::options(),
    )
    .expect("replay failed activation from the unacknowledged fence");

    support::install_receiver_kill_trigger(
        admin,
        "shiba_internal.graph_bootstrap",
        "AFTER UPDATE OF phase",
    );
    assert!(
        catchup.catch_up_next().is_err(),
        "activation commit before ACK must surface failure"
    );
    let activation_lsn: String = admin
        .query_one(
            "SELECT activation_end_lsn::text FROM shiba_internal.graph_bootstrap
         WHERE graph_id=1 AND phase='active'",
            &[],
        )
        .expect("activation committed")
        .get(0);
    drop(catchup);
    support::remove_kill_trigger(admin, "shiba_internal.graph_bootstrap");

    let mut active = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        shiba_protocol::GraphId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(attempt.bootstrap).unwrap(),
        shiba_protocol::SlotGeneration::new(attempt.generation).unwrap(),
        support::options(),
    )
    .expect("resume active-before-ACK");
    assert_eq!(
        active.catch_up_next().expect("ACK activation replay"),
        BootstrapCatchupProgress::Active
    );
    let acknowledged = parse_lsn(&activation_lsn);
    assert!(slot_lsn(admin, attempt.slot) >= acknowledged);
    support::assert_oracle(admin);
    drop(active);

    let mut exact = BootstrapCatchupSession::resume(
        database_url,
        replication_url,
        shiba_protocol::GraphId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(attempt.bootstrap).unwrap(),
        shiba_protocol::SlotGeneration::new(attempt.generation).unwrap(),
        support::options(),
    )
    .expect("resume already-ACKed active attempt");
    let final_state = support::evidence(admin);
    assert_eq!(
        exact.catch_up_next().expect("active exact retry"),
        BootstrapCatchupProgress::Active
    );
    assert_eq!(support::evidence(admin), final_state);
    support::assert_oracle(admin);
}

fn parse_lsn(value: &str) -> u64 {
    let (high, low) = value.split_once('/').expect("LSN shape");
    (u64::from_str_radix(high, 16).expect("LSN high") << 32)
        | u64::from_str_radix(low, 16).expect("LSN low")
}
