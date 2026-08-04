use std::{
    sync::mpsc::{self, TryRecvError},
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_ingress::{AttachOptions, GovernedGraphSession, ReplicationMode, StreamedInput};
use shiba_protocol::{GraphId, SlotGeneration};
use shiba_runtime::{ProcessOutcome, compile_and_register};

mod support;

use support::{count_spec, slot_lsn, wait_for_keepalive_reply, wait_for_slot_lsn};

const SLOT: &str = "shiba_m10_streaming_slot";
const PUBLICATION: &str = "shiba_m10_streaming_pub";
const APPLICATION: &str = "shiba_m10_streaming_receiver";
const ROW_COUNT: i64 = 10_000;

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m10-streaming-ingress.sh must set {name}"))
}

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 2),
                COALESCE((SELECT (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 4), 0),
                (SELECT state_payload FROM shiba_internal.graph_node_state WHERE graph_id = 1 AND node_id = 1 AND namespace = 1),
                (SELECT state_payload FROM shiba_internal.graph_node_state WHERE graph_id = 1 AND node_id = 3 AND namespace = 1),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.graph_continuation)",
            &[],
        )
        .expect("query durable streamed state");
    (
        row.get(0),
        row.get(1),
        row.get::<_, Option<Vec<u8>>>(2)
            .map_or(0, |payload| decode_int8_state(&payload)),
        row.get::<_, Option<Vec<u8>>>(3)
            .map_or(0, |payload| decode_int8_state(&payload)),
        row.get(4),
        row.get(5),
    )
}

fn decode_int8_state(payload: &[u8]) -> i64 {
    let offset = payload.len().saturating_sub(8);
    i64::from_be_bytes(payload[offset..].try_into().expect("aggregate int8 state"))
}

fn attach_session(database_url: &str, replication_url: &str) -> GovernedGraphSession {
    GovernedGraphSession::attach(
        database_url,
        replication_url,
        GraphId::new(1).expect("graph ID"),
        SlotGeneration::new(1).expect("slot generation"),
        AttachOptions::new(ReplicationMode::Streamed, Duration::from_secs(5))
            .expect("attach options"),
    )
    .expect("attach governed streamed session")
}

fn stream_count(client: &mut Client) -> i64 {
    client
        .query_one(
            "SELECT stream_count FROM pg_stat_replication_slots WHERE slot_name = $1",
            &[&SLOT],
        )
        .expect("query logical streaming count")
        .get(0)
}

fn wait_for_stream_output(client: &mut Client, baseline: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if stream_count(client) > baseline {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "streamed receiver did not consume a nonterminal segment"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "requires scripts/test-m10-streaming-ingress.sh"]
#[allow(clippy::too_many_lines, reason = "one ordered streamed slot proof")]
fn production_streams_commit_abort_crash_and_limit_without_early_ack() {
    let database_url = required("SHIBA_M10_STREAMING_DATABASE_URL");
    let replication_url = required("SHIBA_M10_STREAMING_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect source database");
    let mut observer = Client::connect(&database_url, NoTls).expect("connect state observer");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE SCHEMA noise;
             CREATE TABLE noise.unpublished (id bigint PRIMARY KEY);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events (id);
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);"
        ))
        .expect("install streamed source and binding");
    compile_and_register(&mut admin, &count_spec(1)).expect("register CountRows graph");
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .expect("create streamed slot");
    let publication_oid: u32 = admin
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("read publication OID")
        .get(0);
    admin
        .execute(
            "SELECT shiba_internal.configure_graph_ingress(1, $1, $2, 1)",
            &[&publication_oid, &SLOT],
        )
        .expect("configure governed streamed ingress");
    let initial_lsn = slot_lsn(&mut observer, SLOT);

    let mut receiver = attach_session(&database_url, &replication_url);
    let committed_stream_count = stream_count(&mut observer);
    let (terminal_tx, terminal_rx) = mpsc::channel();
    let terminal_thread = thread::spawn(move || {
        let input = receiver.receive_streamed_one();
        terminal_tx
            .send((receiver, input))
            .expect("return committed terminal");
    });
    wait_for_keepalive_reply(&mut observer, APPLICATION, initial_lsn);
    let mut transaction = admin.transaction().expect("begin streamed commit");
    transaction
        .execute(
            "INSERT INTO source.events(id) SELECT generate_series(1::bigint, $1::bigint)",
            &[&ROW_COUNT],
        )
        .expect("write 10,000-row transaction");
    wait_for_stream_output(&mut observer, committed_stream_count);
    assert!(matches!(terminal_rx.try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(durable_state(&mut observer), (0, 0, 0, 0, 0, 0));
    assert_eq!(slot_lsn(&mut observer, SLOT), initial_lsn);
    transaction.commit().expect("commit streamed transaction");
    let (mut receiver, input) = terminal_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("receive stream commit terminal");
    terminal_thread.join().expect("join committed receiver");
    let StreamedInput::Transaction(input) = input.expect("decode committed stream") else {
        panic!("commit returned abort token");
    };
    assert_eq!(durable_state(&mut observer), (0, 0, 0, 0, 0, 0));
    assert_eq!(slot_lsn(&mut observer, SLOT), initial_lsn);
    let applied = receiver
        .apply_received(&input)
        .expect("apply committed stream");
    assert_eq!(applied.outcome(), ProcessOutcome::Applied);
    assert_eq!(
        durable_state(&mut observer),
        (ROW_COUNT, 0, ROW_COUNT, 0, ROW_COUNT, 1)
    );
    assert_eq!(slot_lsn(&mut observer, SLOT), initial_lsn);
    let commit_lsn = applied.end_lsn();
    drop(receiver);

    let mut receiver = attach_session(&database_url, &replication_url);
    let replay = match receiver
        .receive_streamed_one()
        .expect("receive unacknowledged committed replay")
    {
        StreamedInput::Transaction(input) => input,
        StreamedInput::EmptyCommitted(_) => panic!("replay returned empty commit"),
        StreamedInput::Aborted(_) => panic!("replay returned abort token"),
    };
    let replay = receiver
        .apply_received(&replay)
        .expect("apply exact streamed replay");
    assert_eq!(replay.outcome(), ProcessOutcome::AlreadyApplied);
    assert_eq!(replay.end_lsn(), commit_lsn);
    receiver.acknowledge(&replay).expect("ack committed replay");
    wait_for_slot_lsn(&mut observer, SLOT, commit_lsn);
    drop(receiver);

    let mut receiver = attach_session(&database_url, &replication_url);
    let first_empty = match receiver
        .receive_streamed_one()
        .expect("receive Runtime Apply empty commit")
    {
        StreamedInput::EmptyCommitted(token) => token,
        StreamedInput::Transaction(_) => panic!("Runtime Apply WAL decoded as source input"),
        StreamedInput::Aborted(_) => panic!("Runtime Apply WAL decoded as abort"),
    };
    assert_ne!(first_empty.xid(), 0);
    assert_ne!(first_empty.commit_lsn(), 0);
    assert!(first_empty.end_lsn() >= first_empty.commit_lsn());
    assert!(
        first_empty.segment_count() > 1,
        "real Runtime Apply WAL must produce a multi-segment empty commit"
    );
    assert!(receiver.acknowledge(&replay).is_err(), "wrong token kind");
    assert_eq!(
        durable_state(&mut observer),
        (ROW_COUNT, 0, ROW_COUNT, 0, ROW_COUNT, 1)
    );
    assert_eq!(slot_lsn(&mut observer, SLOT), commit_lsn);
    let first_empty_identity = (
        first_empty.xid(),
        first_empty.commit_lsn(),
        first_empty.end_lsn(),
        first_empty.segment_count(),
    );
    drop(receiver);

    let mut receiver = attach_session(&database_url, &replication_url);
    let replayed_empty = match receiver
        .receive_streamed_one()
        .expect("replay unacknowledged empty commit")
    {
        StreamedInput::EmptyCommitted(token) => token,
        StreamedInput::Transaction(_) => panic!("empty replay decoded as source input"),
        StreamedInput::Aborted(_) => panic!("empty replay decoded as abort"),
    };
    assert_eq!(
        (
            replayed_empty.xid(),
            replayed_empty.commit_lsn(),
            replayed_empty.end_lsn(),
            replayed_empty.segment_count(),
        ),
        first_empty_identity
    );
    assert!(
        receiver.acknowledge_empty(&first_empty).is_err(),
        "same-LSN token from the prior receiver must remain foreign"
    );
    receiver
        .acknowledge_empty(&replayed_empty)
        .expect("ack exact Runtime Apply empty commit");
    wait_for_slot_lsn(&mut observer, SLOT, replayed_empty.end_lsn());
    let first_empty_end = replayed_empty.end_lsn();

    let (empty_tx, empty_rx) = mpsc::channel();
    let empty_stream_count = stream_count(&mut observer);
    let empty_thread = thread::spawn(move || {
        let input = receiver.receive_streamed_one();
        empty_tx
            .send((receiver, input))
            .expect("return explicit empty commit");
    });
    wait_for_keepalive_reply(&mut observer, APPLICATION, first_empty_end);
    assert!(matches!(empty_rx.try_recv(), Err(TryRecvError::Empty)));
    let mut transaction = admin.transaction().expect("begin unpublished empty stream");
    let empty_xid: i64 = transaction
        .query_one("SELECT pg_current_xact_id()::text::bigint", &[])
        .expect("read exact empty transaction XID")
        .get(0);
    transaction
        .execute(
            "INSERT INTO noise.unpublished
             SELECT generate_series(1::bigint, $1::bigint)",
            &[&ROW_COUNT],
        )
        .expect("write unpublished streamed transaction");
    wait_for_stream_output(&mut observer, empty_stream_count);
    assert!(matches!(empty_rx.try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(slot_lsn(&mut observer, SLOT), first_empty_end);
    transaction
        .commit()
        .expect("commit unpublished empty stream");
    let (mut receiver, second_empty) = empty_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("receive explicit empty terminal");
    empty_thread.join().expect("join explicit empty receiver");
    let StreamedInput::EmptyCommitted(second_empty) =
        second_empty.expect("assemble explicit empty commit")
    else {
        panic!("unpublished transaction did not return empty token");
    };
    let empty_xid = u32::try_from(empty_xid).expect("XID fits u32");
    assert_eq!(second_empty.xid(), empty_xid);
    assert!(second_empty.end_lsn() >= second_empty.commit_lsn());
    assert!(second_empty.segment_count() > 1);
    assert!(
        receiver.acknowledge_empty(&first_empty).is_err(),
        "stale token"
    );
    assert_eq!(slot_lsn(&mut observer, SLOT), first_empty_end);
    assert_eq!(
        durable_state(&mut observer),
        (ROW_COUNT, 0, ROW_COUNT, 0, ROW_COUNT, 1)
    );
    receiver
        .acknowledge_empty(&second_empty)
        .expect("ack explicit empty commit");
    wait_for_slot_lsn(&mut observer, SLOT, second_empty.end_lsn());
    let second_empty_end = second_empty.end_lsn();

    let (abort_tx, abort_rx) = mpsc::channel();
    let abort_stream_count = stream_count(&mut observer);
    let abort_thread = thread::spawn(move || {
        let input = receiver.receive_streamed_one();
        abort_tx
            .send((receiver, input))
            .expect("return abort terminal");
    });
    wait_for_keepalive_reply(&mut observer, APPLICATION, second_empty_end);
    assert!(matches!(abort_rx.try_recv(), Err(TryRecvError::Empty)));
    let mut transaction = admin.transaction().expect("begin streamed abort");
    transaction
        .execute(
            "INSERT INTO source.events(id)
             SELECT generate_series(20001::bigint, 30000::bigint)",
            &[],
        )
        .expect("write aborted rows");
    wait_for_stream_output(&mut observer, abort_stream_count);
    assert!(matches!(abort_rx.try_recv(), Err(TryRecvError::Empty)));
    transaction.rollback().expect("abort streamed transaction");
    let (mut receiver, aborted) = abort_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("receive abort terminal");
    abort_thread.join().expect("join aborted receiver");
    let StreamedInput::Aborted(aborted) = aborted.expect("assemble abort token") else {
        panic!("abort returned non-abort token");
    };
    assert_eq!(
        durable_state(&mut observer),
        (ROW_COUNT, 0, ROW_COUNT, 0, ROW_COUNT, 1)
    );
    assert_eq!(slot_lsn(&mut observer, SLOT), second_empty_end);
    let abort_lsn = aborted.acknowledgment_lsn();
    receiver
        .acknowledge_abort(&aborted)
        .expect("ack terminal abort coordinate");
    wait_for_slot_lsn(&mut observer, SLOT, abort_lsn);
    drop(receiver);

    let mut receiver = attach_session(&database_url, &replication_url);
    let crash_stream_count = stream_count(&mut observer);
    let (crash_tx, crash_rx) = mpsc::channel();
    let crash_thread = thread::spawn(move || {
        let input = receiver.receive_streamed_one();
        crash_tx.send(input).expect("return crashed receive");
    });
    let mut transaction = admin.transaction().expect("begin crash-window stream");
    transaction
        .execute(
            "INSERT INTO source.events(id)
             SELECT generate_series(40001::bigint, 50000::bigint)",
            &[],
        )
        .expect("write crash-window rows");
    wait_for_stream_output(&mut observer, crash_stream_count);
    let terminated: bool = observer
        .query_one(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_replication
             WHERE application_name = $1",
            &[&APPLICATION],
        )
        .expect("terminate partial streamed receiver")
        .get(0);
    assert!(terminated);
    assert!(
        crash_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("receive transport failure")
            .is_err()
    );
    crash_thread.join().expect("join crashed receiver");
    assert_eq!(
        durable_state(&mut observer),
        (ROW_COUNT, 0, ROW_COUNT, 0, ROW_COUNT, 1)
    );
    assert_eq!(slot_lsn(&mut observer, SLOT), abort_lsn);
    transaction.commit().expect("commit after receiver crash");

    let mut receiver = attach_session(&database_url, &replication_url);
    let retry = match receiver
        .receive_streamed_one()
        .expect("restart and receive full committed stream")
    {
        StreamedInput::Transaction(input) => input,
        StreamedInput::EmptyCommitted(_) => panic!("crash retry returned empty commit"),
        StreamedInput::Aborted(_) => panic!("crash retry returned abort"),
    };
    let retry = receiver
        .apply_received(&retry)
        .expect("apply crash-window retry");
    assert_eq!(retry.outcome(), ProcessOutcome::Applied);
    receiver.acknowledge(&retry).expect("ack crash retry");
    wait_for_slot_lsn(&mut observer, SLOT, retry.end_lsn());
    assert_eq!(
        durable_state(&mut observer),
        (20_000, 0, 20_000, 0, 20_000, 2)
    );
    drop(receiver);

    let mut receiver = attach_session(&database_url, &replication_url);
    let retry_apply_empty = match receiver
        .receive_streamed_one()
        .expect("receive crash-retry Apply empty commit")
    {
        StreamedInput::EmptyCommitted(token) => token,
        StreamedInput::Transaction(_) => panic!("Apply WAL decoded as source transaction"),
        StreamedInput::Aborted(_) => panic!("Apply WAL decoded as abort"),
    };
    assert_eq!(
        durable_state(&mut observer),
        (20_000, 0, 20_000, 0, 20_000, 2)
    );
    receiver
        .acknowledge_empty(&retry_apply_empty)
        .expect("ack crash-retry Apply empty commit");
    wait_for_slot_lsn(&mut observer, SLOT, retry_apply_empty.end_lsn());
    let limit_baseline = retry_apply_empty.end_lsn();
    drop(receiver);

    admin
        .execute(
            "UPDATE shiba_internal.graph_definition
             SET compiler_version = compiler_version WHERE graph_id = 1",
            &[],
        )
        .expect("emit unrelated internal graph Catalog WAL");
    let mut transaction = admin.transaction().expect("begin post-Sum empty stream");
    let post_sum_xid: i64 = transaction
        .query_one("SELECT pg_current_xact_id()::text::bigint", &[])
        .expect("read post-Sum empty XID")
        .get(0);
    transaction
        .execute(
            "INSERT INTO noise.unpublished
             SELECT generate_series(20001::bigint, 30000::bigint)",
            &[],
        )
        .expect("write post-Sum unpublished transaction");
    transaction.commit().expect("commit post-Sum empty stream");
    let mut receiver = attach_session(&database_url, &replication_url);
    let post_sum_empty = match receiver
        .receive_streamed_one()
        .expect("receive post-Sum empty commit")
    {
        StreamedInput::EmptyCommitted(token) => token,
        StreamedInput::Transaction(_) => panic!("post-Sum empty decoded as source transaction"),
        StreamedInput::Aborted(_) => panic!("post-Sum empty decoded as abort"),
    };
    assert_eq!(
        post_sum_empty.xid(),
        u32::try_from(post_sum_xid).expect("XID fits u32")
    );
    assert!(post_sum_empty.segment_count() > 1);
    assert_eq!(
        durable_state(&mut observer),
        (20_000, 0, 20_000, 0, 20_000, 2)
    );
    assert_eq!(slot_lsn(&mut observer, SLOT), limit_baseline);
    receiver
        .acknowledge_empty(&post_sum_empty)
        .expect("ack post-Sum empty commit");
    wait_for_slot_lsn(&mut observer, SLOT, post_sum_empty.end_lsn());
    let limit_baseline = post_sum_empty.end_lsn();
    drop(receiver);

    admin
        .execute(
            "INSERT INTO source.events(id)
             SELECT generate_series(60001::bigint, 70001::bigint)",
            &[],
        )
        .expect("commit 10,001-row rejected transaction");
    let mut receiver = attach_session(&database_url, &replication_url);
    assert!(receiver.receive_streamed_one().is_err());
    assert_eq!(slot_lsn(&mut observer, SLOT), limit_baseline);
    assert_eq!(
        durable_state(&mut observer),
        (20_000, 0, 20_000, 0, 20_000, 2)
    );
}
