use postgres::{Client, NoTls};
use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryFieldV1, QueryInputV1, QueryNodeV1, QueryOperationV1,
    QueryResultFieldV1, QueryResultV1, QuerySelectorV1, QuerySpecV1,
};
use shiba_protocol::{GraphId, SlotGeneration, SourceId};
use shiba_runtime::{
    M2Error, PgoutputGraph, PgoutputSource, ProcessOutcome, compile_and_register,
    decode_committed_changes, process,
};

mod support;

#[path = "m14_graph_runtime/support.rs"]
mod graph_support;

use graph_support::{
    JOIN, SINGLE, assert_identity_binding, configure, durable_join, join_rows, oid,
};

fn named_field(input: u8, name: &str) -> QueryFieldV1 {
    QueryFieldV1 {
        input,
        selector: QuerySelectorV1::Name {
            name: name.into(),
            quoted: false,
        },
    }
}

#[test]
#[ignore = "requires scripts/test-m14-graph-runtime.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered graph authority and atomicity proof"
)]
fn singleton_and_two_source_join_share_one_graph_runtime() {
    let mut client = Client::connect(&SINGLE.required("DATABASE_URL"), NoTls).expect("connect");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_single;
             CREATE SCHEMA source_left;
             CREATE SCHEMA source_right;
             CREATE TABLE source_single.events (id bigint PRIMARY KEY);
             CREATE TABLE source_left.events (id bigint PRIMARY KEY, right_key bigint NULL);
             CREATE TABLE source_right.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION shiba_m14_single_pub FOR TABLE source_single.events;
             CREATE PUBLICATION shiba_m14_join_pub
                 FOR TABLE source_left.events, source_right.events;
             SELECT shiba_internal.register_source(1, 'source_single.events'::regclass);
             SELECT shiba_internal.register_source(2, 'source_left.events'::regclass);
             SELECT shiba_internal.register_source(3, 'source_right.events'::regclass);",
        )
        .expect("install graph sources");

    let single_pk = oid(&mut client, "source_single.events_pkey");
    let left_pk = oid(&mut client, "source_left.events_pkey");
    let right_pk = oid(&mut client, "source_right.events_pkey");
    assert_identity_binding(&mut client, 1, single_pk);
    assert_identity_binding(&mut client, 2, left_pk);
    assert_identity_binding(&mut client, 3, right_pk);

    client
        .batch_execute(
            "CREATE TABLE source_right.explicit_identity (id bigint NOT NULL, payload bigint);
             CREATE UNIQUE INDEX explicit_identity_key
                 ON source_right.explicit_identity(id);
             ALTER TABLE source_right.explicit_identity
                 REPLICA IDENTITY USING INDEX explicit_identity_key;
             SELECT shiba_internal.register_source(
                 4, 'source_right.explicit_identity'::regclass);
             CREATE TABLE source_right.no_identity (id bigint, payload bigint);",
        )
        .expect("install explicit and invalid identity fixtures");
    let explicit_index = oid(&mut client, "source_right.explicit_identity_key");
    assert_identity_binding(&mut client, 4, explicit_index);
    assert!(
        client
            .execute(
                "SELECT shiba_internal.register_source(5, 'source_right.no_identity'::regclass)",
                &[],
            )
            .is_err()
    );
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_binding WHERE source_id = 5",
                &[],
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );

    support::register_count_operator(&mut client, 1, 1);
    SINGLE.create_slot();
    configure(&mut client, 1, "shiba_m14_single_pub", SINGLE.slot);
    client
        .batch_execute("INSERT INTO source_single.events VALUES (1),(2)")
        .unwrap();
    let single_source = PgoutputSource::new(
        SourceId::new(1).unwrap(),
        SlotGeneration::new(1).unwrap(),
        oid(&mut client, "source_single.events"),
    );
    let single = decode_committed_changes(
        &SINGLE.capture(&mut client, "single.pgoutput"),
        &support::singleton_graph(1, single_source),
    )
    .expect("decode singleton graph transaction");
    assert_eq!(
        process(&mut client, &single).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(support::scalar_state(&mut client, 1), 2);

    let source2 = SourceId::new(2).unwrap();
    let source3 = SourceId::new(3).unwrap();
    let graph_spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(2).unwrap(),
        sources: vec![source2, source3],
        nodes: vec![QueryNodeV1 {
            inputs: vec![
                QueryInputV1::Source { source_id: source2 },
                QueryInputV1::Source { source_id: source3 },
            ],
            state_codec_version: Some(1),
            operation: QueryOperationV1::InnerJoin {
                left_id: named_field(0, "id"),
                left_key: named_field(0, "right_key"),
                right_id: named_field(1, "id"),
                right_payload: named_field(1, "payload"),
            },
        }],
        results: vec![QueryResultV1 {
            input_node: 1,
            fields: vec![
                QueryResultFieldV1 {
                    name: "id".into(),
                    value_slot: 0,
                    nullable: false,
                },
                QueryResultFieldV1 {
                    name: "payload".into(),
                    value_slot: 1,
                    nullable: true,
                },
            ],
            key_ordinals: vec![1],
        }],
    };
    compile_and_register(&mut client, &graph_spec).expect("register two-source join graph");
    JOIN.create_slot();
    configure(&mut client, 2, "shiba_m14_join_pub", JOIN.slot);
    let graph = PgoutputGraph::new(
        GraphId::new(2).unwrap(),
        SlotGeneration::new(1).unwrap(),
        vec![
            PgoutputSource::with_nullable_int8_payload(
                source2,
                SlotGeneration::new(1).unwrap(),
                oid(&mut client, "source_left.events"),
            ),
            PgoutputSource::with_nullable_int8_payload(
                source3,
                SlotGeneration::new(1).unwrap(),
                oid(&mut client, "source_right.events"),
            ),
        ],
    )
    .expect("two-source pgoutput descriptor");

    client
        .batch_execute(
            "BEGIN;
         INSERT INTO source_right.events VALUES (10,100),(20,NULL);
         INSERT INTO source_left.events VALUES (1,10),(2,10),(3,NULL);
         COMMIT;",
        )
        .unwrap();
    let initial =
        decode_committed_changes(&JOIN.capture(&mut client, "join-initial.pgoutput"), &graph)
            .expect("decode both relation changes from one commit");
    assert!(
        initial
            .changes
            .iter()
            .any(|change| change.source_id == source2)
    );
    assert!(
        initial
            .changes
            .iter()
            .any(|change| change.source_id == source3)
    );
    assert_eq!(
        process(&mut client, &initial).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(join_rows(&mut client), vec![(1, Some(100)), (2, Some(100))]);

    client
        .batch_execute("UPDATE source_right.events SET payload=110 WHERE id=10")
        .unwrap();
    let fanout =
        decode_committed_changes(&JOIN.capture(&mut client, "join-fanout.pgoutput"), &graph)
            .expect("decode right fanout update");
    assert_eq!(
        process(&mut client, &fanout).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(join_rows(&mut client), vec![(1, Some(110)), (2, Some(110))]);

    client
        .batch_execute(
            "BEGIN;
         INSERT INTO source_left.events VALUES (4,20);
         UPDATE source_right.events SET payload=200 WHERE id=20;
         COMMIT;",
        )
        .unwrap();
    let both = decode_committed_changes(
        &JOIN.capture(&mut client, "join-both-sides.pgoutput"),
        &graph,
    )
    .expect("decode same transaction changes on both sides");
    assert_eq!(
        process(&mut client, &both).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(
        join_rows(&mut client),
        vec![(1, Some(110)), (2, Some(110)), (4, Some(200))]
    );
    let replayed = durable_join(&mut client);
    assert_eq!(
        process(&mut client, &both).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_join(&mut client), replayed);

    client
        .batch_execute("DELETE FROM source_right.events WHERE id=10")
        .unwrap();
    let retract =
        decode_committed_changes(&JOIN.capture(&mut client, "join-retract.pgoutput"), &graph)
            .expect("decode right delete fanout");
    assert_eq!(
        process(&mut client, &retract).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(join_rows(&mut client), vec![(4, Some(200))]);

    client
        .batch_execute(
            "BEGIN;
         INSERT INTO source_left.events VALUES (5,20);
         UPDATE source_right.events SET payload=250 WHERE id=20;
         COMMIT;",
        )
        .unwrap();
    let rollback =
        decode_committed_changes(&JOIN.capture(&mut client, "join-rollback.pgoutput"), &graph)
            .expect("decode rollback fixture");
    let before = durable_join(&mut client);
    client
        .batch_execute(
            "CREATE FUNCTION pg_temp.reject_join_sink() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected graph sink failure'; END $$;
         CREATE TRIGGER reject_join_sink BEFORE INSERT OR UPDATE OR DELETE
         ON shiba_internal.graph_result_row FOR EACH ROW
         EXECUTE FUNCTION pg_temp.reject_join_sink();",
        )
        .unwrap();
    assert!(matches!(
        process(&mut client, &rollback),
        Err(M2Error::Postgres(_))
    ));
    assert_eq!(durable_join(&mut client), before);
    client
        .execute(
            "DROP TRIGGER reject_join_sink ON shiba_internal.graph_result_row",
            &[],
        )
        .unwrap();
    assert_eq!(
        process(&mut client, &rollback).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(join_rows(&mut client), vec![(4, Some(250)), (5, Some(250))]);

    client
        .batch_execute("UPDATE source_right.events SET id=30 WHERE id=20")
        .unwrap();
    let right_key_change = decode_committed_changes(
        &JOIN.capture(&mut client, "join-right-key-change.pgoutput"),
        &graph,
    )
    .expect("decode right key-changing update");
    assert_eq!(
        process(&mut client, &right_key_change).unwrap(),
        ProcessOutcome::Applied
    );
    assert!(join_rows(&mut client).is_empty());

    client
        .batch_execute("UPDATE source_left.events SET right_key=30 WHERE right_key=20")
        .unwrap();
    let left_key_change = decode_committed_changes(
        &JOIN.capture(&mut client, "join-left-key-change.pgoutput"),
        &graph,
    )
    .expect("decode left join-key update");
    assert_eq!(
        process(&mut client, &left_key_change).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(join_rows(&mut client), vec![(4, Some(250)), (5, Some(250))]);
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id=2",
                &[],
            )
            .unwrap()
            .get::<_, i64>(0),
        7
    );

    client
        .batch_execute("INSERT INTO source_left.events VALUES (6,30)")
        .unwrap();
    let pending_ddl = decode_committed_changes(
        &JOIN.capture(&mut client, "join-pending-ddl.pgoutput"),
        &graph,
    )
    .expect("decode transaction before identity replacement");
    let before_ddl = durable_join(&mut client);
    client
        .batch_execute(
            "ALTER TABLE source_right.events DROP CONSTRAINT events_pkey;
             ALTER TABLE source_right.events ADD PRIMARY KEY (id);",
        )
        .expect("replace exact right identity index");
    assert!(matches!(
        process(&mut client, &pending_ddl),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(durable_join(&mut client), before_ddl);
}
