use std::num::NonZeroU32;

use postgres::{Client, NoTls};
use shiba_compiler::{CompilerError, GRAPH_SPEC_VERSION, GraphOutputSpecV1, GraphSpecV1};
use shiba_operator::{NodeId, ObjectAddress, OperatorGraph, OperatorNodeKind};
use shiba_protocol::{GraphId, SourceId};
use shiba_runtime::{M2Error, RegistrationError, compile_and_register};

mod support;

use support::PgoutputCapture;

const ENVIRONMENT: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m9-registration.sh",
    env_prefix: "SHIBA_M9_REGISTRATION",
    slot: "unused_m9_registration_slot",
    publication: "unused_m9_registration_publication",
};

fn node(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).expect("node ID"))
}

fn spec(graph_id: u64, source_id: u64, input_column: &str) -> GraphSpecV1 {
    let source_id = SourceId::new(source_id).expect("source ID");
    GraphSpecV1 {
        version: GRAPH_SPEC_VERSION,
        graph_id: GraphId::new(graph_id).expect("graph ID"),
        sources: vec![source_id],
        outputs: vec![
            GraphOutputSpecV1::CountRows {
                source_id,
                aggregate_node_id: node(1),
                result_node_id: node(101),
            },
            GraphOutputSpecV1::SumInt8 {
                source_id,
                input_column: input_column.into(),
                aggregate_node_id: node(2),
                result_node_id: node(102),
            },
        ],
    }
}

fn authority_counts(client: &mut Client) -> (i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT count(*) FROM shiba_internal.graph_definition),
                (SELECT count(*) FROM shiba_internal.graph_source_member),
                (SELECT count(*) FROM shiba.graph_result)",
            &[],
        )
        .expect("query graph authority counts");
    (row.get(0), row.get(1), row.get(2))
}

fn durable_graph(client: &mut Client) -> OperatorGraph {
    let row = client
        .query_one(
            "SELECT graph_format_version, graph_payload, graph_digest
             FROM shiba_internal.graph_definition WHERE graph_id = 1",
            &[],
        )
        .expect("query durable graph definition");
    assert_eq!(row.get::<_, i32>(0), 1);
    let payload: Vec<u8> = row.get(1);
    let digest: Vec<u8> = row.get(2);
    OperatorGraph::from_canonical_payload(
        &payload,
        digest.try_into().expect("32-byte graph digest"),
    )
    .expect("decode durable canonical graph")
}

fn assert_sum_binding(client: &mut Client, graph: &OperatorGraph) {
    let row = client
        .query_one(
            "SELECT 'pg_class'::regclass::oid::bigint,
                    'source_m9.events'::regclass::oid::bigint, attnum
             FROM pg_catalog.pg_attribute
             WHERE attrelid = 'source_m9.events'::regclass AND attname = 'payload'",
            &[],
        )
        .expect("query live SumInt8 ObjectAddress");
    let expected = ObjectAddress {
        class_id: u32::try_from(row.get::<_, i64>(0)).expect("class oid"),
        object_id: u32::try_from(row.get::<_, i64>(1)).expect("relation oid"),
        sub_id: i32::from(row.get::<_, i16>(2)),
    };
    let source = graph.sources.first().expect("source port");
    assert!(
        source
            .layout
            .iter()
            .any(|column| column.address == expected)
    );
    assert!(
        graph
            .nodes
            .iter()
            .any(|node| matches!(node.kind, OperatorNodeKind::SumInt8 { .. }))
    );
}

fn install_result_failure(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m9_registration_test;
             CREATE FUNCTION m9_registration_test.fail_result()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'injected result registration failure'; END $$;
             CREATE TRIGGER m9_fail_result BEFORE INSERT ON shiba.graph_result
             FOR EACH ROW EXECUTE FUNCTION m9_registration_test.fail_result();",
        )
        .expect("install result registration failure");
}

fn prove_failures_leave_no_partial_rows(client: &mut Client) {
    let before = authority_counts(client);
    assert!(matches!(
        compile_and_register(client, &spec(2, 2, "payload")),
        Err(RegistrationError::Runtime(M2Error::SourceBindingMissing))
    ));
    assert_eq!(authority_counts(client), before);
    assert!(matches!(
        compile_and_register(client, &spec(2, 1, "missing")),
        Err(RegistrationError::Compiler(CompilerError::MissingColumn(column))) if column == "missing"
    ));
    assert_eq!(authority_counts(client), before);
    assert!(matches!(
        compile_and_register(client, &spec(2, 1, "label")),
        Err(RegistrationError::Compiler(CompilerError::WrongColumnType { column, type_oid: 25 })) if column == "label"
    ));
    assert_eq!(authority_counts(client), before);
    assert!(matches!(
        compile_and_register(client, &spec(1, 1, "payload")),
        Err(RegistrationError::Runtime(M2Error::Postgres(_)))
    ));
    assert_eq!(authority_counts(client), before);
}

fn prove_permissions(client: &mut Client) {
    client
        .batch_execute("CREATE ROLE m9_reader; SET ROLE m9_reader")
        .expect("enter ordinary role");
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM shiba.graph_result", &[])
            .expect("ordinary role reads results")
            .get::<_, i64>(0),
        2
    );
    assert!(
        client
            .execute("UPDATE shiba.graph_result SET value_bigint = 9", &[])
            .is_err()
    );
    assert!(
        client
            .query("SELECT * FROM shiba_internal.graph_definition", &[])
            .is_err()
    );
    client.batch_execute("RESET ROLE").expect("restore owner");
    assert_eq!(authority_counts(client), (1, 1, 2));
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m9-registration.sh"]
fn m9_live_graph_compile_and_registration_are_atomic_and_private() {
    let mut client = Client::connect(&ENVIRONMENT.required("DATABASE_URL"), NoTls)
        .expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m9;
             CREATE TABLE source_m9.events (id bigint PRIMARY KEY, payload bigint, label text);
             SELECT shiba_internal.register_source(1, 'source_m9.events'::regclass);",
        )
        .expect("install and bind live source");
    for removed in [
        "shiba_internal.count_state",
        "shiba.count_result",
        "shiba_internal.operator_definition",
        "shiba_internal.operator_state",
        "shiba.operator_result",
    ] {
        assert!(
            client
                .query_one("SELECT to_regclass($1) IS NULL", &[&removed])
                .unwrap()
                .get::<_, bool>(0)
        );
    }

    install_result_failure(&mut client);
    let declaration = spec(1, 1, "payload");
    assert!(matches!(
        compile_and_register(&mut client, &declaration),
        Err(RegistrationError::Runtime(M2Error::Postgres(_)))
    ));
    assert_eq!(authority_counts(&mut client), (0, 0, 0));
    client
        .batch_execute("DROP SCHEMA m9_registration_test CASCADE")
        .unwrap();

    let compiled = compile_and_register(&mut client, &declaration).expect("register graph");
    assert_eq!(authority_counts(&mut client), (1, 1, 2));
    let durable = durable_graph(&mut client);
    assert_eq!(durable, compiled);
    assert_sum_binding(&mut client, &durable);
    prove_failures_leave_no_partial_rows(&mut client);
    prove_permissions(&mut client);
}
