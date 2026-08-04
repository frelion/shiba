use postgres::Client;
use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryExpressionV1, QueryFieldV1, QueryInputV1, QueryNodeV1,
    QueryOperationV1, QueryResultFieldV1, QueryResultV1, QuerySelectorV1, QuerySpecV1,
};
use shiba_ingress::BootstrapSpec;
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};
use shiba_runtime::compile_and_register;

pub const SLOT: &str = "shiba_m11_roles_slot";
pub const SWAPPED_SLOT: &str = "shiba_m11_roles_swapped_slot";
pub const PUBLICATION: &str = "shiba_m11_roles_pub";
pub const SWAPPED_PUBLICATION: &str = "shiba_m11_roles_swapped_pub";
pub const CONTROL_ROLE: &str = "shiba_m11_bootstrap_control";
pub const RECEIVER_ROLE: &str = "shiba_m11_bootstrap_receiver";
pub const READER_ROLE: &str = "shiba_m11_result_reader";

pub fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m11-bootstrap-roles.sh must set {name}"))
}

pub fn as_role(conninfo: &str, role: &str) -> String {
    format!("{conninfo} user={role}")
}

pub fn bootstrap_spec(
    source_id: u64,
    bootstrap_id: u64,
    publication_oid: u32,
    slot: &str,
) -> BootstrapSpec {
    BootstrapSpec {
        graph_id: GraphId::new(source_id).expect("graph ID"),
        bootstrap_id: BootstrapId::new(bootstrap_id).expect("bootstrap ID"),
        publication_oid,
        slot_name: slot.to_owned(),
        slot_generation: SlotGeneration::new(1).expect("slot generation"),
    }
}

pub fn install_fixture(admin: &mut Client) {
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE TABLE source.swapped (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events
               WITH (publish = 'insert, update, delete');
             CREATE PUBLICATION {SWAPPED_PUBLICATION} FOR TABLE source.swapped
               WITH (publish = 'insert, update, delete');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);
             SELECT shiba_internal.register_source(2, 'source.swapped'::regclass);
             INSERT INTO source.events VALUES (1, 10), (2, NULL), (3, 30);
             INSERT INTO source.swapped VALUES (1, 1);
             CREATE ROLE {CONTROL_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
             CREATE ROLE {RECEIVER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION;
             CREATE ROLE {READER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
             GRANT USAGE ON SCHEMA shiba_internal, shiba, source TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.source_binding TO {CONTROL_ROLE};
             GRANT SELECT ON shiba_internal.source_invalidation,
                 shiba_internal.graph_ingress_config,
                 shiba_internal.graph_ingress_source,
                 shiba_internal.graph_ingress_invalidation TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.graph_definition,
                 shiba_internal.graph_ingress_config,
                 shiba_internal.graph_source_member TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.graph_bootstrap,
                 shiba_internal.graph_bootstrap_checkpoint TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE ON shiba_internal.graph_continuation TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE, DELETE ON shiba_internal.source_row_state TO {CONTROL_ROLE};
             GRANT USAGE ON SEQUENCE shiba_internal.source_row_state_row_state_id_seq TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE, DELETE ON shiba_internal.graph_node_state TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba.graph_result TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE, DELETE ON shiba_internal.graph_result_row TO {CONTROL_ROLE};
             GRANT SELECT ON source.events, source.swapped TO {CONTROL_ROLE};
             GRANT USAGE ON SCHEMA source TO {RECEIVER_ROLE};
             GRANT SELECT ON source.events, source.swapped TO {RECEIVER_ROLE};
             GRANT USAGE ON SCHEMA shiba TO {READER_ROLE};
             GRANT SELECT ON shiba.graph_result, shiba.graph_result_rows TO {READER_ROLE};"
        ))
        .expect("install sources and split roles");
    for source_id in [1, 2] {
        compile_and_register(admin, &graph_spec(source_id)).expect("register graph");
    }
}

fn graph_spec(source_id: u64) -> QuerySpecV1 {
    let source_id_value = SourceId::new(source_id).expect("source ID");
    QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(source_id).expect("graph ID"),
        sources: vec![source_id_value],
        nodes: vec![
            QueryNodeV1 {
                inputs: vec![QueryInputV1::Source {
                    source_id: source_id_value,
                }],
                state_codec_version: Some(1),
                operation: QueryOperationV1::CountRows,
            },
            QueryNodeV1 {
                inputs: vec![QueryInputV1::Source {
                    source_id: source_id_value,
                }],
                state_codec_version: Some(1),
                operation: QueryOperationV1::SumInt8 {
                    value: QueryExpressionV1::Column {
                        field: QueryFieldV1 {
                            input: 0,
                            selector: QuerySelectorV1::Name {
                                name: "payload".into(),
                                quoted: false,
                            },
                        },
                    },
                },
            },
        ],
        results: vec![
            QueryResultV1 {
                input_node: 1,
                fields: vec![QueryResultFieldV1 {
                    name: "count".into(),
                    value_slot: 0,
                    nullable: false,
                }],
                key_ordinals: vec![],
            },
            QueryResultV1 {
                input_node: 2,
                fields: vec![QueryResultFieldV1 {
                    name: "sum".into(),
                    value_slot: 0,
                    nullable: true,
                }],
                key_ordinals: vec![],
            },
        ],
    }
}

pub fn load_publication_oid(client: &mut Client, publication: &str) -> u32 {
    client
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .expect("read publication")
        .get(0)
}

pub fn assert_results(client: &mut Client, graph_id: i64, status: &str, values: [Option<i64>; 2]) {
    let actual = client
        .query(
            "SELECT result.result_status,
                    (SELECT CASE
                        WHEN convert_from(row.row_payload,'UTF8')::jsonb
                             #>> '{values,0,type}' = 'null' THEN NULL
                        ELSE (convert_from(row.row_payload,'UTF8')::jsonb
                              #>> '{values,0,value}')::bigint
                     END
                     FROM shiba.graph_result_rows AS row
                     WHERE row.graph_id=result.graph_id
                       AND row.result_id=result.result_id)
             FROM shiba.graph_result AS result
             WHERE result.graph_id = $1 ORDER BY result.result_id",
            &[&graph_id],
        )
        .expect("query public results")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect::<Vec<(String, Option<i64>)>>();
    assert_eq!(
        actual,
        values
            .into_iter()
            .map(|value| (status.to_owned(), value))
            .collect::<Vec<_>>()
    );
}

pub fn assert_no_apply(client: &mut Client, source_id: i64, expected_phase: Option<&str>) {
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_row_state WHERE source_id = $1",
                &[&source_id],
            )
            .expect("count private rows")
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id = $1",
                &[&source_id],
            )
            .expect("count continuations")
            .get::<_, i64>(0),
        0
    );
    let phase = client
        .query_opt(
            "SELECT phase FROM shiba_internal.graph_bootstrap WHERE graph_id = $1",
            &[&source_id],
        )
        .expect("query optional bootstrap")
        .map(|row| row.get::<_, String>(0));
    assert_eq!(phase.as_deref(), expected_phase);
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_node_state AS state
                 WHERE state.graph_id = $1
                   AND state.state_payload NOT IN (
                       convert_to('{\"type\":\"int8\",\"value\":0}', 'UTF8'),
                       convert_to('{\"type\":\"bool\",\"value\":true}', 'UTF8'))",
                &[&source_id],
            )
            .expect("count changed private operator states")
            .get::<_, i64>(0),
        0
    );
}
