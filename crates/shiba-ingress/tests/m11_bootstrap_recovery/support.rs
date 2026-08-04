use std::process::Command;

use postgres::Client;
use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryAggregateCallV1, QueryExpressionV1, QueryFieldV1, QueryInputV1,
    QueryNodeV1, QueryOperationV1, QueryResultFieldV1, QueryResultV1, QuerySelectorV1, QuerySpecV1,
};
use shiba_operator::{AggregateFunctionV1, TypedValue};
use shiba_protocol::{GraphId, SourceId};
use shiba_runtime::compile_and_register;

pub(crate) use crate::pg_support::slot_lsn;

pub(crate) const SLOT: &str = "shiba_m11_recovery_slot";
pub(crate) const OLD_SLOT: &str = "shiba_m11_abandoned_slot";
pub(crate) const FAILED_SLOT: &str = "shiba_m11_failed_create_slot";
pub(crate) const FOREIGN_SLOT: &str = "shiba_m11_foreign_slot";
pub(crate) const PUBLICATION: &str = "shiba_m11_recovery_pub";
const APPLICATION: &str = "shiba_m11_recovery_receiver";

pub(crate) fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("scripts/test-m11-recovery.sh must set {name}"))
}

pub(crate) fn restart_postgres(mode: &str) {
    let pg_ctl = required("SHIBA_TEST_PG_CTL");
    let data = required("SHIBA_TEST_PG_DATA");
    let socket = required("SHIBA_TEST_PG_SOCKET");
    let port = required("SHIBA_TEST_PG_PORT");
    let stopped = Command::new(&pg_ctl)
        .args(["-D", &data, "-m", mode, "-w", "stop"])
        .status()
        .expect("execute pg_ctl stop");
    assert!(stopped.success(), "pg_ctl immediate stop failed");
    let started = Command::new(pg_ctl)
        .args(["-D", &data, "-o"])
        .arg(format!("-k {socket} -p {port}"))
        .args(["-w", "start"])
        .status()
        .expect("execute pg_ctl start");
    assert!(started.success(), "pg_ctl restart failed");
}

fn graph_spec() -> QuerySpecV1 {
    let source_id = SourceId::new(1).expect("source ID");
    QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(1).expect("graph ID"),
        sources: vec![source_id],
        nodes: vec![
            QueryNodeV1 {
                inputs: vec![QueryInputV1::Source { source_id }],
                state_codec_version: Some(1),
                operation: aggregate(AggregateFunctionV1::CountStar, None),
            },
            QueryNodeV1 {
                inputs: vec![QueryInputV1::Source { source_id }],
                state_codec_version: Some(1),
                operation: aggregate(
                    AggregateFunctionV1::SumInt8,
                    Some(QueryExpressionV1::Column {
                        field: QueryFieldV1 {
                            input: 0,
                            selector: QuerySelectorV1::Name {
                                name: "payload".into(),
                                quoted: false,
                            },
                        },
                    }),
                ),
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

fn aggregate(
    function: AggregateFunctionV1,
    expression: Option<QueryExpressionV1>,
) -> QueryOperationV1 {
    QueryOperationV1::Aggregate {
        group_expressions: Vec::new(),
        calls: vec![QueryAggregateCallV1 {
            ordinal: 1,
            function,
            function_version: 1,
            expression,
        }],
        having: None,
    }
}

pub(crate) fn install_source(client: &mut Client) -> u32 {
    client
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events
               WITH (publish = 'insert, update, delete');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);
             INSERT INTO source.events VALUES (1, 10), (2, 20), (3, 10);"
        ))
        .expect("install recovery source");
    compile_and_register(client, &graph_spec()).expect("register graph");
    client
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("query publication OID")
        .get(0)
}

pub(crate) fn states(client: &mut Client) -> Vec<(i64, i64)> {
    let scalar_partition = scalar_state_partition();
    client
        .query(
            "SELECT node_id, state_payload
             FROM shiba_internal.graph_node_state
             WHERE graph_id = 1 AND node_id IN (1, 2)
               AND namespace = 1
               AND partition_key_payload = $1 AND item_key_payload = $2
             ORDER BY node_id",
            &[&scalar_partition, &b"null".as_slice()],
        )
        .expect("query operator states")
        .into_iter()
        .map(|row| {
            let payload: Vec<u8> = row.get(1);
            let value = match payload.as_slice() {
                bytes if bytes.len() == 8 => i64::from_be_bytes(bytes.try_into().unwrap()),
                bytes if bytes.len() == 16 => i64::from_be_bytes(bytes[8..].try_into().unwrap()),
                _ => panic!("invalid aggregate state payload"),
            };
            (row.get(0), value)
        })
        .collect()
}

pub(crate) fn scalar_state_partition() -> Vec<u8> {
    TypedValue::Bool(true)
        .to_canonical_json()
        .expect("canonical scalar state partition")
}

pub(crate) const fn scalar_state_item() -> &'static [u8] {
    b"null"
}

pub(crate) fn rows(client: &mut Client) -> Vec<(i64, Option<i64>)> {
    client
        .query(
            "SELECT source_row_id, payload_int8
             FROM shiba_internal.source_row_state ORDER BY source_row_id",
            &[],
        )
        .expect("query source state")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

pub(crate) fn checkpoint(client: &mut Client) -> (String, i64, Option<i64>) {
    let row = client
        .query_one(
            "SELECT bootstrap.phase, checkpoint.last_batch_ordinal,
                    checkpoint.last_source_row_id
             FROM shiba_internal.graph_bootstrap AS bootstrap
             JOIN shiba_internal.graph_bootstrap_checkpoint AS checkpoint USING (graph_id)
             WHERE bootstrap.graph_id = 1 AND checkpoint.source_id = 1",
            &[],
        )
        .expect("query bootstrap checkpoint");
    (row.get(0), row.get(1), row.get(2))
}

pub(crate) fn install_receiver_kill_trigger(client: &mut Client, target: &str, event: &str) {
    client
        .batch_execute(&format!(
            "CREATE FUNCTION public.kill_m11_receiver() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
               PERFORM pg_catalog.pg_terminate_backend(pid)
               FROM pg_catalog.pg_stat_replication
               WHERE application_name = '{APPLICATION}';
               RETURN NEW;
             END
             $body$;
             CREATE TRIGGER kill_m11_receiver {event} ON {target}
             FOR EACH ROW EXECUTE FUNCTION public.kill_m11_receiver();"
        ))
        .expect("install receiver failure injection");
}

pub(crate) fn remove_receiver_kill_trigger(client: &mut Client, target: &str) {
    client
        .batch_execute(&format!(
            "DROP TRIGGER kill_m11_receiver ON {target};
             DROP FUNCTION public.kill_m11_receiver();"
        ))
        .expect("remove receiver failure injection");
}
