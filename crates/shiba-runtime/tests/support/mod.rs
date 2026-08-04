#![allow(
    dead_code,
    reason = "each integration test compiles only its support subset"
)]

use std::{fs, num::NonZeroU32, path::PathBuf, process::Command};

use postgres::{Client, GenericClient};
use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryAggregateCallV1, QueryExpressionV1, QueryFieldV1, QueryInputV1,
    QueryNodeV1, QueryOperationV1, QueryResultFieldV1, QueryResultV1, QuerySelectorV1, QuerySpecV1,
};
use shiba_operator::{
    AggregateFunctionV1, NodeId, ResultSchemaV1, TypedResultRowV1, TypedValue, ValueType,
};
use shiba_protocol::{GraphId, SourceId};
use shiba_runtime::{PgoutputGraph, PgoutputSource, compile_and_register};

pub(super) fn register_source(client: &mut Client, relation_name: &str) {
    client
        .query_one(
            "SELECT shiba_internal.register_source(1, $1::text::regclass)",
            &[&relation_name],
        )
        .expect("register source relation");
    register_count_operator(client, 1, 1);
}

pub(super) fn register_count_operator(client: &mut Client, source_id: u64, _operator_id: u64) {
    let source_id = SourceId::new(source_id).expect("non-zero source id");
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(source_id.get()).expect("source-backed graph ID"),
        sources: vec![source_id],
        nodes: vec![query_node(source_id, count_rows(), true)],
        results: vec![scalar_result(1, false)],
    };
    compile_and_register(client, &spec).expect("compile and register CountRows graph");
}

pub(super) fn register_count_sum_graph(client: &mut Client, source_id: u64) {
    let source_id = SourceId::new(source_id).expect("non-zero source id");
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(source_id.get()).expect("source-backed graph ID"),
        sources: vec![source_id],
        nodes: vec![
            query_node(source_id, count_rows(), true),
            query_node(source_id, sum_int8(column(0, "payload")), true),
        ],
        results: vec![scalar_result(1, false), scalar_result(2, true)],
    };
    compile_and_register(client, &spec).expect("compile and register CountRows + SumInt8 graph");
}

pub(super) fn count_rows() -> QueryOperationV1 {
    aggregate(AggregateFunctionV1::CountStar, None, Vec::new())
}

pub(super) fn sum_int8(expression: QueryExpressionV1) -> QueryOperationV1 {
    aggregate(AggregateFunctionV1::SumInt8, Some(expression), Vec::new())
}

pub(super) fn aggregate(
    function: AggregateFunctionV1,
    expression: Option<QueryExpressionV1>,
    group_expressions: Vec<QueryExpressionV1>,
) -> QueryOperationV1 {
    QueryOperationV1::Aggregate {
        group_expressions,
        calls: vec![QueryAggregateCallV1 {
            ordinal: 1,
            function,
            function_version: 1,
            expression,
        }],
        having: None,
    }
}

pub(super) fn singleton_graph(graph_id: u64, source: PgoutputSource) -> PgoutputGraph {
    PgoutputGraph::single(GraphId::new(graph_id).expect("non-zero graph ID"), source)
        .expect("valid singleton graph descriptor")
}

pub(super) fn configure_graph_ingress(
    client: &mut Client,
    graph_id: i64,
    publication: &str,
    slot: &str,
) {
    let publication_oid: u32 = client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .expect("read publication OID")
        .get(0);
    client
        .execute(
            "SELECT shiba_internal.configure_graph_ingress($1, $2::oid, $3::name, 1)",
            &[&graph_id, &publication_oid, &slot],
        )
        .expect("configure exact graph ingress");
}

pub(super) fn node_id(value: u64) -> NodeId {
    NodeId::new(NonZeroU32::new(u32::try_from(value).expect("node ID in u32")).expect("node ID"))
}

pub(super) fn source_input(source_id: SourceId) -> QueryInputV1 {
    QueryInputV1::Source { source_id }
}

pub(super) fn query_node(
    source_id: SourceId,
    operation: QueryOperationV1,
    stateful: bool,
) -> QueryNodeV1 {
    QueryNodeV1 {
        inputs: vec![source_input(source_id)],
        state_codec_version: stateful.then_some(1),
        operation,
    }
}

pub(super) fn field_name(input: u8, name: &str) -> QueryFieldV1 {
    QueryFieldV1 {
        input,
        selector: QuerySelectorV1::Name {
            name: name.into(),
            quoted: false,
        },
    }
}

pub(super) fn field_slot(input: u8, slot: u16) -> QueryFieldV1 {
    QueryFieldV1 {
        input,
        selector: QuerySelectorV1::Slot { slot },
    }
}

pub(super) fn column(input: u8, name: &str) -> QueryExpressionV1 {
    QueryExpressionV1::Column {
        field: field_name(input, name),
    }
}

pub(super) fn scalar_result(input_node: u16, nullable: bool) -> QueryResultV1 {
    QueryResultV1 {
        input_node,
        fields: vec![QueryResultFieldV1 {
            name: "value".into(),
            value_slot: 0,
            nullable,
        }],
        key_ordinals: vec![],
    }
}

pub(super) fn decode_scalar_state(payload: &[u8]) -> i64 {
    i64::from_be_bytes(
        payload
            .try_into()
            .expect("scalar operator state is exactly one big-endian i64"),
    )
}

pub(super) fn decode_optional_scalar_state(payload: Option<&[u8]>) -> i64 {
    payload.map_or(0, decode_scalar_state)
}

pub(super) fn scalar_state(client: &mut Client, operator_id: i64) -> i64 {
    let payload: Vec<u8> = client
        .query_one(
            "SELECT state_payload FROM shiba_internal.graph_node_state
             WHERE graph_id = 1 AND node_id = $1 AND namespace = 0",
            &[&operator_id],
        )
        .expect("query opaque scalar operator state")
        .get(0);
    decode_scalar_state(&payload)
}

pub(super) fn scalar_state_sum(client: &mut Client) -> i64 {
    client
        .query(
            "SELECT state_payload FROM shiba_internal.graph_node_state WHERE namespace = 0",
            &[],
        )
        .expect("query opaque scalar operator states")
        .into_iter()
        .map(|row| decode_scalar_state(&row.get::<_, Vec<u8>>(0)))
        .sum()
}

pub(super) fn canonical_result_rows(
    client: &mut impl GenericClient,
    graph_id: i64,
    result_id: i64,
) -> (ResultSchemaV1, Vec<TypedResultRowV1>) {
    let rows = client
        .query(
            "SELECT schema_payload, schema_digest, row_payload
             FROM shiba.graph_result_rows
             WHERE graph_id = $1 AND result_id = $2
             ORDER BY row_identity",
            &[&graph_id, &result_id],
        )
        .expect("query complete canonical result rows");
    let header = client
        .query_one(
            "SELECT schema_payload, schema_digest FROM shiba.graph_result
             WHERE graph_id = $1 AND result_id = $2",
            &[&graph_id, &result_id],
        )
        .expect("query canonical result schema");
    let schema_payload: Vec<u8> = header.get(0);
    let schema_digest = digest(header.get(1));
    let schema = ResultSchemaV1::from_canonical_payload(&schema_payload, schema_digest)
        .expect("validate canonical result schema");
    let decoded = rows
        .into_iter()
        .map(|row| {
            assert_eq!(row.get::<_, Vec<u8>>(0), schema_payload);
            assert_eq!(digest(row.get(1)), schema_digest);
            TypedResultRowV1::from_canonical_payload(&schema, &row.get::<_, Vec<u8>>(2))
                .expect("validate complete canonical result row")
        })
        .collect();
    (schema, decoded)
}

pub(super) fn scalar_int8_result(
    client: &mut impl GenericClient,
    graph_id: i64,
    result_id: i64,
) -> Option<i64> {
    let (schema, rows) = canonical_result_rows(client, graph_id, result_id);
    assert!(schema.is_scalar(), "scalar helper requires scalar schema");
    assert_eq!(rows.len(), 1, "scalar result has exactly one row");
    match rows[0].values.as_slice() {
        [TypedValue::Int8(value)] => Some(*value),
        [TypedValue::Null(_)] => None,
        values => panic!("scalar int8 result has unexpected values: {values:?}"),
    }
}

pub(super) fn set_scalar_int8_result(
    client: &mut impl GenericClient,
    graph_id: i64,
    result_id: i64,
    value: Option<i64>,
) {
    let (schema, rows) = canonical_result_rows(client, graph_id, result_id);
    assert!(schema.is_scalar(), "scalar helper requires scalar schema");
    assert_eq!(rows.len(), 1, "scalar result has exactly one row");
    let value = value.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8);
    let row = TypedResultRowV1::new(&schema, vec![value]).expect("encode scalar result row");
    let payload = row.to_canonical_payload().expect("canonical scalar row");
    assert_eq!(
        client
            .execute(
                "UPDATE shiba_internal.graph_result_row SET row_payload = $1
                 WHERE graph_id = $2 AND result_id = $3",
                &[&payload, &graph_id, &result_id],
            )
            .expect("inject scalar result state"),
        1
    );
}

pub(super) fn keyed_int8_results(
    client: &mut impl GenericClient,
    graph_id: i64,
    result_id: i64,
) -> Vec<(Option<i64>, Option<i64>)> {
    let (schema, rows) = canonical_result_rows(client, graph_id, result_id);
    assert_eq!(schema.key_ordinals, [1]);
    let mut decoded: Vec<_> = rows
        .into_iter()
        .map(|row| match row.values.as_slice() {
            [key, value] => (optional_int8(key), optional_int8(value)),
            values => panic!("keyed int8 result has unexpected values: {values:?}"),
        })
        .collect();
    decoded.sort();
    decoded
}

fn optional_int8(value: &TypedValue) -> Option<i64> {
    match value {
        TypedValue::Int8(value) => Some(*value),
        TypedValue::Null(_) => None,
        value => panic!("expected typed int8 or null, got {value:?}"),
    }
}

fn digest(bytes: Vec<u8>) -> [u8; 32] {
    bytes
        .try_into()
        .expect("result schema digest is exactly 32 bytes")
}

pub(super) struct PgoutputCapture {
    pub script: &'static str,
    pub env_prefix: &'static str,
    pub slot: &'static str,
    pub publication: &'static str,
}

impl PgoutputCapture {
    pub(super) fn required(&self, suffix: &str) -> String {
        let name = format!("{}_{suffix}", self.env_prefix);
        std::env::var(&name).unwrap_or_else(|_| panic!("{} must provide {name}", self.script))
    }

    pub(super) fn command(&self, name: &str) -> Command {
        let mut command = Command::new(PathBuf::from(self.required("PG_BINDIR")).join(name));
        command.args([
            "-h",
            &self.required("HOST"),
            "-p",
            &self.required("PORT"),
            "-U",
            &self.required("USER"),
            "-d",
            "postgres",
        ]);
        command
    }

    pub(super) fn create_slot(&self) {
        let status = self
            .command("pg_recvlogical")
            .args(["-S", self.slot, "-P", "pgoutput", "--create-slot"])
            .status()
            .expect("run pg_recvlogical --create-slot");
        assert!(status.success(), "create logical slot");
    }

    pub(super) fn capture(&self, client: &mut Client, name: &str) -> Vec<u8> {
        let end_lsn: String = client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .expect("read capture end LSN")
            .get(0);
        let output = self.capture_path(name);
        let status = self
            .command("pg_recvlogical")
            .args(["-S", self.slot, "--start", "-f"])
            .arg(&output)
            .args([
                "-n",
                "-E",
                &end_lsn,
                "-o",
                "proto_version=1",
                "-o",
                &format!("publication_names={}", self.publication),
            ])
            .status()
            .expect("capture pgoutput");
        assert!(status.success(), "capture through end LSN {end_lsn}");
        strip_recvlogical_delimiters(&fs::read(output).expect("read captured pgoutput"))
    }

    pub(super) fn capture_streamed(&self, client: &mut Client, name: &str) -> Vec<u8> {
        let end_lsn: String = client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .expect("read capture end LSN")
            .get(0);
        let output = self.capture_path(name);
        let status = self
            .command("pg_recvlogical")
            .args(["-S", self.slot, "--start", "-f"])
            .arg(&output)
            .args([
                "-n",
                "-E",
                &end_lsn,
                "-o",
                "proto_version=2",
                "-o",
                "streaming=on",
                "-o",
                &format!("publication_names={}", self.publication),
            ])
            .status()
            .expect("capture streamed pgoutput");
        assert!(
            status.success(),
            "capture streamed through end LSN {end_lsn}"
        );
        strip_streamed_delimiters(&fs::read(output).expect("read streamed pgoutput"))
    }

    pub(super) fn capture_path(&self, name: &str) -> PathBuf {
        PathBuf::from(self.required("CAPTURE_DIR")).join(name)
    }
}

// pg_recvlogical appends one newline per XLogData payload. Structural message
// lengths identify only that client framing, never tuple content.
pub(super) fn strip_recvlogical_delimiters(capture: &[u8]) -> Vec<u8> {
    assert!(
        framed_message_count(capture).is_some(),
        "incomplete client framing"
    );
    let mut wire = Vec::new();
    let mut start = 0;
    while start < capture.len() {
        let end = message_end(capture, start);
        assert_eq!(capture.get(end), Some(&b'\n'), "missing client delimiter");
        wire.extend_from_slice(&capture[start..end]);
        start = end + 1;
    }
    wire
}

pub(super) fn strip_streamed_delimiters(capture: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    let mut start = 0;
    let mut in_segment = false;
    while start < capture.len() {
        let tag = capture[start];
        let end = stream_message_end(capture, start, in_segment);
        assert_eq!(capture.get(end), Some(&b'\n'), "missing client delimiter");
        wire.extend_from_slice(&capture[start..end]);
        in_segment = match tag {
            b'S' => true,
            b'E' => false,
            _ => in_segment,
        };
        start = end + 1;
    }
    wire
}

pub(super) fn streamed_framed_terminal(capture: &[u8]) -> Option<u8> {
    let mut start = 0;
    let mut in_segment = false;
    let mut terminal = None;
    while start < capture.len() {
        let tag = *capture.get(start)?;
        let end = stream_message_end_checked(capture, start, in_segment)?;
        if capture.get(end) != Some(&b'\n') {
            return None;
        }
        in_segment = match tag {
            b'S' => true,
            b'E' => false,
            _ => in_segment,
        };
        terminal = Some(tag);
        start = end.checked_add(1)?;
    }
    terminal
}

pub(super) fn framed_message_count(capture: &[u8]) -> Option<usize> {
    let mut start = 0;
    let mut count = 0;
    while start < capture.len() {
        let end = message_end_checked(capture, start)?;
        if capture.get(end) != Some(&b'\n') {
            return None;
        }
        start = end.checked_add(1)?;
        count += 1;
    }
    Some(count)
}

pub(super) fn message_end(bytes: &[u8], start: usize) -> usize {
    message_end_checked(bytes, start).expect("complete supported pgoutput message")
}

pub(super) fn stream_message_end(bytes: &[u8], start: usize, in_segment: bool) -> usize {
    stream_message_end_checked(bytes, start, in_segment)
        .expect("complete supported streamed pgoutput message")
}

fn stream_message_end_checked(bytes: &[u8], start: usize, in_segment: bool) -> Option<usize> {
    let mut at = start.checked_add(1)?;
    match *bytes.get(start)? {
        b'S' => bounded_add(at, 5, bytes.len()),
        b'E' => Some(at),
        b'c' => bounded_add(at, 29, bytes.len()),
        b'A' => bounded_add(at, 8, bytes.len()),
        b'R' if in_segment => {
            at = bounded_add(at, 4, bytes.len())?;
            relation_end(bytes, at)
        }
        b'I' if in_segment => {
            at = bounded_add(at, 4, bytes.len())?;
            tuple_change_end(bytes, at)
        }
        _ => None,
    }
}

fn message_end_checked(bytes: &[u8], start: usize) -> Option<usize> {
    let at = start.checked_add(1)?;
    match *bytes.get(start)? {
        b'B' => bounded_add(at, 20, bytes.len()),
        b'C' => bounded_add(at, 25, bytes.len()),
        b'R' => relation_end(bytes, at),
        b'I' => insert_end(bytes, at),
        b'U' => update_end(bytes, at),
        b'D' => delete_end(bytes, at),
        _ => None,
    }
}

fn relation_end(bytes: &[u8], mut at: usize) -> Option<usize> {
    at = bounded_add(at, 4, bytes.len())?;
    at = cstring_end_checked(bytes, at)?;
    at = cstring_end_checked(bytes, at)?;
    at = bounded_add(at, 1, bytes.len())?;
    let columns = read_u16_checked(bytes, at)?;
    at = bounded_add(at, 2, bytes.len())?;
    for _ in 0..columns {
        at = bounded_add(at, 1, bytes.len())?;
        at = cstring_end_checked(bytes, at)?;
        at = bounded_add(at, 8, bytes.len())?;
    }
    Some(at)
}

fn tuple_change_end(bytes: &[u8], mut at: usize) -> Option<usize> {
    at = bounded_add(at, 5, bytes.len())?;
    tuple_end(bytes, at)
}

fn insert_end(bytes: &[u8], at: usize) -> Option<usize> {
    let tuple_tag = bounded_add(at, 4, bytes.len())?;
    if bytes.get(tuple_tag) != Some(&b'N') {
        return None;
    }
    tuple_end(bytes, bounded_add(tuple_tag, 1, bytes.len())?)
}

fn delete_end(bytes: &[u8], at: usize) -> Option<usize> {
    let tuple_tag = bounded_add(at, 4, bytes.len())?;
    if !matches!(bytes.get(tuple_tag), Some(b'K' | b'O')) {
        return None;
    }
    tuple_end(bytes, bounded_add(tuple_tag, 1, bytes.len())?)
}

fn update_end(bytes: &[u8], at: usize) -> Option<usize> {
    let mut tuple_tag = bounded_add(at, 4, bytes.len())?;
    if matches!(bytes.get(tuple_tag), Some(b'K' | b'O')) {
        tuple_tag = tuple_end(bytes, bounded_add(tuple_tag, 1, bytes.len())?)?;
    }
    if bytes.get(tuple_tag) != Some(&b'N') {
        return None;
    }
    tuple_end(bytes, bounded_add(tuple_tag, 1, bytes.len())?)
}

fn tuple_end(bytes: &[u8], mut at: usize) -> Option<usize> {
    let columns = read_u16_checked(bytes, at)?;
    at = bounded_add(at, 2, bytes.len())?;
    for _ in 0..columns {
        let kind = *bytes.get(at)?;
        at = bounded_add(at, 1, bytes.len())?;
        if matches!(kind, b't' | b'b') {
            let length = usize::try_from(read_u32_checked(bytes, at)?).ok()?;
            at = bounded_add(at, 4, bytes.len())?;
            at = bounded_add(at, length, bytes.len())?;
        } else if !matches!(kind, b'n' | b'u') {
            return None;
        }
    }
    Some(at)
}

fn bounded_add(at: usize, amount: usize, length: usize) -> Option<usize> {
    at.checked_add(amount).filter(|next| *next <= length)
}

fn cstring_end_checked(bytes: &[u8], start: usize) -> Option<usize> {
    let offset = bytes.get(start..)?.iter().position(|byte| *byte == 0)?;
    bounded_add(start, offset.checked_add(1)?, bytes.len())
}

pub(super) fn cstring_end(bytes: &[u8], start: usize) -> usize {
    cstring_end_checked(bytes, start).expect("terminated pgoutput string")
}

fn read_u16_checked(bytes: &[u8], at: usize) -> Option<u16> {
    let end = at.checked_add(2)?;
    Some(u16::from_be_bytes(bytes.get(at..end)?.try_into().ok()?))
}

fn read_u32_checked(bytes: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    Some(u32::from_be_bytes(bytes.get(at..end)?.try_into().ok()?))
}

pub(super) fn read_u16(bytes: &[u8], at: usize) -> u16 {
    read_u16_checked(bytes, at).expect("u16 field")
}

pub(super) fn read_u32(bytes: &[u8], at: usize) -> u32 {
    read_u32_checked(bytes, at).expect("u32 field")
}
