use postgres::Client;

use crate::support::{PgoutputCapture, keyed_int8_results};

pub(crate) const SINGLE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m14-graph-runtime.sh",
    env_prefix: "SHIBA_M14_GRAPH_RUNTIME",
    slot: "shiba_m14_single_slot",
    publication: "shiba_m14_single_pub",
};
pub(crate) const JOIN: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m14-graph-runtime.sh",
    env_prefix: "SHIBA_M14_GRAPH_RUNTIME",
    slot: "shiba_m14_join_slot",
    publication: "shiba_m14_join_pub",
};

pub(crate) fn oid(client: &mut Client, object: &str) -> u32 {
    u32::try_from(
        client
            .query_one(&format!("SELECT '{object}'::regclass::oid::bigint"), &[])
            .expect("read object OID")
            .get::<_, i64>(0),
    )
    .expect("OID fits u32")
}

pub(crate) fn configure(client: &mut Client, graph_id: i64, publication: &str, slot: &str) {
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

pub(crate) fn join_rows(client: &mut Client) -> Vec<(i64, Option<i64>)> {
    keyed_int8_results(client, 2, 2)
        .into_iter()
        .map(|(key, value)| (key.expect("join key is non-null"), value))
        .collect()
}

pub(crate) fn durable_join(client: &mut Client) -> (Vec<(i64, Option<i64>)>, String, i64) {
    let source: String = client
        .query_one(
            "SELECT COALESCE(jsonb_agg(to_jsonb(row_state)
                     ORDER BY source_id, source_row_id), '[]')::text
             FROM (SELECT source_id, source_row_id, payload_int8
                   FROM shiba_internal.source_row_state
                   WHERE source_id IN (2,3)) AS row_state",
            &[],
        )
        .expect("query current source rows")
        .get(0);
    let continuation = client
        .query_one(
            "SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id = 2",
            &[],
        )
        .expect("query graph continuation")
        .get(0);
    (join_rows(client), source, continuation)
}

pub(crate) fn assert_identity_binding(client: &mut Client, source_id: i64, expected_index: u32) {
    let rows = client
        .query(
            "SELECT address_classid::bigint, address_objid::bigint, address_objsubid
             FROM shiba_internal.source_binding
             WHERE source_id = $1 AND binding_kind = 'identity_index'",
            &[&source_id],
        )
        .expect("query durable identity-index binding");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), i64::from(oid(client, "pg_class")));
    assert_eq!(rows[0].get::<_, i64>(1), i64::from(expected_index));
    assert_eq!(rows[0].get::<_, i32>(2), 0);
}
