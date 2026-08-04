use std::process::Command;

use postgres::Client;

pub(crate) fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("M12 rebuild performance gate must set {name}"))
}

pub(crate) fn rss_kib() -> Option<u64> {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then_some(output.stdout)?
        .split(u8::is_ascii_whitespace)
        .find(|field| !field.is_empty())
        .and_then(|field| std::str::from_utf8(field).ok())
        .and_then(|field| field.parse().ok())
}

pub(crate) fn retained_wal_bytes(client: &mut Client, slot: &str) -> i64 {
    client
        .query_one(
            "SELECT pg_catalog.pg_wal_lsn_diff(
                        pg_catalog.pg_current_wal_insert_lsn(), restart_lsn
                    )::bigint
             FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .expect("measure target-slot retained WAL")
        .get(0)
}

pub(crate) fn assert_building(client: &mut Client) {
    let rows = client
        .query(
            "SELECT result_status, NULL::bigint
             FROM shiba.graph_result WHERE graph_id = 1 AND result_id IN (3, 4) ORDER BY result_id",
            &[],
        )
        .expect("query rebuild result visibility");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| {
        row.get::<_, &str>(0) == "building" && row.get::<_, Option<i64>>(1).is_none()
    }));
}

pub(crate) fn assert_differential(client: &mut Client) -> (i64, i64) {
    let oracle = client
        .query_one(
            "SELECT count(*)::bigint, COALESCE(sum(payload), 0)::bigint
             FROM target.events",
            &[],
        )
        .expect("query target SQL oracle");
    let expected = (oracle.get::<_, i64>(0), oracle.get::<_, i64>(1));
    let result = client
        .query(
            "SELECT result.result_status,
                    (SELECT (convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,value}')::bigint
                     FROM shiba.graph_result_rows row WHERE row.graph_id=result.graph_id AND row.result_id=result.result_id)
             FROM shiba.graph_result result WHERE graph_id = 1 AND result_id IN (3, 4) ORDER BY result_id",
            &[],
        )
        .expect("query rebuilt public results");
    assert_eq!(
        result
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, Option<i64>>(1)))
            .collect::<Vec<_>>(),
        vec![
            ("active".to_owned(), Some(expected.0)),
            ("active".to_owned(), Some(expected.1)),
        ]
    );
    let state = client
        .query_one(
            "SELECT count(*)::bigint, COALESCE(sum(payload_int8), 0)::bigint
             FROM shiba_internal.source_row_state WHERE source_id = 1",
            &[],
        )
        .expect("query rebuilt current-row authority");
    assert_eq!((state.get(0), state.get(1)), expected);
    expected
}
