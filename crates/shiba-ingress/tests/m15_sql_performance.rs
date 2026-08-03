use std::{
    process::Command,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_protocol::GraphId;
use shiba_sql_registration::compile_sql_and_register;

const WARMUPS: usize = 10;
const SAMPLES: usize = 200;
const REGISTRATION_P95_LIMIT: Duration = Duration::from_millis(25);

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m15-sql-performance.sh must set {name}"))
}

#[test]
#[ignore = "requires scripts/test-m15-sql-performance.sh release-mode PG gate"]
fn sql_bind_compile_and_atomic_registration_meets_frozen_p95() {
    let database_url = required("SHIBA_M15_SQL_PERFORMANCE_DATABASE_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect performance database");
    install_sources(&mut admin);

    for ordinal in 1..=WARMUPS {
        register(&mut admin, ordinal);
    }
    let rss_before = rss_kib();
    let mut elapsed = Vec::with_capacity(SAMPLES);
    for ordinal in (WARMUPS + 1)..=(WARMUPS + SAMPLES) {
        let started = Instant::now();
        register(&mut admin, ordinal);
        elapsed.push(started.elapsed());
    }
    let rss_after = rss_kib();
    elapsed.sort_unstable();
    let median = percentile(&elapsed, 50);
    let p95 = percentile(&elapsed, 95);
    assert!(
        p95 <= REGISTRATION_P95_LIMIT,
        "SQL bind/compile/registration p95 {p95:?} exceeds frozen {REGISTRATION_P95_LIMIT:?}"
    );
    let authority = admin
        .query_one(
            "SELECT (SELECT count(*) FROM shiba_internal.graph_definition),
                    (SELECT count(*) FROM shiba_internal.graph_source_member),
                    (SELECT count(*) FROM shiba.graph_result)",
            &[],
        )
        .expect("count complete registration authorities");
    let expected = i64::try_from(WARMUPS + SAMPLES).expect("sample count fits bigint");
    assert_eq!(authority.get::<_, i64>(0), expected);
    assert_eq!(authority.get::<_, i64>(1), expected);
    assert_eq!(authority.get::<_, i64>(2), expected);
    let rss_growth = rss_before
        .zip(rss_after)
        .map(|(before, after)| after.saturating_sub(before));
    eprintln!(
        "M15 SQL registration performance samples={SAMPLES} warmups={WARMUPS} median={median:?} p95={p95:?} limit={REGISTRATION_P95_LIMIT:?} rss_before_kib={rss_before:?} rss_after_kib={rss_after:?} rss_growth_kib={rss_growth:?}; fixture table/source creation excluded and no competing lock wait"
    );
}

fn install_sources(client: &mut Client) {
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA sql_performance;",
        )
        .expect("install SQL performance catalog");
    for ordinal in 1..=(WARMUPS + SAMPLES) {
        let source = i64::try_from(ordinal).expect("source ID fits bigint");
        client
            .batch_execute(&format!(
                "CREATE TABLE sql_performance.events_{ordinal} (
                     id bigint PRIMARY KEY,payload bigint NULL
                 );"
            ))
            .expect("create independent registration source");
        client
            .execute(
                "SELECT shiba_internal.register_source($1,$2::text::regclass)",
                &[&source, &format!("sql_performance.events_{ordinal}")],
            )
            .expect("pre-register independent source binding");
    }
}

fn register(client: &mut Client, ordinal: usize) {
    let graph = GraphId::new(u64::try_from(ordinal).expect("graph ID fits")).expect("graph ID");
    let sql = format!("SELECT e.id,e.payload FROM sql_performance.events_{ordinal} AS e");
    compile_sql_and_register(client, graph, &sql).expect("bind, compile and register graph");
}

fn percentile(sorted: &[Duration], percent: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percent).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

fn rss_kib() -> Option<u64> {
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
