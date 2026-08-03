use std::{
    hint::black_box,
    process::Command,
    time::{Duration, Instant},
};

use shiba_sql_frontend::parse_sql;

const SAMPLES: usize = 10_000;
const MEDIAN_LIMIT: Duration = Duration::from_millis(1);
const P95_LIMIT: Duration = Duration::from_millis(5);
const MAX_INPUT_LIMIT: Duration = Duration::from_millis(20);
const MAX_SQL_BYTES: usize = 64 * 1024;
const MAX_FRONTEND_RSS_GROWTH_KIB: u64 = 4 * 1024;
const MAX_FRONTEND_HEAP_BYTES: usize = 4 * 1024 * 1024;
// Conservative no-instrumentation model: input plus bounded tokenizer, AST,
// expression and two canonical buffers. Per-entry allowances intentionally
// exceed current stack metadata and charge owned identifier bytes again.
const MODELED_FRONTEND_HEAP_BYTES: usize =
    MAX_SQL_BYTES + 2_048 * 256 + 2_048 * 1_024 + 256 * 512 + 2 * 256 * 1_024;
const _: () = assert!(MODELED_FRONTEND_HEAP_BYTES <= MAX_FRONTEND_HEAP_BYTES);

const QUERIES: [&str; 8] = [
    "SELECT e.id,e.payload FROM source.events AS e",
    "SELECT id,payload+7 FROM source.events WHERE payload>0 AND id<=99",
    "SELECT count(*) FROM source.events",
    "SELECT sum(payload) FROM source.events",
    "SELECT id,count(*) FROM source.events WHERE payload>0 GROUP BY id",
    "SELECT payload,sum(id) FROM source.events GROUP BY payload",
    "SELECT l.id,r.payload FROM left_source.events l INNER JOIN right_source.events r ON l.right_key=r.id",
    "SELECT \"e\".\"id\",\"e\".\"payload\" FROM \"source\".\"events\" AS \"e\"",
];

#[test]
#[ignore = "requires scripts/test-m15-sql-performance.sh release-mode gate"]
fn bounded_sql_parse_and_normalize_meets_frozen_latency_and_rss() {
    for sql in QUERIES {
        normalize(sql);
    }
    let mut elapsed = Vec::with_capacity(SAMPLES);
    for ordinal in 0..SAMPLES {
        let started = Instant::now();
        normalize(black_box(QUERIES[ordinal % QUERIES.len()]));
        elapsed.push(started.elapsed());
    }
    elapsed.sort_unstable();
    let median = percentile(&elapsed, 50);
    let p95 = percentile(&elapsed, 95);
    assert_within("parse+normalize median", median, MEDIAN_LIMIT);
    assert_within("parse+normalize p95", p95, P95_LIMIT);

    let prefix = "SELECT id,payload FROM source.events /*";
    let suffix = "*/";
    let mut maximum = String::with_capacity(MAX_SQL_BYTES);
    maximum.push_str(prefix);
    maximum.push_str(&"x".repeat(MAX_SQL_BYTES - prefix.len() - suffix.len()));
    maximum.push_str(suffix);
    assert_eq!(maximum.len(), MAX_SQL_BYTES);
    let rss_before = rss_kib();
    let started = Instant::now();
    normalize(black_box(&maximum));
    let maximum_elapsed = started.elapsed();
    let rss_after = rss_kib();
    assert_within("maximum admitted SQL", maximum_elapsed, MAX_INPUT_LIMIT);
    let rss_growth = rss_before
        .zip(rss_after)
        .map(|(before, after)| after.saturating_sub(before));
    if let Some(growth) = rss_growth {
        assert!(
            growth <= MAX_FRONTEND_RSS_GROWTH_KIB,
            "maximum admitted frontend input grew RSS {growth} KiB, exceeding frozen {MAX_FRONTEND_RSS_GROWTH_KIB} KiB"
        );
    } else {
        eprintln!(
            "RSS unavailable; strict structural allocation model={MODELED_FRONTEND_HEAP_BYTES} bytes <= {MAX_FRONTEND_HEAP_BYTES} bytes"
        );
    }
    eprintln!(
        "M15 SQL frontend performance samples={SAMPLES} median={median:?} p95={p95:?} maximum_input_bytes={MAX_SQL_BYTES} maximum_elapsed={maximum_elapsed:?} modeled_heap_bytes={MODELED_FRONTEND_HEAP_BYTES} rss_before_kib={rss_before:?} rss_after_kib={rss_after:?} rss_growth_kib={rss_growth:?}"
    );
}

fn normalize(sql: &str) {
    let query = parse_sql(sql).expect("representative SQL must parse");
    black_box(query.canonical_payload().expect("normalize canonical SQL"));
    black_box(query.canonical_digest().expect("digest canonical SQL"));
}

fn percentile(sorted: &[Duration], percent: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percent).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

fn assert_within(label: &str, measured: Duration, limit: Duration) {
    assert!(
        measured <= limit,
        "{label} took {measured:?}, exceeding frozen {limit:?}"
    );
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
