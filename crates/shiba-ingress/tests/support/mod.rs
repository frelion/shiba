use std::{
    thread,
    time::{Duration, Instant},
};

use postgres::Client;

pub fn slot_lsn(client: &mut Client, slot: &str) -> u64 {
    let value: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .expect("read slot position")
        .get(0);
    parse_lsn(&value)
}

pub fn wait_for_slot_lsn(client: &mut Client, slot: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let actual = slot_lsn(client, slot);
        if actual == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "slot position {actual:#x} did not reach exact durable LSN {expected:#x}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

pub fn wait_for_keepalive_reply(client: &mut Client, application: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let observed = client
            .query_opt(
                "SELECT write_lsn::text, flush_lsn::text, replay_lsn::text,
                        reply_time IS NOT NULL
                 FROM pg_stat_replication WHERE application_name = $1",
                &[&application],
            )
            .expect("query replication feedback")
            .and_then(|row| {
                let replied: bool = row.get(3);
                let write = row.get::<_, Option<String>>(0)?;
                let flush = row.get::<_, Option<String>>(1)?;
                let replay = row.get::<_, Option<String>>(2)?;
                replied.then(|| (parse_lsn(&write), parse_lsn(&flush), parse_lsn(&replay)))
            });
        if observed == Some((expected, expected, expected)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "requested keepalive did not report only durable LSN {expected:#x}; observed {observed:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn parse_lsn(value: &str) -> u64 {
    let (high, low) = value.split_once('/').expect("PostgreSQL LSN has slash");
    (u64::from_str_radix(high, 16).expect("valid high LSN") << 32)
        | u64::from_str_radix(low, 16).expect("valid low LSN")
}
