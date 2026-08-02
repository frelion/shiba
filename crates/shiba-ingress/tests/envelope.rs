use std::time::{Duration, UNIX_EPOCH};

use shiba_ingress::{ReplicationMessage, encode_feedback, parse_replication_message};

#[test]
fn xlog_data_preserves_raw_pgoutput() {
    let mut frame = vec![b'w'];
    frame.extend_from_slice(&0x10_u64.to_be_bytes());
    frame.extend_from_slice(&0x20_u64.to_be_bytes());
    frame.extend_from_slice(&0x30_u64.to_be_bytes());
    frame.extend_from_slice(b"BRIC");
    assert_eq!(
        parse_replication_message(&frame).expect("decode XLogData"),
        ReplicationMessage::XLogData {
            wal_start: 0x10,
            wal_end: 0x20,
            server_time_micros: 0x30,
            data: b"BRIC",
        }
    );
}

#[test]
fn keepalive_requires_exact_length_and_boolean_flag() {
    let mut frame = vec![b'k'];
    frame.extend_from_slice(&0x40_u64.to_be_bytes());
    frame.extend_from_slice(&0x50_u64.to_be_bytes());
    frame.push(1);
    assert_eq!(
        parse_replication_message(&frame).expect("decode keepalive"),
        ReplicationMessage::Keepalive {
            wal_end: 0x40,
            server_time_micros: 0x50,
            reply_requested: true,
        }
    );
    frame[17] = 2;
    assert!(parse_replication_message(&frame).is_err());
    frame.push(0);
    assert!(parse_replication_message(&frame).is_err());
}

#[test]
fn invalid_frames_fail_closed() {
    assert!(parse_replication_message(&[]).is_err());
    assert!(parse_replication_message(b"x").is_err());
    assert!(parse_replication_message(b"w").is_err());
    assert!(parse_replication_message(&[b'k'; 17]).is_err());
}

#[test]
fn feedback_reports_one_exact_durable_coordinate() {
    let durable_lsn = 0x1234_5678_9abc_def0;
    let postgres_epoch = UNIX_EPOCH + Duration::from_hours(262_968);
    let feedback = encode_feedback(durable_lsn, postgres_epoch + Duration::from_micros(42))
        .expect("encode deterministic feedback");

    assert_eq!(feedback.len(), 34);
    assert_eq!(feedback[0], b'r');
    assert_eq!(&feedback[1..9], &durable_lsn.to_be_bytes());
    assert_eq!(&feedback[9..17], &durable_lsn.to_be_bytes());
    assert_eq!(&feedback[17..25], &durable_lsn.to_be_bytes());
    assert_eq!(&feedback[25..33], &42_i64.to_be_bytes());
    assert_eq!(
        feedback[33], 0,
        "periodic feedback does not request a reply"
    );
}

#[test]
fn feedback_rejects_time_before_postgres_epoch() {
    let before_postgres_epoch = UNIX_EPOCH + Duration::from_secs(946_684_799);
    assert!(encode_feedback(1, before_postgres_epoch).is_err());
}
