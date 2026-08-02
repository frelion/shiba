use shiba_ingress::{ReplicationMessage, parse_replication_message};

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
