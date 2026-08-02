use crate::{
    IngressError,
    assembler::MAX_TRANSACTION_BYTES,
    streamed::{StreamTerminal, StreamedAssembler},
};

const XID: u32 = 7;

fn start() -> Vec<u8> {
    let mut frame = vec![b'S'];
    frame.extend_from_slice(&XID.to_be_bytes());
    frame.push(1);
    frame
}

fn abort() -> Vec<u8> {
    let mut frame = vec![b'A'];
    frame.extend_from_slice(&XID.to_be_bytes());
    frame.extend_from_slice(&XID.to_be_bytes());
    frame
}

fn commit_fields(xid: u32, flags: u8, commit_lsn: u64, end_lsn: u64) -> Vec<u8> {
    let mut frame = vec![b'c'];
    frame.extend_from_slice(&xid.to_be_bytes());
    frame.push(flags);
    frame.extend_from_slice(&commit_lsn.to_be_bytes());
    frame.extend_from_slice(&end_lsn.to_be_bytes());
    frame.extend_from_slice(&12_u64.to_be_bytes());
    frame
}

fn commit(xid: u32) -> Vec<u8> {
    commit_fields(xid, 0, 10, 11)
}

fn empty_wire(segments: usize, xid: u32) -> Vec<u8> {
    let mut wire = Vec::new();
    for index in 0..segments {
        wire.push(b'S');
        wire.extend_from_slice(&xid.to_be_bytes());
        wire.push(u8::from(index == 0));
        wire.push(b'E');
    }
    wire.extend_from_slice(&commit(xid));
    wire
}

fn relation() -> Vec<u8> {
    let mut frame = vec![b'R'];
    frame.extend_from_slice(&XID.to_be_bytes());
    frame.extend_from_slice(&1_u32.to_be_bytes());
    frame.extend_from_slice(b"s\0t\0");
    frame.push(b'd');
    frame.extend_from_slice(&1_u16.to_be_bytes());
    frame.extend_from_slice(&[1, b'i', b'd', 0]);
    frame.extend_from_slice(&20_u32.to_be_bytes());
    frame.extend_from_slice(&u32::MAX.to_be_bytes());
    frame
}

fn insert() -> Vec<u8> {
    let mut frame = vec![b'I'];
    frame.extend_from_slice(&XID.to_be_bytes());
    frame.extend_from_slice(&1_u32.to_be_bytes());
    frame.extend_from_slice(&[b'N', 0, 1, b't']);
    frame.extend_from_slice(&1_u32.to_be_bytes());
    frame.push(b'1');
    frame
}

fn abort_lsn(terminal: &StreamTerminal) -> u64 {
    match terminal {
        StreamTerminal::Aborted { acknowledgment_lsn } => *acknowledgment_lsn,
        StreamTerminal::Committed(_) | StreamTerminal::EmptyCommitted { .. } => {
            panic!("expected abort")
        }
    }
}

#[test]
fn abort_origin_survives_coalesced_and_split_frames() {
    let mut coalesced = start();
    coalesced.push(b'E');
    coalesced.extend_from_slice(&abort());
    let terminal = StreamedAssembler::new()
        .push(100, &coalesced)
        .unwrap()
        .unwrap();
    assert_eq!(abort_lsn(&terminal), 100);

    let mut assembler = StreamedAssembler::new();
    let split = coalesced.len() - 7;
    assert!(assembler.push(100, &coalesced[..split]).unwrap().is_none());
    let terminal = assembler.push(200, &coalesced[split..]).unwrap().unwrap();
    assert_eq!(abort_lsn(&terminal), 100);

    let start = start();
    let mut assembler = StreamedAssembler::new();
    assert!(assembler.push(100, &start[..2]).unwrap().is_none());
    let mut rest = start[2..].to_vec();
    rest.push(b'E');
    rest.extend_from_slice(&abort());
    let terminal = assembler.push(200, &rest).unwrap().unwrap();
    assert_eq!(abort_lsn(&terminal), 200);
}

#[test]
fn strict_empty_commit_assembles_at_every_split() {
    for expected_segments in [1, 2, 8] {
        let wire = empty_wire(expected_segments, XID);
        for split in 1..wire.len() {
            let mut assembler = StreamedAssembler::new();
            assert!(assembler.push(100, &wire[..split]).unwrap().is_none());
            let terminal = assembler.push(200, &wire[split..]).unwrap().unwrap();
            match terminal {
                StreamTerminal::EmptyCommitted {
                    xid,
                    commit_lsn,
                    end_lsn,
                    segment_count,
                } => assert_eq!(
                    (xid, commit_lsn, end_lsn, segment_count),
                    (XID, 10, 11, expected_segments)
                ),
                StreamTerminal::Committed(_) | StreamTerminal::Aborted { .. } => {
                    panic!("expected empty commit")
                }
            }
        }
    }

    let wire = empty_wire(3, XID);
    let mut assembler = StreamedAssembler::new();
    let mut terminal = None;
    for (index, byte) in wire.iter().enumerate() {
        terminal = assembler
            .push(
                u64::try_from(index + 1).unwrap(),
                std::slice::from_ref(byte),
            )
            .unwrap();
        if index + 1 < wire.len() {
            assert!(terminal.is_none());
        }
    }
    assert!(matches!(
        terminal,
        Some(StreamTerminal::EmptyCommitted {
            segment_count: 3,
            ..
        })
    ));
}

#[test]
fn relation_only_and_relation_insert_are_nonempty() {
    let mut relation_insert = relation();
    relation_insert.extend_from_slice(&insert());
    for middle in [relation(), relation_insert] {
        let mut wire = start();
        wire.extend_from_slice(&middle);
        wire.push(b'E');
        wire.extend_from_slice(&commit(XID));
        assert!(matches!(
            StreamedAssembler::new().push(1, &wire).unwrap(),
            Some(StreamTerminal::Committed(_))
        ));
    }
}

#[test]
fn many_empty_segments_use_constant_state() {
    let wire = empty_wire(10_000, XID);
    let terminal = StreamedAssembler::new().push(1, &wire).unwrap().unwrap();
    assert!(matches!(
        terminal,
        StreamTerminal::EmptyCommitted {
            segment_count: 10_000,
            ..
        }
    ));
}

#[test]
fn malformed_empty_commit_never_returns_empty_token() {
    let invalid_commits = [
        commit_fields(XID, 1, 10, 11),
        commit_fields(XID, 0, 0, 11),
        commit_fields(XID, 0, 12, 11),
    ];
    for commit in invalid_commits {
        let mut wire = start();
        wire.push(b'E');
        wire.extend_from_slice(&commit);
        assert!(StreamedAssembler::new().push(1, &wire).is_err());
    }

    let mut trailing = start();
    trailing.push(b'E');
    trailing.extend_from_slice(&commit(XID));
    trailing.push(b'Z');
    assert!(StreamedAssembler::new().push(1, &trailing).is_err());

    let mut missing = start();
    missing.push(b'E');
    assert!(
        StreamedAssembler::new()
            .push(1, &missing)
            .unwrap()
            .is_none()
    );
}

#[test]
fn corrupt_xid_terminal_order_and_limit_fail_closed() {
    assert!(matches!(
        StreamedAssembler::new().push(1, b"Z"),
        Err(IngressError::InvalidFrame)
    ));

    let mut wrong_xid = start();
    wrong_xid.push(b'E');
    wrong_xid.extend_from_slice(&commit(XID + 1));
    assert!(matches!(
        StreamedAssembler::new().push(1, &wrong_xid),
        Err(IngressError::MessageOrder)
    ));

    let mut mixed_segment = empty_wire(2, XID);
    mixed_segment[8..12].copy_from_slice(&(XID + 1).to_be_bytes());
    assert!(matches!(
        StreamedAssembler::new().push(1, &mixed_segment),
        Err(IngressError::MessageOrder)
    ));

    let mut zero_xid = start();
    zero_xid[1..5].fill(0);
    assert!(matches!(
        StreamedAssembler::new().push(1, &zero_xid),
        Err(IngressError::MessageOrder)
    ));

    let mut wrong_first = start();
    wrong_first[5] = 0;
    assert!(matches!(
        StreamedAssembler::new().push(1, &wrong_first),
        Err(IngressError::MessageOrder)
    ));

    let mut wrong_continuation = empty_wire(2, XID);
    wrong_continuation[12] = 1;
    assert!(matches!(
        StreamedAssembler::new().push(1, &wrong_continuation),
        Err(IngressError::MessageOrder)
    ));

    for tag in [b'U', b'D', b'T', b'M', b'O'] {
        let mut unsupported = start();
        unsupported.push(tag);
        assert!(matches!(
            StreamedAssembler::new().push(1, &unsupported),
            Err(IngressError::InvalidFrame)
        ));
    }

    let mut extra_stop = start();
    extra_stop.extend_from_slice(b"EE");
    assert!(matches!(
        StreamedAssembler::new().push(1, &extra_stop),
        Err(IngressError::MessageOrder)
    ));

    let mut wrong_terminal = start();
    wrong_terminal.extend_from_slice(&commit(XID));
    assert!(matches!(
        StreamedAssembler::new().push(1, &wrong_terminal),
        Err(IngressError::MessageOrder)
    ));

    let oversized = vec![b'S'; MAX_TRANSACTION_BYTES + 1];
    assert!(matches!(
        StreamedAssembler::new().push(1, &oversized),
        Err(IngressError::LimitExceeded)
    ));

    let mut exact = start();
    exact.push(b'R');
    exact.extend_from_slice(&XID.to_be_bytes());
    exact.extend_from_slice(&1_u32.to_be_bytes());
    exact.resize(MAX_TRANSACTION_BYTES, b'x');
    let mut assembler = StreamedAssembler::new();
    assert!(assembler.push(1, &exact).unwrap().is_none());
    assert!(matches!(
        assembler.push(2, b"x"),
        Err(IngressError::LimitExceeded)
    ));
}
