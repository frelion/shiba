use shiba_ingress::{CommittedAssembler, IngressError};

const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;
const END_LSN: u64 = 0x1234;

fn begin() -> Vec<u8> {
    let mut frame = vec![b'B'];
    frame.extend_from_slice(&1_u64.to_be_bytes());
    frame.extend_from_slice(&2_u64.to_be_bytes());
    frame.extend_from_slice(&3_u32.to_be_bytes());
    frame
}

fn relation() -> Vec<u8> {
    let mut frame = vec![b'R'];
    frame.extend_from_slice(&7_u32.to_be_bytes());
    frame.extend_from_slice(b"source\0events\0");
    frame.push(b'd');
    frame.extend_from_slice(&1_u16.to_be_bytes());
    frame.push(1);
    frame.extend_from_slice(b"id\0");
    frame.extend_from_slice(&20_u32.to_be_bytes());
    frame.extend_from_slice(&u32::MAX.to_be_bytes());
    frame
}

fn insert(value: &[u8]) -> Vec<u8> {
    let mut frame = vec![b'I'];
    frame.extend_from_slice(&7_u32.to_be_bytes());
    frame.push(b'N');
    frame.extend_from_slice(&1_u16.to_be_bytes());
    frame.push(b't');
    frame.extend_from_slice(
        &u32::try_from(value.len())
            .expect("small value")
            .to_be_bytes(),
    );
    frame.extend_from_slice(value);
    frame
}

fn commit(end_lsn: u64) -> Vec<u8> {
    let mut frame = vec![b'C', 0];
    frame.extend_from_slice(&(end_lsn - 1).to_be_bytes());
    frame.extend_from_slice(&end_lsn.to_be_bytes());
    frame.extend_from_slice(&2_u64.to_be_bytes());
    frame
}

fn transaction(value: &[u8], end_lsn: u64) -> Vec<u8> {
    [begin(), relation(), insert(value), commit(end_lsn)].concat()
}

#[test]
fn every_split_and_one_byte_chunks_assemble_exactly() {
    let wire = transaction(b"41", END_LSN);
    for split in 0..wire.len() {
        let mut assembler = CommittedAssembler::new();
        assert_eq!(assembler.push(&wire[..split]).unwrap(), None);
        let completed = assembler
            .push(&wire[split..])
            .unwrap()
            .expect("complete at second chunk");
        assert_eq!(completed.bytes, wire);
        assert_eq!(completed.end_lsn, END_LSN);
    }

    let mut assembler = CommittedAssembler::new();
    for byte in &wire[..wire.len() - 1] {
        assert_eq!(assembler.push(&[*byte]).unwrap(), None);
    }
    let completed = assembler
        .push(&wire[wire.len() - 1..])
        .unwrap()
        .expect("complete after final byte");
    assert_eq!(completed.bytes, wire);
}

#[test]
fn coalesced_transactions_remain_bounded_pending() {
    let first = transaction(b"1", 11);
    let second = transaction(b"2", 22);
    let mut assembler = CommittedAssembler::new();
    let completed = assembler
        .push(&[first.clone(), second.clone()].concat())
        .unwrap()
        .expect("first transaction");
    assert_eq!(completed.bytes, first);
    assert_eq!(completed.end_lsn, 11);
    let completed = assembler
        .push(&[])
        .unwrap()
        .expect("pending second transaction");
    assert_eq!(completed.bytes, second);
    assert_eq!(completed.end_lsn, 22);
}

#[test]
fn bad_order_and_unknown_frames_fail_closed_and_reset() {
    let mut assembler = CommittedAssembler::new();
    assert!(matches!(
        assembler.push(&commit(4)),
        Err(IngressError::MessageOrder)
    ));
    assert!(matches!(
        assembler.push(b"X"),
        Err(IngressError::InvalidFrame)
    ));
    let nested = [begin(), begin()].concat();
    assert!(matches!(
        assembler.push(&nested),
        Err(IngressError::MessageOrder)
    ));
    let wire = transaction(b"9", 9);
    assert_eq!(assembler.push(&wire).unwrap().unwrap().bytes, wire);
}

#[test]
fn pending_input_is_hard_bounded_at_16_mib() {
    let mut at_limit = begin();
    at_limit.push(b'R');
    at_limit.extend_from_slice(&7_u32.to_be_bytes());
    at_limit.resize(MAX_TRANSACTION_BYTES, b'a');
    let mut assembler = CommittedAssembler::new();
    assert_eq!(assembler.push(&at_limit).unwrap(), None);
    assert!(matches!(
        assembler.push(&[0]),
        Err(IngressError::LimitExceeded)
    ));

    let mut assembler = CommittedAssembler::new();
    assert!(matches!(
        assembler.push(&vec![0; MAX_TRANSACTION_BYTES + 1]),
        Err(IngressError::LimitExceeded)
    ));
}
