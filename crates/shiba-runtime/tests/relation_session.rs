use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    PgoutputError, PgoutputRelationState, PgoutputSource, decode_committed_changes,
    decode_committed_changes_in_session,
};

mod support;

fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn committed_wire(relation_id: u32, xid: u32, lsn: u64, include_relation: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(b'B');
    push_u64(&mut bytes, lsn);
    push_u64(&mut bytes, 7);
    push_u32(&mut bytes, xid);
    if include_relation {
        bytes.push(b'R');
        push_u32(&mut bytes, relation_id);
        bytes.extend_from_slice(b"source\0events\0");
        bytes.push(b'd');
        push_u16(&mut bytes, 1);
        bytes.push(1);
        bytes.extend_from_slice(b"id\0");
        push_u32(&mut bytes, 20);
        push_u32(&mut bytes, u32::MAX);
    }
    bytes.push(b'I');
    push_u32(&mut bytes, relation_id);
    bytes.push(b'N');
    push_u16(&mut bytes, 1);
    bytes.push(b't');
    push_u32(&mut bytes, 1);
    bytes.push(b'1');
    bytes.push(b'C');
    bytes.push(0);
    push_u64(&mut bytes, lsn);
    push_u64(&mut bytes, lsn + 1);
    push_u64(&mut bytes, 7);
    bytes
}

#[test]
fn connection_state_admits_omitted_relation_only_after_exact_validation() {
    let source = PgoutputSource::new(
        SourceId::new(1).expect("source ID"),
        SlotGeneration::new(1).expect("generation"),
        42,
    );
    let first = committed_wire(42, 1, 100, true);
    let second = committed_wire(42, 2, 200, false);

    assert_eq!(
        decode_committed_changes(&second, &support::singleton_graph(1, source)),
        Err(PgoutputError::MessageOrder),
        "a stateless capture cannot invent prior relation validation"
    );

    let mut state = PgoutputRelationState::new();
    let graph = support::singleton_graph(1, source);
    decode_committed_changes_in_session(&first, &graph, &mut state)
        .expect("validate the first exact relation descriptor");
    let decoded = decode_committed_changes_in_session(&second, &graph, &mut state)
        .expect("same connection may omit unchanged relation metadata");
    assert_eq!(decoded.changes.len(), 1);

    let wrong_source = PgoutputSource::new(
        SourceId::new(2).expect("other source ID"),
        SlotGeneration::new(1).expect("generation"),
        42,
    );
    let third = committed_wire(42, 4, 250, false);
    assert_eq!(
        decode_committed_changes_in_session(
            &third,
            &support::singleton_graph(1, wrong_source),
            &mut state,
        ),
        Err(PgoutputError::MessageOrder),
        "connection-local validation cannot be reused under another graph descriptor"
    );

    let changed_relation = committed_wire(43, 3, 300, true);
    assert_eq!(
        decode_committed_changes_in_session(&changed_relation, &graph, &mut state),
        Err(PgoutputError::RelationMismatch),
        "a repeated relation descriptor is always revalidated"
    );
}
