use std::str::FromStr;

use shiba_protocol::{
    CatalogVersion, CauseId, IngressTransactionId, InputSequence, PostgresLsn, ProtocolVersion,
    SlotGeneration, SourceId, SourceTransactionId, WireEnvelope, WireMessage,
};

fn cause_envelope() -> WireEnvelope {
    let transaction = SourceTransactionId::new(
        SourceId::new(3).unwrap(),
        SlotGeneration::new(7).unwrap(),
        PostgresLsn::from_str("0/64").unwrap(),
        IngressTransactionId::new(11).unwrap(),
    )
    .unwrap();
    WireEnvelope::new(WireMessage::Cause(CauseId::new(
        transaction,
        InputSequence::new(2).unwrap(),
    )))
}

#[test]
fn cleanroom_versions_start_at_one_and_reject_zero_construction() {
    // Old evidence source:
    // /Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/lib/tests.rs
    // old test: protocol_version_is_v2
    // Decision: reject inherited value 2; clean-room wire/catalog both start at 1.
    assert_eq!(ProtocolVersion::INITIAL.get(), 1);
    assert_eq!(CatalogVersion::INITIAL.get(), 1);
    assert!(ProtocolVersion::new(0).is_err());
    assert!(CatalogVersion::new(0).is_err());
    assert!(serde_json::from_str::<ProtocolVersion>("0").is_err());
    assert!(serde_json::from_str::<CatalogVersion>("0").is_err());
}

#[test]
fn canonical_wire_vector_is_stable_and_roundtrips() {
    // Adopted evidence property from:
    // /Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/lib/tests.rs
    // old test: relation_canonical_input_is_stable_but_semantically_sensitive
    // and /Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/primitive.rs
    // old test: primitive_digest_is_stable_and_domain_separated
    // reproduce old evidence: cargo test -p shiba-protocol canonical
    let envelope = cause_envelope();
    let encoded = envelope.to_canonical_json().unwrap();
    assert_eq!(
        encoded,
        br#"{"protocol_version":1,"catalog_version":1,"message":{"kind":"cause","body":{"transaction":{"source_id":3,"slot_generation":7,"commit_lsn":"0/64","ingress_transaction_id":11},"input_sequence":2}}}"#
    );
    assert_eq!(WireEnvelope::from_json(&encoded).unwrap(), envelope);
    assert_eq!(envelope.to_canonical_json().unwrap(), encoded);
    assert_eq!(
        envelope.digest().unwrap().to_hex(),
        "82b80d7d38e26756d89e9d390525b1f57189f4a002d75603892d8f0d7c382b39"
    );
    assert_eq!(envelope.digest().unwrap().as_bytes().len(), 32);

    let WireMessage::Cause(mut cause) = envelope.message() else {
        unreachable!();
    };
    cause.input_sequence = InputSequence::new(3).unwrap();
    let changed = WireEnvelope::new(WireMessage::Cause(cause));
    assert_ne!(changed.to_canonical_json().unwrap(), encoded);
    assert_ne!(changed.digest().unwrap(), envelope.digest().unwrap());
}

#[test]
fn every_phase_one_message_roundtrips() {
    let transaction = match cause_envelope().message() {
        WireMessage::Cause(cause) => cause.transaction,
        _ => unreachable!(),
    };
    let frontier = shiba_protocol::CommitFrontier::new(
        transaction.source_id,
        transaction.slot_generation,
        transaction.commit_lsn,
    )
    .unwrap();
    for message in [
        WireMessage::SourceTransaction(transaction),
        WireMessage::Cause(CauseId::new(transaction, InputSequence::new(1).unwrap())),
        WireMessage::CommitFrontier(frontier),
    ] {
        let envelope = WireEnvelope::new(message);
        assert_eq!(
            WireEnvelope::from_json(&envelope.to_canonical_json().unwrap()).unwrap(),
            envelope
        );
    }
}

#[test]
fn envelope_rejects_zero_and_unknown_versions() {
    let encoded = String::from_utf8(cause_envelope().to_canonical_json().unwrap()).unwrap();
    for (needle, replacement) in [
        ("\"protocol_version\":1", "\"protocol_version\":0"),
        ("\"protocol_version\":1", "\"protocol_version\":2"),
        ("\"catalog_version\":1", "\"catalog_version\":0"),
        ("\"catalog_version\":1", "\"catalog_version\":2"),
    ] {
        assert!(WireEnvelope::from_json(encoded.replace(needle, replacement).as_bytes()).is_err());
    }
}

#[test]
fn every_wire_level_rejects_unknown_fields_and_aliases() {
    let mut root: serde_json::Value =
        serde_json::from_slice(&cause_envelope().to_canonical_json().unwrap()).unwrap();
    root.as_object_mut()
        .unwrap()
        .insert("extra".into(), true.into());
    assert!(serde_json::from_value::<WireEnvelope>(root).is_err());

    let mut message: serde_json::Value =
        serde_json::from_slice(&cause_envelope().to_canonical_json().unwrap()).unwrap();
    message["message"]
        .as_object_mut()
        .unwrap()
        .insert("extra".into(), true.into());
    assert!(serde_json::from_value::<WireEnvelope>(message).is_err());

    let mut body: serde_json::Value =
        serde_json::from_slice(&cause_envelope().to_canonical_json().unwrap()).unwrap();
    body["message"]["body"]
        .as_object_mut()
        .unwrap()
        .insert("input_seq".into(), 2.into());
    assert!(serde_json::from_value::<WireEnvelope>(body).is_err());
}

#[test]
fn message_kind_is_closed_and_trailing_data_is_rejected() {
    let encoded = String::from_utf8(cause_envelope().to_canonical_json().unwrap()).unwrap();
    assert!(
        WireEnvelope::from_json(encoded.replace("\"cause\"", "\"effect\"").as_bytes()).is_err()
    );
    assert!(WireEnvelope::from_json(format!("{encoded} null").as_bytes()).is_err());
}
