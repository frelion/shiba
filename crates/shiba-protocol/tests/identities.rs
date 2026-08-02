use std::str::FromStr;

use shiba_protocol::{
    CauseId, CommitFrontier, IngressTransactionId, InputSequence, PostgresLsn, ProtocolError,
    ScopeMismatch, SlotGeneration, SourceId, SourceTransactionId,
};

fn transaction(source: u64, generation: u64, lsn: &str, ingress: u64) -> SourceTransactionId {
    SourceTransactionId::new(
        SourceId::new(source).unwrap(),
        SlotGeneration::new(generation).unwrap(),
        PostgresLsn::from_str(lsn).unwrap(),
        IngressTransactionId::new(ingress).unwrap(),
    )
    .unwrap()
}

#[test]
fn strong_ids_reject_zero_and_preserve_ordering() {
    assert_eq!(SourceId::new(0), Err(ProtocolError::ZeroValue("source ID")));
    assert!(SlotGeneration::new(0).is_err());
    assert!(IngressTransactionId::new(0).is_err());
    assert!(InputSequence::new(0).is_err());
    assert!(SourceId::new(1).unwrap() < SourceId::new(u64::MAX).unwrap());

    for encoded in ["0", "-1", "null", "\"1\""] {
        assert!(serde_json::from_str::<SourceId>(encoded).is_err());
    }
}

#[test]
fn postgres_lsn_has_canonical_pg_text_and_numeric_order() {
    // PostgreSQL semantic reference command:
    // SELECT '16/B374D848'::pg_lsn::text, '0/FFFFFFFF'::pg_lsn < '1/0'::pg_lsn;
    let lsn = PostgresLsn::from_str("16/B374D848").unwrap();
    assert_eq!(lsn.to_string(), "16/B374D848");
    assert_eq!(lsn.as_u64(), 0x0000_0016_B374_D848);
    assert!(PostgresLsn::from_str("0/FFFFFFFF").unwrap() < PostgresLsn::from_str("1/0").unwrap());
    assert_eq!(serde_json::to_string(&lsn).unwrap(), "\"16/B374D848\"");
    assert_eq!(
        serde_json::from_str::<PostgresLsn>("\"16/B374D848\"").unwrap(),
        lsn
    );

    for invalid in [
        "",
        "16",
        "16/",
        "/1",
        "16/b374d848",
        "016/B374D848",
        "0/00",
        "G/1",
    ] {
        assert!(
            PostgresLsn::from_str(invalid).is_err(),
            "accepted {invalid:?}"
        );
    }
}

#[test]
fn source_transaction_rejects_zero_commit_lsn_and_unknown_fields() {
    // Adopted zero-value and strict-JSON evidence from:
    // /Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/lib/tests.rs
    // old test: cause_identity_and_frontier_semantics_are_distinct
    // reproduce: cargo test -p shiba-protocol cause_identity_and_frontier_semantics_are_distinct
    let result = SourceTransactionId::new(
        SourceId::new(1).unwrap(),
        SlotGeneration::new(1).unwrap(),
        PostgresLsn::ZERO,
        IngressTransactionId::new(1).unwrap(),
    );
    assert_eq!(result, Err(ProtocolError::ZeroCommitLsn));

    let unknown = r#"{"source_id":1,"slot_generation":1,"commit_lsn":"0/1","ingress_transaction_id":1,"xid":7}"#;
    assert!(serde_json::from_str::<SourceTransactionId>(unknown).is_err());
    let zero_lsn =
        r#"{"source_id":1,"slot_generation":1,"commit_lsn":"0/0","ingress_transaction_id":1}"#;
    assert!(serde_json::from_str::<SourceTransactionId>(zero_lsn).is_err());
}

#[test]
fn cause_identity_and_frontier_are_distinct_and_source_scoped() {
    // Adopted observable semantics from:
    // /Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/lib/tests.rs
    // old test: cause_identity_and_frontier_semantics_are_distinct
    // reproduce: cargo test -p shiba-protocol cause_identity_and_frontier_semantics_are_distinct
    let first = CauseId::new(
        transaction(3, 7, "0/64", 11),
        InputSequence::new(1).unwrap(),
    );
    let second = CauseId::new(
        transaction(3, 7, "0/64", 11),
        InputSequence::new(2).unwrap(),
    );
    assert_ne!(first, second);

    let frontier = CommitFrontier::new(
        SourceId::new(3).unwrap(),
        SlotGeneration::new(7).unwrap(),
        PostgresLsn::from_str("0/64").unwrap(),
    )
    .unwrap();
    assert_eq!(first.is_at_or_before(frontier), Ok(true));
    assert_eq!(frontier.covers(transaction(3, 7, "0/65", 12)), Ok(false));
    assert_eq!(
        frontier.covers(transaction(4, 7, "0/64", 11)),
        Err(ScopeMismatch::Source)
    );
    assert_eq!(
        frontier.covers(transaction(3, 8, "0/64", 11)),
        Err(ScopeMismatch::SlotGeneration)
    );
}

#[test]
fn identity_structures_roundtrip_and_reject_unknown_fields() {
    // Strict persistence evidence source:
    // /Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/primitive.rs
    // old test: persistence_rejects_unknown_fields
    // reproduce: cargo test -p shiba-protocol persistence_rejects_unknown_fields
    let cause = CauseId::new(transaction(9, 2, "A/20", 4), InputSequence::new(5).unwrap());
    let encoded = serde_json::to_string(&cause).unwrap();
    assert_eq!(serde_json::from_str::<CauseId>(&encoded).unwrap(), cause);

    let mut value = serde_json::to_value(cause).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("alias".into(), true.into());
    assert!(serde_json::from_value::<CauseId>(value).is_err());

    let frontier = CommitFrontier::new(
        SourceId::new(9).unwrap(),
        SlotGeneration::new(2).unwrap(),
        PostgresLsn::from_str("A/20").unwrap(),
    )
    .unwrap();
    let encoded = serde_json::to_string(&frontier).unwrap();
    assert_eq!(
        serde_json::from_str::<CommitFrontier>(&encoded).unwrap(),
        frontier
    );
}
