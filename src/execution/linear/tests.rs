use super::*;

fn continuation(phase: ScanPhase) -> ScanContinuation {
    match phase {
        ScanPhase::Bootstrap => ScanContinuation {
            phase,
            input_stream_id: 1,
            input_chunk_seq: None,
            next_row_ordinal: None,
            next_bootstrap_seq: Some(1),
            pending_frontier_lsn: None,
        },
        ScanPhase::SnapshotFrontier => ScanContinuation {
            phase,
            input_stream_id: 1,
            input_chunk_seq: None,
            next_row_ordinal: None,
            next_bootstrap_seq: None,
            pending_frontier_lsn: Some(1),
        },
        ScanPhase::Data => ScanContinuation {
            phase,
            input_stream_id: 1,
            input_chunk_seq: Some(2),
            next_row_ordinal: Some(0),
            next_bootstrap_seq: None,
            pending_frontier_lsn: None,
        },
        ScanPhase::SourceFrontier => ScanContinuation {
            phase,
            input_stream_id: 1,
            input_chunk_seq: None,
            next_row_ordinal: None,
            next_bootstrap_seq: None,
            pending_frontier_lsn: Some(3),
        },
    }
}

#[test]
fn scan_phases_have_disjoint_persisted_shapes() {
    for phase in [
        ScanPhase::Bootstrap,
        ScanPhase::SnapshotFrontier,
        ScanPhase::Data,
        ScanPhase::SourceFrontier,
    ] {
        validate_scan_continuation(&continuation(phase)).unwrap();
    }
    let mut invalid = continuation(ScanPhase::SourceFrontier);
    invalid.next_row_ordinal = Some(0);
    assert!(validate_scan_continuation(&invalid).is_err());
}

#[test]
fn phase_decoder_rejects_old_text_or_unknown_codes() {
    assert_eq!(ScanPhase::decode(1), Ok(ScanPhase::Bootstrap));
    assert_eq!(ScanPhase::decode(2), Ok(ScanPhase::SnapshotFrontier));
    assert_eq!(ScanPhase::decode(3), Ok(ScanPhase::Data));
    assert_eq!(ScanPhase::decode(4), Ok(ScanPhase::SourceFrontier));
    assert!(ScanPhase::decode(0).is_err());
    assert!(ScanPhase::decode(5).is_err());
}
