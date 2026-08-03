use shiba_operator::{DeltaBatch, EffectOrigin};
use shiba_protocol::{BootstrapBatchId, BootstrapId, SourceId};

#[test]
fn bootstrap_delta_origin_is_distinct_and_strict() {
    let batch = DeltaBatch {
        origin: EffectOrigin::Bootstrap(
            BootstrapBatchId::new(BootstrapId::new(9).unwrap(), 2).unwrap(),
        ),
        layout_identity: [7; 32],
        rows: Vec::new(),
    };
    let encoded = serde_json::to_vec(&batch).unwrap();
    assert_eq!(
        serde_json::from_slice::<DeltaBatch>(&encoded).unwrap(),
        batch
    );
    assert!(!encoded.windows(9).any(|window| window == b"source_id"));
    assert!(SourceId::new(1).is_ok());
}
