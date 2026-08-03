use shiba_operator::{EffectBatch, EffectOrigin, RowEffect, RowImage, Value};
use shiba_protocol::{BootstrapBatchId, BootstrapId};

#[test]
fn bootstrap_effect_origin_is_distinct_and_strict() {
    let batch = EffectBatch {
        origin: EffectOrigin::Bootstrap(
            BootstrapBatchId::new(BootstrapId::new(7).unwrap(), 3).unwrap(),
        ),
        effects: vec![RowEffect {
            before: None,
            after: Some(RowImage {
                source_row_id: Some(1),
                source_row_sub_id: None,
                payload: Value::Null,
            }),
        }],
    };
    let encoded = serde_json::to_string(&batch).unwrap();
    assert_eq!(
        serde_json::from_str::<EffectBatch>(&encoded).unwrap(),
        batch
    );
    let mut value = serde_json::to_value(batch).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("commit_lsn".into(), 1.into());
    assert!(serde_json::from_value::<EffectBatch>(value).is_err());
}
