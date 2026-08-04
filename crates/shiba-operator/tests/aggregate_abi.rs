use std::collections::BTreeSet;

use shiba_operator::{
    AGGREGATE_FUNCTION_SEMANTIC_VERSION, AGGREGATE_STATE_CODEC_VERSION, AggregateFunctionV1,
    AggregateInputContract, EmptyResultV1, ValueType, aggregate_function_canonical_payload,
    aggregate_function_descriptor, aggregate_function_digest,
};

const FUNCTIONS: [AggregateFunctionV1; 5] = [
    AggregateFunctionV1::CountStar,
    AggregateFunctionV1::Count,
    AggregateFunctionV1::SumInt8,
    AggregateFunctionV1::MinInt8,
    AggregateFunctionV1::MaxInt8,
];

#[test]
fn descriptors_are_the_unique_versioned_function_abi() {
    let mut payloads = BTreeSet::new();
    let mut digests = BTreeSet::new();
    for function in FUNCTIONS {
        let descriptor = aggregate_function_descriptor(function);
        assert_eq!(descriptor.function, function);
        assert_eq!(
            descriptor.semantic_version,
            AGGREGATE_FUNCTION_SEMANTIC_VERSION
        );
        assert_eq!(
            descriptor.state_codec_version,
            AGGREGATE_STATE_CODEC_VERSION
        );
        assert_eq!(descriptor.output_type, ValueType::Int8);
        assert!(descriptor.supports_retraction);
        assert!(payloads.insert(aggregate_function_canonical_payload(function).unwrap()));
        assert!(digests.insert(aggregate_function_digest(function).unwrap()));
    }
    assert_eq!(payloads.len(), FUNCTIONS.len());
    assert_eq!(digests.len(), FUNCTIONS.len());
}

#[test]
fn descriptors_freeze_input_nullability_and_empty_semantics() {
    let count_star = aggregate_function_descriptor(AggregateFunctionV1::CountStar);
    assert_eq!(count_star.input, AggregateInputContract::None);
    assert_eq!(count_star.empty_result, EmptyResultV1::Int8Zero);
    assert!(!count_star.output_nullable);

    let count = aggregate_function_descriptor(AggregateFunctionV1::Count);
    assert_eq!(
        count.input,
        AggregateInputContract::Nullable(ValueType::Int8)
    );
    assert_eq!(count.empty_result, EmptyResultV1::Int8Zero);
    assert!(!count.output_nullable);

    let sum = aggregate_function_descriptor(AggregateFunctionV1::SumInt8);
    assert_eq!(sum.input, AggregateInputContract::Nullable(ValueType::Int8));
    assert_eq!(sum.empty_result, EmptyResultV1::Null(ValueType::Int8));
    assert!(sum.output_nullable);

    for function in [AggregateFunctionV1::MinInt8, AggregateFunctionV1::MaxInt8] {
        let descriptor = aggregate_function_descriptor(function);
        assert_eq!(
            descriptor.input,
            AggregateInputContract::Nullable(ValueType::Int8)
        );
        assert_eq!(
            descriptor.empty_result,
            EmptyResultV1::Null(ValueType::Int8)
        );
        assert!(descriptor.output_nullable);
    }
}
