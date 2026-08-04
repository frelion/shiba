use shiba_compiler::{QueryAggregateCallV1, QueryNodeV1, QueryOperationV1, SourceColumnDescriptor};
use shiba_operator::aggregate_function_descriptor;

use crate::bind::{ResolvedSource, binding, source_column};
use crate::bind_aggregate::BoundCall;
use crate::bind_aggregate_support::{
    aggregate_span, grouped_columns, node_input, predicate_span, slot_expression, slot_for, slots,
    source_input,
};
use crate::bind_expression::{ExpressionType, lower};
use crate::{ErrorCode, FrontendError, UnboundQuery};

pub(crate) fn grouped_nodes(
    query: &UnboundQuery,
    source: &ResolvedSource,
    group: &SourceColumnDescriptor,
    group_ref: &crate::ColumnRef,
    calls: &[BoundCall<'_>],
) -> Result<Vec<QueryNodeV1>, FrontendError> {
    let values = calls.iter().map(|call| call.value).collect::<Vec<_>>();
    let columns = grouped_columns(query, source, group, &values)?;
    let slots = slots(&columns, query)?;
    let mut nodes = vec![QueryNodeV1 {
        inputs: vec![source_input(source)],
        state_codec_version: None,
        operation: QueryOperationV1::Project {
            expressions: columns.iter().map(|column| source_column(column)).collect(),
        },
    }];
    let mut input_node = 1u16;
    if let Some(predicate) = &query.selection {
        let predicate = lower(predicate, &source.descriptor, &slots)?;
        if predicate.value_type != ExpressionType::Bool {
            return Err(binding(ErrorCode::TypeMismatch, predicate_span(query)));
        }
        nodes.push(QueryNodeV1 {
            inputs: vec![node_input(input_node)],
            state_codec_version: None,
            operation: QueryOperationV1::Filter {
                predicate: predicate.expression,
            },
        });
        input_node += 1;
    }
    let group_slot = slot_for(&slots, group, group_ref.span)?;
    nodes.push(QueryNodeV1 {
        inputs: vec![node_input(input_node)],
        state_codec_version: None,
        operation: QueryOperationV1::KeyBy {
            key: slot_expression(group_slot),
        },
    });
    input_node += 1;
    let appended_key = u16::try_from(columns.len())
        .map_err(|_| binding(ErrorCode::QueryTooComplex, query.span))?;
    nodes.push(QueryNodeV1 {
        inputs: vec![node_input(input_node)],
        state_codec_version: Some(1),
        operation: QueryOperationV1::Aggregate {
            group_expressions: vec![slot_expression(appended_key)],
            calls: calls
                .iter()
                .enumerate()
                .map(|(index, call)| query_call_grouped(call, index, &slots, query.span))
                .collect::<Result<Vec<_>, _>>()?,
            having: query
                .having
                .as_ref()
                .map(|value| crate::bind_having::bind(value, calls, source))
                .transpose()?,
        },
    });
    Ok(nodes)
}

fn query_call_grouped(
    call: &BoundCall<'_>,
    index: usize,
    slots: &[(&SourceColumnDescriptor, u16)],
    span: crate::Span,
) -> Result<QueryAggregateCallV1, FrontendError> {
    let descriptor = aggregate_function_descriptor(call.function);
    let expression = call
        .value
        .map(|value| slot_for(slots, value, aggregate_span(call.aggregate)).map(slot_expression))
        .transpose()?;
    Ok(QueryAggregateCallV1 {
        ordinal: u16::try_from(index + 1).map_err(|_| binding(ErrorCode::QueryTooComplex, span))?,
        function: call.function,
        function_version: descriptor.semantic_version,
        expression,
    })
}
