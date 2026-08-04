use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryAggregateCallV1, QueryNodeV1, QueryOperationV1, QueryResultFieldV1,
    QueryResultV1, QuerySpecV1, SourceColumnDescriptor,
};
use shiba_operator::{AggregateFunctionV1, aggregate_function_descriptor};
use shiba_protocol::GraphId;

use crate::bind::{ResolvedSource, binding, source_column};
use crate::bind_aggregate_support::{
    aggregate_span, grouped_columns, node_input, predicate_span, slot_expression, slot_for, slots,
    source_input, validate_source_identity,
};
use crate::bind_expression::{ExpressionType, lower, resolve_column};
use crate::{
    Aggregate, AggregateArgument, ErrorCode, FrontendError, SelectExpression, UnboundExpression,
    UnboundQuery,
};

pub(crate) fn bind(
    graph_id: GraphId,
    query: &UnboundQuery,
    source: &ResolvedSource,
) -> Result<QuerySpecV1, FrontendError> {
    validate_source_identity(source, query)?;
    if query.group_by.is_some() {
        bind_grouped(graph_id, query, source)
    } else {
        bind_scalar(graph_id, query, source)
    }
}

fn bind_scalar(
    graph_id: GraphId,
    query: &UnboundQuery,
    source: &ResolvedSource,
) -> Result<QuerySpecV1, FrontendError> {
    if query.selection.is_some() {
        // The proven scalar kernels consume a SourcePort directly. A filtered
        // scalar aggregate requires a separate generic topology extension.
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    }
    let [item] = query.projection.as_slice() else {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    };
    let SelectExpression::Aggregate(aggregate) = &item.expression else {
        return Err(binding(ErrorCode::UnsupportedSyntax, item.span));
    };
    let (function, expression, default_name) = match aggregate_call(aggregate)? {
        (function, None) => (function, None, "count"),
        (function, Some(input)) => {
            let UnboundExpression::Column(column) = input else {
                return Err(binding(ErrorCode::UnsupportedSyntax, input.span()));
            };
            let descriptor = resolve_column(&source.descriptor, column)?;
            (function, Some(source_column(descriptor)), "sum")
        }
    };
    let descriptor = aggregate_function_descriptor(function);
    finish(
        graph_id,
        source,
        vec![QueryNodeV1 {
            inputs: vec![source_input(source)],
            state_codec_version: Some(1),
            operation: QueryOperationV1::Aggregate {
                group_expressions: vec![],
                calls: vec![QueryAggregateCallV1 {
                    ordinal: 1,
                    function,
                    function_version: descriptor.semantic_version,
                    expression,
                }],
            },
        }],
        vec![QueryResultFieldV1 {
            name: crate::bind::result_name(item, default_name),
            value_slot: 0,
            nullable: descriptor.output_nullable,
        }],
        vec![],
        query.span,
    )
}

fn bind_grouped(
    graph_id: GraphId,
    query: &UnboundQuery,
    source: &ResolvedSource,
) -> Result<QuerySpecV1, FrontendError> {
    let [group_item, aggregate_item] = query.projection.as_slice() else {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    };
    let (
        SelectExpression::Expression(UnboundExpression::Column(group_ref)),
        SelectExpression::Aggregate(aggregate),
        Some(UnboundExpression::Column(group_by)),
    ) = (
        &group_item.expression,
        &aggregate_item.expression,
        query.group_by.as_ref(),
    )
    else {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    };
    let group = resolve_column(&source.descriptor, group_ref)?;
    let grouped_same_column = resolve_column(&source.descriptor, group_by)?;
    if group.address != grouped_same_column.address {
        return Err(binding(ErrorCode::UnknownColumn, group_by.span));
    }

    let (function, argument) = aggregate_call(aggregate)?;
    let value = match argument {
        None => None,
        Some(input) => {
            let UnboundExpression::Column(column) = input else {
                return Err(binding(ErrorCode::UnsupportedSyntax, input.span()));
            };
            Some(resolve_column(&source.descriptor, column)?)
        }
    };
    let nodes = grouped_nodes(query, source, group, group_ref, aggregate, function, value)?;
    finish(
        graph_id,
        source,
        nodes,
        vec![
            QueryResultFieldV1 {
                name: crate::bind::result_name(group_item, &group.name),
                value_slot: 0,
                nullable: group.nullable,
            },
            QueryResultFieldV1 {
                name: crate::bind::result_name(
                    aggregate_item,
                    if value.is_some() { "sum" } else { "count" },
                ),
                value_slot: 1,
                nullable: aggregate_function_descriptor(function).output_nullable,
            },
        ],
        vec![1],
        query.span,
    )
}

fn grouped_nodes(
    query: &UnboundQuery,
    source: &ResolvedSource,
    group: &SourceColumnDescriptor,
    group_ref: &crate::ColumnRef,
    aggregate: &Aggregate,
    function: AggregateFunctionV1,
    value: Option<&SourceColumnDescriptor>,
) -> Result<Vec<QueryNodeV1>, FrontendError> {
    let columns = grouped_columns(query, source, group, value)?;
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
    let expression = value
        .map(|value| slot_for(&slots, value, aggregate_span(aggregate)).map(slot_expression))
        .transpose()?;
    nodes.push(QueryNodeV1 {
        inputs: vec![node_input(input_node)],
        state_codec_version: Some(1),
        operation: QueryOperationV1::Aggregate {
            group_expressions: vec![slot_expression(appended_key)],
            calls: vec![QueryAggregateCallV1 {
                ordinal: 1,
                function,
                function_version: aggregate_function_descriptor(function).semantic_version,
                expression,
            }],
        },
    });
    Ok(nodes)
}

fn aggregate_call(
    aggregate: &Aggregate,
) -> Result<(AggregateFunctionV1, Option<&UnboundExpression>), FrontendError> {
    match (aggregate.function.as_str(), &aggregate.argument) {
        ("count", AggregateArgument::Star) => Ok((AggregateFunctionV1::CountStar, None)),
        ("sum", AggregateArgument::Expression(input)) => {
            Ok((AggregateFunctionV1::SumInt8, Some(input)))
        }
        _ => Err(binding(ErrorCode::UnsupportedSyntax, aggregate.span)),
    }
}

fn finish(
    graph_id: GraphId,
    source: &ResolvedSource,
    nodes: Vec<QueryNodeV1>,
    fields: Vec<QueryResultFieldV1>,
    key_ordinals: Vec<u16>,
    span: crate::Span,
) -> Result<QuerySpecV1, FrontendError> {
    let input_node =
        u16::try_from(nodes.len()).map_err(|_| binding(ErrorCode::QueryTooComplex, span))?;
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id,
        sources: vec![source.descriptor.source_id],
        nodes,
        results: vec![QueryResultV1 {
            input_node,
            fields,
            key_ordinals,
        }],
    };
    spec.to_canonical_json()
        .map_err(|_| binding(ErrorCode::CanonicalizationFailed, span))?;
    Ok(spec)
}
