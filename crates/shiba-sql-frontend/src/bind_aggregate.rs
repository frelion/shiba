use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryAggregateCallV1, QueryNodeV1, QueryOperationV1, QueryResultFieldV1,
    QueryResultV1, QuerySpecV1, SourceColumnDescriptor,
};
use shiba_operator::{AggregateFunctionV1, aggregate_function_descriptor};
use shiba_protocol::GraphId;

use crate::bind::{ResolvedSource, binding, source_column};
use crate::bind_aggregate_support::{source_input, validate_source_identity};
use crate::bind_expression::resolve_column;
use crate::{
    Aggregate, AggregateArgument, ErrorCode, FrontendError, SelectExpression, UnboundExpression,
    UnboundQuery, UnboundSelectItem,
};

pub(crate) struct BoundCall<'a> {
    pub(crate) item: &'a UnboundSelectItem,
    pub(crate) aggregate: &'a Aggregate,
    pub(crate) function: AggregateFunctionV1,
    pub(crate) value: Option<&'a SourceColumnDescriptor>,
}

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
    if query.projection.len() > shiba_operator::MAX_AGGREGATE_CALLS {
        return Err(binding(ErrorCode::QueryTooComplex, query.span));
    }
    let calls = query
        .projection
        .iter()
        .map(|item| bind_call(item, source))
        .collect::<Result<Vec<_>, _>>()?;
    let fields = calls
        .iter()
        .enumerate()
        .map(|(index, call)| {
            Ok(QueryResultFieldV1 {
                name: crate::bind::result_name(call.item, default_name(call.function)),
                value_slot: u16::try_from(index)
                    .map_err(|_| binding(ErrorCode::QueryTooComplex, call.item.span))?,
                nullable: aggregate_function_descriptor(call.function).output_nullable,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_result_fields(&fields, query.span)?;
    finish(
        graph_id,
        source,
        vec![QueryNodeV1 {
            inputs: vec![source_input(source)],
            state_codec_version: Some(1),
            operation: QueryOperationV1::Aggregate {
                group_expressions: vec![],
                calls: calls
                    .iter()
                    .enumerate()
                    .map(|(index, call)| query_call_scalar(call, index))
                    .collect::<Result<Vec<_>, _>>()?,
            },
        }],
        fields,
        vec![],
        query.span,
    )
}

fn bind_grouped(
    graph_id: GraphId,
    query: &UnboundQuery,
    source: &ResolvedSource,
) -> Result<QuerySpecV1, FrontendError> {
    let Some(group_by) = query.group_by.as_ref() else {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    };
    let SelectExpression::Expression(UnboundExpression::Column(group_ref)) =
        &query.projection[0].expression
    else {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    };
    let group = resolve_column(&source.descriptor, group_ref)?;
    let UnboundExpression::Column(group_by_ref) = group_by else {
        return Err(binding(ErrorCode::UnsupportedSyntax, group_by.span()));
    };
    let grouped_same_column = resolve_column(&source.descriptor, group_by_ref)?;
    if group.address != grouped_same_column.address {
        return Err(binding(ErrorCode::UnknownColumn, group_by.span()));
    }
    let calls = query.projection[1..]
        .iter()
        .map(|item| bind_call(item, source))
        .collect::<Result<Vec<_>, _>>()?;
    // One result field is reserved for the grouped key, so the wide-row bound
    // leaves at most fifteen aggregate calls in a grouped result.
    if calls.len() >= shiba_operator::MAX_AGGREGATE_CALLS {
        return Err(binding(ErrorCode::QueryTooComplex, query.span));
    }
    let nodes =
        crate::bind_aggregate_nodes::grouped_nodes(query, source, group, group_ref, &calls)?;
    let mut fields = vec![QueryResultFieldV1 {
        name: crate::bind::result_name(&query.projection[0], &group.name),
        value_slot: 0,
        nullable: group.nullable,
    }];
    let aggregate_fields = calls
        .iter()
        .enumerate()
        .map(|(index, call)| {
            Ok(QueryResultFieldV1 {
                name: crate::bind::result_name(call.item, default_name(call.function)),
                value_slot: u16::try_from(index + 1)
                    .map_err(|_| binding(ErrorCode::QueryTooComplex, call.item.span))?,
                nullable: aggregate_function_descriptor(call.function).output_nullable,
            })
        })
        .collect::<Result<Vec<_>, FrontendError>>()?;
    fields.extend(aggregate_fields);
    validate_result_fields(&fields, query.span)?;
    finish(graph_id, source, nodes, fields, vec![1], query.span)
}

fn bind_call<'a>(
    item: &'a UnboundSelectItem,
    source: &'a ResolvedSource,
) -> Result<BoundCall<'a>, FrontendError> {
    let SelectExpression::Aggregate(aggregate) = &item.expression else {
        return Err(binding(ErrorCode::UnsupportedSyntax, item.span));
    };
    let (function, argument) = aggregate_call(aggregate)?;
    let value = argument
        .map(|input| {
            let UnboundExpression::Column(column) = input else {
                return Err(binding(ErrorCode::UnsupportedSyntax, input.span()));
            };
            resolve_column(&source.descriptor, column)
        })
        .transpose()?;
    Ok(BoundCall {
        item,
        aggregate,
        function,
        value,
    })
}

fn query_call_scalar(
    call: &BoundCall<'_>,
    index: usize,
) -> Result<QueryAggregateCallV1, FrontendError> {
    let descriptor = aggregate_function_descriptor(call.function);
    Ok(QueryAggregateCallV1 {
        ordinal: u16::try_from(index + 1)
            .map_err(|_| binding(ErrorCode::QueryTooComplex, call.item.span))?,
        function: call.function,
        function_version: descriptor.semantic_version,
        expression: call.value.map(source_column),
    })
}

fn aggregate_call(
    aggregate: &Aggregate,
) -> Result<(AggregateFunctionV1, Option<&UnboundExpression>), FrontendError> {
    match (aggregate.function.as_str(), &aggregate.argument) {
        ("count", AggregateArgument::Star) => Ok((AggregateFunctionV1::CountStar, None)),
        ("count", AggregateArgument::Expression(input)) => {
            Ok((AggregateFunctionV1::Count, Some(input)))
        }
        ("sum", AggregateArgument::Expression(input)) => {
            Ok((AggregateFunctionV1::SumInt8, Some(input)))
        }
        ("min", AggregateArgument::Expression(input)) => {
            Ok((AggregateFunctionV1::MinInt8, Some(input)))
        }
        ("max", AggregateArgument::Expression(input)) => {
            Ok((AggregateFunctionV1::MaxInt8, Some(input)))
        }
        _ => Err(binding(ErrorCode::UnsupportedSyntax, aggregate.span)),
    }
}

fn default_name(function: AggregateFunctionV1) -> &'static str {
    match function {
        AggregateFunctionV1::CountStar | AggregateFunctionV1::Count => "count",
        AggregateFunctionV1::SumInt8 => "sum",
        AggregateFunctionV1::MinInt8 => "min",
        AggregateFunctionV1::MaxInt8 => "max",
    }
}

fn validate_result_fields(
    fields: &[QueryResultFieldV1],
    span: crate::Span,
) -> Result<(), FrontendError> {
    for (index, field) in fields.iter().enumerate() {
        if fields[..index].iter().any(|other| other.name == field.name) {
            return Err(binding(ErrorCode::DuplicateAlias, span));
        }
    }
    Ok(())
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
