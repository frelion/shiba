use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryNodeV1, QueryOperationV1, QueryResultFieldV1, QueryResultV1,
    QuerySpecV1,
};
use shiba_protocol::GraphId;

use crate::bind::{ResolvedSource, binding, source_column};
use crate::bind_aggregate_support::{
    aggregate_span, grouped_columns, node_input, predicate_span, slot_expression, slot_field,
    slot_for, slots, source_input, validate_source_identity,
};
use crate::bind_expression::{ExpressionType, lower, resolve_column};
use crate::{
    Aggregate, ErrorCode, FrontendError, SelectExpression, UnboundExpression, UnboundQuery,
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
    let (operation, value_nullable, default_name) = match &item.expression {
        SelectExpression::Aggregate(Aggregate::CountStar { .. }) => {
            (QueryOperationV1::CountRows, false, "count")
        }
        SelectExpression::Aggregate(Aggregate::Sum { input, .. }) => {
            let UnboundExpression::Column(column) = input else {
                return Err(binding(ErrorCode::UnsupportedSyntax, input.span()));
            };
            let descriptor = resolve_column(&source.descriptor, column)?;
            (
                QueryOperationV1::SumInt8 {
                    value: source_column(descriptor),
                },
                true,
                "sum",
            )
        }
        _ => return Err(binding(ErrorCode::UnsupportedSyntax, item.span)),
    };
    finish(
        graph_id,
        source,
        vec![QueryNodeV1 {
            inputs: vec![source_input(source)],
            state_codec_version: Some(1),
            operation,
        }],
        vec![QueryResultFieldV1 {
            name: crate::bind::result_name(item, default_name),
            value_slot: 0,
            nullable: value_nullable,
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

    let sum = match aggregate {
        Aggregate::CountStar { .. } => None,
        Aggregate::Sum { input, .. } => {
            let UnboundExpression::Column(column) = input else {
                return Err(binding(ErrorCode::UnsupportedSyntax, input.span()));
            };
            Some(resolve_column(&source.descriptor, column)?)
        }
    };
    let columns = grouped_columns(query, source, group, sum)?;
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
    let operation = match sum {
        None => QueryOperationV1::GroupedCount {
            key: slot_field(appended_key),
        },
        Some(value) => QueryOperationV1::GroupedSumInt8 {
            key: slot_field(appended_key),
            value: slot_field(slot_for(&slots, value, aggregate_span(aggregate))?),
        },
    };
    nodes.push(QueryNodeV1 {
        inputs: vec![node_input(input_node)],
        state_codec_version: Some(1),
        operation,
    });
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
                    if sum.is_some() { "sum" } else { "count" },
                ),
                value_slot: 1,
                nullable: sum.is_some(),
            },
        ],
        vec![1],
        query.span,
    )
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
