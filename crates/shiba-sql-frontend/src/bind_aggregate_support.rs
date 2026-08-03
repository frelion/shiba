use shiba_compiler::{
    QueryExpressionV1, QueryFieldV1, QueryInputV1, QuerySelectorV1, SourceColumnDescriptor,
};

use crate::bind::{ResolvedSource, binding, validate_identity};
use crate::bind_expression::resolve_column;
use crate::{Aggregate, ErrorCode, FrontendError, UnboundExpression, UnboundQuery};

pub(crate) fn validate_source_identity(
    source: &ResolvedSource,
    query: &UnboundQuery,
) -> Result<(), FrontendError> {
    let key = source
        .descriptor
        .columns
        .iter()
        .find(|column| column.address == source.identity.key_column)
        .ok_or_else(|| binding(ErrorCode::IdentityMismatch, query.sources[0].span))?;
    validate_identity(source, key, query.sources[0].span)
}

pub(crate) fn grouped_columns<'a>(
    query: &UnboundQuery,
    source: &'a ResolvedSource,
    group: &'a SourceColumnDescriptor,
    sum: Option<&'a SourceColumnDescriptor>,
) -> Result<Vec<&'a SourceColumnDescriptor>, FrontendError> {
    let mut columns = vec![group];
    if let Some(sum) = sum
        && sum.address != group.address
    {
        columns.push(sum);
    }
    let mut stack = query.selection.iter().collect::<Vec<_>>();
    while let Some(expression) = stack.pop() {
        match expression {
            UnboundExpression::Column(column) => {
                let descriptor = resolve_column(&source.descriptor, column)?;
                if !columns
                    .iter()
                    .any(|current| current.address == descriptor.address)
                {
                    columns.push(descriptor);
                }
            }
            UnboundExpression::Binary { left, right, .. } => {
                stack.extend([right.as_ref(), left.as_ref()]);
            }
            UnboundExpression::Unary { input, .. } => stack.push(input),
            UnboundExpression::Int8(..) | UnboundExpression::Null(_) => {}
        }
    }
    if columns.len() > 2 {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    }
    columns[1..].sort_by(|left, right| left.name.cmp(&right.name));
    Ok(columns)
}

pub(crate) fn slots<'a>(
    columns: &[&'a SourceColumnDescriptor],
    query: &UnboundQuery,
) -> Result<Vec<(&'a SourceColumnDescriptor, u16)>, FrontendError> {
    columns
        .iter()
        .enumerate()
        .map(|(slot, column)| {
            Ok((
                *column,
                u16::try_from(slot).map_err(|_| binding(ErrorCode::QueryTooComplex, query.span))?,
            ))
        })
        .collect()
}

pub(crate) fn source_input(source: &ResolvedSource) -> QueryInputV1 {
    QueryInputV1::Source {
        source_id: source.descriptor.source_id,
    }
}

pub(crate) fn node_input(node: u16) -> QueryInputV1 {
    QueryInputV1::Node { node }
}

pub(crate) fn slot_expression(slot: u16) -> QueryExpressionV1 {
    QueryExpressionV1::Column {
        field: slot_field(slot),
    }
}

pub(crate) fn slot_field(slot: u16) -> QueryFieldV1 {
    QueryFieldV1 {
        input: 0,
        selector: QuerySelectorV1::Slot { slot },
    }
}

pub(crate) fn slot_for(
    slots: &[(&SourceColumnDescriptor, u16)],
    column: &SourceColumnDescriptor,
    span: crate::Span,
) -> Result<u16, FrontendError> {
    slots
        .iter()
        .find_map(|(candidate, slot)| (candidate.address == column.address).then_some(*slot))
        .ok_or_else(|| binding(ErrorCode::CanonicalizationFailed, span))
}

pub(crate) fn predicate_span(query: &UnboundQuery) -> crate::Span {
    query
        .selection
        .as_ref()
        .map_or(query.span, UnboundExpression::span)
}

pub(crate) fn aggregate_span(aggregate: &Aggregate) -> crate::Span {
    match aggregate {
        Aggregate::CountStar { span } | Aggregate::Sum { span, .. } => *span,
    }
}
