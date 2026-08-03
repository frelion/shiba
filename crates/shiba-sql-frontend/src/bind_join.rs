use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryFieldV1, QueryInputV1, QueryNodeV1, QueryOperationV1,
    QueryResultShapeV1, QueryResultV1, QuerySelectorV1, QuerySpecV1, SourceColumnDescriptor,
};
use shiba_protocol::GraphId;

use crate::bind::{ResolvedSource, binding, validate_identity};
use crate::bind_expression::resolve_column;
use crate::{
    ColumnRef, ErrorCode, FrontendError, SelectExpression, UnboundExpression, UnboundQuery,
};

pub(crate) fn bind(
    graph_id: GraphId,
    query: &UnboundQuery,
    sources: &[ResolvedSource],
) -> Result<QuerySpecV1, FrontendError> {
    let ([left, right], Some(join), [left_item, right_item]) =
        (sources, query.join.as_ref(), query.projection.as_slice())
    else {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    };
    if query.sources.len() != 2
        || query.selection.is_some()
        || query.group_by.is_some()
        || left.descriptor.source_id == right.descriptor.source_id
    {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    }
    let (
        SelectExpression::Expression(UnboundExpression::Column(left_projection)),
        SelectExpression::Expression(UnboundExpression::Column(right_projection)),
    ) = (&left_item.expression, &right_item.expression)
    else {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    };
    let left_id = resolve_at(left, left_projection, 0)?;
    let left_key = resolve_at(left, &join.left, 0)?;
    let right_id = resolve_at(right, &join.right, 1)?;
    let right_payload = resolve_at(right, right_projection, 1)?;
    validate_identity(left, left_id, left_projection.span)?;
    validate_identity(right, right_id, join.right.span)?;

    let mut source_ids = vec![left.descriptor.source_id, right.descriptor.source_id];
    source_ids.sort_unstable();
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id,
        sources: source_ids,
        nodes: vec![QueryNodeV1 {
            inputs: vec![
                QueryInputV1::Source {
                    source_id: left.descriptor.source_id,
                },
                QueryInputV1::Source {
                    source_id: right.descriptor.source_id,
                },
            ],
            state_codec_version: Some(1),
            operation: QueryOperationV1::InnerJoin {
                left_id: name_field(0, left_id),
                left_key: name_field(0, left_key),
                right_id: name_field(1, right_id),
                right_payload: name_field(1, right_payload),
            },
        }],
        results: vec![QueryResultV1 {
            input_node: 1,
            shape: QueryResultShapeV1::Keyed {
                key_slot: 0,
                key_nullable: false,
                value_slot: 1,
                value_nullable: right_payload.nullable,
            },
        }],
    };
    spec.to_canonical_json()
        .map_err(|_| binding(ErrorCode::CanonicalizationFailed, query.span))?;
    Ok(spec)
}

fn resolve_at<'a>(
    source: &'a ResolvedSource,
    column: &ColumnRef,
    expected: u8,
) -> Result<&'a SourceColumnDescriptor, FrontendError> {
    if column.source != expected {
        return Err(binding(ErrorCode::AmbiguousColumn, column.span));
    }
    let normalized = ColumnRef {
        source: 0,
        name: column.name.clone(),
        span: column.span,
    };
    resolve_column(&source.descriptor, &normalized)
}

fn name_field(input: u8, column: &SourceColumnDescriptor) -> QueryFieldV1 {
    QueryFieldV1 {
        input,
        selector: QuerySelectorV1::Name {
            name: column.name.clone(),
            quoted: !is_unquoted(&column.name),
        },
    }
}

fn is_unquoted(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_lowercase() || byte == b'_'
            } else {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'$')
            }
        })
}
