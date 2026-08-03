use shiba_compiler::{
    IdentityIndexDescriptor, POSTGRES_INT8_TYPE_OID, QUERY_SPEC_VERSION, QueryExpressionV1,
    QueryFieldV1, QueryInputV1, QueryNodeV1, QueryOperationV1, QueryResultShapeV1, QueryResultV1,
    QuerySelectorV1, QuerySpecV1, SourceColumnDescriptor, SourceDescriptor,
};
use shiba_protocol::GraphId;

use crate::bind_expression::{ExpressionType, lower, resolve_column};
use crate::{
    ColumnRef, ErrorCode, FrontendError, SelectExpression, UnboundExpression, UnboundQuery,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedSource {
    pub descriptor: SourceDescriptor,
    pub identity: IdentityIndexDescriptor,
}

/// Binds one M15.4 single-source keyed projection to a canonical declaration.
///
/// Source descriptors are ordered by SQL source ordinal and must have been read
/// and locked by the registration transaction. This pure function performs no
/// database access and creates no durable state.
///
/// # Errors
///
/// Rejects unsupported query shapes, missing or ambiguous columns, non-int8
/// inputs, and any drift from the exact effective identity index.
pub fn bind_query(
    graph_id: GraphId,
    query: &UnboundQuery,
    sources: &[ResolvedSource],
) -> Result<QuerySpecV1, FrontendError> {
    crate::ast_validate::validate(query)?;
    let [resolved] = sources else {
        return Err(binding(ErrorCode::UnknownRelation, query.span));
    };
    if query.sources.len() != 1 || query.join.is_some() || query.group_by.is_some() {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    }
    let [key_item, value_item] = query.projection.as_slice() else {
        return Err(binding(ErrorCode::UnsupportedSyntax, query.span));
    };
    let SelectExpression::Expression(UnboundExpression::Column(key_ref)) = &key_item.expression
    else {
        return Err(binding(ErrorCode::IdentityMismatch, key_item.span));
    };
    let SelectExpression::Expression(value_expression) = &value_item.expression else {
        return Err(binding(ErrorCode::UnsupportedSyntax, value_item.span));
    };

    let key = resolve_column(&resolved.descriptor, key_ref)?;
    validate_identity(resolved, key, key_ref)?;
    let columns = referenced_columns(query, &resolved.descriptor, key)?;
    let slots = columns
        .iter()
        .enumerate()
        .map(|(slot, column)| {
            Ok((
                *column,
                u16::try_from(slot).map_err(|_| binding(ErrorCode::QueryTooComplex, query.span))?,
            ))
        })
        .collect::<Result<Vec<_>, FrontendError>>()?;

    let mut nodes = vec![QueryNodeV1 {
        inputs: vec![QueryInputV1::Source {
            source_id: resolved.descriptor.source_id,
        }],
        state_codec_version: None,
        operation: QueryOperationV1::Project {
            expressions: columns.iter().map(|column| source_column(column)).collect(),
        },
    }];
    let mut input_node = 1u16;
    if let Some(predicate) = &query.selection {
        let predicate = lower(predicate, &resolved.descriptor, &slots)?;
        if predicate.value_type != ExpressionType::Bool {
            return Err(binding(ErrorCode::TypeMismatch, predicate_span(query)));
        }
        nodes.push(QueryNodeV1 {
            inputs: vec![QueryInputV1::Node { node: input_node }],
            state_codec_version: None,
            operation: QueryOperationV1::Filter {
                predicate: predicate.expression,
            },
        });
        input_node += 1;
    }

    let key = lower(
        &UnboundExpression::Column(key_ref.clone()),
        &resolved.descriptor,
        &slots,
    )?;
    let value = lower(value_expression, &resolved.descriptor, &slots)?;
    if key.value_type != ExpressionType::Int8
        || key.nullable
        || value.value_type != ExpressionType::Int8
    {
        return Err(binding(ErrorCode::TypeMismatch, value_expression.span()));
    }
    nodes.push(QueryNodeV1 {
        inputs: vec![QueryInputV1::Node { node: input_node }],
        state_codec_version: None,
        operation: QueryOperationV1::Project {
            expressions: vec![key.expression, value.expression],
        },
    });
    let result_node = input_node + 1;
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id,
        sources: vec![resolved.descriptor.source_id],
        nodes,
        results: vec![QueryResultV1 {
            input_node: result_node,
            shape: QueryResultShapeV1::Keyed {
                key_slot: 0,
                key_nullable: false,
                value_slot: 1,
                value_nullable: value.nullable,
            },
        }],
    };
    spec.to_canonical_json()
        .map_err(|_| binding(ErrorCode::CanonicalizationFailed, query.span))?;
    Ok(spec)
}

fn validate_identity(
    source: &ResolvedSource,
    key: &SourceColumnDescriptor,
    key_ref: &ColumnRef,
) -> Result<(), FrontendError> {
    let index = &source.identity;
    let descriptor = &source.descriptor;
    if key.type_oid != POSTGRES_INT8_TYPE_OID
        || key.nullable
        || key.address != index.key_column
        || descriptor.columns.first().map(|column| column.address) != Some(key.address)
        || index.relation != descriptor.relation
        || index.address.class_id != descriptor.relation.class_id
        || index.address.object_id == 0
        || index.address.sub_id != 0
        || index.key_arity != 1
        || !index.unique
        || !index.valid
        || !index.ready
        || index.has_expression
        || index.has_predicate
        || !index.effective_replica_identity
    {
        return Err(binding(ErrorCode::IdentityMismatch, key_ref.span));
    }
    Ok(())
}

fn referenced_columns<'a>(
    query: &UnboundQuery,
    source: &'a SourceDescriptor,
    key: &'a SourceColumnDescriptor,
) -> Result<Vec<&'a SourceColumnDescriptor>, FrontendError> {
    let mut columns = vec![key];
    let mut stack = query
        .projection
        .iter()
        .filter_map(|item| match &item.expression {
            SelectExpression::Expression(expression) => Some(expression),
            SelectExpression::Aggregate(_) => None,
        })
        .chain(query.selection.iter())
        .collect::<Vec<_>>();
    while let Some(expression) = stack.pop() {
        match expression {
            UnboundExpression::Column(column) => {
                let resolved = resolve_column(source, column)?;
                if !columns
                    .iter()
                    .any(|current| current.address == resolved.address)
                {
                    columns.push(resolved);
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

fn source_column(column: &SourceColumnDescriptor) -> QueryExpressionV1 {
    QueryExpressionV1::Column {
        field: QueryFieldV1 {
            input: 0,
            selector: QuerySelectorV1::Name {
                name: column.name.clone(),
                quoted: !is_unquoted(&column.name),
            },
        },
    }
}

fn is_unquoted(name: &str) -> bool {
    name.bytes().enumerate().all(|(index, byte)| {
        if index == 0 {
            byte.is_ascii_lowercase() || byte == b'_'
        } else {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'$')
        }
    })
}

fn predicate_span(query: &UnboundQuery) -> crate::Span {
    query
        .selection
        .as_ref()
        .map_or(query.span, UnboundExpression::span)
}

fn binding(code: ErrorCode, span: crate::Span) -> FrontendError {
    FrontendError::binding(code, span)
}
