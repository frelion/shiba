use serde::de;
use shiba_protocol::SourceId;

use crate::{
    QUERY_SPEC_VERSION, QueryExpressionV1, QueryFieldV1, QueryInputV1, QueryNodeV1,
    QueryOperationV1, QueryResultV1, QuerySelectorV1,
};

const MAX_QUERY_NODES: usize = 31;
const MAX_COMPILED_NODES: usize = 32;
const MAX_IDENTIFIER_BYTES: usize = 63;
const MAX_EXPRESSION_NODES: usize = 256;
const MAX_EXPRESSION_DEPTH: usize = 32;
const MAX_BOOLEAN_TERMS: usize = 64;
const MAX_PROJECTED_ITEMS: usize = 2;
const MAX_RESULT_FIELDS: usize = 16;

pub(crate) fn validate_query<E: de::Error>(
    version: u32,
    sources: &[SourceId],
    nodes: &[QueryNodeV1],
    results: &[QueryResultV1],
) -> Result<(), E> {
    if version != QUERY_SPEC_VERSION
        || !(1..=2).contains(&sources.len())
        || sources.windows(2).any(|pair| pair[0] >= pair[1])
        || nodes.is_empty()
        || results.is_empty()
        || nodes.len() > MAX_QUERY_NODES
        || nodes.len() + results.len() > MAX_COMPILED_NODES
    {
        return Err(E::custom("invalid query envelope"));
    }
    validate_references::<E>(sources, nodes, results)
}

fn validate_references<E: de::Error>(
    sources: &[SourceId],
    nodes: &[QueryNodeV1],
    results: &[QueryResultV1],
) -> Result<(), E> {
    let mut expression_nodes = 0usize;
    let mut boolean_terms = 0usize;
    let mut referenced = vec![false; nodes.len()];
    for (index, node) in nodes.iter().enumerate() {
        if node.inputs.is_empty() || node.inputs.len() > 2 {
            return Err(E::custom("invalid node input arity"));
        }
        for input in &node.inputs {
            match input {
                QueryInputV1::Source { source_id } if sources.contains(source_id) => {}
                QueryInputV1::Node { node } if usize::from(*node) <= index && *node != 0 => {
                    referenced[usize::from(*node - 1)] = true;
                }
                _ => return Err(E::custom("invalid or forward input reference")),
            }
        }
        if matches!(&node.operation, QueryOperationV1::Project { expressions } | QueryOperationV1::Compute { expressions } if expressions.is_empty() || expressions.len() > MAX_PROJECTED_ITEMS)
        {
            return Err(E::custom("invalid expression list"));
        }
        validate_operation::<E>(&node.operation, &mut expression_nodes, &mut boolean_terms)?;
    }
    let mut terminals = Vec::with_capacity(results.len());
    for result in results {
        if result.input_node == 0
            || usize::from(result.input_node) > nodes.len()
            || result.fields.is_empty()
            || result.fields.len() > MAX_RESULT_FIELDS
            || result
                .fields
                .iter()
                .any(|field| field.name.is_empty() || field.name.len() > MAX_IDENTIFIER_BYTES)
            || result
                .key_ordinals
                .iter()
                .any(|ordinal| *ordinal == 0 || usize::from(*ordinal) > result.fields.len())
            || result
                .key_ordinals
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(E::custom("invalid result input reference"));
        }
        referenced[usize::from(result.input_node - 1)] = true;
        terminals.push(result.input_node);
    }
    terminals.sort_unstable();
    if terminals.windows(2).any(|pair| pair[0] == pair[1]) || referenced.iter().any(|value| !value)
    {
        return Err(E::custom("duplicate terminal or disconnected node"));
    }
    Ok(())
}

fn validate_operation<E: de::Error>(
    operation: &QueryOperationV1,
    nodes: &mut usize,
    boolean_terms: &mut usize,
) -> Result<(), E> {
    let expressions: Vec<&QueryExpressionV1> = match operation {
        QueryOperationV1::Aggregate {
            group_expressions,
            calls,
        } => {
            if calls.is_empty()
                || calls.len() > shiba_operator::MAX_AGGREGATE_CALLS
                || group_expressions.len() > shiba_operator::MAX_GROUP_EXPRESSIONS
                || calls.iter().enumerate().any(|(index, call)| {
                    let descriptor = shiba_operator::aggregate_function_descriptor(call.function);
                    usize::from(call.ordinal) != index + 1
                        || call.function_version != descriptor.semantic_version
                        || match descriptor.input {
                            shiba_operator::AggregateInputContract::None => {
                                call.expression.is_some()
                            }
                            shiba_operator::AggregateInputContract::Nullable(_) => {
                                call.expression.is_none()
                            }
                        }
                })
            {
                return Err(E::custom("invalid aggregate calls"));
            }
            group_expressions
                .iter()
                .chain(calls.iter().filter_map(|call| call.expression.as_ref()))
                .collect()
        }
        QueryOperationV1::Filter { predicate } => vec![predicate],
        QueryOperationV1::Project { expressions } | QueryOperationV1::Compute { expressions } => {
            expressions.iter().collect()
        }
        QueryOperationV1::KeyBy { key } => vec![key],
        QueryOperationV1::InnerJoin {
            left_id,
            left_key,
            right_id,
            right_payload,
        } => {
            for field in [left_id, left_key, right_id, right_payload] {
                validate_field::<E>(field)?;
            }
            vec![]
        }
    };
    for expression in expressions {
        validate_expression::<E>(expression, 1, nodes, boolean_terms)?;
    }
    Ok(())
}

fn validate_expression<E: de::Error>(
    expression: &QueryExpressionV1,
    depth: usize,
    nodes: &mut usize,
    boolean_terms: &mut usize,
) -> Result<(), E> {
    *nodes = nodes
        .checked_add(1)
        .ok_or_else(|| E::custom("expression overflow"))?;
    if depth > MAX_EXPRESSION_DEPTH || *nodes > MAX_EXPRESSION_NODES {
        return Err(E::custom("expression bound exceeded"));
    }
    match expression {
        QueryExpressionV1::Column { field } => validate_field::<E>(field)?,
        QueryExpressionV1::Int8Literal { .. } | QueryExpressionV1::NullInt8 => {}
        QueryExpressionV1::IsNull { input } | QueryExpressionV1::Not { input } => {
            *boolean_terms += 1;
            validate_expression::<E>(input, depth + 1, nodes, boolean_terms)?;
        }
        QueryExpressionV1::Equal { left, right }
        | QueryExpressionV1::NotEqual { left, right }
        | QueryExpressionV1::Less { left, right }
        | QueryExpressionV1::LessEqual { left, right }
        | QueryExpressionV1::Greater { left, right }
        | QueryExpressionV1::GreaterEqual { left, right }
        | QueryExpressionV1::And { left, right }
        | QueryExpressionV1::Or { left, right } => {
            *boolean_terms += 1;
            validate_expression::<E>(left, depth + 1, nodes, boolean_terms)?;
            validate_expression::<E>(right, depth + 1, nodes, boolean_terms)?;
        }
        QueryExpressionV1::CheckedAdd { left, right }
        | QueryExpressionV1::CheckedSubtract { left, right } => {
            validate_expression::<E>(left, depth + 1, nodes, boolean_terms)?;
            validate_expression::<E>(right, depth + 1, nodes, boolean_terms)?;
        }
    }
    if *boolean_terms > MAX_BOOLEAN_TERMS {
        return Err(E::custom("boolean bound exceeded"));
    }
    Ok(())
}

fn validate_field<E: de::Error>(field: &QueryFieldV1) -> Result<(), E> {
    if matches!(&field.selector, QuerySelectorV1::Name { name, quoted } if name.is_empty()
        || name.len() > MAX_IDENTIFIER_BYTES
        || (!quoted && name.bytes().any(|byte| byte.is_ascii_uppercase())))
    {
        return Err(E::custom("invalid logical identifier"));
    }
    Ok(())
}
