use serde::de;
use shiba_protocol::SourceId;

use crate::{
    QUERY_SPEC_VERSION, QueryExpressionV1, QueryFieldV1, QueryHavingExpressionV1, QueryInputV1,
    QueryNodeV1, QueryOperationV1, QueryResultV1, QuerySelectorV1,
};

const MAX_QUERY_NODES: usize = 31;
const MAX_COMPILED_NODES: usize = 32;
const MAX_IDENTIFIER_BYTES: usize = shiba_operator::MAX_RESULT_IDENTIFIER_BYTES;
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
    validate_references::<E>(sources, nodes, results)?;
    validate_runtime_topology::<E>(nodes, results)
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
            || result.fields.iter().enumerate().any(|(index, field)| {
                result.fields[..index]
                    .iter()
                    .any(|other| other.name == field.name)
            })
            || result
                .key_ordinals
                .iter()
                .any(|ordinal| *ordinal == 0 || usize::from(*ordinal) > result.fields.len())
            || result
                .key_ordinals
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || result
                .key_ordinals
                .iter()
                .enumerate()
                .any(|(index, ordinal)| usize::from(*ordinal) != index + 1)
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

fn validate_runtime_topology<E: de::Error>(
    nodes: &[QueryNodeV1],
    results: &[QueryResultV1],
) -> Result<(), E> {
    for (index, node) in nodes.iter().enumerate() {
        if !matches!(node.operation, QueryOperationV1::Aggregate { .. }) {
            continue;
        }
        let node_number = u16::try_from(index + 1).map_err(|_| E::custom("invalid node id"))?;
        if results
            .iter()
            .filter(|result| result.input_node == node_number)
            .count()
            != 1
            || nodes.iter().enumerate().any(|(child_index, child)| {
                child_index > index
                    && child.inputs.iter().any(|input| {
                        matches!(input, QueryInputV1::Node { node } if *node == node_number)
                    })
            })
        {
            return Err(E::custom("aggregate must have one terminal result"));
        }
        let mut input = node
            .inputs
            .first()
            .ok_or_else(|| E::custom("invalid aggregate input"))?;
        loop {
            match input {
                QueryInputV1::Source { .. } => break,
                QueryInputV1::Node { node } => {
                    let upstream = nodes
                        .get(usize::from(*node).saturating_sub(1))
                        .ok_or_else(|| E::custom("invalid aggregate upstream"))?;
                    if !matches!(
                        upstream.operation,
                        QueryOperationV1::Filter { .. }
                            | QueryOperationV1::Project { .. }
                            | QueryOperationV1::Compute { .. }
                            | QueryOperationV1::KeyBy { .. }
                    ) {
                        return Err(E::custom("unsupported aggregate topology"));
                    }
                    input = upstream
                        .inputs
                        .first()
                        .ok_or_else(|| E::custom("invalid upstream input"))?;
                }
            }
        }
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
            having,
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
            if having.is_some() && group_expressions.is_empty() {
                return Err(E::custom("scalar HAVING is unsupported"));
            }
            if let Some(having) = having
                && validate_having::<E>(having, calls.len())? != HavingType::Bool
            {
                return Err(E::custom("HAVING must be boolean"));
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum HavingType {
    Int8,
    Bool,
}

fn validate_having<E: de::Error>(
    expression: &QueryHavingExpressionV1,
    calls: usize,
) -> Result<HavingType, E> {
    let mut nodes = 0;
    let mut boolean_terms = 0;
    validate_having_inner(expression, calls, 0, &mut nodes, &mut boolean_terms)
}

fn validate_having_inner<E: de::Error>(
    expression: &QueryHavingExpressionV1,
    calls: usize,
    depth: usize,
    nodes: &mut usize,
    boolean_terms: &mut usize,
) -> Result<HavingType, E> {
    use QueryHavingExpressionV1 as H;
    *nodes = nodes
        .checked_add(1)
        .ok_or_else(|| E::custom("HAVING node overflow"))?;
    if depth > shiba_operator::MAX_HAVING_DEPTH {
        return Err(E::custom("HAVING depth bound exceeded"));
    }
    if *nodes > shiba_operator::MAX_HAVING_NODES {
        return Err(E::custom("HAVING node bound exceeded"));
    }
    match expression {
        H::Call { ordinal } if *ordinal != 0 && usize::from(*ordinal) <= calls => {
            Ok(HavingType::Int8)
        }
        H::Call { .. } => Err(E::custom("invalid HAVING call")),
        H::Int8Literal { .. } | H::NullLiteral => Ok(HavingType::Int8),
        H::Equal { left, right }
        | H::NotEqual { left, right }
        | H::Less { left, right }
        | H::LessEqual { left, right }
        | H::Greater { left, right }
        | H::GreaterEqual { left, right } => {
            *boolean_terms = boolean_terms
                .checked_add(1)
                .ok_or_else(|| E::custom("HAVING boolean overflow"))?;
            if *boolean_terms > shiba_operator::MAX_HAVING_BOOLEAN_TERMS {
                return Err(E::custom("HAVING boolean bound exceeded"));
            }
            if validate_having_inner(left, calls, depth + 1, nodes, boolean_terms)?
                == HavingType::Int8
                && validate_having_inner(right, calls, depth + 1, nodes, boolean_terms)?
                    == HavingType::Int8
            {
                Ok(HavingType::Bool)
            } else {
                Err(E::custom("invalid HAVING comparison"))
            }
        }
        H::IsNull { input } => {
            *boolean_terms = boolean_terms
                .checked_add(1)
                .ok_or_else(|| E::custom("HAVING boolean overflow"))?;
            if *boolean_terms > shiba_operator::MAX_HAVING_BOOLEAN_TERMS {
                return Err(E::custom("HAVING boolean bound exceeded"));
            }
            validate_having_inner(input, calls, depth + 1, nodes, boolean_terms)?;
            Ok(HavingType::Bool)
        }
        H::And { left, right } | H::Or { left, right } => {
            *boolean_terms = boolean_terms
                .checked_add(1)
                .ok_or_else(|| E::custom("HAVING boolean overflow"))?;
            if *boolean_terms > shiba_operator::MAX_HAVING_BOOLEAN_TERMS {
                return Err(E::custom("HAVING boolean bound exceeded"));
            }
            if validate_having_inner(left, calls, depth + 1, nodes, boolean_terms)?
                == HavingType::Bool
                && validate_having_inner(right, calls, depth + 1, nodes, boolean_terms)?
                    == HavingType::Bool
            {
                Ok(HavingType::Bool)
            } else {
                Err(E::custom("invalid HAVING boolean"))
            }
        }
        H::Not { input } => {
            *boolean_terms = boolean_terms
                .checked_add(1)
                .ok_or_else(|| E::custom("HAVING boolean overflow"))?;
            if *boolean_terms > shiba_operator::MAX_HAVING_BOOLEAN_TERMS {
                return Err(E::custom("HAVING boolean bound exceeded"));
            }
            if validate_having_inner(input, calls, depth + 1, nodes, boolean_terms)?
                == HavingType::Bool
            {
                Ok(HavingType::Bool)
            } else {
                Err(E::custom("invalid HAVING negation"))
            }
        }
    }
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
