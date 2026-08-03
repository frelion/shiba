use std::collections::{BTreeMap, BTreeSet};

use postgres::Transaction;
use shiba_operator::{ResultDelta, TypedValue};

use crate::M2Error;

mod keyed;

const MAX_GRAPH_RESULT_MUTATIONS: usize = 100_000;

#[derive(Clone, Copy)]
enum Shape {
    Scalar {
        value_nullable: bool,
    },
    Keyed {
        key_nullable: bool,
        value_nullable: bool,
    },
}

pub(crate) struct LockedResults {
    contracts: BTreeMap<i64, Shape>,
}

impl LockedResults {
    pub(crate) fn contract_count(&self) -> usize {
        self.contracts.len()
    }
}

pub(crate) fn lock(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    expected_status: &str,
) -> Result<LockedResults, M2Error> {
    let rows = transaction.query(
        "SELECT result_id, output_shape, output_key_type,
                output_key_nullable, output_value_type,
                output_value_nullable, result_status
         FROM shiba.graph_result WHERE graph_id = $1
         ORDER BY result_id FOR UPDATE",
        &[&graph_id],
    )?;
    if rows.is_empty() {
        return Err(M2Error::MissingSourceOperator);
    }
    let mut contracts = BTreeMap::new();
    for row in rows {
        let result_id: i64 = row.get(0);
        let shape = match row.get::<_, &str>(1) {
            "scalar"
                if row.get::<_, Option<&str>>(2).is_none()
                    && !row.get::<_, bool>(3)
                    && row.get::<_, &str>(4) == "int8" =>
            {
                Shape::Scalar {
                    value_nullable: row.get(5),
                }
            }
            "keyed"
                if row.get::<_, Option<&str>>(2) == Some("int8")
                    && row.get::<_, &str>(4) == "int8" =>
            {
                Shape::Keyed {
                    key_nullable: row.get(3),
                    value_nullable: row.get(5),
                }
            }
            _ => return Err(M2Error::InvalidOperatorDefinition),
        };
        if row.get::<_, &str>(6) != expected_status || contracts.insert(result_id, shape).is_some()
        {
            return Err(M2Error::InvalidOperatorDefinition);
        }
    }
    Ok(LockedResults { contracts })
}

pub(crate) fn persist(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    locked: &LockedResults,
    results: Vec<ResultDelta>,
    publish: bool,
    activate: bool,
) -> Result<(), M2Error> {
    let mut seen = BTreeSet::new();
    let mut mutation_count = 0usize;
    for result in results {
        let (node_id, shape) = match &result {
            ResultDelta::Scalar { node_id, .. } | ResultDelta::Keyed { node_id, .. } => (
                i64::from(node_id.get()),
                *locked
                    .contracts
                    .get(&i64::from(node_id.get()))
                    .ok_or(M2Error::InvalidOperatorDefinition)?,
            ),
        };
        if !seen.insert(node_id) || !shape_matches(locked.contracts.get(&node_id), shape) {
            return Err(M2Error::InvalidOperatorDefinition);
        }
        match result {
            ResultDelta::Scalar { value, .. } => {
                let Shape::Scalar { value_nullable } = shape else {
                    return Err(M2Error::InvalidOperatorDefinition);
                };
                if activate {
                    activate_scalar(transaction, graph_id, node_id, &value, value_nullable)?;
                } else if publish {
                    persist_scalar(transaction, graph_id, node_id, &value, value_nullable)?;
                }
            }
            ResultDelta::Keyed { mutations, .. } => {
                mutation_count = mutation_count
                    .checked_add(mutations.len())
                    .ok_or(M2Error::TransactionLimitExceeded)?;
                let Shape::Keyed {
                    key_nullable,
                    value_nullable,
                } = shape
                else {
                    return Err(M2Error::InvalidOperatorDefinition);
                };
                keyed::persist(
                    transaction,
                    graph_id,
                    node_id,
                    mutations,
                    key_nullable,
                    value_nullable,
                )?;
                if activate {
                    activate_keyed(transaction, graph_id, node_id)?;
                }
            }
        }
    }
    if mutation_count > MAX_GRAPH_RESULT_MUTATIONS || seen.len() != locked.contracts.len() {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn activate_scalar(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    value: &TypedValue,
    value_nullable: bool,
) -> Result<(), M2Error> {
    let bigint = scalar_value(value, value_nullable)?;
    let payload = value
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    if transaction.execute(
        "UPDATE shiba.graph_result SET result_status = 'active',
                value_payload = $1, value_bigint = $2
         WHERE graph_id = $3 AND result_id = $4
           AND result_status = 'building' AND output_shape = 'scalar'",
        &[&payload, &bigint, &graph_id, &result_id],
    )? != 1
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn activate_keyed(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
) -> Result<(), M2Error> {
    if transaction.execute(
        "UPDATE shiba.graph_result SET result_status = 'active'
         WHERE graph_id = $1 AND result_id = $2
           AND result_status = 'building' AND output_shape = 'keyed'",
        &[&graph_id, &result_id],
    )? != 1
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn shape_matches(expected: Option<&Shape>, actual: Shape) -> bool {
    match (expected, actual) {
        (
            Some(Shape::Scalar {
                value_nullable: expected,
            }),
            Shape::Scalar {
                value_nullable: actual,
            },
        ) => *expected == actual,
        (Some(Shape::Keyed { .. }), Shape::Keyed { .. }) => true,
        _ => false,
    }
}

fn persist_scalar(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    value: &TypedValue,
    value_nullable: bool,
) -> Result<(), M2Error> {
    let bigint = scalar_value(value, value_nullable)?;
    let payload = value
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    if transaction.execute(
        "UPDATE shiba.graph_result SET value_payload = $1, value_bigint = $2
         WHERE graph_id = $3 AND result_id = $4
           AND result_status = 'active' AND output_shape = 'scalar'",
        &[&payload, &bigint, &graph_id, &result_id],
    )? != 1
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

pub(crate) fn scalar_value(value: &TypedValue, nullable: bool) -> Result<Option<i64>, M2Error> {
    match value {
        TypedValue::Int8(value) => Ok(Some(*value)),
        TypedValue::Null(shiba_operator::ValueType::Int8) if nullable => Ok(None),
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
}
