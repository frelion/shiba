use std::collections::{BTreeMap, BTreeSet};

use postgres::Transaction;
use shiba_operator::{MAX_RESULT_MUTATIONS, OperatorGraph, ResultDelta, ResultSchemaV1};

use crate::M2Error;

mod rows;

pub(crate) struct LockedResults {
    contracts: BTreeMap<i64, ResultSchemaV1>,
}

impl LockedResults {
    pub(crate) fn contract_count(&self) -> usize {
        self.contracts.len()
    }
}

pub(crate) fn lock(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    graph: &OperatorGraph,
    expected_status: &str,
) -> Result<LockedResults, M2Error> {
    let expected = graph
        .result_contracts()
        .map(|(result_id, output)| (i64::from(result_id.get()), &output.schema))
        .collect::<BTreeMap<_, _>>();
    let rows = transaction.query(
        "SELECT result_id, result_status, schema_payload, schema_digest
         FROM shiba.graph_result WHERE graph_id = $1
         ORDER BY result_id FOR UPDATE",
        &[&graph_id],
    )?;
    if rows.len() != expected.len() || rows.is_empty() {
        return Err(M2Error::MissingSourceOperator);
    }
    let mut contracts = BTreeMap::new();
    for row in rows {
        let result_id: i64 = row.get(0);
        let digest = exact_digest(row.get(3))?;
        let schema = ResultSchemaV1::from_canonical_payload(&row.get::<_, Vec<u8>>(2), digest)
            .map_err(|_| M2Error::InvalidOperatorDefinition)?;
        if row.get::<_, &str>(1) != expected_status
            || expected.get(&result_id).copied() != Some(&schema)
            || contracts.insert(result_id, schema).is_some()
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
    _publish: bool,
    activate: bool,
) -> Result<(), M2Error> {
    let mut seen = BTreeSet::new();
    let mut mutation_count = 0usize;
    for result in results {
        let result_id = i64::from(result.node_id.get());
        let schema = locked
            .contracts
            .get(&result_id)
            .ok_or(M2Error::InvalidOperatorDefinition)?;
        if !seen.insert(result_id) {
            return Err(M2Error::InvalidOperatorDefinition);
        }
        mutation_count = mutation_count
            .checked_add(result.mutations.len())
            .ok_or(M2Error::TransactionLimitExceeded)?;
        if mutation_count > MAX_RESULT_MUTATIONS {
            return Err(M2Error::TransactionLimitExceeded);
        }
        rows::persist(transaction, graph_id, result_id, schema, result.mutations)?;
    }
    if seen.len() != locked.contracts.len() {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    if activate {
        activate_results(transaction, graph_id, &locked.contracts)?;
    }
    Ok(())
}

fn activate_results(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    contracts: &BTreeMap<i64, ResultSchemaV1>,
) -> Result<(), M2Error> {
    for (result_id, schema) in contracts {
        if schema.is_scalar() && rows::count(transaction, graph_id, *result_id)? != 1 {
            return Err(M2Error::InvalidOperatorDefinition);
        }
        if transaction.execute(
            "UPDATE shiba.graph_result SET result_status = 'active'
             WHERE graph_id = $1 AND result_id = $2 AND result_status = 'building'",
            &[&graph_id, result_id],
        )? != 1
        {
            return Err(M2Error::InvalidOperatorDefinition);
        }
    }
    Ok(())
}

fn exact_digest(value: Vec<u8>) -> Result<[u8; 32], M2Error> {
    value
        .try_into()
        .map_err(|_| M2Error::InvalidOperatorDefinition)
}
