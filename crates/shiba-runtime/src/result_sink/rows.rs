use std::collections::BTreeSet;

use postgres::Transaction;
use shiba_operator::{ResultMutation, ResultRowKey, ResultSchemaV1, TypedResultRowV1};

use crate::M2Error;

type Upsert = (Vec<u8>, Vec<u8>);

pub(super) fn persist(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    schema: &ResultSchemaV1,
    mutations: Vec<ResultMutation>,
) -> Result<(), M2Error> {
    let mut seen = BTreeSet::new();
    let mut deletes = Vec::new();
    let mut upserts = Vec::new();
    for mutation in mutations {
        let (identity, row) = mutation_payload(schema, mutation)?;
        if !seen.insert(identity.clone()) {
            return Err(M2Error::InvalidOperatorDefinition);
        }
        if let Some(row) = row {
            upserts.push((identity, row));
        } else {
            deletes.push(identity);
        }
    }
    delete_rows(transaction, graph_id, result_id, &deletes)?;
    upsert_rows(transaction, graph_id, result_id, schema.digest, &upserts)
}

fn mutation_payload(
    schema: &ResultSchemaV1,
    mutation: ResultMutation,
) -> Result<(Vec<u8>, Option<Vec<u8>>), M2Error> {
    let invalid = |_| M2Error::InvalidOperatorDefinition;
    match mutation {
        ResultMutation::ReplaceScalar { row } if schema.is_scalar() => Ok((
            ResultRowKey::scalar(schema)
                .and_then(|key| key.to_canonical_payload())
                .map_err(invalid)?,
            Some(row_payload(schema, &row)?),
        )),
        ResultMutation::Delete { key } if !schema.is_scalar() => {
            key.validate(schema).map_err(invalid)?;
            Ok((key.to_canonical_payload().map_err(invalid)?, None))
        }
        ResultMutation::Upsert { key, row } if !schema.is_scalar() => {
            key.validate(schema).map_err(invalid)?;
            row.validate(schema).map_err(invalid)?;
            if ResultRowKey::from_row(schema, &row).map_err(invalid)? != key {
                return Err(M2Error::InvalidOperatorDefinition);
            }
            Ok((
                key.to_canonical_payload().map_err(invalid)?,
                Some(row_payload(schema, &row)?),
            ))
        }
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
}

fn row_payload(schema: &ResultSchemaV1, row: &TypedResultRowV1) -> Result<Vec<u8>, M2Error> {
    row.validate(schema)
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    row.to_canonical_payload()
        .map_err(|_| M2Error::InvalidOperatorDefinition)
}

fn delete_rows(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    identities: &[Vec<u8>],
) -> Result<(), M2Error> {
    if identities.is_empty() {
        return Ok(());
    }
    let changed = transaction.execute(
        "DELETE FROM shiba_internal.graph_result_row
         WHERE graph_id = $1 AND result_id = $2 AND row_identity = ANY($3)",
        &[&graph_id, &result_id, &identities],
    )?;
    if usize::try_from(changed).ok() != Some(identities.len()) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn upsert_rows(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    schema_digest: [u8; 32],
    rows: &[Upsert],
) -> Result<(), M2Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let identities: Vec<Vec<u8>> = rows.iter().map(|row| row.0.clone()).collect();
    let payloads: Vec<Vec<u8>> = rows.iter().map(|row| row.1.clone()).collect();
    let changed = transaction.execute(
        "INSERT INTO shiba_internal.graph_result_row (
             graph_id, result_id, schema_digest, row_identity, row_payload)
         SELECT $1, $2, $3, * FROM unnest($4::bytea[], $5::bytea[])
         ON CONFLICT (graph_id, result_id, row_identity) DO UPDATE SET
             schema_digest = EXCLUDED.schema_digest,
             row_payload = EXCLUDED.row_payload",
        &[
            &graph_id,
            &result_id,
            &&schema_digest[..],
            &identities,
            &payloads,
        ],
    )?;
    if usize::try_from(changed).ok() != Some(rows.len()) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

pub(super) fn count(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
) -> Result<i64, M2Error> {
    Ok(transaction
        .query_one(
            "SELECT count(*) FROM shiba_internal.graph_result_row
             WHERE graph_id = $1 AND result_id = $2",
            &[&graph_id, &result_id],
        )?
        .get(0))
}
