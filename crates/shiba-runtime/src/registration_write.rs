use postgres::Transaction;
use shiba_compiler::{CompilerError, QuerySpecV1};
use shiba_operator::{OperatorGraph, ResultRowKey};

use crate::{
    M2Error,
    registration::{GraphResultContract, RegistrationError},
};

pub(super) fn insert_graph(
    transaction: &mut Transaction<'_>,
    spec: &QuerySpecV1,
    graph: &OperatorGraph,
    status: &str,
) -> Result<(), RegistrationError> {
    let graph_id = bigint(graph.graph_id.get())?;
    let spec_payload = spec
        .to_canonical_json()
        .map_err(|_| CompilerError::GraphEncoding)?;
    transaction.execute(
        "INSERT INTO shiba_internal.graph_definition (
             graph_id, source_count, compiler_version, spec_payload,
             graph_format_version, graph_payload, graph_digest, state_codec_version)
         VALUES ($1, $2, 3, $3, $4, $5, $6, 1)",
        &[
            &graph_id,
            &i16::try_from(graph.sources.len()).map_err(|_| M2Error::InvalidOperatorDefinition)?,
            &spec_payload,
            &i32::try_from(graph.format_version).map_err(|_| M2Error::InvalidOperatorDefinition)?,
            &graph.canonical_payload,
            &&graph.digest[..],
        ],
    )?;
    for (ordinal, source) in graph.sources.iter().enumerate() {
        transaction.execute(
            "INSERT INTO shiba_internal.graph_source_member
                 (graph_id, source_id, input_ordinal, graph_digest)
             VALUES ($1, $2, $3, $4)",
            &[
                &graph_id,
                &bigint(source.source_id.get())?,
                &i16::try_from(ordinal).map_err(|_| M2Error::InvalidOperatorDefinition)?,
                &&graph.digest[..],
            ],
        )?;
    }
    insert_results(transaction, graph, status)
}

fn insert_results(
    transaction: &mut Transaction<'_>,
    graph: &OperatorGraph,
    status: &str,
) -> Result<(), RegistrationError> {
    let graph_id = bigint(graph.graph_id.get())?;
    for (result_id, output) in graph.result_contracts() {
        output
            .validate()
            .map_err(|_| M2Error::InvalidOperatorDefinition)?;
        let result_id = i64::from(result_id.get());
        transaction.execute(
            "INSERT INTO shiba.graph_result (
                 graph_id, result_id, result_status, schema_payload, schema_digest)
             VALUES ($1, $2, $3, $4, $5)",
            &[
                &graph_id,
                &result_id,
                &status,
                &output.schema.canonical_payload,
                &&output.schema.digest[..],
            ],
        )?;
        if let Some(row) = &output.initial_row {
            let identity = ResultRowKey::scalar(&output.schema)
                .and_then(|key| key.to_canonical_payload())
                .map_err(|_| M2Error::InvalidOperatorDefinition)?;
            let payload = row
                .to_canonical_payload()
                .map_err(|_| M2Error::InvalidOperatorDefinition)?;
            transaction.execute(
                "INSERT INTO shiba_internal.graph_result_row (
                     graph_id, result_id, schema_digest, row_identity, row_payload)
                 VALUES ($1, $2, $3, $4, $5)",
                &[
                    &graph_id,
                    &result_id,
                    &&output.schema.digest[..],
                    &identity,
                    &payload,
                ],
            )?;
        }
    }
    Ok(())
}

pub(super) fn result_contracts(graph: &OperatorGraph) -> Vec<GraphResultContract> {
    graph
        .result_contracts()
        .map(|(result_id, output)| GraphResultContract {
            result_id: i64::from(result_id.get()),
            schema_payload: output.schema.canonical_payload.clone(),
            schema_digest: output.schema.digest,
        })
        .collect()
}

pub(super) fn bigint(value: u64) -> Result<i64, M2Error> {
    i64::try_from(value).map_err(|_| M2Error::InvalidOperatorDefinition)
}
