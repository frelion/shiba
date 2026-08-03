use postgres::Transaction;
use shiba_compiler::{CompilerError, QuerySpecV1};
use shiba_operator::{OperatorGraph, OutputContract, TypedValue, ValueType};

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
         VALUES ($1, $2, 2, $3, $4, $5, $6, 1)",
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
    insert_results(transaction, graph, status)?;
    Ok(())
}

fn insert_results(
    transaction: &mut Transaction<'_>,
    graph: &OperatorGraph,
    status: &str,
) -> Result<(), RegistrationError> {
    let graph_id = bigint(graph.graph_id.get())?;
    for (result_id, output) in graph.result_contracts() {
        let (shape, key_type, key_nullable, value_type, value_nullable) = metadata(output);
        let initial = match output {
            OutputContract::Scalar { nullable: true, .. } => {
                Some(TypedValue::Null(ValueType::Int8))
            }
            OutputContract::Scalar {
                nullable: false, ..
            } => Some(TypedValue::Int8(0)),
            OutputContract::KeyedRows { .. } => None,
        };
        let payload = initial
            .as_ref()
            .map(TypedValue::to_canonical_json)
            .transpose()
            .map_err(|_| M2Error::InvalidOperatorDefinition)?;
        let scalar_nullable = matches!(output, OutputContract::Scalar { nullable: true, .. });
        let value = initial
            .as_ref()
            .map(|value| crate::result_sink::scalar_value(value, scalar_nullable))
            .transpose()?
            .flatten();
        transaction.execute(
            "INSERT INTO shiba.graph_result (
                 graph_id, result_id, output_shape, output_key_type,
                 output_key_nullable, output_value_type, output_value_nullable,
                 result_status, value_payload, value_bigint)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            &[
                &graph_id,
                &i64::from(result_id.get()),
                &shape,
                &key_type,
                &key_nullable,
                &value_type,
                &value_nullable,
                &status,
                &if status == "active" { payload } else { None },
                &if status == "active" { value } else { None },
            ],
        )?;
    }
    Ok(())
}

pub(super) fn result_contracts(graph: &OperatorGraph) -> Vec<GraphResultContract> {
    graph
        .result_contracts()
        .map(|(result_id, output)| {
            let (shape, _, key_nullable, _, value_nullable) = metadata(output);
            GraphResultContract {
                result_id: i64::from(result_id.get()),
                output_shape: shape,
                key_nullable,
                value_nullable,
            }
        })
        .collect()
}

fn metadata(
    contract: &OutputContract,
) -> (&'static str, Option<&'static str>, bool, &'static str, bool) {
    match contract {
        OutputContract::Scalar {
            value_type,
            nullable,
        } => ("scalar", None, false, type_name(*value_type), *nullable),
        OutputContract::KeyedRows {
            key_type,
            key_nullable,
            value_type,
            nullable,
        } => (
            "keyed",
            Some(type_name(*key_type)),
            *key_nullable,
            type_name(*value_type),
            *nullable,
        ),
    }
}

fn type_name(value: ValueType) -> &'static str {
    match value {
        ValueType::Bool => "bool",
        ValueType::Int8 => "int8",
        ValueType::Text => "text",
    }
}

pub(super) fn bigint(value: u64) -> Result<i64, M2Error> {
    i64::try_from(value).map_err(|_| M2Error::InvalidOperatorDefinition)
}
