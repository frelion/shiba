use shiba_operator::{
    CompiledPlan, InputBinding, InputRole, OutputContract, PlanImplementation, ValueType,
};

use crate::{
    CompilerError, OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1,
    POSTGRES_INT8_TYPE_OID, SourceColumnDescriptor, SourceDescriptor,
};

/// Compiles a strict specification to the canonical M13 plan contract.
///
/// # Errors
///
/// Fails closed for identity/version errors, ambiguous or invalid bindings,
/// and canonical codec failure.
pub fn compile_plan(
    spec: &OperatorSpecV1,
    source: &SourceDescriptor,
) -> Result<CompiledPlan, CompilerError> {
    if spec.version != OPERATOR_SPEC_VERSION {
        return Err(CompilerError::UnsupportedVersion(spec.version));
    }
    if spec.source_id != source.source_id {
        return Err(CompilerError::SourceMismatch);
    }
    let (inputs, output, implementation) = match &spec.operation {
        OperatorOperationV1::CountRows => (
            Vec::new(),
            OutputContract::Scalar {
                value_type: ValueType::Int8,
            },
            PlanImplementation::CountRows,
        ),
        OperatorOperationV1::SumInt8 { input_column } => {
            let (input_slot, column) = resolve_int8(source, input_column)?;
            (
                vec![InputBinding {
                    role: InputRole::Payload,
                    address: column.address,
                }],
                OutputContract::Scalar {
                    value_type: ValueType::Int8,
                },
                PlanImplementation::SumInt8 {
                    input: column.address,
                    input_slot,
                },
            )
        }
        OperatorOperationV1::MaterializedProject { .. }
        | OperatorOperationV1::GroupedCount { .. }
        | OperatorOperationV1::GroupedSumInt8 { .. } => {
            let graph = crate::compile_graph(spec, source)?;
            let inputs = graph
                .source_layout
                .iter()
                .enumerate()
                .map(|(index, binding)| InputBinding {
                    role: if index == 0 {
                        InputRole::Key
                    } else {
                        InputRole::Payload
                    },
                    address: binding.address,
                })
                .collect();
            let (key_nullable, nullable) = match &spec.operation {
                OperatorOperationV1::MaterializedProject { .. } => (false, true),
                OperatorOperationV1::GroupedCount { key_column } => (
                    source
                        .columns
                        .iter()
                        .find(|column| column.name == *key_column)
                        .ok_or_else(|| CompilerError::MissingColumn(key_column.clone()))?
                        .nullable,
                    false,
                ),
                OperatorOperationV1::GroupedSumInt8 { key_column, .. } => (
                    source
                        .columns
                        .iter()
                        .find(|column| column.name == *key_column)
                        .ok_or_else(|| CompilerError::MissingColumn(key_column.clone()))?
                        .nullable,
                    true,
                ),
                _ => unreachable!("graph operations matched"),
            };
            (
                inputs,
                OutputContract::KeyedRows {
                    key_type: ValueType::Int8,
                    key_nullable,
                    value_type: ValueType::Int8,
                    nullable,
                },
                PlanImplementation::Graph { graph },
            )
        }
    };
    CompiledPlan::build(
        spec.operator_id,
        spec.source_id,
        inputs,
        output,
        implementation,
    )
    .map_err(|_| CompilerError::PlanEncoding)
}

fn resolve_int8<'a>(
    source: &'a SourceDescriptor,
    name: &str,
) -> Result<(u16, &'a SourceColumnDescriptor), CompilerError> {
    if name.trim().is_empty() {
        return Err(CompilerError::BlankInputColumn);
    }
    let mut matches = source
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name == name);
    let (index, column) = matches
        .next()
        .ok_or_else(|| CompilerError::MissingColumn(name.to_owned()))?;
    if matches.next().is_some() {
        return Err(CompilerError::DuplicateColumn(name.to_owned()));
    }
    if column.type_oid != POSTGRES_INT8_TYPE_OID {
        return Err(CompilerError::WrongColumnType {
            column: name.to_owned(),
            type_oid: column.type_oid,
        });
    }
    Ok((
        u16::try_from(index).map_err(|_| CompilerError::PlanEncoding)?,
        column,
    ))
}
