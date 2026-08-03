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
            let column = resolve_int8(source, input_column)?;
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
                },
            )
        }
        OperatorOperationV1::ProjectRows {
            key_column,
            input_column,
        } => {
            let key = resolve_int8(source, key_column)?;
            if key.nullable {
                return Err(CompilerError::NullableKey(key_column.clone()));
            }
            let value = resolve_int8(source, input_column)?;
            (
                vec![
                    InputBinding {
                        role: InputRole::Key,
                        address: key.address,
                    },
                    InputBinding {
                        role: InputRole::Payload,
                        address: value.address,
                    },
                ],
                OutputContract::KeyedRows {
                    key_type: ValueType::Int8,
                    value_type: ValueType::Int8,
                    nullable: true,
                },
                PlanImplementation::ProjectRows {
                    key: key.address,
                    value: value.address,
                },
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
) -> Result<&'a SourceColumnDescriptor, CompilerError> {
    if name.trim().is_empty() {
        return Err(CompilerError::BlankInputColumn);
    }
    let mut matches = source.columns.iter().filter(|column| column.name == name);
    let column = matches
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
    Ok(column)
}
