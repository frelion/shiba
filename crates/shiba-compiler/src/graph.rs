use core::num::NonZeroU32;

use shiba_operator::{
    EmptyResultV1, NodeId, NodeInput, OperatorGraph, OperatorNode, OperatorNodeKind,
    OutputContract, ResultField, ResultSchemaV1, StateContract, TypedLayout, TypedResultRowV1,
    TypedValue, aggregate_function_descriptor, source_typed_layout,
};

use crate::binding::{identity_for, source, source_port};
use crate::expression::InputBinding;
use crate::{
    CompilerError, IdentityIndexDescriptor, QUERY_SPEC_VERSION, QueryInputV1, QueryResultV1,
    QuerySpecV1, SourceDescriptor,
};

pub(crate) struct CompiledInput<'a> {
    pub(crate) input: NodeInput,
    pub(crate) binding: InputBinding<'a>,
    pub(crate) layout: TypedLayout,
}

/// Compiles one strict query into its only durable canonical operator graph.
///
/// # Errors
///
/// Rejects noncanonical declarations or descriptor, identity, topology, type, and bound drift.
pub fn compile_query(
    spec: &QuerySpecV1,
    descriptors: &[SourceDescriptor],
    indexes: &[IdentityIndexDescriptor],
) -> Result<OperatorGraph, CompilerError> {
    let indexes = indexes.iter().cloned().map(Some).collect::<Vec<_>>();
    compile_query_with_optional_identities(spec, descriptors, &indexes)
}

/// Compiles a query while permitting only the proven identity-free scalar aggregate shape.
///
/// # Errors
///
/// Rejects every other missing, extra, or invalid source identity.
pub fn compile_query_with_optional_identities(
    spec: &QuerySpecV1,
    descriptors: &[SourceDescriptor],
    indexes: &[Option<IdentityIndexDescriptor>],
) -> Result<OperatorGraph, CompilerError> {
    if spec.version != QUERY_SPEC_VERSION
        || QuerySpecV1::from_json(
            &spec
                .to_canonical_json()
                .map_err(|_| CompilerError::InvalidSpec)?,
        )
        .ok()
        .as_ref()
            != Some(spec)
        || descriptors
            .iter()
            .map(|value| value.source_id)
            .collect::<Vec<_>>()
            != spec.sources
        || descriptors.len() != indexes.len()
    {
        return Err(CompilerError::InvalidSpec);
    }

    let ports = descriptors
        .iter()
        .zip(indexes)
        .map(|(descriptor, index)| {
            let identity = index
                .as_ref()
                .map(|value| identity_for(descriptor, core::slice::from_ref(value)))
                .transpose()?
                .map(|value| value.address);
            source_port(descriptor, identity)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut nodes = Vec::with_capacity(spec.nodes.len() + spec.results.len());
    let mut layouts = Vec::with_capacity(spec.nodes.len());

    for (index, declaration) in spec.nodes.iter().enumerate() {
        let node_id = node_id(index + 1)?;
        let inputs = declaration
            .inputs
            .iter()
            .map(|input| compile_input(input, descriptors, &ports, &layouts))
            .collect::<Result<Vec<_>, _>>()?;
        let (input, kind, output_types, nullable, stateful) =
            crate::node_compile::compile(&declaration.operation, &inputs, indexes, descriptors)?;
        if declaration.state_codec_version != stateful.then_some(1) {
            return Err(CompilerError::InvalidSpec);
        }
        let input_layout = inputs
            .first()
            .ok_or(CompilerError::InvalidTopology)?
            .layout
            .clone();
        let output_layout = if matches!(kind, OperatorNodeKind::Filter { .. }) {
            input_layout
        } else {
            TypedLayout::derive(&input_layout, node_id, output_types, nullable)
                .map_err(|_| CompilerError::GraphEncoding)?
        };
        nodes.push(OperatorNode {
            node_id,
            input,
            state_contract: stateful.then_some(StateContract { codec_version: 1 }),
            kind,
        });
        layouts.push(output_layout);
    }

    for (index, result) in spec.results.iter().enumerate() {
        let input_index = usize::from(result.input_node - 1);
        let input_layout = layouts
            .get(input_index)
            .ok_or(CompilerError::InvalidTopology)?;
        let output = result_contract(result, input_layout, &spec.nodes[input_index].operation)?;
        nodes.push(OperatorNode {
            node_id: node_id(spec.nodes.len() + index + 1)?,
            input: NodeInput::Node(node_id(usize::from(result.input_node))?),
            state_contract: None,
            kind: OperatorNodeKind::Materialize {
                field_slots: result.fields.iter().map(|field| field.value_slot).collect(),
                output,
            },
        });
    }
    OperatorGraph::build(spec.graph_id, ports, nodes).map_err(|_| CompilerError::GraphEncoding)
}

fn compile_input<'a>(
    input: &QueryInputV1,
    descriptors: &'a [SourceDescriptor],
    ports: &[shiba_operator::SourcePort],
    layouts: &[TypedLayout],
) -> Result<CompiledInput<'a>, CompilerError> {
    match *input {
        QueryInputV1::Source { source_id } => {
            let descriptor = source(descriptors, source_id)?;
            let port = ports
                .iter()
                .find(|port| port.source_id == source_id)
                .ok_or(CompilerError::SourceMismatch)?;
            Ok(CompiledInput {
                input: NodeInput::SourcePort(source_id),
                binding: InputBinding::Source(descriptor),
                layout: source_typed_layout(source_id, &port.layout)
                    .map_err(|_| CompilerError::GraphEncoding)?,
            })
        }
        QueryInputV1::Node { node } => Ok(CompiledInput {
            input: NodeInput::Node(node_id(usize::from(node))?),
            binding: InputBinding::Node,
            layout: layouts
                .get(usize::from(node - 1))
                .ok_or(CompilerError::InvalidTopology)?
                .clone(),
        }),
    }
}

fn result_contract(
    result: &QueryResultV1,
    layout: &TypedLayout,
    operation: &crate::QueryOperationV1,
) -> Result<OutputContract, CompilerError> {
    let fields = result
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value_type = *layout
                .value_types
                .get(usize::from(field.value_slot))
                .ok_or(CompilerError::WrongType)?;
            let nullable = *layout
                .nullable
                .get(usize::from(field.value_slot))
                .ok_or(CompilerError::WrongType)?;
            if nullable != field.nullable {
                return Err(CompilerError::WrongType);
            }
            Ok(ResultField {
                ordinal: u16::try_from(index + 1).map_err(|_| CompilerError::GraphEncoding)?,
                name: field.name.clone(),
                value_type,
                nullable,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schema = ResultSchemaV1::new(fields, result.key_ordinals.clone())
        .map_err(|_| CompilerError::GraphEncoding)?;
    let initial_row = if schema.is_scalar() {
        let crate::QueryOperationV1::Aggregate {
            group_expressions,
            calls,
            ..
        } = operation
        else {
            return Err(CompilerError::InvalidSpec);
        };
        if !group_expressions.is_empty() {
            return Err(CompilerError::InvalidSpec);
        }
        let aggregate_values = calls
            .iter()
            .map(
                |call| match aggregate_function_descriptor(call.function).empty_result {
                    EmptyResultV1::Int8Zero => TypedValue::Int8(0),
                    EmptyResultV1::Null(value_type) => TypedValue::Null(value_type),
                },
            )
            .collect::<Vec<_>>();
        let values = result
            .fields
            .iter()
            .map(|field| {
                aggregate_values
                    .get(usize::from(field.value_slot))
                    .cloned()
                    .ok_or(CompilerError::WrongType)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Some(TypedResultRowV1::new(&schema, values).map_err(|_| CompilerError::WrongType)?)
    } else {
        None
    };
    Ok(OutputContract {
        schema,
        initial_row,
    })
}

fn node_id(value: usize) -> Result<NodeId, CompilerError> {
    let value = u32::try_from(value)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(CompilerError::GraphEncoding)?;
    Ok(NodeId::new(value))
}
