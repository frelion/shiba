use core::num::NonZeroU32;

use shiba_operator::{
    NodeId, NodeInput, OperatorGraph, OperatorNode, OperatorNodeKind, OutputContract,
    StateContract, TypedLayout, ValueType, source_typed_layout,
};

use crate::binding::{identity_for, source, source_port};
use crate::expression::InputBinding;
use crate::{
    CompilerError, IdentityIndexDescriptor, QUERY_SPEC_VERSION, QueryInputV1, QueryResultShapeV1,
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

/// Compiles a query while permitting only the proven identity-free empty-layout `CountRows` shape.
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
        let (input, kind, output_types, stateful) =
            crate::node_compile::compile(&declaration.operation, &inputs, indexes, descriptors)?;
        if declaration.state_codec_version != stateful.then_some(1) {
            return Err(CompilerError::InvalidSpec);
        }
        nodes.push(OperatorNode {
            node_id,
            input,
            state_contract: stateful.then_some(StateContract { codec_version: 1 }),
            kind,
        });
        layouts.push(layout(index + 1, output_types)?);
    }

    for (index, result) in spec.results.iter().enumerate() {
        let input_index = usize::from(result.input_node - 1);
        let input_layout = layouts
            .get(input_index)
            .ok_or(CompilerError::InvalidTopology)?;
        let (key_slot, value_slot, output) = result_contract(&result.shape, input_layout)?;
        nodes.push(OperatorNode {
            node_id: node_id(spec.nodes.len() + index + 1)?,
            input: NodeInput::Node(node_id(usize::from(result.input_node))?),
            state_contract: None,
            kind: OperatorNodeKind::Materialize {
                key_slot,
                value_slot,
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
    shape: &QueryResultShapeV1,
    layout: &TypedLayout,
) -> Result<(u16, u16, OutputContract), CompilerError> {
    match *shape {
        QueryResultShapeV1::Scalar {
            value_slot,
            value_nullable,
        } if layout.value_types.get(usize::from(value_slot)) == Some(&ValueType::Int8) => Ok((
            0,
            value_slot,
            OutputContract::Scalar {
                value_type: ValueType::Int8,
                nullable: value_nullable,
            },
        )),
        QueryResultShapeV1::Keyed {
            key_slot,
            key_nullable,
            value_slot,
            value_nullable,
        } if layout.value_types.get(usize::from(key_slot)) == Some(&ValueType::Int8)
            && layout.value_types.get(usize::from(value_slot)) == Some(&ValueType::Int8) =>
        {
            Ok((
                key_slot,
                value_slot,
                OutputContract::KeyedRows {
                    key_type: ValueType::Int8,
                    key_nullable,
                    value_type: ValueType::Int8,
                    nullable: value_nullable,
                },
            ))
        }
        _ => Err(CompilerError::WrongType),
    }
}

fn node_id(value: usize) -> Result<NodeId, CompilerError> {
    let value = u32::try_from(value)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(CompilerError::GraphEncoding)?;
    Ok(NodeId::new(value))
}

fn layout(ordinal: usize, value_types: Vec<ValueType>) -> Result<TypedLayout, CompilerError> {
    let mut identity = [0; 32];
    identity[..8].copy_from_slice(&u64::try_from(ordinal).unwrap_or(u64::MAX).to_be_bytes());
    TypedLayout::new(identity, value_types).map_err(|_| CompilerError::GraphEncoding)
}
