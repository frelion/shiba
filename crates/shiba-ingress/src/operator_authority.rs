use std::collections::HashSet;

use postgres::GenericClient;
use shiba_operator::{ObjectAddress, OperatorGraph, OutputContract};
use shiba_protocol::{GraphId, SourceId};

use crate::IngressError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphFingerprint {
    pub(crate) graph_id: GraphId,
    pub(crate) digest: [u8; 32],
}

pub(crate) fn load_graph_fingerprint(
    client: &mut impl GenericClient,
    graph_id: GraphId,
) -> Result<GraphFingerprint, IngressError> {
    load_graph(client, graph_id, None, false)
}

pub(crate) fn validate_prepared_graph(
    client: &mut impl GenericClient,
    graph_id: GraphId,
    expected: &GraphFingerprint,
) -> Result<(), IngressError> {
    let actual = load_graph(client, graph_id, Some("building"), true)?;
    if &actual == expected {
        Ok(())
    } else {
        Err(IngressError::Governance("prepared graph authority drifted"))
    }
}

fn load_graph(
    client: &mut impl GenericClient,
    graph_id: GraphId,
    expected_status: Option<&str>,
    require_pristine: bool,
) -> Result<GraphFingerprint, IngressError> {
    let graph_key = i64::try_from(graph_id.get())
        .map_err(|_| IngressError::Governance("graph ID exceeds bigint"))?;
    let row = client
        .query_opt(
            "SELECT compiler_version, graph_format_version, graph_payload,
                graph_digest, state_codec_version, source_count
         FROM shiba_internal.graph_definition WHERE graph_id = $1",
            &[&graph_key],
        )?
        .ok_or(IngressError::Governance("graph definition is missing"))?;
    let digest = exact_digest(row.get(3))?;
    let graph = OperatorGraph::from_canonical_payload(&row.get::<_, Vec<u8>>(2), digest)
        .map_err(|_| IngressError::Governance("compiled graph is invalid"))?;
    if graph.graph_id != graph_id
        || row.get::<_, i32>(0) != 3
        || u32::try_from(row.get::<_, i32>(1)).ok() != Some(graph.format_version)
        || row.get::<_, i32>(4) != 1
        || usize::try_from(row.get::<_, i16>(5)).ok() != Some(graph.sources.len())
    {
        return Err(IngressError::Governance(
            "graph definition metadata drifted",
        ));
    }
    let members = load_members(client, graph_key)?;
    if graph.sources.len() != members.len()
        || graph.sources.iter().any(|source| {
            !members.get(&source.source_id).is_some_and(|inputs| {
                source
                    .identity_index
                    .is_some_and(|index| inputs.contains(&index))
                    && source
                        .layout
                        .iter()
                        .all(|column| inputs.contains(&column.address))
            })
        })
    {
        return Err(IngressError::Governance("graph source binding drifted"));
    }
    validate_results(client, graph_key, &graph, expected_status, require_pristine)?;
    Ok(GraphFingerprint { graph_id, digest })
}

fn validate_results(
    client: &mut impl GenericClient,
    graph_key: i64,
    graph: &OperatorGraph,
    expected_status: Option<&str>,
    require_pristine: bool,
) -> Result<(), IngressError> {
    let outputs = graph
        .result_contracts()
        .map(|(result_id, output)| (i64::from(result_id.get()), output))
        .collect::<Vec<_>>();
    if outputs.is_empty() {
        return Err(IngressError::Governance("graph has no materialized result"));
    }
    let result_count: i64 = client
        .query_one(
            "SELECT count(*) FROM shiba.graph_result WHERE graph_id = $1",
            &[&graph_key],
        )?
        .get(0);
    if usize::try_from(result_count).ok() != Some(outputs.len()) {
        return Err(IngressError::Governance(
            "graph result header set is incomplete",
        ));
    }
    for (result_id, output) in outputs {
        let result = client
            .query_opt(
                "SELECT result_status, schema_payload, schema_digest
             FROM shiba.graph_result WHERE graph_id = $1 AND result_id = $2",
                &[&graph_key, &result_id],
            )?
            .ok_or(IngressError::Governance("graph result header is missing"))?;
        if !output_matches(output, &result)
            || expected_status.is_some_and(|status| result.get::<_, &str>(0) != status)
        {
            return Err(IngressError::Governance("graph result authority drifted"));
        }
    }
    if require_pristine {
        let state_count: i64 = client
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_node_state WHERE graph_id = $1",
                &[&graph_key],
            )?
            .get(0);
        let result_count: i64 = client
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_result_row WHERE graph_id = $1",
                &[&graph_key],
            )?
            .get(0);
        if state_count != 0 || result_count != 0 {
            return Err(IngressError::Governance(
                "prepared graph state is not pristine",
            ));
        }
    }
    Ok(())
}

fn load_members(
    client: &mut impl GenericClient,
    graph_id: i64,
) -> Result<std::collections::BTreeMap<SourceId, HashSet<ObjectAddress>>, IngressError> {
    let mut members = std::collections::BTreeMap::new();
    for row in client.query(
        "SELECT member.source_id, binding.address_classid::bigint,
                binding.address_objid::bigint, binding.address_objsubid
         FROM shiba_internal.graph_source_member AS member
         JOIN shiba_internal.source_binding AS binding USING (source_id)
         WHERE member.graph_id = $1 ORDER BY member.input_ordinal, binding.binding_kind",
        &[&graph_id],
    )? {
        let raw: i64 = row.get(0);
        let source = u64::try_from(raw)
            .ok()
            .and_then(|value| SourceId::new(value).ok())
            .ok_or(IngressError::Governance("source ID is invalid"))?;
        members
            .entry(source)
            .or_insert_with(HashSet::new)
            .insert(ObjectAddress {
                class_id: u32::try_from(row.get::<_, i64>(1))
                    .map_err(|_| IngressError::Governance("binding class ID is invalid"))?,
                object_id: u32::try_from(row.get::<_, i64>(2))
                    .map_err(|_| IngressError::Governance("binding object ID is invalid"))?,
                sub_id: row.get(3),
            });
    }
    Ok(members)
}

fn output_matches(contract: &OutputContract, row: &postgres::Row) -> bool {
    contract.validate().is_ok()
        && row.get::<_, Vec<u8>>(1) == contract.schema.canonical_payload
        && exact_digest(row.get(2)).ok() == Some(contract.schema.digest)
}

fn exact_digest(value: Vec<u8>) -> Result<[u8; 32], IngressError> {
    value
        .try_into()
        .map_err(|_| IngressError::Governance("graph digest is invalid"))
}
