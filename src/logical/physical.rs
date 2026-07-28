//! Versioned physical DAG compiled from the persisted logical plan.
//!
//! The physical plan records fusion and materialization decisions without
//! generating SQL or consulting PostgreSQL catalogs. Runtime execution can
//! therefore persist, inspect, and deterministically reproduce the same stage
//! graph. Stage materialization is a cache boundary, not a worker boundary.

use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::model::{LogicalPlan, OperatorKind};
use super::validate::{ExecutionDescriptor, ExecutionJoinType, ExecutionPipeline};

pub(super) const PHYSICAL_PLAN_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum PhysicalKernel {
    Source,
    Stateless,
    Aggregate,
    Join,
    Distinct,
    Window,
    TopN,
    Sink,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(super) enum StageStorage {
    Inline,
    StatementMaterialized,
    Unlogged,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(super) enum MaterializationReason {
    ReusedWithinStatement,
    MultiplePhysicalConsumers,
    JoinInputDelta,
    JoinDelta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct PhysicalInput {
    pub(super) stage_id: u32,
    pub(super) input: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct PhysicalStage {
    pub(super) stage_id: u32,
    pub(super) kernel: PhysicalKernel,
    pub(super) node_ids: Vec<String>,
    pub(super) inputs: Vec<PhysicalInput>,
    pub(super) consumer_count: u32,
    pub(super) consumer_ids: Vec<u32>,
    pub(super) storage: StageStorage,
    pub(super) materialization_reasons: Vec<MaterializationReason>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct PhysicalDagPlan {
    pub(super) version: u32,
    pub(super) result_oid: u32,
    pub(super) descriptor: ExecutionDescriptor,
    /// Sorted to keep both the in-memory plan and its JSON representation stable.
    pub(super) source_oids: Vec<u32>,
    pub(super) stages: Vec<PhysicalStage>,
    encoded_descriptor: String,
}

impl PhysicalDagPlan {
    pub(super) fn compile(
        logical: &LogicalPlan,
        descriptor: ExecutionDescriptor,
        source_oids: HashSet<u32>,
    ) -> Result<Self, String> {
        let result_oid = logical
            .nodes
            .iter()
            .find(|node| node.operator == OperatorKind::Sink)
            .and_then(|node| node.config.get("result_oid"))
            .and_then(serde_json::Value::as_u64)
            .and_then(|oid| u32::try_from(oid).ok())
            .ok_or_else(|| "physical DAG sink has no valid result OID".to_string())?;
        let mut node_indexes = HashMap::with_capacity(logical.nodes.len());
        for (index, node) in logical.nodes.iter().enumerate() {
            if node_indexes.insert(node.id.as_str(), index).is_some() {
                return Err(format!("duplicate physical node ID {}", node.id));
            }
        }

        let mut incoming = vec![0_usize; logical.nodes.len()];
        let mut outgoing = vec![Vec::new(); logical.nodes.len()];
        let mut incoming_edges = vec![Vec::new(); logical.nodes.len()];
        for edge in &logical.edges {
            let from = *node_indexes
                .get(edge.from.as_str())
                .ok_or_else(|| format!("physical edge has missing source {}", edge.from))?;
            let to = *node_indexes
                .get(edge.to.as_str())
                .ok_or_else(|| format!("physical edge has missing target {}", edge.to))?;
            incoming[to] += 1;
            outgoing[from].push((to, edge.input));
            incoming_edges[to].push((from, edge.input));
        }

        for edges in &mut outgoing {
            edges.sort_by(|(left_index, left_input), (right_index, right_input)| {
                logical.nodes[*left_index]
                    .id
                    .cmp(&logical.nodes[*right_index].id)
                    .then_with(|| left_input.cmp(right_input))
            });
        }
        for edges in &mut incoming_edges {
            edges.sort_by(|(left_index, left_input), (right_index, right_input)| {
                left_input.cmp(right_input).then_with(|| {
                    logical.nodes[*left_index]
                        .id
                        .cmp(&logical.nodes[*right_index].id)
                })
            });
        }

        let mut queue: BTreeSet<_> = incoming
            .iter()
            .enumerate()
            .filter(|(_, degree)| **degree == 0)
            .map(|(index, _)| (logical.nodes[index].id.as_str(), index))
            .collect();
        let mut remaining = incoming;
        let mut order = Vec::with_capacity(logical.nodes.len());
        while let Some((_, node_index)) = queue.pop_first() {
            order.push(node_index);
            for &(downstream, _) in &outgoing[node_index] {
                remaining[downstream] -= 1;
                if remaining[downstream] == 0 {
                    queue.insert((logical.nodes[downstream].id.as_str(), downstream));
                }
            }
        }
        if order.len() != logical.nodes.len() {
            return Err("physical DAG compilation encountered a cycle".into());
        }

        let mut stages = Vec::<PhysicalStage>::new();
        let mut node_stage = vec![usize::MAX; logical.nodes.len()];
        for node_index in order {
            let node = &logical.nodes[node_index];
            let kernel = kernel_for(node.operator);
            let fused_stage =
                if is_fusible_stateless(node.operator) && incoming_edges[node_index].len() == 1 {
                    let upstream = incoming_edges[node_index][0].0;
                    let stage_index = node_stage[upstream];
                    (outgoing[upstream].len() == 1
                        && matches!(
                            stages[stage_index].kernel,
                            PhysicalKernel::Source | PhysicalKernel::Stateless
                        ))
                    .then_some(stage_index)
                } else {
                    None
                };

            if let Some(stage_index) = fused_stage {
                stages[stage_index].node_ids.push(node.id.clone());
                node_stage[node_index] = stage_index;
                continue;
            }

            let inputs = incoming_edges[node_index]
                .iter()
                .map(|(upstream, input)| PhysicalInput {
                    stage_id: node_stage[*upstream] as u32,
                    input: *input,
                })
                .collect();
            let stage_id = stages.len() as u32;
            stages.push(PhysicalStage {
                stage_id,
                kernel,
                node_ids: vec![node.id.clone()],
                inputs,
                consumer_count: 0,
                consumer_ids: Vec::new(),
                storage: StageStorage::Inline,
                materialization_reasons: Vec::new(),
            });
            node_stage[node_index] = stage_id as usize;
        }

        annotate_consumers_and_materialization(&mut stages);

        let mut source_oids: Vec<_> = source_oids.into_iter().collect();
        source_oids.sort_unstable();
        let encoded_descriptor = serde_json::to_string(&descriptor)
            .map_err(|error| format!("execution descriptor is not serializable: {error}"))?;
        Ok(Self {
            version: PHYSICAL_PLAN_VERSION,
            result_oid,
            descriptor,
            source_oids,
            stages,
            encoded_descriptor,
        })
    }

    pub(super) fn encoded_descriptor(&self) -> &str {
        &self.encoded_descriptor
    }

    pub(super) fn validate_for_result(&self, result_oid: u32) -> Result<(), String> {
        if self.version != PHYSICAL_PLAN_VERSION {
            return Err(format!(
                "unsupported physical plan version {} (expected {PHYSICAL_PLAN_VERSION})",
                self.version
            ));
        }
        if self.result_oid != result_oid {
            return Err(format!(
                "physical plan result OID {} does not match requested result {result_oid}",
                self.result_oid
            ));
        }
        let mut descriptor_sources = vec![self.descriptor.left_source_oid];
        if let Some(right_source_oid) = self.descriptor.right_source_oid {
            descriptor_sources.push(right_source_oid);
        }
        descriptor_sources.sort_unstable();
        descriptor_sources.dedup();
        if descriptor_sources != self.source_oids {
            return Err("physical plan source OIDs do not match its execution descriptor".into());
        }
        match self.descriptor.pipeline {
            ExecutionPipeline::Join
                if self.descriptor.right_source_oid.is_none()
                    || self.descriptor.join_type.is_none() =>
            {
                return Err("join physical plan requires a right source and join type".into());
            }
            ExecutionPipeline::Join => {}
            _ if self.descriptor.right_source_oid.is_some()
                || self.descriptor.join_type.is_some() =>
            {
                return Err(
                    "non-join physical plan cannot have a right source or join type".into(),
                );
            }
            _ => {}
        }
        if !self.source_oids.windows(2).all(|oids| oids[0] < oids[1]) {
            return Err("physical plan source OIDs must be sorted and unique".into());
        }
        if self.stages.is_empty() {
            return Err("physical plan has no stages".into());
        }

        let mut actual_consumers = vec![BTreeSet::new(); self.stages.len()];
        for (index, stage) in self.stages.iter().enumerate() {
            if stage.stage_id as usize != index {
                return Err("physical plan stage IDs must be contiguous and ordered".into());
            }
            for input in &stage.inputs {
                if input.stage_id >= stage.stage_id {
                    return Err(format!(
                        "physical stage {} has a non-upstream input",
                        stage.stage_id
                    ));
                }
                actual_consumers[input.stage_id as usize].insert(stage.stage_id);
            }
            if stage.consumer_count as usize != stage.consumer_ids.len()
                || !stage
                    .consumer_ids
                    .windows(2)
                    .all(|consumers| consumers[0] < consumers[1])
                || stage
                    .consumer_ids
                    .iter()
                    .any(|consumer| *consumer <= stage.stage_id)
            {
                return Err(format!(
                    "physical stage {} has invalid consumers",
                    stage.stage_id
                ));
            }
            if !stage
                .materialization_reasons
                .windows(2)
                .all(|reasons| reasons[0] < reasons[1])
            {
                return Err(format!(
                    "physical stage {} has unsorted or duplicate materialization reasons",
                    stage.stage_id
                ));
            }
            if (stage.storage == StageStorage::Inline) != stage.materialization_reasons.is_empty() {
                return Err(format!(
                    "physical stage {} has inconsistent materialization storage and reasons",
                    stage.stage_id
                ));
            }
        }
        for stage in &self.stages {
            let expected: Vec<_> = actual_consumers[stage.stage_id as usize]
                .iter()
                .copied()
                .collect();
            if stage.consumer_ids != expected {
                return Err(format!(
                    "physical stage {} consumer metadata does not match its inputs",
                    stage.stage_id
                ));
            }
        }

        let sinks: Vec<_> = self
            .stages
            .iter()
            .filter(|stage| stage.kernel == PhysicalKernel::Sink)
            .collect();
        if sinks.len() != 1 || sinks[0].inputs.len() != 1 || sinks[0].consumer_count != 0 {
            return Err("physical plan must have exactly one terminal Sink with one input".into());
        }
        let source_stage_count = self
            .stages
            .iter()
            .filter(|stage| stage.kernel == PhysicalKernel::Source)
            .count();
        if source_stage_count != self.source_oids.len() {
            return Err("physical plan Source stages do not match its execution descriptor".into());
        }

        let joins: Vec<_> = self
            .stages
            .iter()
            .filter(|stage| stage.kernel == PhysicalKernel::Join)
            .collect();
        if (self.descriptor.pipeline == ExecutionPipeline::Join) != (joins.len() == 1) {
            return Err("physical plan Join stages do not match its execution descriptor".into());
        }
        for join in joins {
            if join.inputs.len() != 2
                || join.storage != StageStorage::Unlogged
                || !join
                    .materialization_reasons
                    .contains(&MaterializationReason::JoinDelta)
                || join.inputs.iter().any(|input| {
                    let input_stage = &self.stages[input.stage_id as usize];
                    input_stage.storage != StageStorage::StatementMaterialized
                        || !input_stage
                            .materialization_reasons
                            .contains(&MaterializationReason::JoinInputDelta)
                })
            {
                return Err(format!(
                    "physical Join stage {} has invalid delta materialization",
                    join.stage_id
                ));
            }
        }
        Ok(())
    }
}

fn is_fusible_stateless(operator: OperatorKind) -> bool {
    matches!(
        operator,
        OperatorKind::Filter | OperatorKind::Project | OperatorKind::Having
    )
}

fn annotate_consumers_and_materialization(stages: &mut [PhysicalStage]) {
    let mut consumers = vec![BTreeSet::new(); stages.len()];
    let mut references = vec![0_usize; stages.len()];
    for stage in stages.iter() {
        for input in &stage.inputs {
            let upstream = input.stage_id as usize;
            consumers[upstream].insert(stage.stage_id);
            references[upstream] += 1;
        }
    }

    for stage in stages.iter_mut() {
        let stage_index = stage.stage_id as usize;
        stage.consumer_ids = consumers[stage_index].iter().copied().collect();
        stage.consumer_count = stage.consumer_ids.len() as u32;
        if stage.consumer_count > 1 {
            promote_stage(
                stage,
                StageStorage::Unlogged,
                MaterializationReason::MultiplePhysicalConsumers,
            );
        } else if references[stage_index] > 1 {
            promote_stage(
                stage,
                StageStorage::StatementMaterialized,
                MaterializationReason::ReusedWithinStatement,
            );
        }
        if matches!(
            stage.kernel,
            PhysicalKernel::Aggregate
                | PhysicalKernel::Distinct
                | PhysicalKernel::Window
                | PhysicalKernel::TopN
        ) {
            // These kernels feed more than one state/sink DML consumer inside
            // their generated statement even when the logical graph has only
            // one downstream edge.
            promote_stage(
                stage,
                StageStorage::StatementMaterialized,
                MaterializationReason::ReusedWithinStatement,
            );
        }
    }

    let join_stage_ids: Vec<_> = stages
        .iter()
        .filter(|stage| stage.kernel == PhysicalKernel::Join)
        .map(|stage| stage.stage_id)
        .collect();
    for join_stage_id in join_stage_ids {
        let input_stage_ids: BTreeSet<_> = stages[join_stage_id as usize]
            .inputs
            .iter()
            .map(|input| input.stage_id)
            .collect();
        for input_stage_id in input_stage_ids {
            promote_stage(
                &mut stages[input_stage_id as usize],
                StageStorage::StatementMaterialized,
                MaterializationReason::JoinInputDelta,
            );
        }
        promote_stage(
            &mut stages[join_stage_id as usize],
            StageStorage::Unlogged,
            MaterializationReason::JoinDelta,
        );
    }
}

fn promote_stage(stage: &mut PhysicalStage, storage: StageStorage, reason: MaterializationReason) {
    stage.storage = stage.storage.max(storage);
    if !stage.materialization_reasons.contains(&reason) {
        stage.materialization_reasons.push(reason);
        stage.materialization_reasons.sort_unstable();
    }
}

fn kernel_for(operator: OperatorKind) -> PhysicalKernel {
    match operator {
        OperatorKind::Scan => PhysicalKernel::Source,
        OperatorKind::Filter | OperatorKind::Project | OperatorKind::Having => {
            PhysicalKernel::Stateless
        }
        OperatorKind::Aggregate => PhysicalKernel::Aggregate,
        operator if operator.is_join() => PhysicalKernel::Join,
        OperatorKind::Distinct => PhysicalKernel::Distinct,
        OperatorKind::Window => PhysicalKernel::Window,
        OperatorKind::TopN => PhysicalKernel::TopN,
        OperatorKind::Sink => PhysicalKernel::Sink,
        _ => unreachable!("all logical operator kinds have a physical kernel"),
    }
}

#[derive(Serialize, Deserialize)]
struct PhysicalDagPlanWire {
    version: u32,
    result_oid: u32,
    descriptor: ExecutionDescriptorWire,
    source_oids: Vec<u32>,
    stages: Vec<PhysicalStage>,
}

#[derive(Serialize, Deserialize)]
struct ExecutionDescriptorWire {
    pipeline: ExecutionPipelineWire,
    left_source_oid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    right_source_oid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    join_type: Option<ExecutionJoinTypeWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionPipelineWire {
    Aggregate,
    Join,
    Window,
    Distinct,
    #[serde(rename = "topn")]
    TopN,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionJoinTypeWire {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    NullAnti,
}

impl From<&ExecutionDescriptor> for ExecutionDescriptorWire {
    fn from(descriptor: &ExecutionDescriptor) -> Self {
        Self {
            pipeline: match descriptor.pipeline {
                ExecutionPipeline::Aggregate => ExecutionPipelineWire::Aggregate,
                ExecutionPipeline::Join => ExecutionPipelineWire::Join,
                ExecutionPipeline::Window => ExecutionPipelineWire::Window,
                ExecutionPipeline::Distinct => ExecutionPipelineWire::Distinct,
                ExecutionPipeline::TopN => ExecutionPipelineWire::TopN,
            },
            left_source_oid: descriptor.left_source_oid,
            right_source_oid: descriptor.right_source_oid,
            join_type: descriptor.join_type.map(|join_type| match join_type {
                ExecutionJoinType::Inner => ExecutionJoinTypeWire::Inner,
                ExecutionJoinType::Left => ExecutionJoinTypeWire::Left,
                ExecutionJoinType::Right => ExecutionJoinTypeWire::Right,
                ExecutionJoinType::Full => ExecutionJoinTypeWire::Full,
                ExecutionJoinType::Semi => ExecutionJoinTypeWire::Semi,
                ExecutionJoinType::Anti => ExecutionJoinTypeWire::Anti,
                ExecutionJoinType::NullAnti => ExecutionJoinTypeWire::NullAnti,
            }),
        }
    }
}

impl From<ExecutionDescriptorWire> for ExecutionDescriptor {
    fn from(descriptor: ExecutionDescriptorWire) -> Self {
        Self {
            pipeline: match descriptor.pipeline {
                ExecutionPipelineWire::Aggregate => ExecutionPipeline::Aggregate,
                ExecutionPipelineWire::Join => ExecutionPipeline::Join,
                ExecutionPipelineWire::Window => ExecutionPipeline::Window,
                ExecutionPipelineWire::Distinct => ExecutionPipeline::Distinct,
                ExecutionPipelineWire::TopN => ExecutionPipeline::TopN,
            },
            left_source_oid: descriptor.left_source_oid,
            right_source_oid: descriptor.right_source_oid,
            join_type: descriptor.join_type.map(|join_type| match join_type {
                ExecutionJoinTypeWire::Inner => ExecutionJoinType::Inner,
                ExecutionJoinTypeWire::Left => ExecutionJoinType::Left,
                ExecutionJoinTypeWire::Right => ExecutionJoinType::Right,
                ExecutionJoinTypeWire::Full => ExecutionJoinType::Full,
                ExecutionJoinTypeWire::Semi => ExecutionJoinType::Semi,
                ExecutionJoinTypeWire::Anti => ExecutionJoinType::Anti,
                ExecutionJoinTypeWire::NullAnti => ExecutionJoinType::NullAnti,
            }),
        }
    }
}

impl Serialize for PhysicalDagPlan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        PhysicalDagPlanWire {
            version: self.version,
            result_oid: self.result_oid,
            descriptor: ExecutionDescriptorWire::from(&self.descriptor),
            source_oids: self.source_oids.clone(),
            stages: self.stages.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PhysicalDagPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PhysicalDagPlanWire::deserialize(deserializer)?;
        let descriptor = ExecutionDescriptor::from(wire.descriptor);
        let encoded_descriptor =
            serde_json::to_string(&descriptor).map_err(serde::de::Error::custom)?;
        let plan = Self {
            version: wire.version,
            result_oid: wire.result_oid,
            descriptor,
            source_oids: wire.source_oids,
            stages: wire.stages,
            encoded_descriptor,
        };
        plan.validate_for_result(plan.result_oid)
            .map_err(serde::de::Error::custom)?;
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::logical::model::{LogicalEdge, LogicalNode, LOGICAL_PLAN_VERSION};

    fn descriptor(pipeline: ExecutionPipeline) -> ExecutionDescriptor {
        ExecutionDescriptor {
            pipeline,
            left_source_oid: 41,
            right_source_oid: None,
            join_type: None,
        }
    }

    fn node(id: &str, operator: OperatorKind) -> LogicalNode {
        LogicalNode {
            id: id.into(),
            operator,
            config: if operator == OperatorKind::Sink {
                json!({"result_oid": 99})
            } else {
                json!({})
            },
        }
    }

    fn edge(from: &str, to: &str, input: u16) -> LogicalEdge {
        LogicalEdge {
            from: from.into(),
            to: to.into(),
            input,
        }
    }

    fn plan(nodes: Vec<LogicalNode>, edges: Vec<LogicalEdge>) -> LogicalPlan {
        LogicalPlan {
            version: LOGICAL_PLAN_VERSION,
            nodes,
            edges,
        }
    }

    #[test]
    fn fuses_a_single_consumer_stateless_source_chain() {
        let logical = plan(
            vec![
                node("scan", OperatorKind::Scan),
                node("filter", OperatorKind::Filter),
                node("project", OperatorKind::Project),
                node("sink", OperatorKind::Sink),
            ],
            vec![
                edge("scan", "filter", 0),
                edge("filter", "project", 0),
                edge("project", "sink", 0),
            ],
        );
        let physical = PhysicalDagPlan::compile(
            &logical,
            descriptor(ExecutionPipeline::Distinct),
            HashSet::from([41]),
        )
        .unwrap();

        assert_eq!(physical.stages.len(), 2);
        assert_eq!(physical.stages[0].stage_id, 0);
        assert_eq!(physical.stages[0].node_ids, ["scan", "filter", "project"]);
        assert_eq!(physical.stages[0].storage, StageStorage::Inline);
        assert_eq!(physical.stages[0].consumer_ids, [1]);
        assert_eq!(physical.stages[1].kernel, PhysicalKernel::Sink);
        assert_eq!(
            physical.stages[1].inputs,
            [PhysicalInput {
                stage_id: 0,
                input: 0
            }]
        );
    }

    #[test]
    fn materializes_fanout_for_multiple_physical_consumers() {
        let logical = plan(
            vec![
                node("scan", OperatorKind::Scan),
                node("filter_a", OperatorKind::Filter),
                node("filter_b", OperatorKind::Filter),
                node("sink_a", OperatorKind::Sink),
                node("sink_b", OperatorKind::Sink),
            ],
            vec![
                edge("scan", "filter_a", 0),
                edge("scan", "filter_b", 0),
                edge("filter_a", "sink_a", 0),
                edge("filter_b", "sink_b", 0),
            ],
        );
        let physical = PhysicalDagPlan::compile(
            &logical,
            descriptor(ExecutionPipeline::Distinct),
            HashSet::from([41]),
        )
        .unwrap();
        let scan = physical
            .stages
            .iter()
            .find(|stage| stage.node_ids == ["scan"])
            .unwrap();

        assert_eq!(scan.consumer_count, 2);
        assert_eq!(scan.storage, StageStorage::Unlogged);
        assert_eq!(
            scan.materialization_reasons,
            [MaterializationReason::MultiplePhysicalConsumers]
        );
    }

    #[test]
    fn annotates_join_input_and_output_delta_stages() {
        let logical = plan(
            vec![
                node("left_scan", OperatorKind::Scan),
                node("left_filter", OperatorKind::Filter),
                node("right_scan", OperatorKind::Scan),
                node("join", OperatorKind::InnerJoin),
                node("aggregate", OperatorKind::Aggregate),
                node("sink", OperatorKind::Sink),
            ],
            vec![
                edge("left_scan", "left_filter", 0),
                edge("left_filter", "join", 0),
                edge("right_scan", "join", 1),
                edge("join", "aggregate", 0),
                edge("aggregate", "sink", 0),
            ],
        );
        let physical = PhysicalDagPlan::compile(
            &logical,
            ExecutionDescriptor {
                pipeline: ExecutionPipeline::Join,
                left_source_oid: 41,
                right_source_oid: Some(42),
                join_type: Some(ExecutionJoinType::Inner),
            },
            HashSet::from([42, 41]),
        )
        .unwrap();
        let join = physical
            .stages
            .iter()
            .find(|stage| stage.kernel == PhysicalKernel::Join)
            .unwrap();

        assert_eq!(join.storage, StageStorage::Unlogged);
        assert!(join
            .materialization_reasons
            .contains(&MaterializationReason::JoinDelta));
        assert_eq!(join.inputs.len(), 2);
        for input in &join.inputs {
            let input_stage = &physical.stages[input.stage_id as usize];
            assert_eq!(input_stage.storage, StageStorage::StatementMaterialized);
            assert!(input_stage
                .materialization_reasons
                .contains(&MaterializationReason::JoinInputDelta));
        }
    }

    #[test]
    fn compilation_and_json_are_deterministic_and_round_trip() {
        let nodes = vec![
            node("right_scan", OperatorKind::Scan),
            node("sink", OperatorKind::Sink),
            node("join", OperatorKind::InnerJoin),
            node("left_scan", OperatorKind::Scan),
        ];
        let edges = vec![
            edge("join", "sink", 0),
            edge("right_scan", "join", 1),
            edge("left_scan", "join", 0),
        ];
        let first = PhysicalDagPlan::compile(
            &plan(nodes.clone(), edges.clone()),
            ExecutionDescriptor {
                pipeline: ExecutionPipeline::Join,
                left_source_oid: 41,
                right_source_oid: Some(42),
                join_type: Some(ExecutionJoinType::Inner),
            },
            HashSet::from([42, 41]),
        )
        .unwrap();
        let second = PhysicalDagPlan::compile(
            &plan(
                nodes.into_iter().rev().collect(),
                edges.into_iter().rev().collect(),
            ),
            ExecutionDescriptor {
                pipeline: ExecutionPipeline::Join,
                left_source_oid: 41,
                right_source_oid: Some(42),
                join_type: Some(ExecutionJoinType::Inner),
            },
            HashSet::from([41, 42]),
        )
        .unwrap();

        assert_eq!(first, second);
        let first_json = serde_json::to_string(&first).unwrap();
        assert_eq!(first_json, serde_json::to_string(&second).unwrap());
        let decoded: PhysicalDagPlan = serde_json::from_str(&first_json).unwrap();
        assert_eq!(decoded, first);
        assert_eq!(decoded.encoded_descriptor(), first.encoded_descriptor());
    }

    #[test]
    fn persisted_plan_validation_cross_checks_result_sources_and_consumers() {
        let logical = plan(
            vec![
                node("scan", OperatorKind::Scan),
                node("distinct", OperatorKind::Distinct),
                node("sink", OperatorKind::Sink),
            ],
            vec![edge("scan", "distinct", 0), edge("distinct", "sink", 0)],
        );
        let physical = PhysicalDagPlan::compile(
            &logical,
            descriptor(ExecutionPipeline::Distinct),
            HashSet::from([41]),
        )
        .unwrap();

        assert_eq!(physical.validate_for_result(99), Ok(()));
        assert!(physical.validate_for_result(100).is_err());

        let mut wrong_source = physical.clone();
        wrong_source.source_oids = vec![42];
        assert!(wrong_source.validate_for_result(99).is_err());

        let mut wrong_consumers = physical;
        wrong_consumers.stages[0].consumer_ids.clear();
        wrong_consumers.stages[0].consumer_count = 0;
        assert!(wrong_consumers.validate_for_result(99).is_err());
    }
}
