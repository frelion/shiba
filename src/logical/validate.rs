//! Validation and runtime execution descriptors.
//!
//! Persisted nodes intentionally keep `serde_json::Value` for wire
//! compatibility. At this boundary every config is decoded into an
//! operator-specific struct before it can select a physical pipeline.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

use super::model::{LogicalNode, LogicalPlan, OperatorKind, LOGICAL_PLAN_VERSION};

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExecutionPipeline {
    Aggregate,
    Join,
    Window,
    Distinct,
    #[serde(rename = "topn")]
    TopN,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExecutionJoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    NullAnti,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(super) struct ExecutionDescriptor {
    pub(super) pipeline: ExecutionPipeline,
    pub(super) left_source_oid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) right_source_oid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) join_type: Option<ExecutionJoinType>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ExecutionPlan {
    pub(super) descriptor: ExecutionDescriptor,
    pub(super) source_oids: HashSet<u32>,
}

impl LogicalPlan {
    pub(super) fn validate_for(&self, result_oid: u32) -> Result<ExecutionPlan, String> {
        if self.version != LOGICAL_PLAN_VERSION {
            return Err(format!(
                "unsupported logical plan version {} (expected {LOGICAL_PLAN_VERSION})",
                self.version
            ));
        }
        if self.nodes.is_empty() {
            return Err("logical plan has no nodes".into());
        }

        let mut node_indexes = HashMap::with_capacity(self.nodes.len());
        for (index, node) in self.nodes.iter().enumerate() {
            if node.id.is_empty() {
                return Err("logical plan contains an empty node ID".into());
            }
            if node_indexes.insert(node.id.as_str(), index).is_some() {
                return Err(format!("duplicate logical plan node ID {}", node.id));
            }
        }

        let mut incoming = vec![0_usize; self.nodes.len()];
        let mut outgoing = vec![Vec::new(); self.nodes.len()];
        let mut destination_inputs = HashSet::with_capacity(self.edges.len());
        for edge in &self.edges {
            let from = *node_indexes
                .get(edge.from.as_str())
                .ok_or_else(|| format!("edge references missing upstream node {}", edge.from))?;
            let to = *node_indexes
                .get(edge.to.as_str())
                .ok_or_else(|| format!("edge references missing downstream node {}", edge.to))?;
            if !destination_inputs.insert((to, edge.input)) {
                return Err(format!(
                    "node {} has duplicate input {}",
                    edge.to, edge.input
                ));
            }
            if !self.nodes[to].operator.is_join() && edge.input != 0 {
                return Err(format!(
                    "non-join node {} uses input {}",
                    edge.to, edge.input
                ));
            }
            incoming[to] += 1;
            outgoing[from].push(to);
        }

        let mut source_oids = HashSet::new();
        let mut sinks = Vec::new();
        for (index, node) in self.nodes.iter().enumerate() {
            match node.operator {
                OperatorKind::Scan => {
                    if incoming[index] != 0 {
                        return Err(format!("scan node {} has an upstream input", node.id));
                    }
                    let source_oid = source_oid(node)?;
                    if !source_oids.insert(source_oid) {
                        return Err(format!("duplicate scan source OID {source_oid}"));
                    }
                }
                OperatorKind::Sink => {
                    sinks.push(index);
                    if node.config["result_oid"].as_u64() != Some(u64::from(result_oid)) {
                        return Err(format!(
                            "sink node {} targets the wrong result OID",
                            node.id
                        ));
                    }
                }
                _ if incoming[index] == 0 => {
                    return Err(format!("non-scan node {} is unreachable", node.id));
                }
                _ => {}
            }
            if node.operator.is_join()
                && (incoming[index] != 2
                    || !destination_inputs.contains(&(index, 0))
                    || !destination_inputs.contains(&(index, 1)))
            {
                return Err(format!(
                    "join node {} must have exactly inputs 0 and 1",
                    node.id
                ));
            }
            if node.operator != OperatorKind::Scan
                && !node.operator.is_join()
                && incoming[index] != 1
            {
                return Err(format!(
                    "unary node {} must have exactly one input",
                    node.id
                ));
            }
            if node.operator == OperatorKind::Sink {
                if !outgoing[index].is_empty() {
                    return Err(format!("sink node {} has downstream nodes", node.id));
                }
            } else if outgoing[index].is_empty() {
                return Err(format!("node {} does not lead to the sink", node.id));
            }
        }
        if source_oids.is_empty() {
            return Err("logical plan has no scan nodes".into());
        }
        if sinks.len() != 1 {
            return Err(format!(
                "logical plan must contain exactly one sink, found {}",
                sinks.len()
            ));
        }

        reject_cycles(&incoming, &outgoing)?;
        self.validate_execution_grammar(&node_indexes, source_oids)
    }

    fn validate_execution_grammar(
        &self,
        node_indexes: &HashMap<&str, usize>,
        source_oids: HashSet<u32>,
    ) -> Result<ExecutionPlan, String> {
        let join_nodes: Vec<_> = self
            .nodes
            .iter()
            .filter(|node| node.operator.is_join())
            .collect();
        let count = |kind| {
            self.nodes
                .iter()
                .filter(|node| node.operator == kind)
                .count()
        };
        let aggregate_count = count(OperatorKind::Aggregate);
        let window_count = count(OperatorKind::Window);
        let topn_count = count(OperatorKind::TopN);
        let distinct_count = count(OperatorKind::Distinct);
        let core_count = aggregate_count
            + window_count
            + topn_count
            + usize::from(aggregate_count == 0) * distinct_count;
        if join_nodes.len() > 1
            || aggregate_count > 1
            || window_count > 1
            || topn_count > 1
            || distinct_count > 1
            || core_count != 1
            || (!join_nodes.is_empty() && aggregate_count != 1)
        {
            return Err("logical plan must contain exactly one supported operator core".into());
        }

        let pipeline = if !join_nodes.is_empty() {
            ExecutionPipeline::Join
        } else if aggregate_count == 1 {
            ExecutionPipeline::Aggregate
        } else if window_count == 1 {
            ExecutionPipeline::Window
        } else if topn_count == 1 {
            ExecutionPipeline::TopN
        } else {
            ExecutionPipeline::Distinct
        };
        let join_type = join_nodes
            .first()
            .map(|node| execution_join_type(node.operator))
            .transpose()?;

        let node = |id: &str| -> Result<&LogicalNode, String> {
            node_indexes
                .get(id)
                .map(|index| &self.nodes[*index])
                .ok_or_else(|| format!("logical plan is missing required node {id}"))
        };
        let optional = |id: &str| node_indexes.contains_key(id);
        let mut expected_nodes = vec![
            ("scan_left", OperatorKind::Scan),
            ("project", OperatorKind::Project),
            ("sink", OperatorKind::Sink),
        ];
        if optional("filter_left") {
            expected_nodes.push(("filter_left", OperatorKind::Filter));
        }
        let left_tail = if optional("filter_left") {
            "filter_left"
        } else {
            "scan_left"
        };
        let mut expected_edges = Vec::new();
        if optional("filter_left") {
            expected_edges.push(("scan_left", "filter_left", 0));
        }

        match pipeline {
            ExecutionPipeline::Join => {
                expected_nodes.push(("scan_right", OperatorKind::Scan));
                expected_nodes.push(("join", join_nodes[0].operator));
                if optional("filter_right") {
                    expected_nodes.push(("filter_right", OperatorKind::Filter));
                    expected_edges.push(("scan_right", "filter_right", 0));
                }
                let right_tail = if optional("filter_right") {
                    "filter_right"
                } else {
                    "scan_right"
                };
                expected_edges.push((left_tail, "join", 0));
                expected_edges.push((right_tail, "join", 1));
                let mut tail = "join";
                if optional("filter_join") {
                    expected_nodes.push(("filter_join", OperatorKind::Filter));
                    expected_edges.push((tail, "filter_join", 0));
                    tail = "filter_join";
                }
                if optional("distinct") {
                    expected_nodes.push(("distinct", OperatorKind::Distinct));
                    expected_edges.push((tail, "distinct", 0));
                    tail = "distinct";
                }
                expected_nodes.push(("aggregate", OperatorKind::Aggregate));
                expected_edges.push((tail, "aggregate", 0));
                finish_expected_tail(
                    &mut expected_nodes,
                    &mut expected_edges,
                    "aggregate",
                    optional("having"),
                );
            }
            ExecutionPipeline::Aggregate => {
                let mut tail = left_tail;
                if optional("distinct") {
                    expected_nodes.push(("distinct", OperatorKind::Distinct));
                    expected_edges.push((tail, "distinct", 0));
                    tail = "distinct";
                }
                expected_nodes.push(("aggregate", OperatorKind::Aggregate));
                expected_edges.push((tail, "aggregate", 0));
                finish_expected_tail(
                    &mut expected_nodes,
                    &mut expected_edges,
                    "aggregate",
                    optional("having"),
                );
            }
            ExecutionPipeline::Window => {
                expected_nodes.push(("window", OperatorKind::Window));
                expected_edges.extend([
                    (left_tail, "window", 0),
                    ("window", "project", 0),
                    ("project", "sink", 0),
                ]);
            }
            ExecutionPipeline::Distinct => {
                expected_nodes.push(("distinct", OperatorKind::Distinct));
                expected_edges.extend([
                    (left_tail, "distinct", 0),
                    ("distinct", "project", 0),
                    ("project", "sink", 0),
                ]);
            }
            ExecutionPipeline::TopN => {
                expected_nodes.push(("topn", OperatorKind::TopN));
                expected_edges.extend([
                    (left_tail, "topn", 0),
                    ("topn", "project", 0),
                    ("project", "sink", 0),
                ]);
            }
        }
        if expected_nodes.len() != self.nodes.len() {
            return Err("logical plan contains an operator outside the supported grammar".into());
        }
        for (id, operator) in &expected_nodes {
            let actual = node(id)?;
            if actual.operator != *operator {
                return Err(format!("node {id} has an invalid operator or position"));
            }
            validate_operator_config(actual, pipeline)?;
        }

        if matches!(
            pipeline,
            ExecutionPipeline::Aggregate | ExecutionPipeline::Join
        ) {
            validate_aggregate_chain(&node, optional, pipeline)?;
        }
        let actual_edges: HashSet<_> = self
            .edges
            .iter()
            .map(|edge| (edge.from.as_str(), edge.to.as_str(), edge.input))
            .collect();
        let expected_edges: HashSet<_> = expected_edges.into_iter().collect();
        if actual_edges != expected_edges {
            return Err("logical plan edges do not match the supported operator order".into());
        }

        let left_source_oid = source_oid(node("scan_left")?)?;
        let right_source_oid = if pipeline == ExecutionPipeline::Join {
            Some(source_oid(node("scan_right")?)?)
        } else {
            None
        };
        let expected_sources: HashSet<_> = [Some(left_source_oid), right_source_oid]
            .into_iter()
            .flatten()
            .collect();
        if source_oids != expected_sources {
            return Err("logical plan scan sources do not match its execution inputs".into());
        }
        Ok(ExecutionPlan {
            descriptor: ExecutionDescriptor {
                pipeline,
                left_source_oid,
                right_source_oid,
                join_type,
            },
            source_oids,
        })
    }
}

fn reject_cycles(incoming: &[usize], outgoing: &[Vec<usize>]) -> Result<(), String> {
    let mut queue: VecDeque<_> = incoming
        .iter()
        .enumerate()
        .filter_map(|(index, degree)| (*degree == 0).then_some(index))
        .collect();
    let mut remaining = incoming.to_vec();
    let mut visited = 0;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        for &downstream in &outgoing[node] {
            remaining[downstream] -= 1;
            if remaining[downstream] == 0 {
                queue.push_back(downstream);
            }
        }
    }
    if visited == incoming.len() {
        Ok(())
    } else {
        Err("logical plan contains a cycle".into())
    }
}

fn finish_expected_tail<'a>(
    nodes: &mut Vec<(&'a str, OperatorKind)>,
    edges: &mut Vec<(&'a str, &'a str, u16)>,
    mut tail: &'a str,
    has_having: bool,
) {
    if has_having {
        nodes.push(("having", OperatorKind::Having));
        edges.push((tail, "having", 0));
        tail = "having";
    }
    edges.push((tail, "project", 0));
    edges.push(("project", "sink", 0));
}

fn execution_join_type(operator: OperatorKind) -> Result<ExecutionJoinType, String> {
    match operator {
        OperatorKind::InnerJoin => Ok(ExecutionJoinType::Inner),
        OperatorKind::LeftJoin => Ok(ExecutionJoinType::Left),
        OperatorKind::RightJoin => Ok(ExecutionJoinType::Right),
        OperatorKind::FullJoin => Ok(ExecutionJoinType::Full),
        OperatorKind::SemiJoin => Ok(ExecutionJoinType::Semi),
        OperatorKind::AntiJoin => Ok(ExecutionJoinType::Anti),
        OperatorKind::NullAwareAntiJoin => Ok(ExecutionJoinType::NullAnti),
        _ => Err("non-join operator cannot define a join execution type".into()),
    }
}

fn source_oid(node: &LogicalNode) -> Result<u32, String> {
    decode_config::<ScanConfig>(node, &["source_oid"])
        .and_then(|config| {
            (config.source_oid != 0)
                .then_some(config.source_oid)
                .ok_or_else(|| "source_oid must be nonzero".into())
        })
        .map_err(|_| format!("scan node {} has invalid source_oid", node.id))
}

fn validate_aggregate_chain<'a>(
    node: &impl Fn(&str) -> Result<&'a LogicalNode, String>,
    optional: impl Fn(&str) -> bool,
    pipeline: ExecutionPipeline,
) -> Result<(), String> {
    let aggregate = &node("aggregate")?.config;
    let has_distinct = optional("distinct");
    if aggregate["count_distinct"].as_bool() != Some(has_distinct) {
        return Err("aggregate count_distinct config disagrees with the operator pipeline".into());
    }
    if pipeline == ExecutionPipeline::Join && aggregate["group_source"].is_null() {
        return Err("join aggregate config must identify its group input side".into());
    }
    if has_distinct {
        let distinct = &node("distinct")?.config;
        if distinct["group_source"] != aggregate["group_source"]
            || distinct["group_column"] != aggregate["group_column"]
            || distinct["value_source"] != aggregate["count_input_source"]
            || distinct["value_column"] != aggregate["count_input_column"]
        {
            return Err("distinct config disagrees with its downstream aggregate config".into());
        }
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanConfig {
    source_oid: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PredicateConfig {
    predicate_sql: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinFilterConfig {
    left_predicate_sql: Option<String>,
    right_predicate_sql: Option<String>,
    join_predicate_sql: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinConfig {
    left_key: String,
    right_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionConfig {
    source_columns: Vec<String>,
    output_columns: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregateDistinctConfig {
    group_source: Option<InputSide>,
    group_column: Option<String>,
    value_source: InputSide,
    value_column: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum InputSide {
    Left,
    Right,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregateConfig {
    group_source: Option<InputSide>,
    group_column: Option<String>,
    count_column: Option<String>,
    count_distinct: bool,
    count_input_source: Option<InputSide>,
    count_input_column: Option<String>,
    sum_input: Option<String>,
    sum_column: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WindowConfig {
    partition_column: String,
    result_partition_column: String,
    order_column: String,
    order_direction: String,
    nulls_first: bool,
    output_columns: Vec<String>,
    target_expressions: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TopNConfig {
    order_column: String,
    order_direction: String,
    nulls_first: bool,
    limit_count: i64,
    limit_offset: i64,
    source_columns: Vec<String>,
    output_columns: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregateProjectConfig {
    source_group: Option<String>,
    result_group: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WindowProjectConfig {
    output_columns: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SinkConfig {
    result_oid: u64,
}

fn decode_config<T: for<'de> Deserialize<'de>>(
    node: &LogicalNode,
    keys: &[&str],
) -> Result<T, String> {
    let object = node
        .config
        .as_object()
        .ok_or_else(|| format!("node {} config must be an object", node.id))?;
    if object.len() != keys.len() || !keys.iter().all(|key| object.contains_key(*key)) {
        return Err(format!(
            "node {} has an invalid or incomplete operator config",
            node.id
        ));
    }
    serde_json::from_value(node.config.clone()).map_err(|_| {
        format!(
            "node {} has an invalid or incomplete operator config",
            node.id
        )
    })
}

fn order_is_valid(direction: &str) -> bool {
    direction.eq_ignore_ascii_case("asc") || direction.eq_ignore_ascii_case("desc")
}

fn validate_operator_config(node: &LogicalNode, pipeline: ExecutionPipeline) -> Result<(), String> {
    let invalid = || {
        format!(
            "node {} has an invalid or incomplete operator config",
            node.id
        )
    };
    let valid = match node.operator {
        OperatorKind::Scan => source_oid(node).is_ok(),
        OperatorKind::Filter if node.id == "filter_join" => {
            let config = decode_config::<JoinFilterConfig>(
                node,
                &[
                    "left_predicate_sql",
                    "right_predicate_sql",
                    "join_predicate_sql",
                ],
            )?;
            config.left_predicate_sql.is_some()
                || config.right_predicate_sql.is_some()
                || config.join_predicate_sql.is_some()
        }
        OperatorKind::Filter | OperatorKind::Having => {
            let config = decode_config::<PredicateConfig>(node, &["predicate_sql"])?;
            let _ = config.predicate_sql;
            true
        }
        operator if operator.is_join() => {
            let config = decode_config::<JoinConfig>(node, &["left_key", "right_key"])?;
            let _ = (config.left_key, config.right_key);
            true
        }
        OperatorKind::Distinct if pipeline == ExecutionPipeline::Distinct => {
            let config =
                decode_config::<ProjectionConfig>(node, &["source_columns", "output_columns"])?;
            config.source_columns.len() == config.output_columns.len()
        }
        OperatorKind::Distinct => {
            let config = decode_config::<AggregateDistinctConfig>(
                node,
                &[
                    "group_source",
                    "group_column",
                    "value_source",
                    "value_column",
                ],
            )?;
            let group_is_valid = if pipeline == ExecutionPipeline::Aggregate {
                config.group_source.is_none()
            } else {
                config.group_source.is_some()
            };
            let _ = (
                config.group_column,
                config.value_source,
                config.value_column,
            );
            group_is_valid
        }
        OperatorKind::Aggregate => {
            let config = decode_config::<AggregateConfig>(
                node,
                &[
                    "group_source",
                    "group_column",
                    "count_column",
                    "count_distinct",
                    "count_input_source",
                    "count_input_column",
                    "sum_input",
                    "sum_column",
                ],
            )?;
            let count_input_consistent =
                config.count_input_source.is_none() == config.count_input_column.is_none();
            let sum_input_consistent = config.sum_input.is_none() == config.sum_column.is_none();
            let distinct_has_input = !config.count_distinct || config.count_input_source.is_some();
            let has_output = config.count_column.is_some() || config.sum_column.is_some();
            let _ = (config.group_source, config.group_column);
            count_input_consistent && sum_input_consistent && distinct_has_input && has_output
        }
        OperatorKind::Window => {
            let config = decode_config::<WindowConfig>(
                node,
                &[
                    "partition_column",
                    "result_partition_column",
                    "order_column",
                    "order_direction",
                    "nulls_first",
                    "output_columns",
                    "target_expressions",
                ],
            )?;
            let _ = (
                config.partition_column,
                config.result_partition_column,
                config.order_column,
                config.nulls_first,
            );
            order_is_valid(&config.order_direction)
                && config.output_columns.len() == config.target_expressions.len()
        }
        OperatorKind::TopN => validate_topn(node)?,
        OperatorKind::Project => match pipeline {
            ExecutionPipeline::Aggregate | ExecutionPipeline::Join => {
                let config = decode_config::<AggregateProjectConfig>(
                    node,
                    &["source_group", "result_group"],
                )?;
                let _ = (config.source_group, config.result_group);
                true
            }
            ExecutionPipeline::Window => {
                let config = decode_config::<WindowProjectConfig>(node, &["output_columns"])?;
                let _ = config.output_columns;
                true
            }
            ExecutionPipeline::Distinct => {
                let config =
                    decode_config::<ProjectionConfig>(node, &["source_columns", "output_columns"])?;
                config.source_columns.len() == config.output_columns.len()
            }
            ExecutionPipeline::TopN => validate_topn(node)?,
        },
        OperatorKind::Sink => {
            let config = decode_config::<SinkConfig>(node, &["result_oid"])?;
            let _ = config.result_oid;
            true
        }
        _ => false,
    };
    valid.then_some(()).ok_or_else(invalid)
}

fn validate_topn(node: &LogicalNode) -> Result<bool, String> {
    let config = decode_config::<TopNConfig>(
        node,
        &[
            "order_column",
            "order_direction",
            "nulls_first",
            "limit_count",
            "limit_offset",
            "source_columns",
            "output_columns",
        ],
    )?;
    let _ = (config.order_column, config.nulls_first);
    Ok(order_is_valid(&config.order_direction)
        && config.limit_count >= 0
        && config.limit_offset >= 0
        && config.source_columns.len() == config.output_columns.len())
}
