//! Stable Shiba logical plan and common delta/operator contracts.

use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeltaRow {
    pub input: String,
    pub row: Value,
    pub diff: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeltaBatch {
    pub epoch: String,
    pub rows: Vec<DeltaRow>,
}

/// Every physical operator consumes and emits differential batches. Stateful
/// implementations receive a transaction-scoped state handle when execution
/// migrates from the current SQL dispatcher.
#[allow(dead_code)]
pub trait Operator {
    fn apply(&mut self, input: DeltaBatch) -> Result<Vec<DeltaBatch>, String>;
}

/// Transactional bridge used while physical operators are moved out of the
/// SQL dispatcher one by one. It already makes the persisted logical plan the
/// authority for accepted source inputs.
pub struct DagRuntime {
    result_oid: pg_sys::Oid,
    plan: LogicalPlan,
}

impl DagRuntime {
    pub fn load(result_oid: pg_sys::Oid) -> Result<Self, String> {
        let argument = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
        let serialized = Spi::get_one_with_args::<String>(
            "SELECT logical_plan::text FROM shiba_internal.stream_graphs WHERE result_oid = $1",
            &argument,
        )
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("result OID {result_oid} has no logical plan"))?;
        let plan = serde_json::from_str(&serialized)
            .map_err(|error| format!("invalid logical plan: {error}"))?;
        Ok(Self { result_oid, plan })
    }

    pub fn apply_batch(&self, batch: DeltaBatch) -> Result<(), String> {
        let encoded = encode_batch_events(&self.plan, self.result_oid, batch.rows)?.to_string();
        let arguments = unsafe {
            [
                DatumWithOid::new(self.result_oid, pg_sys::OIDOID),
                DatumWithOid::new(encoded.as_str(), pg_sys::TEXTOID),
                DatumWithOid::new(batch.epoch.as_str(), pg_sys::TEXTOID),
            ]
        };
        Spi::run_with_args(
            "SELECT shiba._apply_dag_delta_batch($1, $2::jsonb, $3)",
            &arguments,
        )
        .map_err(|error| error.to_string())
    }
}

fn encode_batch_events(
    plan: &LogicalPlan,
    result_oid: pg_sys::Oid,
    rows: Vec<DeltaRow>,
) -> Result<Value, String> {
    if rows.is_empty() {
        return Err("DAG delta batch must not be empty".into());
    }
    let mut encoded = Vec::with_capacity(rows.len());
    for delta in rows {
        let source_oid_u32 = delta
            .input
            .parse::<u32>()
            .map_err(|_| format!("invalid DAG input OID {}", delta.input))?;
        let source_is_planned = plan.nodes.iter().any(|node| {
            node.operator == OperatorKind::Scan
                && node.config["source_oid"].as_u64() == Some(u64::from(source_oid_u32))
        });
        if !source_is_planned {
            return Err(format!(
                "source OID {source_oid_u32} is not an input of result {result_oid}"
            ));
        }
        if !delta.row.is_object() {
            return Err("source delta row must be a JSON object".into());
        }
        let diff = i32::try_from(delta.diff)
            .map_err(|_| format!("differential weight {} exceeds int32", delta.diff))?;
        if !matches!(diff, -1 | 1) {
            return Err(format!("invalid source differential weight {diff}"));
        }
        encoded.push(json!({
            "source_oid": source_oid_u32,
            "row_data": delta.row,
            "delta": diff,
        }));
    }
    Ok(Value::Array(encoded))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogicalPlan {
    pub version: u32,
    pub nodes: Vec<LogicalNode>,
    pub edges: Vec<LogicalEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogicalNode {
    pub id: String,
    pub operator: OperatorKind,
    pub config: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OperatorKind {
    Scan,
    Filter,
    Project,
    InnerJoin,
    LeftJoin,
    RightJoin,
    FullJoin,
    SemiJoin,
    AntiJoin,
    NullAwareAntiJoin,
    Distinct,
    Aggregate,
    Having,
    Window,
    TopN,
    Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogicalEdge {
    pub from: String,
    pub to: String,
    pub input: u16,
}

fn push_node(
    nodes: &mut Vec<LogicalNode>,
    edges: &mut Vec<LogicalEdge>,
    upstream: Option<&str>,
    id: &str,
    operator: OperatorKind,
    config: Value,
    input: u16,
) {
    nodes.push(LogicalNode {
        id: id.into(),
        operator,
        config,
    });
    if let Some(from) = upstream {
        edges.push(LogicalEdge {
            from: from.into(),
            to: id.into(),
            input,
        });
    }
}

#[pg_extern]
pub fn compile_logical_plan(result_oid: pg_sys::Oid) -> JsonB {
    let argument = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    let metadata = Spi::get_one_with_args::<String>(
        "SELECT jsonb_build_object(
             'left_source', v.source_oid::integer,
             'view_kind', v.view_kind,
             'source_group', v.group_column,
             'result_group', v.result_group_column,
             'count_column', v.count_column,
             'count_distinct', v.count_distinct,
             'count_input_source', v.count_input_source,
             'count_input_column', v.count_input_column,
             'sum_input', v.sum_input_column,
             'sum_column', v.sum_column,
             'having', (
                 SELECT predicate_sql FROM shiba_internal.stream_having
                 WHERE result_oid = v.result_oid
             ),
             'left_filter', (
                 SELECT predicate_sql FROM shiba_internal.stream_filters
                 WHERE result_oid = v.result_oid AND input_side = 'left'
             ),
             'left_filter_phase', (
                 SELECT phase FROM shiba_internal.stream_filters
                 WHERE result_oid = v.result_oid AND input_side = 'left'
             ),
             'right_source', j.right_source_oid::integer,
             'join_type', j.join_type,
             'left_join_column', j.left_join_column,
             'right_join_column', j.right_join_column,
             'group_source', j.group_source,
             'right_filter', (
                 SELECT predicate_sql FROM shiba_internal.stream_filters
                 WHERE result_oid = v.result_oid AND input_side = 'right'
             )
             ,
             'right_filter_phase', (
                 SELECT phase FROM shiba_internal.stream_filters
                 WHERE result_oid = v.result_oid AND input_side = 'right'
             ),
             'join_filter', (
               SELECT predicate_sql
               FROM shiba_internal.stream_join_filters
               WHERE result_oid=v.result_oid
             ),
             'window', (
               SELECT jsonb_build_object(
                 'partition_column',w.partition_column,
                 'result_partition_column',w.result_partition_column,
                 'order_column',w.order_column,
                 'order_direction',w.order_direction,
                 'nulls_first',w.nulls_first,
                 'output_columns',w.output_columns,
                 'target_expressions',w.target_expressions
               )
               FROM shiba_internal.window_views w
               WHERE w.result_oid=v.result_oid
             ),
             'distinct_projection', (
               SELECT jsonb_build_object(
                 'source_columns',d.source_columns,
                 'output_columns',d.output_columns
               )
               FROM shiba_internal.distinct_views d
               WHERE d.result_oid=v.result_oid
             ),
             'topn', (
               SELECT jsonb_build_object(
                 'order_column',t.order_column,
                 'order_direction',t.order_direction,
                 'nulls_first',t.nulls_first,
                 'limit_count',t.limit_count,
                 'limit_offset',t.limit_offset,
                 'source_columns',t.source_columns,
                 'output_columns',t.output_columns
               )
               FROM shiba_internal.topn_views t
               WHERE t.result_oid=v.result_oid
             )
         )::text
         FROM shiba_internal.stream_views v
         LEFT JOIN shiba_internal.inner_join_views j USING (result_oid)
         WHERE v.result_oid = $1::oid",
        &argument,
    )
    .expect("Shiba could not read logical-plan metadata")
    .unwrap_or_else(|| error!("Shiba result OID {result_oid} is not registered"));
    let metadata: Value =
        serde_json::from_str(&metadata).expect("Shiba logical-plan metadata is invalid JSON");

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    if metadata["view_kind"] == "topn" {
        push_node(
            &mut nodes,
            &mut edges,
            None,
            "scan_left",
            OperatorKind::Scan,
            json!({ "source_oid": metadata["left_source"] }),
            0,
        );
        let mut tail = "scan_left";
        if !metadata["left_filter"].is_null() {
            push_node(
                &mut nodes,
                &mut edges,
                Some(tail),
                "filter_left",
                OperatorKind::Filter,
                json!({ "predicate_sql": metadata["left_filter"] }),
                0,
            );
            tail = "filter_left";
        }
        push_node(
            &mut nodes,
            &mut edges,
            Some(tail),
            "topn",
            OperatorKind::TopN,
            metadata["topn"].clone(),
            0,
        );
        push_node(
            &mut nodes,
            &mut edges,
            Some("topn"),
            "project",
            OperatorKind::Project,
            metadata["topn"].clone(),
            0,
        );
        push_node(
            &mut nodes,
            &mut edges,
            Some("project"),
            "sink",
            OperatorKind::Sink,
            json!({ "result_oid": result_oid.to_u32() }),
            0,
        );
        return JsonB(
            serde_json::to_value(LogicalPlan {
                version: 1,
                nodes,
                edges,
            })
            .expect("Shiba logical plan is not serializable"),
        );
    }
    if metadata["view_kind"] == "distinct" {
        push_node(
            &mut nodes,
            &mut edges,
            None,
            "scan_left",
            OperatorKind::Scan,
            json!({ "source_oid": metadata["left_source"] }),
            0,
        );
        let mut tail = "scan_left";
        if !metadata["left_filter"].is_null() {
            push_node(
                &mut nodes,
                &mut edges,
                Some(tail),
                "filter_left",
                OperatorKind::Filter,
                json!({ "predicate_sql": metadata["left_filter"] }),
                0,
            );
            tail = "filter_left";
        }
        push_node(
            &mut nodes,
            &mut edges,
            Some(tail),
            "distinct",
            OperatorKind::Distinct,
            metadata["distinct_projection"].clone(),
            0,
        );
        push_node(
            &mut nodes,
            &mut edges,
            Some("distinct"),
            "project",
            OperatorKind::Project,
            metadata["distinct_projection"].clone(),
            0,
        );
        push_node(
            &mut nodes,
            &mut edges,
            Some("project"),
            "sink",
            OperatorKind::Sink,
            json!({ "result_oid": result_oid.to_u32() }),
            0,
        );
        return JsonB(
            serde_json::to_value(LogicalPlan {
                version: 1,
                nodes,
                edges,
            })
            .expect("Shiba logical plan is not serializable"),
        );
    }
    if metadata["view_kind"] == "window" {
        push_node(
            &mut nodes,
            &mut edges,
            None,
            "scan_left",
            OperatorKind::Scan,
            json!({ "source_oid": metadata["left_source"] }),
            0,
        );
        let mut tail = "scan_left";
        if !metadata["left_filter"].is_null() {
            push_node(
                &mut nodes,
                &mut edges,
                Some(tail),
                "filter_left",
                OperatorKind::Filter,
                json!({ "predicate_sql": metadata["left_filter"] }),
                0,
            );
            tail = "filter_left";
        }
        push_node(
            &mut nodes,
            &mut edges,
            Some(tail),
            "window",
            OperatorKind::Window,
            metadata["window"].clone(),
            0,
        );
        push_node(
            &mut nodes,
            &mut edges,
            Some("window"),
            "project",
            OperatorKind::Project,
            json!({ "output_columns": metadata["window"]["output_columns"] }),
            0,
        );
        push_node(
            &mut nodes,
            &mut edges,
            Some("project"),
            "sink",
            OperatorKind::Sink,
            json!({ "result_oid": result_oid.to_u32() }),
            0,
        );
        return JsonB(
            serde_json::to_value(LogicalPlan {
                version: 1,
                nodes,
                edges,
            })
            .expect("Shiba logical plan is not serializable"),
        );
    }
    push_node(
        &mut nodes,
        &mut edges,
        None,
        "scan_left",
        OperatorKind::Scan,
        json!({ "source_oid": metadata["left_source"] }),
        0,
    );
    let mut left_tail = "scan_left";
    let join_operator = match metadata["join_type"].as_str() {
        Some("left") => OperatorKind::LeftJoin,
        Some("right") => OperatorKind::RightJoin,
        Some("full") => OperatorKind::FullJoin,
        Some("semi") => OperatorKind::SemiJoin,
        Some("anti") => OperatorKind::AntiJoin,
        Some("null_anti") => OperatorKind::NullAwareAntiJoin,
        _ => OperatorKind::InnerJoin,
    };
    if !metadata["left_filter"].is_null() && metadata["left_filter_phase"] != "post" {
        push_node(
            &mut nodes,
            &mut edges,
            Some(left_tail),
            "filter_left",
            OperatorKind::Filter,
            json!({ "predicate_sql": metadata["left_filter"] }),
            0,
        );
        left_tail = "filter_left";
    }

    let aggregate_input;
    if !metadata["right_source"].is_null() {
        push_node(
            &mut nodes,
            &mut edges,
            None,
            "scan_right",
            OperatorKind::Scan,
            json!({ "source_oid": metadata["right_source"] }),
            0,
        );
        let mut right_tail = "scan_right";
        if !metadata["right_filter"].is_null() && metadata["right_filter_phase"] != "post" {
            push_node(
                &mut nodes,
                &mut edges,
                Some(right_tail),
                "filter_right",
                OperatorKind::Filter,
                json!({ "predicate_sql": metadata["right_filter"] }),
                0,
            );
            right_tail = "filter_right";
        }
        push_node(
            &mut nodes,
            &mut edges,
            Some(left_tail),
            "join",
            join_operator,
            json!({
                "left_key": metadata["left_join_column"],
                "right_key": metadata["right_join_column"]
            }),
            0,
        );
        edges.push(LogicalEdge {
            from: right_tail.into(),
            to: "join".into(),
            input: 1,
        });
        let mut join_tail = "join";
        if metadata["left_filter_phase"] == "post"
            || metadata["right_filter_phase"] == "post"
            || !metadata["join_filter"].is_null()
        {
            push_node(
                &mut nodes,
                &mut edges,
                Some(join_tail),
                "filter_join",
                OperatorKind::Filter,
                json!({
                    "left_predicate_sql": metadata["left_filter"],
                    "right_predicate_sql": metadata["right_filter"],
                    "join_predicate_sql": metadata["join_filter"]
                }),
                0,
            );
            join_tail = "filter_join";
        }
        aggregate_input = join_tail;
    } else {
        aggregate_input = left_tail;
    }
    let mut aggregate_input = aggregate_input;
    if metadata["count_distinct"] == true {
        push_node(
            &mut nodes,
            &mut edges,
            Some(aggregate_input),
            "distinct",
            OperatorKind::Distinct,
            json!({
                "group_source": metadata["group_source"],
                "group_column": metadata["source_group"],
                "value_source": metadata["count_input_source"],
                "value_column": metadata["count_input_column"]
            }),
            0,
        );
        aggregate_input = "distinct";
    }
    push_node(
        &mut nodes,
        &mut edges,
        Some(aggregate_input),
        "aggregate",
        OperatorKind::Aggregate,
        json!({
            "group_source": metadata["group_source"],
            "group_column": metadata["source_group"],
            "count_column": metadata["count_column"],
            "count_distinct": metadata["count_distinct"],
            "count_input_source": metadata["count_input_source"],
            "count_input_column": metadata["count_input_column"],
            "sum_input": metadata["sum_input"],
            "sum_column": metadata["sum_column"]
        }),
        0,
    );
    let mut aggregate_tail = "aggregate";
    if !metadata["having"].is_null() {
        push_node(
            &mut nodes,
            &mut edges,
            Some(aggregate_tail),
            "having",
            OperatorKind::Having,
            json!({ "predicate_sql": metadata["having"] }),
            0,
        );
        aggregate_tail = "having";
    }
    push_node(
        &mut nodes,
        &mut edges,
        Some(aggregate_tail),
        "project",
        OperatorKind::Project,
        json!({
            "source_group": metadata["source_group"],
            "result_group": metadata["result_group"]
        }),
        0,
    );
    push_node(
        &mut nodes,
        &mut edges,
        Some("project"),
        "sink",
        OperatorKind::Sink,
        json!({ "result_oid": result_oid.to_u32() }),
        0,
    );

    JsonB(
        serde_json::to_value(LogicalPlan {
            version: 1,
            nodes,
            edges,
        })
        .expect("Shiba logical plan is not serializable"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Identity;

    impl Operator for Identity {
        fn apply(&mut self, input: DeltaBatch) -> Result<Vec<DeltaBatch>, String> {
            Ok(vec![input])
        }
    }

    #[test]
    fn operator_preserves_differential_batch() {
        let batch = DeltaBatch {
            epoch: "0/10".into(),
            rows: vec![DeltaRow {
                input: "left".into(),
                row: json!({"id": 1}),
                diff: -1,
            }],
        };
        assert_eq!(Identity.apply(batch.clone()).unwrap(), vec![batch]);
    }

    #[test]
    fn batch_encoding_preserves_cross_source_wal_order() {
        let plan = LogicalPlan {
            version: 1,
            nodes: vec![
                LogicalNode {
                    id: "left".into(),
                    operator: OperatorKind::Scan,
                    config: json!({"source_oid": 41}),
                },
                LogicalNode {
                    id: "right".into(),
                    operator: OperatorKind::Scan,
                    config: json!({"source_oid": 42}),
                },
            ],
            edges: vec![],
        };
        let events = encode_batch_events(
            &plan,
            pg_sys::Oid::from(99),
            vec![
                DeltaRow {
                    input: "41".into(),
                    row: json!({"id": 7, "value": "old"}),
                    diff: -1,
                },
                DeltaRow {
                    input: "42".into(),
                    row: json!({"id": 7, "value": "dimension"}),
                    diff: 1,
                },
                DeltaRow {
                    input: "41".into(),
                    row: json!({"id": 7, "value": "new"}),
                    diff: 1,
                },
            ],
        )
        .unwrap();

        assert_eq!(events[0]["source_oid"], 41);
        assert_eq!(events[0]["delta"], -1);
        assert_eq!(events[1]["source_oid"], 42);
        assert_eq!(events[2]["row_data"]["value"], "new");
    }

    #[test]
    fn batch_encoding_rejects_empty_unplanned_and_non_object_inputs() {
        let plan = LogicalPlan {
            version: 1,
            nodes: vec![LogicalNode {
                id: "scan".into(),
                operator: OperatorKind::Scan,
                config: json!({"source_oid": 41}),
            }],
            edges: vec![],
        };
        let result_oid = pg_sys::Oid::from(99);

        assert!(encode_batch_events(&plan, result_oid, vec![])
            .unwrap_err()
            .contains("must not be empty"));
        assert!(encode_batch_events(
            &plan,
            result_oid,
            vec![DeltaRow {
                input: "42".into(),
                row: json!({"id": 1}),
                diff: 1,
            }]
        )
        .unwrap_err()
        .contains("not an input"));
        assert!(encode_batch_events(
            &plan,
            result_oid,
            vec![DeltaRow {
                input: "41".into(),
                row: json!([1, 2]),
                diff: 1,
            }]
        )
        .unwrap_err()
        .contains("JSON object"));
    }

    #[test]
    fn logical_plan_round_trips_every_operator_kind() {
        let operators = vec![
            OperatorKind::Scan,
            OperatorKind::Filter,
            OperatorKind::Project,
            OperatorKind::InnerJoin,
            OperatorKind::LeftJoin,
            OperatorKind::RightJoin,
            OperatorKind::FullJoin,
            OperatorKind::SemiJoin,
            OperatorKind::AntiJoin,
            OperatorKind::NullAwareAntiJoin,
            OperatorKind::Distinct,
            OperatorKind::Aggregate,
            OperatorKind::Having,
            OperatorKind::Window,
            OperatorKind::TopN,
            OperatorKind::Sink,
        ];
        let plan = LogicalPlan {
            version: 1,
            nodes: operators
                .into_iter()
                .enumerate()
                .map(|(index, operator)| LogicalNode {
                    id: format!("node_{index}"),
                    operator,
                    config: json!({"index": index}),
                })
                .collect(),
            edges: vec![LogicalEdge {
                from: "node_0".into(),
                to: "node_1".into(),
                input: u16::MAX,
            }],
        };

        let serialized = serde_json::to_string(&plan).unwrap();
        assert!(serialized.contains("\"null_aware_anti_join\""));
        assert_eq!(
            serde_json::from_str::<LogicalPlan>(&serialized).unwrap(),
            plan
        );
    }

    #[test]
    fn push_node_only_adds_an_edge_when_upstream_exists() {
        let mut nodes = vec![];
        let mut edges = vec![];
        push_node(
            &mut nodes,
            &mut edges,
            None,
            "scan",
            OperatorKind::Scan,
            json!({}),
            0,
        );
        push_node(
            &mut nodes,
            &mut edges,
            Some("scan"),
            "sink",
            OperatorKind::Sink,
            json!({}),
            1,
        );

        assert_eq!(nodes.len(), 2);
        assert_eq!(
            edges,
            vec![LogicalEdge {
                from: "scan".into(),
                to: "sink".into(),
                input: 1,
            }]
        );
    }
}
