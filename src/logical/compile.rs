//! Logical-plan compiler.
//!
//! Registration metadata is read once and translated into the stable model in
//! `model.rs`. Keeping construction here makes the persisted node names, edge
//! order, and JSON configs easy to audit without runtime concerns mixed in.

use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::prelude::*;
use serde_json::{json, Value};

use super::model::{LogicalEdge, LogicalNode, LogicalPlan, OperatorKind, LOGICAL_PLAN_VERSION};

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
             ),
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

    JsonB(
        serde_json::to_value(build_logical_plan(&metadata, result_oid.to_u32()))
            .expect("Shiba logical plan is not serializable"),
    )
}

#[derive(Default)]
pub(super) struct LogicalPlanBuilder {
    nodes: Vec<LogicalNode>,
    edges: Vec<LogicalEdge>,
}

impl LogicalPlanBuilder {
    pub(super) fn root(&mut self, id: &str, operator: OperatorKind, config: Value) {
        self.node(id, operator, config, None);
    }

    fn unary(&mut self, upstream: &str, id: &str, operator: OperatorKind, config: Value) {
        self.node(id, operator, config, Some((upstream, 0)));
    }

    fn input(&mut self, upstream: &str, downstream: &str, input: u16) {
        self.edges.push(LogicalEdge {
            from: upstream.into(),
            to: downstream.into(),
            input,
        });
    }

    pub(super) fn node(
        &mut self,
        id: &str,
        operator: OperatorKind,
        config: Value,
        upstream: Option<(&str, u16)>,
    ) {
        self.nodes.push(LogicalNode {
            id: id.into(),
            operator,
            config,
        });
        if let Some((from, input)) = upstream {
            self.input(from, id, input);
        }
    }

    pub(super) fn finish(self) -> LogicalPlan {
        LogicalPlan {
            version: LOGICAL_PLAN_VERSION,
            nodes: self.nodes,
            edges: self.edges,
        }
    }
}

fn build_single_source_plan(
    metadata: &Value,
    result_oid: u32,
    id: &str,
    operator: OperatorKind,
    operator_config: Value,
    project_config: Value,
) -> LogicalPlan {
    let mut plan = LogicalPlanBuilder::default();
    plan.root(
        "scan_left",
        OperatorKind::Scan,
        json!({ "source_oid": metadata["left_source"] }),
    );
    let mut tail = "scan_left";
    if !metadata["left_filter"].is_null() {
        plan.unary(
            tail,
            "filter_left",
            OperatorKind::Filter,
            json!({ "predicate_sql": metadata["left_filter"] }),
        );
        tail = "filter_left";
    }
    plan.unary(tail, id, operator, operator_config);
    plan.unary(id, "project", OperatorKind::Project, project_config);
    plan.unary(
        "project",
        "sink",
        OperatorKind::Sink,
        json!({ "result_oid": result_oid }),
    );
    plan.finish()
}

fn join_operator(metadata: &Value) -> OperatorKind {
    match metadata["join_type"].as_str() {
        Some("left") => OperatorKind::LeftJoin,
        Some("right") => OperatorKind::RightJoin,
        Some("full") => OperatorKind::FullJoin,
        Some("semi") => OperatorKind::SemiJoin,
        Some("anti") => OperatorKind::AntiJoin,
        Some("null_anti") => OperatorKind::NullAwareAntiJoin,
        _ => OperatorKind::InnerJoin,
    }
}

fn build_aggregate_plan(metadata: &Value, result_oid: u32) -> LogicalPlan {
    let mut plan = LogicalPlanBuilder::default();
    plan.root(
        "scan_left",
        OperatorKind::Scan,
        json!({ "source_oid": metadata["left_source"] }),
    );
    let mut left_tail = "scan_left";
    if !metadata["left_filter"].is_null() && metadata["left_filter_phase"] != "post" {
        plan.unary(
            left_tail,
            "filter_left",
            OperatorKind::Filter,
            json!({ "predicate_sql": metadata["left_filter"] }),
        );
        left_tail = "filter_left";
    }

    let mut aggregate_input = left_tail;
    if !metadata["right_source"].is_null() {
        plan.root(
            "scan_right",
            OperatorKind::Scan,
            json!({ "source_oid": metadata["right_source"] }),
        );
        let mut right_tail = "scan_right";
        if !metadata["right_filter"].is_null() && metadata["right_filter_phase"] != "post" {
            plan.unary(
                right_tail,
                "filter_right",
                OperatorKind::Filter,
                json!({ "predicate_sql": metadata["right_filter"] }),
            );
            right_tail = "filter_right";
        }
        plan.unary(
            left_tail,
            "join",
            join_operator(metadata),
            json!({
                "left_key": metadata["left_join_column"],
                "right_key": metadata["right_join_column"]
            }),
        );
        plan.input(right_tail, "join", 1);
        aggregate_input = "join";
        if metadata["left_filter_phase"] == "post"
            || metadata["right_filter_phase"] == "post"
            || !metadata["join_filter"].is_null()
        {
            plan.unary(
                aggregate_input,
                "filter_join",
                OperatorKind::Filter,
                json!({
                    "left_predicate_sql": metadata["left_filter"],
                    "right_predicate_sql": metadata["right_filter"],
                    "join_predicate_sql": metadata["join_filter"]
                }),
            );
            aggregate_input = "filter_join";
        }
    }

    if metadata["count_distinct"] == true {
        plan.unary(
            aggregate_input,
            "distinct",
            OperatorKind::Distinct,
            json!({
                "group_source": metadata["group_source"],
                "group_column": metadata["source_group"],
                "value_source": metadata["count_input_source"],
                "value_column": metadata["count_input_column"]
            }),
        );
        aggregate_input = "distinct";
    }
    plan.unary(
        aggregate_input,
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
    );
    let mut tail = "aggregate";
    if !metadata["having"].is_null() {
        plan.unary(
            tail,
            "having",
            OperatorKind::Having,
            json!({ "predicate_sql": metadata["having"] }),
        );
        tail = "having";
    }
    plan.unary(
        tail,
        "project",
        OperatorKind::Project,
        json!({
            "source_group": metadata["source_group"],
            "result_group": metadata["result_group"]
        }),
    );
    plan.unary(
        "project",
        "sink",
        OperatorKind::Sink,
        json!({ "result_oid": result_oid }),
    );
    plan.finish()
}

pub(super) fn build_logical_plan(metadata: &Value, result_oid: u32) -> LogicalPlan {
    match metadata["view_kind"].as_str() {
        Some("topn") => build_single_source_plan(
            metadata,
            result_oid,
            "topn",
            OperatorKind::TopN,
            metadata["topn"].clone(),
            metadata["topn"].clone(),
        ),
        Some("distinct") => build_single_source_plan(
            metadata,
            result_oid,
            "distinct",
            OperatorKind::Distinct,
            metadata["distinct_projection"].clone(),
            metadata["distinct_projection"].clone(),
        ),
        Some("window") => build_single_source_plan(
            metadata,
            result_oid,
            "window",
            OperatorKind::Window,
            metadata["window"].clone(),
            json!({ "output_columns": metadata["window"]["output_columns"] }),
        ),
        _ => build_aggregate_plan(metadata, result_oid),
    }
}
