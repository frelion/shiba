//! Stable logical-plan API.
//!
//! The module is split by responsibility so readers can follow the data flow:
//! persisted model -> compiler -> validator -> PostgreSQL runtime bridge.

mod compile;
mod model;
mod runtime;
mod validate;

#[allow(unused_imports)]
pub use compile::compile_logical_plan;
#[allow(unused_imports)]
pub use model::{DeltaBatch, DeltaRow, LogicalEdge, LogicalNode, LogicalPlan, OperatorKind};
pub use runtime::DagRuntime;

#[cfg(test)]
use compile::{build_logical_plan, LogicalPlanBuilder};
#[cfg(test)]
use model::LOGICAL_PLAN_VERSION;
#[cfg(test)]
use runtime::encode_batch_events;
#[cfg(test)]
use validate::{ExecutionDescriptor, ExecutionJoinType, ExecutionPipeline, ExecutionPlan};
#[cfg(test)]
use {pgrx::pg_sys, serde_json::json, serde_json::Value};

#[cfg(test)]
mod tests {
    use super::*;

    fn test_execution(source_oids: &[u32]) -> ExecutionPlan {
        ExecutionPlan {
            descriptor: ExecutionDescriptor {
                pipeline: ExecutionPipeline::Aggregate,
                left_source_oid: source_oids[0],
                right_source_oid: source_oids.get(1).copied(),
                join_type: None,
            },
            source_oids: source_oids.iter().copied().collect(),
        }
    }

    #[test]
    fn batch_encoding_preserves_cross_source_wal_order() {
        let plan = test_execution(&[41, 42]);
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
        let plan = test_execution(&[41]);
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
    fn plan_builder_only_adds_edges_for_connected_nodes() {
        let mut builder = LogicalPlanBuilder::default();
        builder.root("scan", OperatorKind::Scan, json!({}));
        builder.node("sink", OperatorKind::Sink, json!({}), Some(("scan", 1)));
        let plan = builder.finish();

        assert_eq!(plan.nodes.len(), 2);
        assert_eq!(
            plan.edges,
            vec![LogicalEdge {
                from: "scan".into(),
                to: "sink".into(),
                input: 1,
            }]
        );
    }

    fn node_shape(plan: &LogicalPlan) -> Vec<(&str, &OperatorKind)> {
        plan.nodes
            .iter()
            .map(|node| (node.id.as_str(), &node.operator))
            .collect()
    }

    #[test]
    fn topn_plan_shape_preserves_filter_projection_and_sink() {
        let topn = json!({
            "order_column": "score",
            "order_direction": "DESC",
            "nulls_first": false,
            "limit_count": 10,
            "limit_offset": 2,
            "source_columns": ["id", "score"],
            "output_columns": ["id", "score"]
        });
        let metadata = json!({
            "view_kind": "topn",
            "left_source": 41,
            "left_filter": "score > 0",
            "topn": topn
        });

        let plan = build_logical_plan(&metadata, 99);

        assert_eq!(
            node_shape(&plan),
            vec![
                ("scan_left", &OperatorKind::Scan),
                ("filter_left", &OperatorKind::Filter),
                ("topn", &OperatorKind::TopN),
                ("project", &OperatorKind::Project),
                ("sink", &OperatorKind::Sink),
            ]
        );
        assert_eq!(plan.nodes[2].config, topn);
        assert_eq!(plan.nodes[3].config, topn);
        assert_eq!(plan.nodes[4].config, json!({"result_oid": 99}));
        assert_eq!(
            plan.edges,
            vec![
                LogicalEdge {
                    from: "scan_left".into(),
                    to: "filter_left".into(),
                    input: 0,
                },
                LogicalEdge {
                    from: "filter_left".into(),
                    to: "topn".into(),
                    input: 0,
                },
                LogicalEdge {
                    from: "topn".into(),
                    to: "project".into(),
                    input: 0,
                },
                LogicalEdge {
                    from: "project".into(),
                    to: "sink".into(),
                    input: 0,
                },
            ]
        );
    }

    #[test]
    fn join_plan_shape_preserves_input_numbers_and_post_filter_order() {
        let metadata = json!({
            "view_kind": "aggregate",
            "left_source": 41,
            "right_source": 42,
            "join_type": "left",
            "left_join_column": "customer_id",
            "right_join_column": "id",
            "left_filter": "orders.active",
            "left_filter_phase": "pre",
            "right_filter": "customers.enabled",
            "right_filter_phase": "post",
            "join_filter": "orders.region = customers.region",
            "group_source": "left",
            "source_group": "region",
            "result_group": "region",
            "count_column": "row_count",
            "count_distinct": true,
            "count_input_source": "left",
            "count_input_column": "customer_id",
            "sum_input": "amount",
            "sum_column": "total",
            "having": "row_count > 1"
        });

        let plan = build_logical_plan(&metadata, 99);

        assert_eq!(
            node_shape(&plan),
            vec![
                ("scan_left", &OperatorKind::Scan),
                ("filter_left", &OperatorKind::Filter),
                ("scan_right", &OperatorKind::Scan),
                ("join", &OperatorKind::LeftJoin),
                ("filter_join", &OperatorKind::Filter),
                ("distinct", &OperatorKind::Distinct),
                ("aggregate", &OperatorKind::Aggregate),
                ("having", &OperatorKind::Having),
                ("project", &OperatorKind::Project),
                ("sink", &OperatorKind::Sink),
            ]
        );
        assert_eq!(
            plan.edges[1],
            LogicalEdge {
                from: "filter_left".into(),
                to: "join".into(),
                input: 0,
            }
        );
        assert_eq!(
            plan.edges[2],
            LogicalEdge {
                from: "scan_right".into(),
                to: "join".into(),
                input: 1,
            }
        );
        assert_eq!(
            plan.nodes[4].config,
            json!({
                "left_predicate_sql": "orders.active",
                "right_predicate_sql": "customers.enabled",
                "join_predicate_sql": "orders.region = customers.region"
            })
        );
    }

    #[test]
    fn window_and_distinct_plans_keep_their_existing_project_configs() {
        let window = json!({
            "partition_column": "account_id",
            "output_columns": ["account_id", "rank"],
            "target_expressions": ["account_id", "rank()"]
        });
        let window_plan = build_logical_plan(
            &json!({
                "view_kind": "window",
                "left_source": 41,
                "left_filter": null,
                "window": window
            }),
            99,
        );
        assert_eq!(window_plan.nodes[1].config, window);
        assert_eq!(
            window_plan.nodes[2].config,
            json!({"output_columns": ["account_id", "rank"]})
        );

        let projection = json!({
            "source_columns": ["account_id"],
            "output_columns": ["account_id"]
        });
        let distinct_plan = build_logical_plan(
            &json!({
                "view_kind": "distinct",
                "left_source": 41,
                "left_filter": null,
                "distinct_projection": projection
            }),
            99,
        );
        assert_eq!(distinct_plan.nodes[1].config, projection);
        assert_eq!(distinct_plan.nodes[2].config, projection);
    }

    fn aggregate_metadata() -> Value {
        json!({
            "view_kind": "aggregate",
            "left_source": 41,
            "right_source": null,
            "join_type": null,
            "left_join_column": null,
            "right_join_column": null,
            "left_filter": null,
            "left_filter_phase": null,
            "right_filter": null,
            "right_filter_phase": null,
            "join_filter": null,
            "group_source": "left",
            "source_group": "region",
            "result_group": "region",
            "count_column": "row_count",
            "count_distinct": false,
            "count_input_source": null,
            "count_input_column": null,
            "sum_input": "amount",
            "sum_column": "total",
            "having": null
        })
    }

    #[test]
    fn validated_plan_selects_every_physical_pipeline() {
        let aggregate = build_logical_plan(&aggregate_metadata(), 99);
        assert_eq!(
            aggregate.validate_for(99).unwrap().descriptor.pipeline,
            ExecutionPipeline::Aggregate
        );

        for (join_type, execution_join_type) in [
            ("inner", ExecutionJoinType::Inner),
            ("left", ExecutionJoinType::Left),
            ("right", ExecutionJoinType::Right),
            ("full", ExecutionJoinType::Full),
            ("semi", ExecutionJoinType::Semi),
            ("anti", ExecutionJoinType::Anti),
            ("null_anti", ExecutionJoinType::NullAnti),
        ] {
            let mut metadata = aggregate_metadata();
            metadata["right_source"] = json!(42);
            metadata["join_type"] = json!(join_type);
            metadata["left_join_column"] = json!("customer_id");
            metadata["right_join_column"] = json!("id");
            metadata["left_filter"] = json!("active");
            metadata["left_filter_phase"] = json!("post");
            metadata["count_distinct"] = json!(true);
            metadata["count_input_source"] = json!("left");
            metadata["count_input_column"] = json!("customer_id");
            metadata["having"] = json!("row_count > 1");
            let descriptor = build_logical_plan(&metadata, 99)
                .validate_for(99)
                .unwrap()
                .descriptor;
            assert_eq!(descriptor.pipeline, ExecutionPipeline::Join, "{join_type}");
            assert_eq!(descriptor.left_source_oid, 41);
            assert_eq!(descriptor.right_source_oid, Some(42));
            assert_eq!(descriptor.join_type, Some(execution_join_type));
        }

        for (view_kind, pipeline, field, config) in [
            (
                "window",
                ExecutionPipeline::Window,
                "window",
                json!({
                    "partition_column": "account_id",
                    "result_partition_column": "account_id",
                    "order_column": "created_at",
                    "order_direction": "asc",
                    "nulls_first": false,
                    "output_columns": ["account_id", "rank"],
                    "target_expressions": ["account_id", "rank()"]
                }),
            ),
            (
                "distinct",
                ExecutionPipeline::Distinct,
                "distinct_projection",
                json!({
                    "source_columns": ["account_id"],
                    "output_columns": ["account_id"]
                }),
            ),
            (
                "topn",
                ExecutionPipeline::TopN,
                "topn",
                json!({
                    "order_column": "score",
                    "order_direction": "desc",
                    "nulls_first": false,
                    "limit_count": 10,
                    "limit_offset": 0,
                    "source_columns": ["id", "score"],
                    "output_columns": ["id", "score"]
                }),
            ),
        ] {
            let mut metadata = json!({
                "view_kind": view_kind,
                "left_source": 41,
                "left_filter": null
            });
            metadata[field] = config;
            let descriptor = build_logical_plan(&metadata, 99)
                .validate_for(99)
                .unwrap()
                .descriptor;
            assert_eq!(descriptor.pipeline, pipeline);
            assert_eq!(
                serde_json::to_value(&descriptor).unwrap()["pipeline"],
                view_kind
            );
        }
    }

    #[test]
    fn validator_rejects_duplicate_or_orphan_nodes_and_wrong_sink() {
        let valid = build_logical_plan(&aggregate_metadata(), 99);

        let mut duplicate = valid.clone();
        duplicate.nodes[1].id = duplicate.nodes[0].id.clone();
        assert!(duplicate
            .validate_for(99)
            .unwrap_err()
            .contains("duplicate"));

        let mut orphan = valid.clone();
        orphan.nodes.push(LogicalNode {
            id: "orphan".into(),
            operator: OperatorKind::Project,
            config: json!({}),
        });
        assert!(orphan.validate_for(99).unwrap_err().contains("unreachable"));

        let mut wrong_sink = valid;
        wrong_sink
            .nodes
            .iter_mut()
            .find(|node| node.operator == OperatorKind::Sink)
            .unwrap()
            .config = json!({"result_oid": 100});
        assert!(wrong_sink
            .validate_for(99)
            .unwrap_err()
            .contains("wrong result OID"));
    }

    #[test]
    fn validator_rejects_cycles_even_when_all_nodes_have_inputs() {
        let plan = LogicalPlan {
            version: LOGICAL_PLAN_VERSION,
            nodes: vec![
                LogicalNode {
                    id: "scan".into(),
                    operator: OperatorKind::Scan,
                    config: json!({"source_oid": 41}),
                },
                LogicalNode {
                    id: "aggregate".into(),
                    operator: OperatorKind::Aggregate,
                    config: json!({}),
                },
                LogicalNode {
                    id: "sink".into(),
                    operator: OperatorKind::Sink,
                    config: json!({"result_oid": 99}),
                },
                LogicalNode {
                    id: "cycle_a".into(),
                    operator: OperatorKind::Filter,
                    config: json!({}),
                },
                LogicalNode {
                    id: "cycle_b".into(),
                    operator: OperatorKind::Project,
                    config: json!({}),
                },
            ],
            edges: vec![
                LogicalEdge {
                    from: "scan".into(),
                    to: "aggregate".into(),
                    input: 0,
                },
                LogicalEdge {
                    from: "aggregate".into(),
                    to: "sink".into(),
                    input: 0,
                },
                LogicalEdge {
                    from: "cycle_a".into(),
                    to: "cycle_b".into(),
                    input: 0,
                },
                LogicalEdge {
                    from: "cycle_b".into(),
                    to: "cycle_a".into(),
                    input: 0,
                },
            ],
        };
        assert!(plan.validate_for(99).unwrap_err().contains("cycle"));
    }

    fn join_metadata_for_validation() -> Value {
        let mut metadata = aggregate_metadata();
        metadata["right_source"] = json!(42);
        metadata["join_type"] = json!("left");
        metadata["left_join_column"] = json!("customer_id");
        metadata["right_join_column"] = json!("id");
        metadata
    }

    #[test]
    fn validator_rejects_swapped_join_inputs_and_wrong_operator_order() {
        let mut swapped = build_logical_plan(&join_metadata_for_validation(), 99);
        let mut join_edges: Vec<_> = swapped
            .edges
            .iter_mut()
            .filter(|edge| edge.to == "join")
            .collect();
        join_edges[0].input = 1;
        join_edges[1].input = 0;
        assert!(swapped
            .validate_for(99)
            .unwrap_err()
            .contains("operator order"));

        let mut wrong_order = build_logical_plan(&aggregate_metadata(), 99);
        let aggregate = wrong_order
            .nodes
            .iter_mut()
            .find(|node| node.id == "aggregate")
            .unwrap();
        aggregate.operator = OperatorKind::Project;
        let project = wrong_order
            .nodes
            .iter_mut()
            .find(|node| node.id == "project")
            .unwrap();
        project.operator = OperatorKind::Aggregate;
        assert!(wrong_order.validate_for(99).is_err());
    }

    #[test]
    fn validator_rejects_two_execution_cores_and_incomplete_config() {
        let mut double_core = build_logical_plan(&aggregate_metadata(), 99);
        double_core
            .nodes
            .iter_mut()
            .find(|node| node.id == "project")
            .unwrap()
            .operator = OperatorKind::Aggregate;
        assert!(double_core
            .validate_for(99)
            .unwrap_err()
            .contains("exactly one"));

        let mut incomplete = build_logical_plan(&join_metadata_for_validation(), 99);
        incomplete
            .nodes
            .iter_mut()
            .find(|node| node.id == "join")
            .unwrap()
            .config = json!({"left_key": "customer_id"});
        assert!(incomplete
            .validate_for(99)
            .unwrap_err()
            .contains("incomplete"));
    }

    #[test]
    fn validator_decodes_configs_into_closed_typed_shapes() {
        let mut unknown_field = build_logical_plan(&aggregate_metadata(), 99);
        unknown_field
            .nodes
            .iter_mut()
            .find(|node| node.id == "aggregate")
            .unwrap()
            .config["unexpected"] = json!(true);
        assert!(unknown_field.validate_for(99).is_err());

        let mut invalid_side = build_logical_plan(&join_metadata_for_validation(), 99);
        invalid_side
            .nodes
            .iter_mut()
            .find(|node| node.id == "aggregate")
            .unwrap()
            .config["group_source"] = json!("middle");
        assert!(invalid_side.validate_for(99).is_err());
    }
}
