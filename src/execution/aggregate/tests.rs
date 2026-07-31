use super::*;
use crate::execution::WorkUsage;
use crate::planner::model::{BindingId, ScalarExpr, SlotId, SlotType};

fn budget(output_rows: usize) -> WorkBudget {
    WorkBudget::new(4, 100, output_rows, 100)
}

fn position(row: i64) -> InputPosition {
    InputPosition::new(7, 11, row).unwrap()
}

fn no_output(_continuation: u64) -> PrimitiveFacts {
    PrimitiveFacts::default()
}

fn one_effect(_continuation: u64, chunk_seq: i64) -> PrimitiveFacts {
    PrimitiveFacts {
        usage: WorkUsage {
            output_rows: 1,
            output_bytes: 9,
            ..WorkUsage::default()
        },
        output: OutputFacts::Data { chunk_seq },
        ..PrimitiveFacts::default()
    }
}

fn emit_continuation(after: AfterDrain) -> AggregateContinuation {
    AggregateContinuation {
        input_stream_id: 7,
        input: None,
        phase: AggregatePhase::DrainEmit {
            group_queue_id: 13,
            leg: EmitLeg::Decide,
            after,
        },
    }
}

fn sort_key(binding: u32, sort_operator_oid: u32, nulls_first: bool) -> SortGroupExpr {
    let type_ = SlotType {
        type_oid: pg_sys::INT4OID.to_u32(),
        typmod: -1,
        collation_oid: 0,
        nullable: true,
    };
    SortGroupExpr {
        expr: ScalarExpr::Input {
            binding: BindingId(binding),
        },
        type_,
        equality_operator_oid: 96,
        sort_operator_oid,
        nulls_first,
        hashable: true,
    }
}

fn aggregate_with_order(
    distinct: Vec<SortGroupExpr>,
    order_by: Vec<SortGroupExpr>,
) -> AggregateExpr {
    AggregateExpr {
        ref_id: 1,
        output: SlotId(1),
        function_oid: 1,
        input_collation_oid: 0,
        args: Vec::new(),
        direct_args: Vec::new(),
        distinct,
        filter: None,
        order_by,
        type_: SlotType {
            type_oid: pg_sys::INT8OID.to_u32(),
            typmod: -1,
            collation_oid: 0,
            nullable: true,
        },
    }
}

#[test]
fn distinct_rebuild_orders_by_order_keys_then_uncovered_tuple_keys() {
    let first = sort_key(1, 97, false);
    let second = sort_key(2, 97, false);
    let descending_second = sort_key(2, 521, true);
    let aggregate = aggregate_with_order(vec![first, second], vec![descending_second]);
    let order = aggregate_effective_order(3, &aggregate).unwrap();
    assert_eq!(
        order
            .iter()
            .map(|key| key.column.as_str())
            .collect::<Vec<_>>(),
        vec!["agg_3_order_1", "agg_3_distinct_1"]
    );
}

#[test]
fn distinct_rebuild_rejects_an_order_key_outside_the_distinct_tuple() {
    let aggregate =
        aggregate_with_order(vec![sort_key(1, 97, false)], vec![sort_key(2, 97, false)]);
    assert_eq!(
        aggregate_effective_order(1, &aggregate).unwrap_err(),
        "Aggregate DISTINCT ordering is not covered by its DISTINCT tuple"
    );
}

#[test]
fn aggregate_key_equality_uses_the_resolved_operator_and_explicit_nulls() {
    let equality = aggregate_null_safe_equality("left.key", "right.key", "OPERATOR(pg_catalog.=)");
    assert!(equality.contains("left.key OPERATOR(pg_catalog.=) right.key"));
    assert!(!equality.contains("IS TRUE"));
    assert!(equality.contains("left.key IS NULL AND right.key IS NULL"));
    assert!(!equality.contains("IS NOT DISTINCT FROM"));
}

#[test]
fn an_uncommitted_action_replays_from_the_same_durable_state() {
    let machine = AggregateMachine::new(2).unwrap();
    let durable = AggregateContinuation {
        input_stream_id: 7,
        input: Some(position(1)),
        phase: AggregatePhase::Apply,
    };
    let action = machine.action(durable).unwrap();
    assert_eq!(action, machine.action(durable).unwrap());

    let committed = machine
        .apply(
            durable,
            AggregateActionResult::Applied(AppliedPage {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        input_rows: 1,
                        input_bytes: 12,
                        ..WorkUsage::default()
                    },
                    state_rows: 2,
                    output: OutputFacts::None,
                },
                target: ApplyTarget::Drain {
                    first_group_queue_id: 13,
                    after: AfterDrain::Apply(position(2)),
                },
            }),
            budget(1),
        )
        .unwrap();
    let AggregateTransition::Committed {
        continuation: Some(after_commit),
        ..
    } = committed
    else {
        panic!("Apply must commit a Drain continuation");
    };
    assert_ne!(machine.action(after_commit).unwrap(), action);
    assert!(matches!(
        machine.action(after_commit).unwrap(),
        AggregateAction::DrainRebuild {
            aggregate_ordinal: 1,
            ..
        }
    ));
}

#[test]
fn rebuild_ordinal_and_cursor_are_bounded() {
    let machine = AggregateMachine::new(2).unwrap();
    let durable = AggregateContinuation {
        input_stream_id: 7,
        input: None,
        phase: AggregatePhase::DrainRebuild {
            group_queue_id: 13,
            aggregate_ordinal: 1,
            after: AfterDrain::Idle,
        },
    };
    let partial = machine
        .apply(
            durable,
            AggregateActionResult::Rebuilt(RebuiltPage {
                page: PageFacts {
                    usage: WorkUsage {
                        input_rows: 1,
                        input_bytes: 8,
                        ..WorkUsage::default()
                    },
                    last_row_id: Some(21),
                    complete: false,
                },
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        input_rows: 1,
                        input_bytes: 8,
                        ..WorkUsage::default()
                    },
                    state_rows: 1,
                    output: OutputFacts::None,
                },
            }),
            budget(1),
        )
        .unwrap();
    let AggregateTransition::Committed {
        continuation: Some(partial),
        ..
    } = partial
    else {
        panic!("partial rebuild must resume");
    };
    assert!(matches!(
        partial.phase,
        AggregatePhase::DrainRebuild {
            aggregate_ordinal: 1,
            ..
        }
    ));

    let second = machine
        .apply(
            partial,
            AggregateActionResult::Rebuilt(RebuiltPage {
                page: PageFacts {
                    usage: WorkUsage::default(),
                    last_row_id: None,
                    complete: true,
                },
                facts: PrimitiveFacts {
                    state_rows: 1,
                    ..PrimitiveFacts::default()
                },
            }),
            budget(1),
        )
        .unwrap();
    let AggregateTransition::Committed {
        continuation: Some(second),
        ..
    } = second
    else {
        panic!("the next aggregate must resume");
    };
    assert!(matches!(
        second.phase,
        AggregatePhase::DrainRebuild {
            aggregate_ordinal: 2,
            ..
        }
    ));
}

#[test]
fn replacement_uses_two_committed_one_row_legs() {
    let machine = AggregateMachine::new(1).unwrap();
    let durable = emit_continuation(AfterDrain::Idle);
    let old_action = machine.action(durable).unwrap();
    assert_eq!(
        old_action,
        AggregateAction::PrepareOutput { group_queue_id: 13 }
    );

    // A crash before commit still observes `durable` and retracts old
    // again only in a transaction that replaces the uncommitted attempt.
    assert_eq!(machine.action(durable).unwrap(), old_action);
    let old_commit = machine
        .apply(
            durable,
            AggregateActionResult::OutputPrepared(PreparedOutput::ReplacementRetracted {
                facts: one_effect(1, 31),
            }),
            budget(1),
        )
        .unwrap();
    let AggregateTransition::Committed {
        continuation: Some(insert_pending),
        ..
    } = old_commit
    else {
        panic!("replacement retract must persist its second leg");
    };
    assert_eq!(
        machine.action(insert_pending).unwrap(),
        AggregateAction::EmitPending { group_queue_id: 13 }
    );

    // After the first commit a restart can only emit the persisted new row.
    let new_commit = machine
        .apply(
            insert_pending,
            AggregateActionResult::PendingEmitted(PendingOutput {
                facts: one_effect(0, 32),
                next_group_queue_id: None,
            }),
            budget(1),
        )
        .unwrap();
    assert!(matches!(
        new_commit,
        AggregateTransition::Committed {
            continuation: None,
            ..
        }
    ));
}

#[test]
fn unchanged_insert_and_delete_complete_without_a_second_leg() {
    let machine = AggregateMachine::new(1).unwrap();
    for prepared in [
        PreparedOutput::Unchanged {
            facts: no_output(0),
            next_group_queue_id: None,
        },
        PreparedOutput::Inserted {
            facts: one_effect(0, 41),
            next_group_queue_id: None,
        },
        PreparedOutput::Deleted {
            facts: one_effect(0, 42),
            next_group_queue_id: None,
        },
    ] {
        let transition = machine
            .apply(
                emit_continuation(AfterDrain::Idle),
                AggregateActionResult::OutputPrepared(prepared),
                budget(1),
            )
            .unwrap();
        assert!(matches!(
            transition,
            AggregateTransition::Committed {
                continuation: None,
                ..
            }
        ));
    }
}

#[test]
fn completed_group_selects_the_next_queue_before_resuming_input() {
    let machine = AggregateMachine::new(1).unwrap();
    let transition = machine
        .apply(
            emit_continuation(AfterDrain::Apply(position(4))),
            AggregateActionResult::OutputPrepared(PreparedOutput::Unchanged {
                facts: no_output(1),
                next_group_queue_id: Some(17),
            }),
            budget(1),
        )
        .unwrap();
    let AggregateTransition::Committed {
        continuation: Some(next),
        ..
    } = transition
    else {
        panic!("next dirty group must remain durable");
    };
    assert!(matches!(
        next.phase,
        AggregatePhase::DrainRebuild {
            group_queue_id: 17,
            aggregate_ordinal: 1,
            ..
        }
    ));
}

#[test]
fn committed_frontier_finishes_the_input() {
    let machine = AggregateMachine::new(1).unwrap();
    let durable = AggregateContinuation {
        input_stream_id: 7,
        input: Some(position(0)),
        phase: AggregatePhase::Frontier,
    };
    let committed = machine
        .apply(
            durable,
            AggregateActionResult::Frontier(FrontierResult::Forwarded {
                facts: PrimitiveFacts {
                    output: OutputFacts::Frontier { chunk_seq: 51 },
                    ..PrimitiveFacts::default()
                },
            }),
            budget(1),
        )
        .unwrap();
    assert!(matches!(
        committed,
        AggregateTransition::Committed {
            continuation: None,
            ..
        }
    ));
}

#[test]
fn global_empty_input_rebuilds_before_forwarding_frontier() {
    let machine = AggregateMachine::new(1).unwrap();
    let durable = AggregateContinuation {
        input_stream_id: 7,
        input: Some(position(0)),
        phase: AggregatePhase::Frontier,
    };
    let queued = machine
        .apply(
            durable,
            AggregateActionResult::Frontier(FrontierResult::GlobalGroupQueued {
                facts: PrimitiveFacts {
                    state_rows: 1,
                    ..PrimitiveFacts::default()
                },
                group_queue_id: 61,
            }),
            budget(1),
        )
        .unwrap();
    let AggregateTransition::Committed {
        continuation: Some(next),
        ..
    } = queued
    else {
        panic!("global bootstrap must be resumable");
    };
    assert!(matches!(
        next.phase,
        AggregatePhase::DrainRebuild {
            group_queue_id: 61,
            after: AfterDrain::Frontier(_),
            ..
        }
    ));
}
