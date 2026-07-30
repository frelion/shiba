use super::*;
use crate::execution::WorkUsage;

fn budget() -> WorkBudget {
    WorkBudget::new(3, 30, 1, 10)
}

fn position(row: i64) -> InputPosition {
    InputPosition::new(5, 9, row).unwrap()
}

fn input_continuation(row: i64) -> DistinctContinuation {
    DistinctContinuation {
        input: position(row),
        phase: DistinctPhase::Apply,
    }
}

fn drain_continuation(row: i64) -> DistinctContinuation {
    DistinctContinuation {
        input: position(row),
        phase: DistinctPhase::Drain,
    }
}

#[test]
fn crash_before_commit_replays_the_same_prefix() {
    let machine = DistinctMachine;
    let durable = input_continuation(1);
    let action = machine.action(durable).unwrap();
    assert_eq!(action, machine.action(durable).unwrap());

    let committed = machine
        .apply(
            durable,
            DistinctActionResult::Applied(AppliedPrefix {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        input_rows: 2,
                        input_bytes: 12,
                        ..WorkUsage::default()
                    },
                    state_rows: 2,
                    continuation_rows: 1,
                    output: OutputFacts::None,
                },
                occupancy: OccupancyDiff {
                    touched_keys: 2,
                    external_effects: 1,
                },
                next: Some(DistinctContinuation {
                    input: position(3),
                    phase: DistinctPhase::Drain,
                }),
            }),
            budget(),
        )
        .unwrap();
    let DistinctTransition::Committed {
        continuation: Some(after_commit),
        ..
    } = committed
    else {
        panic!("partial Distinct prefix must resume");
    };
    assert_eq!(
        machine.action(after_commit).unwrap(),
        DistinctAction::DrainEffects { input: position(3) }
    );
}

#[test]
fn occupancy_can_change_without_an_external_diff() {
    let committed = DistinctMachine
        .apply(
            input_continuation(1),
            DistinctActionResult::Applied(AppliedPrefix {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        input_rows: 2,
                        input_bytes: 8,
                        ..WorkUsage::default()
                    },
                    state_rows: 1,
                    continuation_rows: 0,
                    output: OutputFacts::None,
                },
                occupancy: OccupancyDiff {
                    touched_keys: 1,
                    external_effects: 0,
                },
                next: None,
            }),
            budget(),
        )
        .unwrap();
    assert!(matches!(
        committed,
        DistinctTransition::Committed {
            continuation: None,
            ..
        }
    ));
}

#[test]
fn one_oversized_typed_effect_row_is_valid() {
    DistinctMachine
        .apply(
            drain_continuation(1),
            DistinctActionResult::Drained(AppliedPrefix {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        output_rows: 1,
                        output_bytes: 11,
                        ..WorkUsage::default()
                    },
                    state_rows: 1,
                    continuation_rows: 0,
                    output: OutputFacts::Data { chunk_seq: 18 },
                },
                occupancy: OccupancyDiff {
                    touched_keys: 0,
                    external_effects: 1,
                },
                next: None,
            }),
            budget(),
        )
        .unwrap();
}

#[test]
fn committed_frontier_has_no_continuation() {
    let durable = DistinctContinuation {
        input: position(0),
        phase: DistinctPhase::Frontier,
    };
    let committed = DistinctMachine
        .apply(
            durable,
            DistinctActionResult::FrontierForwarded(PrimitiveFacts {
                output: OutputFacts::Frontier { chunk_seq: 19 },
                ..PrimitiveFacts::default()
            }),
            budget(),
        )
        .unwrap();
    assert!(matches!(
        committed,
        DistinctTransition::Committed {
            continuation: None,
            ..
        }
    ));
}

#[test]
fn drain_output_count_must_equal_the_effect_summary() {
    let error = DistinctMachine
        .apply(
            drain_continuation(1),
            DistinctActionResult::Drained(AppliedPrefix {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        output_rows: 1,
                        output_bytes: 4,
                        ..WorkUsage::default()
                    },
                    continuation_rows: 0,
                    output: OutputFacts::Data { chunk_seq: 20 },
                    ..PrimitiveFacts::default()
                },
                occupancy: OccupancyDiff {
                    touched_keys: 0,
                    external_effects: 0,
                },
                next: None,
            }),
            budget(),
        )
        .unwrap_err();
    assert!(error.contains("effect"));
}

#[test]
fn replacement_queues_two_legs_under_a_one_row_output_budget() {
    let applied = DistinctMachine
        .apply(
            input_continuation(4),
            DistinctActionResult::Applied(AppliedPrefix {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        input_rows: 1,
                        input_bytes: 8,
                        ..WorkUsage::default()
                    },
                    state_rows: 3,
                    continuation_rows: 1,
                    output: OutputFacts::None,
                },
                occupancy: OccupancyDiff {
                    touched_keys: 1,
                    external_effects: 2,
                },
                next: Some(drain_continuation(5)),
            }),
            budget(),
        )
        .unwrap();
    let DistinctTransition::Committed {
        continuation: Some(first_drain),
        ..
    } = applied
    else {
        panic!("replacement must persist its Drain phase");
    };

    let first_leg = DistinctMachine
        .apply(
            first_drain,
            DistinctActionResult::Drained(AppliedPrefix {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        output_rows: 1,
                        output_bytes: 6,
                        ..WorkUsage::default()
                    },
                    state_rows: 1,
                    continuation_rows: 1,
                    output: OutputFacts::Data { chunk_seq: 21 },
                },
                occupancy: OccupancyDiff {
                    touched_keys: 0,
                    external_effects: 1,
                },
                next: Some(first_drain),
            }),
            budget(),
        )
        .unwrap();
    assert!(matches!(
        first_leg,
        DistinctTransition::Committed {
            continuation: Some(DistinctContinuation {
                phase: DistinctPhase::Drain,
                ..
            }),
            ..
        }
    ));
}

#[test]
fn sql_contract_checks_both_negative_prefixes_and_bounds_representative_lookup() {
    let production = [
        include_str!("mod.rs"),
        include_str!("runtime.rs"),
        include_str!("provision.rs"),
    ]
    .concat();
    let reconcile = production
        .split_once("fn reconcile_representatives(")
        .expect("Distinct must reconcile representatives")
        .1
        .split_once("fn distinct_null_safe_equality(")
        .expect("representative reconciliation must remain a bounded helper")
        .0;

    assert!(production.contains("min(key_prefix) AS min_prefix"));
    assert!(production.contains("min(physical_prefix) AS min_prefix"));
    assert!(production.contains("minimum_multiplicity<0"));
    assert!(production.contains("UNIQUE(group_state_id,output_key)"));
    assert!(production.contains("CHECK((multiplicity=0)=((output_row)::text IS NULL))"));
    assert!(production.contains("SELECT DISTINCT ON ({qualified_key_column_list})"));
    assert!(production.contains("ON CONFLICT({conflict_keys}) DO UPDATE"));
    assert!(production.contains("JOIN resolved_groups ON {group_match}"));
    assert!(!production.contains("distinct_bag_order"));
    assert!(production.contains(
        "WHERE bag.group_state_id=physical_collapsed.group_state_id
              AND bag.output_key=physical_collapsed.output_key
            LIMIT 1
            FOR UPDATE"
    ));
    assert!(reconcile.contains(
        "WITH touched_page AS MATERIALIZED (
              DELETE FROM {touched}
              RETURNING group_state_id,net_weight"
    ));
    assert!(reconcile.contains(
        "WHERE groups.group_state_id=touched_page.group_state_id
                LIMIT 1
                FOR UPDATE"
    ));
    assert!(reconcile.contains(
        "WHERE bag.group_state_id=locked.group_state_id
                ORDER BY bag.output_key
                LIMIT 1"
    ));
}
