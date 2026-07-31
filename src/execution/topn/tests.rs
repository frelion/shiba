use super::*;
use crate::execution::WorkUsage;

fn budget() -> WorkBudget {
    WorkBudget::new(2, 20, 1, 12)
}

fn position(row: i64) -> InputPosition {
    InputPosition::new(3, 9, row).unwrap()
}

fn frontier_position() -> InputPosition {
    InputPosition::new(3, 10, 0).unwrap()
}

fn internal_page(last_row_id: Option<i64>, complete: bool) -> TopNPage {
    TopNPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: u64::from(last_row_id.is_some()),
                input_bytes: u64::from(last_row_id.is_some()) * 8,
                ..WorkUsage::default()
            },
            state_rows: u64::from(last_row_id.is_some()),
            output: OutputFacts::None,
        },
        last_row_id,
        complete,
    }
}

fn diff_page(last_row_id: Option<i64>, complete: bool, chunk_seq: Option<i64>) -> TopNDiffPage {
    let output_rows = u64::from(chunk_seq.is_some());
    TopNDiffPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: u64::from(last_row_id.is_some()),
                input_bytes: u64::from(last_row_id.is_some()) * 8,
                output_rows,
                output_bytes: output_rows * 9,
            },
            state_rows: output_rows,
            output: chunk_seq.map_or(OutputFacts::None, |chunk_seq| OutputFacts::Data {
                chunk_seq,
            }),
        },
        last_row_id,
        complete,
        repeat_cursor: false,
    }
}

fn committed_continuation(transition: TopNTransition) -> TopNContinuation {
    let TopNTransition::Committed {
        continuation: Some(continuation),
        ..
    } = transition
    else {
        panic!("step should have a continuation");
    };
    continuation
}

fn select_continuation(machine: TopNMachine) -> TopNContinuation {
    let admitted = machine
        .apply(
            TopNContinuation {
                input_stream_id: 3,
                input: Some(position(1)),
                phase: TopNPhase::Admit,
            },
            TopNActionResult::Admitted(TopNAdmission {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        input_rows: 1,
                        input_bytes: 8,
                        ..WorkUsage::default()
                    },
                    state_rows: 1,
                    output: OutputFacts::None,
                },
                target: TopNAdmissionTarget::Drain {
                    generation_id: 17,
                    after_drain: AfterDrain::Frontier(frontier_position()),
                },
            }),
            budget(),
        )
        .unwrap();
    committed_continuation(admitted)
}

#[test]
fn strict_phase_codes_have_no_idle_or_unknown_decoder() {
    assert_eq!(
        TopNPhaseKind::from_code(TopNPhaseKind::Select.code()).unwrap(),
        TopNPhaseKind::Select
    );
    assert!(PhaseCode::active(0).is_err());
    assert!(TopNPhaseKind::from_code(PhaseCode::active(99).unwrap()).is_err());
}

#[test]
fn large_offset_limit_and_ties_resume_with_scalar_ids() {
    let machine = TopNMachine::new(2, 3, true);
    let mut continuation = select_continuation(machine);
    assert!(matches!(
        continuation.phase,
        TopNPhase::Select {
            progress: SelectionProgress {
                offset_remaining: 3,
                limit_remaining: 2,
                tie_boundary_row_id: None,
                ..
            },
            ..
        }
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                TopNActionResult::Selected(TopNSelection {
                    page: internal_page(Some(21), false),
                    progress: SelectionProgress {
                        cursor: TopNCursor { row_id: Some(21) },
                        offset_remaining: 1,
                        limit_remaining: 2,
                        tie_boundary_row_id: None,
                    },
                }),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        machine.action(continuation).unwrap(),
        TopNAction::SelectCandidates {
            progress: SelectionProgress {
                cursor: TopNCursor { row_id: Some(21) },
                ..
            },
            ..
        }
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                TopNActionResult::Selected(TopNSelection {
                    page: internal_page(Some(25), false),
                    progress: SelectionProgress {
                        cursor: TopNCursor { row_id: Some(25) },
                        offset_remaining: 0,
                        limit_remaining: 0,
                        tie_boundary_row_id: Some(25),
                    },
                }),
                budget(),
            )
            .unwrap(),
    );

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                TopNActionResult::Selected(TopNSelection {
                    page: internal_page(Some(28), true),
                    progress: SelectionProgress {
                        cursor: TopNCursor { row_id: Some(28) },
                        offset_remaining: 0,
                        limit_remaining: 0,
                        tie_boundary_row_id: Some(25),
                    },
                }),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        continuation.phase,
        TopNPhase::Diff {
            leg: DiffLeg::Remove,
            ..
        }
    ));
}

#[test]
fn final_cleanup_can_resume_the_same_input_chunk() {
    let machine = TopNMachine::new(5, 0, false);
    let transition = machine
        .apply(
            TopNContinuation {
                input_stream_id: 3,
                input: None,
                phase: TopNPhase::Cleanup {
                    generation_id: 17,
                    cursor: TopNCursor::default(),
                    after_drain: AfterDrain::Admit(position(2)),
                },
            },
            TopNActionResult::Cleaned(internal_page(None, true)),
            budget(),
        )
        .unwrap();
    assert_eq!(
        committed_continuation(transition),
        TopNContinuation {
            input_stream_id: 3,
            input: Some(position(2)),
            phase: TopNPhase::Admit,
        }
    );
}

#[test]
fn frontier_waits_for_remove_add_and_cleanup() {
    let machine = TopNMachine::new(5, 0, false);
    let mut continuation = TopNContinuation {
        input_stream_id: 3,
        input: None,
        phase: TopNPhase::Diff {
            generation_id: 17,
            leg: DiffLeg::Remove,
            cursor: TopNDiffCursor::default(),
            after_drain: AfterDrain::Frontier(frontier_position()),
        },
    };

    assert!(machine
        .apply(
            continuation,
            TopNActionResult::FrontierForwarded(PrimitiveFacts {
                output: OutputFacts::Frontier { chunk_seq: 50 },
                ..PrimitiveFacts::default()
            }),
            budget(),
        )
        .is_err());

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                TopNActionResult::Diffed(diff_page(Some(31), true, Some(51))),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        continuation.phase,
        TopNPhase::Diff {
            leg: DiffLeg::Add,
            ..
        }
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                TopNActionResult::Diffed(diff_page(None, true, None)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(continuation.phase, TopNPhase::Cleanup { .. }));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                TopNActionResult::Cleaned(internal_page(None, true)),
                budget(),
            )
            .unwrap(),
    );
    assert_eq!(continuation.phase, TopNPhase::Frontier);
    assert!(matches!(
        machine.action(continuation).unwrap(),
        TopNAction::ForwardFrontier { .. }
    ));
}

#[test]
fn diff_cursor_advances_on_zero_difference_and_repeats_only_residuals() {
    let machine = TopNMachine::new(5, 0, false);
    let start = TopNContinuation {
        input_stream_id: 3,
        input: None,
        phase: TopNPhase::Diff {
            generation_id: 17,
            leg: DiffLeg::Remove,
            cursor: TopNDiffCursor::default(),
            after_drain: AfterDrain::FinishInput,
        },
    };
    let after_equal_prefix = committed_continuation(
        machine
            .apply(
                start,
                TopNActionResult::Diffed(diff_page(Some(31), false, None)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        after_equal_prefix.phase,
        TopNPhase::Diff {
            cursor: TopNDiffCursor {
                row_id: Some(31),
                repeat: false,
            },
            ..
        }
    ));
    assert!(machine
        .apply(
            after_equal_prefix,
            TopNActionResult::Diffed(diff_page(Some(31), false, None)),
            budget(),
        )
        .is_err());

    let mut residual = diff_page(Some(41), false, Some(52));
    residual.repeat_cursor = true;
    let after_residual = committed_continuation(
        machine
            .apply(start, TopNActionResult::Diffed(residual), budget())
            .unwrap(),
    );
    assert!(matches!(
        after_residual.phase,
        TopNPhase::Diff {
            cursor: TopNDiffCursor {
                row_id: Some(41),
                repeat: true,
            },
            ..
        }
    ));
    let after_final_slice = committed_continuation(
        machine
            .apply(
                after_residual,
                TopNActionResult::Diffed(diff_page(Some(41), false, Some(53))),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        after_final_slice.phase,
        TopNPhase::Diff {
            cursor: TopNDiffCursor {
                row_id: Some(41),
                repeat: false,
            },
            ..
        }
    ));
}

#[test]
fn diff_repeat_cursor_roundtrips_through_the_typed_continuation() {
    let continuation = TopNContinuation {
        input_stream_id: 3,
        input: None,
        phase: TopNPhase::Diff {
            generation_id: 17,
            leg: DiffLeg::Add,
            cursor: TopNDiffCursor {
                row_id: Some(41),
                repeat: true,
            },
            after_drain: AfterDrain::Frontier(frontier_position()),
        },
    };
    let fields = encode_topn_fields(continuation);
    assert!(fields.cursor_repeat);
    assert_eq!(decode_topn_fields(fields).unwrap(), continuation);
}

#[test]
fn zero_limit_skips_candidate_selection() {
    let machine = TopNMachine::new(0, u64::MAX, true);
    let continuation = select_continuation(machine);
    assert!(matches!(
        continuation.phase,
        TopNPhase::Diff {
            leg: DiffLeg::Remove,
            ..
        }
    ));
}

#[test]
fn selection_reuses_the_bounded_terminal_row_for_has_more() {
    let production = include_str!("runtime.rs");
    let selection = production
        .split_once("fn run_topn_selection(")
        .expect("TopN must have a selection primitive")
        .1
        .split_once("fn run_topn_diff(")
        .expect("TopN selection must end before diff")
        .0;
    assert!(selection.contains(
        "SELECT page.*\n          FROM bounded AS page\n          ORDER BY page.page_ordinal DESC\n          LIMIT 1"
    ));
    assert!(!selection.contains("JOIN {state} AS input_row USING(entry_id)"));
}

#[test]
fn selection_rejects_counter_and_tie_boundary_regressions() {
    let machine = TopNMachine::new(2, 3, true);
    let continuation = select_continuation(machine);
    let invalid = TopNSelection {
        page: internal_page(Some(21), false),
        progress: SelectionProgress {
            cursor: TopNCursor { row_id: Some(21) },
            offset_remaining: 4,
            limit_remaining: 2,
            tie_boundary_row_id: None,
        },
    };
    assert!(machine
        .apply(continuation, TopNActionResult::Selected(invalid), budget(),)
        .is_err());
}

#[test]
fn one_oversized_effect_row_is_allowed_but_two_are_not() {
    let machine = TopNMachine::new(5, 0, false);
    let durable = TopNContinuation {
        input_stream_id: 3,
        input: None,
        phase: TopNPhase::Diff {
            generation_id: 17,
            leg: DiffLeg::Add,
            cursor: TopNDiffCursor::default(),
            after_drain: AfterDrain::FinishInput,
        },
    };
    let oversized = TopNDiffPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: 1,
                input_bytes: 21,
                output_rows: 1,
                output_bytes: 13,
            },
            state_rows: 1,
            output: OutputFacts::Data { chunk_seq: 61 },
        },
        last_row_id: Some(1),
        complete: false,
        repeat_cursor: false,
    };
    machine
        .apply(durable, TopNActionResult::Diffed(oversized), budget())
        .unwrap();

    let two_rows = TopNDiffPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: 2,
                input_bytes: 21,
                ..WorkUsage::default()
            },
            ..PrimitiveFacts::default()
        },
        last_row_id: Some(2),
        complete: false,
        repeat_cursor: false,
    };
    assert!(machine
        .apply(durable, TopNActionResult::Diffed(two_rows), budget())
        .is_err());
}

#[test]
fn admission_can_commit_without_starting_a_drain() {
    let machine = TopNMachine::new(5, 0, false);
    let transition = machine
        .apply(
            TopNContinuation {
                input_stream_id: 3,
                input: Some(position(0)),
                phase: TopNPhase::Admit,
            },
            TopNActionResult::Admitted(TopNAdmission {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        input_rows: 1,
                        input_bytes: 8,
                        ..WorkUsage::default()
                    },
                    state_rows: 2,
                    output: OutputFacts::None,
                },
                target: TopNAdmissionTarget::Idle,
            }),
            budget(),
        )
        .unwrap();
    assert!(matches!(
        transition,
        TopNTransition::Committed {
            continuation: None,
            ..
        }
    ));
}

#[test]
fn drain_continuation_does_not_retain_consumed_chunk() {
    let machine = TopNMachine::new(5, 0, false);
    let continuation = committed_continuation(
        machine
            .apply(
                TopNContinuation {
                    input_stream_id: 3,
                    input: Some(position(1)),
                    phase: TopNPhase::Admit,
                },
                TopNActionResult::Admitted(TopNAdmission {
                    facts: PrimitiveFacts {
                        usage: WorkUsage {
                            input_rows: 1,
                            input_bytes: 8,
                            ..WorkUsage::default()
                        },
                        state_rows: 2,
                        output: OutputFacts::None,
                    },
                    target: TopNAdmissionTarget::Drain {
                        generation_id: 17,
                        after_drain: AfterDrain::FinishInput,
                    },
                }),
                budget(),
            )
            .unwrap(),
    );
    assert_eq!(continuation.input_stream_id, 3);
    assert_eq!(continuation.input, None);
    assert!(matches!(continuation.phase, TopNPhase::Select { .. }));
}

#[test]
fn partial_chunk_drain_keeps_only_its_resume_target() {
    let machine = TopNMachine::new(5, 0, false);
    let continuation = committed_continuation(
        machine
            .apply(
                TopNContinuation {
                    input_stream_id: 3,
                    input: Some(position(0)),
                    phase: TopNPhase::Admit,
                },
                TopNActionResult::Admitted(TopNAdmission {
                    facts: PrimitiveFacts {
                        usage: WorkUsage {
                            input_rows: 2,
                            input_bytes: 16,
                            ..WorkUsage::default()
                        },
                        state_rows: 3,
                        output: OutputFacts::None,
                    },
                    target: TopNAdmissionTarget::Drain {
                        generation_id: 17,
                        after_drain: AfterDrain::Admit(position(2)),
                    },
                }),
                budget(),
            )
            .unwrap(),
    );
    assert_eq!(continuation.input, None);
    assert!(matches!(
        continuation.phase,
        TopNPhase::Select {
            after_drain: AfterDrain::Admit(input),
            ..
        } if input == position(2)
    ));
}
