use super::*;
use crate::execution::WorkUsage;

fn budget() -> WorkBudget {
    WorkBudget::new(2, 20, 1, 12)
}

fn position(row: i64) -> InputPosition {
    InputPosition::new(5, 7, row).unwrap()
}

fn frontier_position() -> InputPosition {
    InputPosition::new(5, 8, 0).unwrap()
}

fn internal_page(last_row_id: Option<i64>, complete: bool) -> WindowPage {
    WindowPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: u64::from(last_row_id.is_some()),
                input_bytes: u64::from(last_row_id.is_some()) * 8,
                ..WorkUsage::default()
            },
            state_rows: u64::from(last_row_id.is_some()),
            continuation_rows: 1,
            output: OutputFacts::None,
        },
        last_row_id,
        complete,
    }
}

fn fold_page(
    next_cursor: Option<WindowFoldCursor>,
    input_rows: u64,
    work_items: usize,
) -> WindowFoldPage {
    WindowFoldPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows,
                input_bytes: input_rows * 8,
                ..WorkUsage::default()
            },
            state_rows: 1,
            continuation_rows: 1,
            output: OutputFacts::None,
        },
        next_cursor,
        work_items,
    }
}

fn native_machine() -> WindowMachine {
    WindowMachine::new(vec![WindowFunctionKind::Native]).unwrap()
}

fn diff_page(last_row_id: Option<i64>, complete: bool, chunk_seq: Option<i64>) -> WindowDiffPage {
    let output_rows = u64::from(chunk_seq.is_some());
    WindowDiffPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: u64::from(last_row_id.is_some()),
                input_bytes: u64::from(last_row_id.is_some()) * 8,
                output_rows,
                output_bytes: output_rows * 9,
            },
            state_rows: output_rows,
            continuation_rows: 1,
            output: chunk_seq.map_or(OutputFacts::None, |chunk_seq| OutputFacts::Data {
                chunk_seq,
            }),
        },
        last_row_id,
        complete,
        repeat_cursor: false,
    }
}

fn committed_continuation(transition: WindowTransition) -> WindowContinuation {
    let WindowTransition::Committed {
        continuation: Some(continuation),
        ..
    } = transition
    else {
        panic!("step should have a continuation");
    };
    continuation
}

#[test]
fn strict_phase_codes_have_no_idle_or_unknown_decoder() {
    assert_eq!(
        WindowPhaseKind::from_code(WindowPhaseKind::Frames.code()).unwrap(),
        WindowPhaseKind::Frames
    );
    assert!(PhaseCode::active(0).is_err());
    assert!(WindowPhaseKind::from_code(PhaseCode::active(99).unwrap()).is_err());
}

#[test]
fn admission_can_idle_with_a_durable_dirty_queue() {
    let machine = native_machine();
    let continuation = WindowContinuation {
        input_stream_id: 5,
        input: Some(position(0)),
        phase: WindowPhase::Admit,
    };
    let transition = machine
        .apply(
            continuation,
            WindowActionResult::Admitted(WindowAdmission {
                facts: PrimitiveFacts {
                    usage: WorkUsage {
                        input_rows: 2,
                        input_bytes: 16,
                        ..WorkUsage::default()
                    },
                    state_rows: 3,
                    continuation_rows: 0,
                    output: OutputFacts::None,
                },
                target: WindowAdmissionTarget::Idle,
            }),
            budget(),
        )
        .unwrap();
    assert!(matches!(
        transition,
        WindowTransition::Committed {
            continuation: None,
            ..
        }
    ));
}

#[test]
fn window_resumes_every_bounded_build_phase() {
    let machine = WindowMachine::new(vec![
        WindowFunctionKind::Aggregate,
        WindowFunctionKind::Native,
    ])
    .unwrap();
    let mut continuation = WindowContinuation {
        input_stream_id: 5,
        input: Some(position(1)),
        phase: WindowPhase::Admit,
    };
    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::Admitted(WindowAdmission {
                    facts: PrimitiveFacts {
                        usage: WorkUsage {
                            input_rows: 1,
                            input_bytes: 8,
                            ..WorkUsage::default()
                        },
                        state_rows: 1,
                        continuation_rows: 1,
                        output: OutputFacts::None,
                    },
                    target: WindowAdmissionTarget::Drain {
                        first_partition_queue_id: 11,
                        after_partitions: AfterPartitions::Frontier(frontier_position()),
                    },
                }),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        machine.action(continuation).unwrap(),
        WindowAction::Enumerate { cursor, .. } if cursor.row_id.is_none()
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::Enumerated(internal_page(Some(21), false)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        continuation.phase,
        WindowPhase::Enumerate {
            cursor: WindowCursor { row_id: Some(21) },
            ..
        }
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::Enumerated(internal_page(None, true)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(continuation.phase, WindowPhase::Peers { .. }));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::PeersBuilt(internal_page(None, true)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(continuation.phase, WindowPhase::Frames { .. }));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::FramesBuilt(internal_page(None, true)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        continuation.phase,
        WindowPhase::FoldAggregate {
            function_ordinal: 1,
            cursor: WindowFoldCursor {
                output_ordinal: 1,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            },
            ..
        }
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::AggregateFolded(fold_page(
                    Some(WindowFoldCursor {
                        output_ordinal: 1,
                        last_frame_ordinal: Some(29),
                        ready_to_finalize: false,
                    }),
                    1,
                    1,
                )),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        continuation.phase,
        WindowPhase::FoldAggregate {
            function_ordinal: 1,
            cursor: WindowFoldCursor {
                output_ordinal: 1,
                last_frame_ordinal: Some(29),
                ready_to_finalize: false,
            },
            ..
        }
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::AggregateFolded(fold_page(
                    Some(WindowFoldCursor {
                        output_ordinal: 2,
                        last_frame_ordinal: None,
                        ready_to_finalize: false,
                    }),
                    1,
                    1,
                )),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        continuation.phase,
        WindowPhase::FoldAggregate {
            function_ordinal: 1,
            cursor: WindowFoldCursor {
                output_ordinal: 2,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            },
            ..
        }
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::AggregateFolded(fold_page(None, 0, 1)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        continuation.phase,
        WindowPhase::Evaluate {
            function_ordinal: 2,
            ..
        }
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::Evaluated(internal_page(None, true)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        continuation.phase,
        WindowPhase::Diff {
            leg: DiffLeg::Remove,
            ..
        }
    ));
}

#[test]
fn aggregate_fold_commits_multiple_output_ordinals_per_step() {
    let machine = WindowMachine::new(vec![WindowFunctionKind::Aggregate]).unwrap();
    let start = WindowContinuation {
        input_stream_id: 5,
        input: None,
        phase: WindowPhase::FoldAggregate {
            partition_queue_id: 11,
            function_ordinal: 1,
            cursor: WindowFoldCursor {
                output_ordinal: 1,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            },
            after_partitions: AfterPartitions::FinishInput,
        },
    };
    let next = committed_continuation(
        machine
            .apply(
                start,
                WindowActionResult::AggregateFolded(fold_page(
                    Some(WindowFoldCursor {
                        output_ordinal: 3,
                        last_frame_ordinal: None,
                        ready_to_finalize: false,
                    }),
                    2,
                    2,
                )),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        next.phase,
        WindowPhase::FoldAggregate {
            cursor: WindowFoldCursor {
                output_ordinal: 3,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            },
            ..
        }
    ));
}

#[test]
fn aggregate_fold_work_items_bound_empty_frames() {
    let machine = WindowMachine::new(vec![WindowFunctionKind::Aggregate]).unwrap();
    let start = WindowContinuation {
        input_stream_id: 5,
        input: None,
        phase: WindowPhase::FoldAggregate {
            partition_queue_id: 11,
            function_ordinal: 1,
            cursor: WindowFoldCursor {
                output_ordinal: 1,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            },
            after_partitions: AfterPartitions::FinishInput,
        },
    };
    let page = fold_page(
        Some(WindowFoldCursor {
            output_ordinal: i64::try_from(WINDOW_FOLD_WORK_ITEM_CAP).unwrap() + 1,
            last_frame_ordinal: None,
            ready_to_finalize: false,
        }),
        0,
        WINDOW_FOLD_WORK_ITEM_CAP,
    );
    let next = committed_continuation(
        machine
            .apply(start, WindowActionResult::AggregateFolded(page), budget())
            .unwrap(),
    );
    assert!(matches!(
        next.phase,
        WindowPhase::FoldAggregate {
            cursor: WindowFoldCursor {
                output_ordinal,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            },
            ..
        } if output_ordinal == i64::try_from(WINDOW_FOLD_WORK_ITEM_CAP).unwrap() + 1
    ));

    let mut beyond_cap = page;
    beyond_cap.work_items += 1;
    assert!(machine
        .apply(
            start,
            WindowActionResult::AggregateFolded(beyond_cap),
            budget(),
        )
        .is_err());

    let mut skipped = page;
    skipped.work_items -= 1;
    assert!(machine
        .apply(
            start,
            WindowActionResult::AggregateFolded(skipped),
            budget()
        )
        .is_err());
}

#[test]
fn aggregate_fold_ready_state_roundtrips_and_resumes_finalization() {
    let machine = WindowMachine::new(vec![WindowFunctionKind::Aggregate]).unwrap();
    let start = WindowContinuation {
        input_stream_id: 5,
        input: None,
        phase: WindowPhase::FoldAggregate {
            partition_queue_id: 11,
            function_ordinal: 1,
            cursor: WindowFoldCursor {
                output_ordinal: 1,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            },
            after_partitions: AfterPartitions::FinishInput,
        },
    };
    let ready = committed_continuation(
        machine
            .apply(
                start,
                WindowActionResult::AggregateFolded(fold_page(
                    Some(WindowFoldCursor {
                        output_ordinal: 1,
                        last_frame_ordinal: None,
                        ready_to_finalize: true,
                    }),
                    0,
                    1,
                )),
                budget(),
            )
            .unwrap(),
    );
    assert_eq!(
        decode_window_fields(encode_window_fields(ready).unwrap()).unwrap(),
        ready
    );

    let next = committed_continuation(
        machine
            .apply(
                ready,
                WindowActionResult::AggregateFolded(fold_page(
                    Some(WindowFoldCursor {
                        output_ordinal: 2,
                        last_frame_ordinal: None,
                        ready_to_finalize: false,
                    }),
                    1,
                    1,
                )),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        next.phase,
        WindowPhase::FoldAggregate {
            cursor: WindowFoldCursor {
                output_ordinal: 2,
                ready_to_finalize: false,
                ..
            },
            ..
        }
    ));
}

#[test]
fn finalize_budget_blocks_then_allows_one_oversized_item() {
    validate_window_finalize_decision(false, 5_000, 1, 100, false).unwrap();
    assert!(validate_window_finalize_decision(true, 5_000, 1, 100, false).is_err());
    assert!(validate_window_finalize_decision(false, 80, 1, 100, false).is_err());
    validate_window_finalize_decision(true, 5_000, 1, 100, true).unwrap();
    assert!(validate_window_finalize_decision(true, 5_000, 0, 100, true).is_err());
}

#[test]
fn missing_window_frame_is_not_an_empty_frame() {
    assert!(validate_window_fold_status("missing_frame").is_err());
    validate_window_fold_status("ok").unwrap();
}

#[test]
fn frontier_waits_for_both_diff_legs_and_cleanup() {
    let machine = native_machine();
    let mut continuation = WindowContinuation {
        input_stream_id: 5,
        input: None,
        phase: WindowPhase::Diff {
            partition_queue_id: 11,
            leg: DiffLeg::Remove,
            cursor: WindowDiffCursor::default(),
            after_partitions: AfterPartitions::Frontier(frontier_position()),
        },
    };

    assert!(machine
        .apply(
            continuation,
            WindowActionResult::FrontierForwarded(PrimitiveFacts {
                output: OutputFacts::Frontier { chunk_seq: 30 },
                ..PrimitiveFacts::default()
            }),
            budget(),
        )
        .is_err());

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::Diffed(diff_page(Some(31), true, Some(40))),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        continuation.phase,
        WindowPhase::Diff {
            leg: DiffLeg::Add,
            ..
        }
    ));

    continuation = committed_continuation(
        machine
            .apply(
                continuation,
                WindowActionResult::Diffed(diff_page(None, true, None)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(continuation.phase, WindowPhase::Cleanup { .. }));

    for relation_ordinal in 0..machine.cleanup_relation_count() {
        assert!(matches!(
            machine.action(continuation).unwrap(),
            WindowAction::Cleanup {
                cursor: WindowCleanupCursor {
                    relation_ordinal: actual,
                    ..
                },
                ..
            } if actual == relation_ordinal
        ));
        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::Cleaned(WindowCleanup {
                        page: internal_page(None, true),
                        next_partition_queue_id: None,
                    }),
                    budget(),
                )
                .unwrap(),
        );
    }
    assert_eq!(continuation.phase, WindowPhase::Frontier);
    assert!(matches!(
        machine.action(continuation).unwrap(),
        WindowAction::ForwardFrontier { .. }
    ));
}

#[test]
fn diff_cursor_advances_on_zero_difference_and_repeats_only_residuals() {
    let machine = native_machine();
    let start = WindowContinuation {
        input_stream_id: 5,
        input: None,
        phase: WindowPhase::Diff {
            partition_queue_id: 11,
            leg: DiffLeg::Remove,
            cursor: WindowDiffCursor::default(),
            after_partitions: AfterPartitions::FinishInput,
        },
    };
    let after_equal_prefix = committed_continuation(
        machine
            .apply(
                start,
                WindowActionResult::Diffed(diff_page(Some(31), false, None)),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        after_equal_prefix.phase,
        WindowPhase::Diff {
            cursor: WindowDiffCursor {
                row_id: Some(31),
                repeat: false,
            },
            ..
        }
    ));
    assert!(machine
        .apply(
            after_equal_prefix,
            WindowActionResult::Diffed(diff_page(Some(31), false, None)),
            budget(),
        )
        .is_err());

    let mut residual = diff_page(Some(41), false, Some(42));
    residual.repeat_cursor = true;
    let after_residual = committed_continuation(
        machine
            .apply(start, WindowActionResult::Diffed(residual), budget())
            .unwrap(),
    );
    assert!(matches!(
        after_residual.phase,
        WindowPhase::Diff {
            cursor: WindowDiffCursor {
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
                WindowActionResult::Diffed(diff_page(Some(41), false, Some(43))),
                budget(),
            )
            .unwrap(),
    );
    assert!(matches!(
        after_final_slice.phase,
        WindowPhase::Diff {
            cursor: WindowDiffCursor {
                row_id: Some(41),
                repeat: false,
            },
            ..
        }
    ));
}

#[test]
fn diff_repeat_cursor_roundtrips_through_the_typed_continuation() {
    let continuation = WindowContinuation {
        input_stream_id: 5,
        input: None,
        phase: WindowPhase::Diff {
            partition_queue_id: 11,
            leg: DiffLeg::Add,
            cursor: WindowDiffCursor {
                row_id: Some(41),
                repeat: true,
            },
            after_partitions: AfterPartitions::Frontier(frontier_position()),
        },
    };
    let fields = encode_window_fields(continuation).unwrap();
    assert!(fields.cursor_repeat);
    assert_eq!(decode_window_fields(fields).unwrap(), continuation);
}

#[test]
fn final_partition_cleanup_can_resume_the_same_input_chunk() {
    let machine = native_machine();
    let transition = machine
        .apply(
            WindowContinuation {
                input_stream_id: 5,
                input: None,
                phase: WindowPhase::Cleanup {
                    partition_queue_id: 11,
                    cursor: WindowCleanupCursor {
                        relation_ordinal: machine.cleanup_relation_count() - 1,
                        row: WindowCursor::default(),
                    },
                    after_partitions: AfterPartitions::Admit(position(2)),
                },
            },
            WindowActionResult::Cleaned(WindowCleanup {
                page: internal_page(None, true),
                next_partition_queue_id: None,
            }),
            budget(),
        )
        .unwrap();
    assert_eq!(
        committed_continuation(transition),
        WindowContinuation {
            input_stream_id: 5,
            input: Some(position(2)),
            phase: WindowPhase::Admit,
        }
    );
}

#[test]
fn one_oversized_row_is_the_only_byte_budget_exception() {
    let machine = native_machine();
    let durable = WindowContinuation {
        input_stream_id: 5,
        input: None,
        phase: WindowPhase::Diff {
            partition_queue_id: 11,
            leg: DiffLeg::Add,
            cursor: WindowDiffCursor::default(),
            after_partitions: AfterPartitions::FinishInput,
        },
    };
    let oversized = WindowDiffPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: 1,
                input_bytes: 21,
                output_rows: 1,
                output_bytes: 13,
            },
            state_rows: 1,
            continuation_rows: 1,
            output: OutputFacts::Data { chunk_seq: 50 },
        },
        last_row_id: Some(1),
        complete: false,
        repeat_cursor: false,
    };
    machine
        .apply(durable, WindowActionResult::Diffed(oversized), budget())
        .unwrap();

    let two_rows = WindowDiffPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: 2,
                input_bytes: 21,
                ..WorkUsage::default()
            },
            continuation_rows: 1,
            ..PrimitiveFacts::default()
        },
        last_row_id: Some(2),
        complete: false,
        repeat_cursor: false,
    };
    assert!(machine
        .apply(durable, WindowActionResult::Diffed(two_rows), budget(),)
        .is_err());
}
