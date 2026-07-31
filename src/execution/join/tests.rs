use super::*;

fn position(stream_id: i64, chunk_seq: i64, row_ordinal: i64) -> InputPosition {
    InputPosition::new(stream_id, chunk_seq, row_ordinal).unwrap()
}

fn positions() -> InputPositions {
    InputPositions::new(position(11, 3, 1), position(12, 4, 2)).unwrap()
}

fn event(side: InputSide, weight: i64) -> InputEventFacts {
    InputEventFacts::new(side, positions(), weight, 7).unwrap()
}

fn start(event: InputEventFacts) -> JoinContinuation {
    JoinContinuation::start_input(event, OwnExpectation::absent()).unwrap()
}

fn budget(input_rows: usize, output_rows: usize, output_bytes: usize) -> WorkBudget {
    WorkBudget::new(input_rows, 1_000, output_rows, output_bytes)
}

#[test]
fn generic_theta_state_counts_share_one_filtered_scan() {
    let production = include_str!("runtime.rs");
    let own_state = production
        .split_once("fn apply_inner_page_own_state(")
        .expect("Join must apply its own state")
        .1
        .split_once("fn load_continuation(")
        .expect("Join own-state SQL must end before continuation loading")
        .0;
    assert!(own_state.contains("let counts_join = if layout.keyed()"));
    assert!(own_state.contains("FILTER (WHERE ({condition}) IS TRUE)"));
    assert!(own_state.contains("FILTER (WHERE ({condition}) IS NULL)"));
    assert!(own_state.contains("FROM {opposite_state} AS {opposite_alias}"));
    assert!(own_state.contains("counts.match_count"));
    assert!(own_state.contains("counts.unknown_count"));
}

fn candidate(
    row_id: i64,
    multiplicity: u64,
    truth: MatchTruth,
    matched: u64,
    unknown: u64,
    pair_bytes: u64,
    candidate_only_bytes: u64,
) -> CandidateProbe {
    CandidateProbe::new(
        row_id,
        multiplicity,
        truth,
        MatchCounts::new(matched, unknown).unwrap(),
        5,
        ProjectionBytes::new(Some(pair_bytes), Some(candidate_only_bytes)).unwrap(),
    )
    .unwrap()
}

fn data_facts(plan: &ActionPlan, state_rows: u64) -> PrimitiveFacts {
    PrimitiveFacts {
        usage: plan.usage(),
        state_rows,
        output: if plan.actions().is_empty() {
            super::super::OutputFacts::None
        } else {
            super::super::OutputFacts::Data { chunk_seq: 9 }
        },
    }
}

#[test]
fn phase_codes_are_exact_and_have_no_idle_or_legacy_decoder() {
    for phase in [
        JoinPhase::Preflight,
        JoinPhase::Probe,
        JoinPhase::PendingTransition,
        JoinPhase::Finalize,
        JoinPhase::Frontier,
    ] {
        assert_eq!(JoinPhase::from_code(phase.code()).unwrap(), phase);
    }
    assert!(PhaseCode::active(0).is_err());
    assert!(JoinPhase::from_code(PhaseCode::active(6).unwrap()).is_err());
    assert_eq!(
        InputSide::from_code(InputSide::Left.code()).unwrap(),
        InputSide::Left
    );
    assert_eq!(
        MatchTruth::from_code(MatchTruth::Unknown.code()).unwrap(),
        MatchTruth::Unknown
    );
    assert!(InputSide::from_code(2).is_err());
    assert!(MatchTruth::from_code(2).is_err());
}

#[test]
fn continuation_accepts_zero_based_input_row_positions() {
    let zero_based = InputPositions::new(position(11, 3, 0), position(12, 4, 0)).unwrap();
    start(InputEventFacts::new(InputSide::Left, zero_based, 1, 1).unwrap());
}

#[test]
fn pending_restore_rejects_a_forged_non_pair_transition() {
    let event = event(InputSide::Right, 1);
    let progress = InputProgress::restore(
        event.positions,
        event.side,
        event.weight,
        event.row_bytes,
        OwnExpectation::absent(),
        None,
        MatchCounts::default(),
    )
    .unwrap();
    let forged = CandidateExpectation::new(
        8,
        2,
        MatchTruth::Unknown,
        MatchCounts::default(),
        MatchCounts::new(0, 1).unwrap(),
    )
    .unwrap();
    assert!(JoinContinuation::restore_input(
        JoinPhase::PendingTransition.code(),
        progress,
        Some(forged)
    )
    .is_err());
}

#[test]
fn continuation_rejects_changed_input_coordinates_and_event_facts() {
    let original = event(InputSide::Left, 2);
    let continuation = start(original);
    continuation.validate_input_resume(original).unwrap();

    let moved = InputEventFacts::new(
        InputSide::Left,
        InputPositions::new(position(11, 3, 2), position(12, 4, 2)).unwrap(),
        2,
        7,
    )
    .unwrap();
    assert!(continuation.validate_input_resume(moved).is_err());
    assert!(continuation
        .validate_input_resume(InputEventFacts::new(InputSide::Left, positions(), 3, 7).unwrap())
        .is_err());
    assert!(continuation
        .validate_input_resume(InputEventFacts::new(InputSide::Left, positions(), 2, 8).unwrap())
        .is_err());
}

#[test]
fn inner_fanout_advances_across_multiple_bounded_pages() {
    let input = event(InputSide::Left, 2);
    let mut continuation = start(input);
    let all = [
        candidate(1, 3, MatchTruth::True, 0, 0, 4, 4),
        candidate(2, 5, MatchTruth::False, 0, 0, 4, 4),
        candidate(3, 7, MatchTruth::True, 1, 0, 4, 4),
        candidate(4, 11, MatchTruth::True, 2, 0, 4, 4),
    ];

    let page_one = ProbePage::new(all[..2].to_vec(), false).unwrap();
    let plan_one = plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &page_one,
        budget(2, 1, 100),
    )
    .unwrap();
    assert_eq!(plan_one.actions().len(), 1);
    assert_eq!(plan_one.actions()[0].weight, 6);
    assert_eq!(plan_one.candidate_changes().len(), 1);
    plan_one.validate_commit(data_facts(&plan_one, 1)).unwrap();
    continuation = plan_one.next_continuation().clone();
    let progress = continuation.input_progress().unwrap();
    assert_eq!(progress.candidate_after(), Some(2));
    assert_eq!(progress.opposite_counts().matched, 3);

    let page_two = ProbePage::new(all[2..].to_vec(), true).unwrap();
    let plan_two = plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &page_two,
        budget(2, 1, 100),
    )
    .unwrap();
    assert_eq!(plan_two.actions().len(), 1);
    assert_eq!(plan_two.actions()[0].weight, 14);
    assert_eq!(
        plan_two
            .next_continuation()
            .input_progress()
            .unwrap()
            .candidate_after(),
        Some(3)
    );
    assert_eq!(plan_two.next_continuation().phase(), JoinPhase::Probe);
    continuation = plan_two.next_continuation().clone();

    let last_page = ProbePage::new(all[3..].to_vec(), true).unwrap();
    let plan_three = plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &last_page,
        budget(1, 1, 100),
    )
    .unwrap();
    assert_eq!(plan_three.actions()[0].weight, 22);
    assert_eq!(plan_three.next_continuation().phase(), JoinPhase::Finalize);
    assert_eq!(
        plan_three
            .next_continuation()
            .input_progress()
            .unwrap()
            .opposite_counts()
            .matched,
        21
    );
}

#[test]
fn pair_and_outer_transition_split_without_applying_old_state_twice() {
    let input = event(InputSide::Right, 1);
    let continuation = start(input);
    let first_probe =
        ProbePage::new(vec![candidate(8, 4, MatchTruth::True, 0, 0, 3, 5)], true).unwrap();
    let pair = plan_actions(
        JoinMode::Left,
        &continuation,
        input,
        &first_probe,
        budget(1, 1, 100),
    )
    .unwrap();

    assert_eq!(
        pair.actions()
            .iter()
            .map(|action| action.kind)
            .collect::<Vec<_>>(),
        vec![OutputActionKind::Pair]
    );
    assert!(pair.candidate_changes().is_empty());
    assert_eq!(
        pair.next_continuation().phase(),
        JoinPhase::PendingTransition
    );
    pair.validate_commit(data_facts(&pair, 0)).unwrap();
    let pending = pair.next_continuation().clone();

    let stale_after_early_apply =
        ProbePage::new(vec![candidate(8, 4, MatchTruth::True, 1, 0, 3, 5)], true).unwrap();
    assert!(plan_actions(
        JoinMode::Left,
        &pending,
        input,
        &stale_after_early_apply,
        budget(1, 1, 100)
    )
    .is_err());

    let transition = plan_actions(
        JoinMode::Left,
        &pending,
        input,
        &first_probe,
        budget(1, 1, 100),
    )
    .unwrap();
    assert_eq!(transition.actions().len(), 1);
    assert_eq!(
        transition.actions()[0].kind,
        OutputActionKind::CandidateEligibility
    );
    assert_eq!(transition.actions()[0].weight, -4);
    assert_eq!(transition.candidate_changes().len(), 1);
    assert_eq!(transition.next_continuation().phase(), JoinPhase::Finalize);
    assert_eq!(
        transition.candidate_changes()[0]
            .expected
            .old_counts
            .matched,
        0
    );
    assert_eq!(
        transition.candidate_changes()[0]
            .expected
            .new_counts
            .matched,
        1
    );
    transition
        .validate_commit(data_facts(&transition, 1))
        .unwrap();

    let committed_state =
        ProbePage::new(vec![candidate(8, 4, MatchTruth::True, 1, 0, 3, 5)], true).unwrap();
    assert!(plan_actions(
        JoinMode::Left,
        &pending,
        input,
        &committed_state,
        budget(1, 1, 100)
    )
    .is_err());
}

#[test]
fn output_byte_budget_selects_one_prefix_and_allows_one_oversized_row() {
    let input = event(InputSide::Left, 1);
    let continuation = start(input);
    let page = ProbePage::new(
        vec![
            candidate(1, 1, MatchTruth::True, 0, 0, 12, 2),
            candidate(2, 1, MatchTruth::True, 0, 0, 4, 2),
        ],
        true,
    )
    .unwrap();
    let plan = plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &page,
        budget(2, 5, 10),
    )
    .unwrap();
    assert_eq!(plan.actions().len(), 1);
    assert_eq!(plan.usage().output_bytes, 12);
    assert_eq!(
        plan.next_continuation()
            .input_progress()
            .unwrap()
            .candidate_after(),
        Some(1)
    );
}

#[test]
fn probe_page_itself_must_obey_the_input_budget() {
    let input = event(InputSide::Left, 1);
    let continuation = start(input);
    let page = ProbePage::new(
        vec![
            candidate(1, 1, MatchTruth::False, 0, 0, 1, 1),
            candidate(2, 1, MatchTruth::False, 0, 0, 1, 1),
        ],
        false,
    )
    .unwrap();
    assert!(plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &page,
        budget(1, 1, 100)
    )
    .is_err());

    let oversized = CandidateProbe::new(
        1,
        1,
        MatchTruth::False,
        MatchCounts::default(),
        2_000,
        ProjectionBytes::new(None, None).unwrap(),
    )
    .unwrap();
    plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &ProbePage::new(vec![oversized], true).unwrap(),
        WorkBudget::new(1, 10, 1, 10),
    )
    .unwrap();
}

#[test]
fn probe_only_requires_projection_bytes_for_actions_it_selects() {
    let input = event(InputSide::Left, 1);
    let continuation = start(input);
    let no_output = CandidateProbe::new(
        1,
        1,
        MatchTruth::False,
        MatchCounts::default(),
        3,
        ProjectionBytes::new(None, None).unwrap(),
    )
    .unwrap();
    plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &ProbePage::new(vec![no_output], true).unwrap(),
        budget(1, 1, 100),
    )
    .unwrap();

    let missing_pair = CandidateProbe::new(
        1,
        1,
        MatchTruth::True,
        MatchCounts::default(),
        3,
        ProjectionBytes::new(None, None).unwrap(),
    )
    .unwrap();
    assert!(plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &ProbePage::new(vec![missing_pair], true).unwrap(),
        budget(1, 1, 100)
    )
    .is_err());
}

#[test]
fn action_budget_can_continue_through_candidates_without_output() {
    let input = event(InputSide::Left, 1);
    let continuation = start(input);
    let page = ProbePage::new(
        vec![
            candidate(1, 2, MatchTruth::True, 0, 0, 3, 3),
            candidate(2, 3, MatchTruth::False, 0, 0, 3, 3),
            candidate(3, 5, MatchTruth::False, 0, 0, 3, 3),
            candidate(4, 7, MatchTruth::True, 0, 0, 3, 3),
        ],
        true,
    )
    .unwrap();
    let plan = plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &page,
        budget(4, 1, 100),
    )
    .unwrap();
    assert_eq!(plan.actions().len(), 1);
    assert_eq!(
        plan.next_continuation()
            .input_progress()
            .unwrap()
            .candidate_after(),
        Some(3)
    );
}

#[test]
fn right_and_full_outer_finalize_only_the_preserved_current_side() {
    let right = event(InputSide::Right, 2);
    let right_scan = start(right);
    let empty = ProbePage::new(Vec::new(), true).unwrap();
    let right_ready = plan_actions(
        JoinMode::Right,
        &right_scan,
        right,
        &empty,
        budget(1, 1, 100),
    )
    .unwrap()
    .next_continuation()
    .clone();
    let own = OwnStateProbe::absent(9).unwrap();
    let right_plan =
        plan_finalize(JoinMode::Right, &right_ready, right, own, budget(1, 1, 100)).unwrap();
    assert_eq!(right_plan.output().unwrap().weight, 2);
    right_plan
        .validate_commit(
            PrimitiveFacts {
                usage: right_plan.usage(),
                state_rows: 1,
                output: super::super::OutputFacts::Data { chunk_seq: 10 },
            },
            false,
        )
        .unwrap();

    let left_mode =
        plan_finalize(JoinMode::Left, &right_ready, right, own, budget(1, 1, 100)).unwrap();
    assert!(left_mode.output().is_none());

    let full = plan_finalize(JoinMode::Full, &right_ready, right, own, budget(1, 1, 100)).unwrap();
    assert!(full.output().is_some());
}

#[test]
fn semi_anti_and_null_aware_anti_have_distinct_final_eligibility() {
    let left = event(InputSide::Left, 1);
    let scan = start(left);
    let page =
        ProbePage::new(vec![candidate(1, 2, MatchTruth::Unknown, 0, 0, 3, 3)], true).unwrap();

    let naa_ready = plan_actions(
        JoinMode::NullAwareAnti,
        &scan,
        left,
        &page,
        budget(1, 1, 100),
    )
    .unwrap()
    .next_continuation()
    .clone();
    assert_eq!(
        naa_ready
            .input_progress()
            .unwrap()
            .opposite_counts()
            .unknown,
        2
    );
    assert!(plan_finalize(
        JoinMode::NullAwareAnti,
        &naa_ready,
        left,
        OwnStateProbe::absent(4).unwrap(),
        budget(1, 1, 100)
    )
    .unwrap()
    .output()
    .is_none());

    let anti_ready = plan_actions(JoinMode::Anti, &scan, left, &page, budget(1, 1, 100))
        .unwrap()
        .next_continuation()
        .clone();
    assert!(plan_finalize(
        JoinMode::Anti,
        &anti_ready,
        left,
        OwnStateProbe::absent(4).unwrap(),
        budget(1, 1, 100)
    )
    .unwrap()
    .output()
    .is_some());

    let matched_page =
        ProbePage::new(vec![candidate(2, 3, MatchTruth::True, 0, 0, 3, 3)], true).unwrap();
    let semi_ready = plan_actions(
        JoinMode::Semi,
        &scan,
        left,
        &matched_page,
        budget(1, 1, 100),
    )
    .unwrap()
    .next_continuation()
    .clone();
    assert!(plan_finalize(
        JoinMode::Semi,
        &semi_ready,
        left,
        OwnStateProbe::absent(4).unwrap(),
        budget(1, 1, 100)
    )
    .unwrap()
    .output()
    .is_some());
}

#[test]
fn right_input_drives_left_semi_and_anti_zero_crossings() {
    let right_insert = event(InputSide::Right, 1);
    let continuation = start(right_insert);
    let zero_to_one =
        ProbePage::new(vec![candidate(4, 6, MatchTruth::True, 0, 0, 3, 8)], true).unwrap();

    let semi = plan_actions(
        JoinMode::Semi,
        &continuation,
        right_insert,
        &zero_to_one,
        budget(1, 1, 100),
    )
    .unwrap();
    assert_eq!(semi.actions().len(), 1);
    assert_eq!(semi.actions()[0].weight, 6);

    let anti = plan_actions(
        JoinMode::Anti,
        &continuation,
        right_insert,
        &zero_to_one,
        budget(1, 1, 100),
    )
    .unwrap();
    assert_eq!(anti.actions().len(), 1);
    assert_eq!(anti.actions()[0].weight, -6);
}

#[test]
fn delete_underflow_and_weight_overflow_are_rejected_before_a_plan() {
    let deletion = event(InputSide::Left, -2);
    let continuation = JoinContinuation::start_input(
        deletion,
        OwnExpectation::present(9, 2, MatchCounts::default()).unwrap(),
    )
    .unwrap();
    let underflow =
        ProbePage::new(vec![candidate(1, 1, MatchTruth::True, 1, 0, 3, 3)], true).unwrap();
    assert!(plan_actions(
        JoinMode::Inner,
        &continuation,
        deletion,
        &underflow,
        budget(1, 1, 100)
    )
    .is_err());

    let huge = event(InputSide::Left, i64::MAX);
    let continuation = start(huge);
    let overflow =
        ProbePage::new(vec![candidate(1, 2, MatchTruth::True, 0, 0, 3, 3)], true).unwrap();
    assert!(plan_actions(
        JoinMode::Inner,
        &continuation,
        huge,
        &overflow,
        budget(1, 1, 100)
    )
    .is_err());
}

#[test]
fn finalize_requires_exact_own_counts_and_prevents_multiplicity_underflow() {
    let deletion = event(InputSide::Left, -2);
    assert!(JoinContinuation::start_input(deletion, OwnExpectation::absent()).is_err());
    assert!(JoinContinuation::start_input(
        deletion,
        OwnExpectation::present(9, 1, MatchCounts::default()).unwrap()
    )
    .is_err());
    let scan = JoinContinuation::start_input(
        deletion,
        OwnExpectation::present(9, 2, MatchCounts::default()).unwrap(),
    )
    .unwrap();
    let ready = plan_actions(
        JoinMode::Inner,
        &scan,
        deletion,
        &ProbePage::new(Vec::new(), true).unwrap(),
        budget(1, 1, 100),
    )
    .unwrap()
    .next_continuation()
    .clone();

    assert!(plan_finalize(
        JoinMode::Inner,
        &ready,
        deletion,
        OwnStateProbe::absent(3).unwrap(),
        budget(1, 1, 100)
    )
    .is_err());
    assert!(plan_finalize(
        JoinMode::Inner,
        &ready,
        deletion,
        OwnStateProbe::present(9, 1, MatchCounts::default(), 3).unwrap(),
        budget(1, 1, 100)
    )
    .is_err());
    let valid = plan_finalize(
        JoinMode::Inner,
        &ready,
        deletion,
        OwnStateProbe::present(9, 2, MatchCounts::default(), 3).unwrap(),
        budget(1, 1, 100),
    )
    .unwrap();
    assert_eq!(valid.own_change().kind, OwnStateChangeKind::Delete);
}

#[test]
fn primitive_facts_must_match_the_exact_planned_prefix() {
    let input = event(InputSide::Left, 1);
    let continuation = start(input);
    let page = ProbePage::new(vec![candidate(1, 2, MatchTruth::True, 0, 0, 3, 3)], true).unwrap();
    let plan = plan_actions(
        JoinMode::Inner,
        &continuation,
        input,
        &page,
        budget(1, 1, 100),
    )
    .unwrap();
    let mut facts = data_facts(&plan, 1);
    plan.validate_commit(facts).unwrap();
    facts.state_rows = 0;
    assert!(plan.validate_commit(facts).is_err());
    facts = data_facts(&plan, 1);
    facts.usage.output_bytes += 1;
    assert!(plan.validate_commit(facts).is_err());
}

#[test]
fn frontier_never_passes_an_input_or_pending_continuation() {
    let input = event(InputSide::Right, 1);
    let active = start(input);
    let frontier_facts = FrontierInputFacts::new(InputSide::Left, positions(), 30).unwrap();
    let state = FrontierState {
        consumed: InputFrontiers {
            left: 10,
            right: 20,
        },
        published: 10,
        latest_output_data: Some(18),
    };
    assert!(plan_frontier(&active, frontier_facts, state, budget(1, 1, 100)).is_err());

    let page = ProbePage::new(vec![candidate(5, 2, MatchTruth::True, 0, 0, 3, 3)], true).unwrap();
    let pending = plan_actions(JoinMode::Left, &active, input, &page, budget(1, 1, 100))
        .unwrap()
        .next_continuation()
        .clone();
    assert_eq!(pending.phase(), JoinPhase::PendingTransition);
    assert!(plan_frontier(&pending, frontier_facts, state, budget(1, 1, 100)).is_err());
}

#[test]
fn frontier_uses_both_inputs_and_waits_for_output_data() {
    let facts = FrontierInputFacts::new(InputSide::Left, positions(), 30).unwrap();
    let continuation = JoinContinuation::start_frontier(facts).unwrap();
    let state = FrontierState {
        consumed: InputFrontiers {
            left: 10,
            right: 20,
        },
        published: 10,
        latest_output_data: Some(18),
    };
    let plan = plan_frontier(&continuation, facts, state, budget(1, 1, 100)).unwrap();
    assert_eq!(plan.new.left, 30);
    assert_eq!(plan.new.right, 20);
    assert_eq!(plan.publish, Some(20));

    let data_ahead = FrontierState {
        latest_output_data: Some(25),
        ..state
    };
    let waiting = plan_frontier(&continuation, facts, data_ahead, budget(1, 1, 100)).unwrap();
    assert_eq!(waiting.publish, None);
}

#[test]
fn frontier_commit_requires_a_frontier_chunk_only_when_planned() {
    let facts = FrontierInputFacts::new(InputSide::Left, positions(), 30).unwrap();
    let continuation = JoinContinuation::start_frontier(facts).unwrap();
    let state = FrontierState {
        consumed: InputFrontiers {
            left: 10,
            right: 20,
        },
        published: 10,
        latest_output_data: None,
    };
    let plan = plan_frontier(&continuation, facts, state, budget(1, 1, 100)).unwrap();
    plan.validate_commit(PrimitiveFacts {
        output: super::super::OutputFacts::Frontier { chunk_seq: 4 },
        ..PrimitiveFacts::default()
    })
    .unwrap();
    assert!(plan.validate_commit(PrimitiveFacts::default()).is_err());
}
