//! Pure control state for a bounded, resumable differential Join.
//!
//! Typed rows never enter this module. PostgreSQL probes and evaluates them,
//! then gives Rust stable row identifiers, counts, truth values, and measured
//! byte sizes. Rust decides one ordered action prefix. The caller must append
//! that prefix, apply the listed compare-and-set mutations, and replace the
//! continuation in the same database transaction.
//!
//! An arrangement identifies its own bag entries by a unique, indexed key.
//! The shared named-composite text roundtrip first removes representation
//! details that do not survive pgoutput while retaining PostgreSQL text
//! semantics such as array lower bounds; `record_send` then produces the key.
//! Candidate evaluation still scans stable `row_id` keysets and applies the
//! arbitrary Join condition.

use crate::execution::{
    InputPosition, KernelCompletion, KernelPhase, OutputFacts, PhaseCode, PrimitiveFacts, WorkUsage,
};
use crate::planner::WorkBudget;

const MAX_COUNT: u64 = i64::MAX as u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinPhase {
    Preflight,
    Probe,
    PendingTransition,
    Finalize,
    Frontier,
}

impl JoinPhase {
    pub(crate) fn code(self) -> PhaseCode {
        let value = match self {
            Self::Preflight => 1,
            Self::Probe => 2,
            Self::PendingTransition => 3,
            Self::Finalize => 4,
            Self::Frontier => 5,
        };
        PhaseCode::active(value).expect("Join phase constants are positive")
    }

    pub(crate) fn from_code(code: PhaseCode) -> Result<Self, String> {
        match code.value() {
            1 => Ok(Self::Preflight),
            2 => Ok(Self::Probe),
            3 => Ok(Self::PendingTransition),
            4 => Ok(Self::Finalize),
            5 => Ok(Self::Frontier),
            0 => Err("idle phase means that no Join continuation exists".into()),
            value => Err(format!("unknown Join phase code {value}")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputSide {
    Left,
    Right,
}

impl InputSide {
    pub(crate) const fn code(self) -> i16 {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }

    pub(crate) fn from_code(code: i16) -> Result<Self, String> {
        match code {
            0 => Ok(Self::Left),
            1 => Ok(Self::Right),
            value => Err(format!("unknown Join input side code {value}")),
        }
    }

    pub(crate) const fn opposite(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MatchTruth {
    False,
    True,
    Unknown,
}

impl MatchTruth {
    pub(crate) const fn code(self) -> i16 {
        match self {
            Self::False => 0,
            Self::True => 1,
            Self::Unknown => -1,
        }
    }

    pub(crate) fn from_code(code: i16) -> Result<Self, String> {
        match code {
            0 => Ok(Self::False),
            1 => Ok(Self::True),
            -1 => Ok(Self::Unknown),
            value => Err(format!("unknown Join match truth code {value}")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinMode {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    NullAwareAnti,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InputPositions {
    pub(crate) left: InputPosition,
    pub(crate) right: InputPosition,
}

impl InputPositions {
    pub(crate) fn new(left: InputPosition, right: InputPosition) -> Result<Self, String> {
        validate_input_position(left)?;
        validate_input_position(right)?;
        Ok(Self { left, right })
    }

    pub(crate) const fn get(self, side: InputSide) -> InputPosition {
        match side {
            InputSide::Left => self.left,
            InputSide::Right => self.right,
        }
    }

    pub(super) fn validate(self) -> Result<(), String> {
        validate_input_position(self.left)?;
        validate_input_position(self.right)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InputEventFacts {
    pub(crate) side: InputSide,
    pub(crate) positions: InputPositions,
    pub(crate) weight: i64,
    pub(crate) row_bytes: u64,
}

impl InputEventFacts {
    pub(crate) fn new(
        side: InputSide,
        positions: InputPositions,
        weight: i64,
        row_bytes: u64,
    ) -> Result<Self, String> {
        let facts = Self {
            side,
            positions,
            weight,
            row_bytes,
        };
        facts.validate()?;
        Ok(facts)
    }

    pub(super) fn validate(self) -> Result<(), String> {
        self.positions.validate()?;
        if self.weight == 0 {
            return Err("Join input event has zero weight".into());
        }
        if self.row_bytes == 0 {
            return Err("Join input event has zero measured bytes".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MatchCounts {
    pub(crate) matched: u64,
    pub(crate) unknown: u64,
}

impl MatchCounts {
    pub(crate) fn new(matched: u64, unknown: u64) -> Result<Self, String> {
        let counts = Self { matched, unknown };
        counts.validate()?;
        Ok(counts)
    }

    pub(super) fn validate(self) -> Result<(), String> {
        validate_count(self.matched, "matched")?;
        validate_count(self.unknown, "unknown")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CandidateExpectation {
    pub(crate) row_id: i64,
    pub(crate) multiplicity: u64,
    pub(crate) truth: MatchTruth,
    pub(crate) old_counts: MatchCounts,
    pub(crate) new_counts: MatchCounts,
}

impl CandidateExpectation {
    pub(crate) fn new(
        row_id: i64,
        multiplicity: u64,
        truth: MatchTruth,
        old_counts: MatchCounts,
        new_counts: MatchCounts,
    ) -> Result<Self, String> {
        let expectation = Self {
            row_id,
            multiplicity,
            truth,
            old_counts,
            new_counts,
        };
        expectation.validate()?;
        Ok(expectation)
    }

    pub(super) fn validate(self) -> Result<(), String> {
        validate_stable_id(self.row_id, "Join candidate")?;
        validate_positive_count(self.multiplicity, "Join candidate multiplicity")?;
        self.old_counts.validate()?;
        self.new_counts.validate()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OwnExpectation {
    pub(crate) row_id: Option<i64>,
    pub(crate) multiplicity: u64,
    pub(crate) counts: MatchCounts,
}

impl OwnExpectation {
    pub(crate) const fn absent() -> Self {
        Self {
            row_id: None,
            multiplicity: 0,
            counts: MatchCounts {
                matched: 0,
                unknown: 0,
            },
        }
    }

    pub(crate) fn present(
        row_id: i64,
        multiplicity: u64,
        counts: MatchCounts,
    ) -> Result<Self, String> {
        let expected = Self {
            row_id: Some(row_id),
            multiplicity,
            counts,
        };
        expected.validate()?;
        Ok(expected)
    }

    pub(super) fn validate(self) -> Result<(), String> {
        self.counts.validate()?;
        match self.row_id {
            None if self.multiplicity == 0 && self.counts == MatchCounts::default() => Ok(()),
            Some(row_id) => {
                validate_stable_id(row_id, "Join own arrangement row")?;
                validate_positive_count(self.multiplicity, "Join own multiplicity")
            }
            None => Err("absent Join own state contains counts or multiplicity".into()),
        }
    }

    pub(super) fn validate_event(self, event_weight: i64) -> Result<(), String> {
        self.validate()?;
        checked_signed_count(self.multiplicity, event_weight, "Join own multiplicity")?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InputProgress {
    positions: InputPositions,
    side: InputSide,
    event_weight: i64,
    event_bytes: u64,
    expected_own: OwnExpectation,
    candidate_after: Option<i64>,
    opposite_counts: MatchCounts,
}

impl InputProgress {
    pub(crate) fn restore(
        positions: InputPositions,
        side: InputSide,
        event_weight: i64,
        event_bytes: u64,
        expected_own: OwnExpectation,
        candidate_after: Option<i64>,
        opposite_counts: MatchCounts,
    ) -> Result<Self, String> {
        let progress = Self {
            positions,
            side,
            event_weight,
            event_bytes,
            expected_own,
            candidate_after,
            opposite_counts,
        };
        progress.validate()?;
        Ok(progress)
    }

    pub(crate) const fn positions(self) -> InputPositions {
        self.positions
    }

    pub(crate) const fn side(self) -> InputSide {
        self.side
    }

    pub(crate) const fn event_weight(self) -> i64 {
        self.event_weight
    }

    pub(crate) const fn event_bytes(self) -> u64 {
        self.event_bytes
    }

    pub(crate) const fn expected_own(self) -> OwnExpectation {
        self.expected_own
    }

    pub(crate) const fn candidate_after(self) -> Option<i64> {
        self.candidate_after
    }

    pub(crate) const fn opposite_counts(self) -> MatchCounts {
        self.opposite_counts
    }

    pub(super) fn from_event(event: InputEventFacts, expected_own: OwnExpectation) -> Self {
        Self {
            positions: event.positions,
            side: event.side,
            event_weight: event.weight,
            event_bytes: event.row_bytes,
            expected_own,
            candidate_after: None,
            opposite_counts: MatchCounts::default(),
        }
    }

    pub(super) fn validate(self) -> Result<(), String> {
        self.positions.validate()?;
        if self.event_weight == 0 {
            return Err("Join continuation contains a zero input weight".into());
        }
        if self.event_bytes == 0 {
            return Err("Join continuation contains zero input bytes".into());
        }
        self.expected_own.validate_event(self.event_weight)?;
        if let Some(row_id) = self.candidate_after {
            validate_stable_id(row_id, "Join candidate cursor")?;
        }
        self.opposite_counts.validate()
    }

    pub(super) fn validate_resume(self, event: InputEventFacts) -> Result<(), String> {
        event.validate()?;
        if self.side != event.side {
            return Err("Join input side changed while resuming".into());
        }
        if self.positions != event.positions {
            return Err("Join input positions changed while resuming".into());
        }
        if self.event_weight != event.weight {
            return Err("Join input weight changed while resuming".into());
        }
        if self.event_bytes != event.row_bytes {
            return Err("Join input byte size changed while resuming".into());
        }
        Ok(())
    }

    pub(super) fn complete_candidate(
        &mut self,
        mode: JoinMode,
        candidate: CandidateExpectation,
    ) -> Result<(), String> {
        self.candidate_after = Some(candidate.row_id);
        match candidate.truth {
            MatchTruth::True => {
                self.opposite_counts.matched = checked_count_sum(
                    self.opposite_counts.matched,
                    candidate.multiplicity,
                    "Join event matched count",
                )?;
            }
            MatchTruth::Unknown if mode == JoinMode::NullAwareAnti => {
                self.opposite_counts.unknown = checked_count_sum(
                    self.opposite_counts.unknown,
                    candidate.multiplicity,
                    "Join event unknown count",
                )?;
            }
            MatchTruth::False | MatchTruth::Unknown => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrontierInputFacts {
    pub(crate) side: InputSide,
    pub(crate) positions: InputPositions,
    pub(crate) frontier: u64,
}

impl FrontierInputFacts {
    pub(crate) fn new(
        side: InputSide,
        positions: InputPositions,
        frontier: u64,
    ) -> Result<Self, String> {
        let facts = Self {
            side,
            positions,
            frontier,
        };
        facts.validate()?;
        Ok(facts)
    }

    pub(super) fn validate(self) -> Result<(), String> {
        self.positions.validate()?;
        if self.frontier == 0 {
            return Err("Join input frontier is zero".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrontierContinuation {
    positions: InputPositions,
    side: InputSide,
    frontier: u64,
}

impl FrontierContinuation {
    pub(crate) const fn positions(self) -> InputPositions {
        self.positions
    }

    pub(crate) const fn side(self) -> InputSide {
        self.side
    }

    pub(crate) const fn frontier(self) -> u64 {
        self.frontier
    }

    pub(super) fn from_facts(facts: FrontierInputFacts) -> Self {
        Self {
            positions: facts.positions,
            side: facts.side,
            frontier: facts.frontier,
        }
    }

    pub(super) fn validate_resume(self, facts: FrontierInputFacts) -> Result<(), String> {
        facts.validate()?;
        if self.side != facts.side {
            return Err("Join frontier side changed while resuming".into());
        }
        if self.positions != facts.positions {
            return Err("Join frontier input positions changed while resuming".into());
        }
        if self.frontier != facts.frontier {
            return Err("Join frontier value changed while resuming".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JoinContinuation {
    Preflight {
        positions: InputPositions,
        side: InputSide,
    },
    Probe(InputProgress),
    PendingTransition {
        progress: InputProgress,
        candidate: CandidateExpectation,
    },
    Finalize(InputProgress),
    Frontier(FrontierContinuation),
}

impl JoinContinuation {
    pub(crate) fn start_preflight(
        positions: InputPositions,
        side: InputSide,
    ) -> Result<Self, String> {
        positions.validate()?;
        Ok(Self::Preflight { positions, side })
    }

    pub(crate) fn start_input(
        event: InputEventFacts,
        expected_own: OwnExpectation,
    ) -> Result<Self, String> {
        event.validate()?;
        expected_own.validate_event(event.weight)?;
        Ok(Self::Probe(InputProgress::from_event(event, expected_own)))
    }

    pub(crate) fn start_frontier(facts: FrontierInputFacts) -> Result<Self, String> {
        facts.validate()?;
        Ok(Self::Frontier(FrontierContinuation::from_facts(facts)))
    }

    pub(crate) fn restore_input(
        phase_code: PhaseCode,
        progress: InputProgress,
        pending_candidate: Option<CandidateExpectation>,
    ) -> Result<Self, String> {
        progress.validate()?;
        let continuation = match JoinPhase::from_code(phase_code)? {
            JoinPhase::Preflight => {
                return Err("Preflight phase cannot decode an input-progress continuation".into());
            }
            JoinPhase::Probe => {
                if pending_candidate.is_some() {
                    return Err("Probe continuation contains a pending candidate".into());
                }
                Self::Probe(progress)
            }
            JoinPhase::PendingTransition => {
                let candidate = pending_candidate.ok_or_else(|| {
                    "pending-transition continuation omitted its candidate".to_string()
                })?;
                candidate.validate()?;
                validate_pending_candidate(progress, candidate)?;
                Self::PendingTransition {
                    progress,
                    candidate,
                }
            }
            JoinPhase::Finalize => {
                if pending_candidate.is_some() {
                    return Err("Finalize continuation contains a pending candidate".into());
                }
                Self::Finalize(progress)
            }
            JoinPhase::Frontier => {
                return Err("Frontier phase cannot decode an input continuation".into());
            }
        };
        Ok(continuation)
    }

    pub(crate) const fn phase(&self) -> JoinPhase {
        match self {
            Self::Preflight { .. } => JoinPhase::Preflight,
            Self::Probe(_) => JoinPhase::Probe,
            Self::PendingTransition { .. } => JoinPhase::PendingTransition,
            Self::Finalize(_) => JoinPhase::Finalize,
            Self::Frontier(_) => JoinPhase::Frontier,
        }
    }

    pub(crate) fn input_progress(&self) -> Option<InputProgress> {
        match self {
            Self::Probe(progress)
            | Self::PendingTransition { progress, .. }
            | Self::Finalize(progress) => Some(*progress),
            Self::Preflight { .. } | Self::Frontier(_) => None,
        }
    }

    pub(crate) fn validate_input_resume(&self, event: InputEventFacts) -> Result<(), String> {
        let progress = self.input_progress().ok_or_else(|| {
            "input event cannot resume a Preflight or Frontier continuation".to_string()
        })?;
        progress.validate_resume(event)
    }

    pub(crate) fn validate_frontier_resume(&self, facts: FrontierInputFacts) -> Result<(), String> {
        match self {
            Self::Frontier(frontier) => frontier.validate_resume(facts),
            _ => Err("frontier cannot pass an active Join input continuation".into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionBytes {
    pub(crate) pair: Option<u64>,
    pub(crate) candidate_only: Option<u64>,
}

impl ProjectionBytes {
    pub(crate) fn new(pair: Option<u64>, candidate_only: Option<u64>) -> Result<Self, String> {
        let projections = Self {
            pair,
            candidate_only,
        };
        projections.validate()?;
        Ok(projections)
    }

    pub(super) fn validate(self) -> Result<(), String> {
        if self.pair == Some(0) || self.candidate_only == Some(0) {
            return Err("measured Join projection has zero bytes".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CandidateProbe {
    pub(crate) row_id: i64,
    pub(crate) multiplicity: u64,
    pub(crate) truth: MatchTruth,
    pub(crate) old_counts: MatchCounts,
    pub(crate) row_bytes: u64,
    pub(crate) output_bytes: ProjectionBytes,
}

impl CandidateProbe {
    pub(crate) fn new(
        row_id: i64,
        multiplicity: u64,
        truth: MatchTruth,
        old_counts: MatchCounts,
        row_bytes: u64,
        output_bytes: ProjectionBytes,
    ) -> Result<Self, String> {
        let candidate = Self {
            row_id,
            multiplicity,
            truth,
            old_counts,
            row_bytes,
            output_bytes,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    pub(super) fn validate(self) -> Result<(), String> {
        validate_stable_id(self.row_id, "Join candidate")?;
        validate_positive_count(self.multiplicity, "Join candidate multiplicity")?;
        self.old_counts.validate()?;
        if self.row_bytes == 0 {
            return Err("Join candidate has zero measured bytes".into());
        }
        self.output_bytes.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProbePage {
    candidates: Vec<CandidateProbe>,
    complete: bool,
}

impl ProbePage {
    pub(crate) fn new(candidates: Vec<CandidateProbe>, complete: bool) -> Result<Self, String> {
        if candidates.is_empty() && !complete {
            return Err("partial Join probe page is empty".into());
        }
        for candidate in &candidates {
            candidate.validate()?;
        }
        Ok(Self {
            candidates,
            complete,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutputActionKind {
    Pair,
    CandidateEligibility,
    CurrentEligibility,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OutputAction {
    pub(crate) kind: OutputActionKind,
    pub(crate) current_side: InputSide,
    pub(crate) candidate_row_id: Option<i64>,
    pub(crate) weight: i64,
    pub(crate) row_bytes: u64,
}

impl OutputAction {
    pub(super) fn validate(self) -> Result<(), String> {
        if self.weight == 0 {
            return Err("Join output action has zero weight".into());
        }
        if self.row_bytes == 0 {
            return Err("Join output action has zero measured bytes".into());
        }
        match (self.kind, self.candidate_row_id) {
            (OutputActionKind::Pair | OutputActionKind::CandidateEligibility, Some(row_id)) => {
                validate_stable_id(row_id, "Join action candidate")
            }
            (OutputActionKind::CurrentEligibility, None) => Ok(()),
            _ => Err("Join output action has an invalid candidate identity".into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CandidateStateChange {
    pub(crate) expected: CandidateExpectation,
}

impl CandidateStateChange {
    pub(super) fn validate(self) -> Result<(), String> {
        self.expected.validate()?;
        if self.expected.old_counts == self.expected.new_counts {
            return Err("Join candidate mutation does not change state".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActionPlan {
    budget: WorkBudget,
    usage: WorkUsage,
    actions: Vec<OutputAction>,
    candidate_changes: Vec<CandidateStateChange>,
    next: JoinContinuation,
}

impl ActionPlan {
    pub(crate) const fn usage(&self) -> WorkUsage {
        self.usage
    }

    pub(crate) fn actions(&self) -> &[OutputAction] {
        &self.actions
    }

    pub(crate) fn candidate_changes(&self) -> &[CandidateStateChange] {
        &self.candidate_changes
    }

    pub(crate) const fn next_continuation(&self) -> &JoinContinuation {
        &self.next
    }

    pub(crate) fn validate_commit(&self, facts: PrimitiveFacts) -> Result<(), String> {
        facts.validate_protocol(
            self.budget,
            KernelPhase::Process,
            KernelCompletion::Continue,
        )?;
        if facts.usage != self.usage {
            return Err("Join action commit usage differs from its plan".into());
        }
        let expected_state_rows = usize_to_u64(
            self.candidate_changes.len(),
            "Join candidate mutation count",
        )?;
        if facts.state_rows != expected_state_rows {
            return Err("Join action commit changed an unexpected number of candidate rows".into());
        }
        if facts.continuation_rows != 1 {
            return Err("Join action commit did not replace exactly one continuation".into());
        }
        if self.actions.is_empty() && facts.output != OutputFacts::None {
            return Err("Join action commit created output for an empty action prefix".into());
        }
        Ok(())
    }
}

pub(crate) fn plan_actions(
    mode: JoinMode,
    continuation: &JoinContinuation,
    event: InputEventFacts,
    page: &ProbePage,
    budget: WorkBudget,
) -> Result<ActionPlan, String> {
    continuation.validate_input_resume(event)?;
    let (mut progress, pending) = match continuation {
        JoinContinuation::Preflight { .. } => {
            return Err("Preflight continuation has not validated its input event".into());
        }
        JoinContinuation::Probe(progress) => (*progress, None),
        JoinContinuation::PendingTransition {
            progress,
            candidate,
        } => (*progress, Some(*candidate)),
        JoinContinuation::Finalize(_) => {
            return Err("Finalize continuation cannot probe more Join candidates".into());
        }
        JoinContinuation::Frontier(_) => {
            return Err("Frontier continuation cannot probe Join candidates".into());
        }
    };
    progress.validate()?;
    validate_probe_page_order(progress, pending, page)?;

    let input_rows = usize_to_u64(page.candidates.len(), "Join probe row count")?;
    let input_bytes = page.candidates.iter().try_fold(0_u64, |total, candidate| {
        total
            .checked_add(candidate.row_bytes)
            .ok_or_else(|| "Join probe input byte count overflow".to_string())
    })?;
    let probe_usage = WorkUsage {
        input_rows,
        input_bytes,
        ..WorkUsage::default()
    };
    probe_usage.validate(budget)?;

    let mut selected_actions = Vec::new();
    let mut state_changes = Vec::new();
    let mut output_rows = 0_u64;
    let mut output_bytes = 0_u64;
    let mut all_page_candidates_completed = true;
    let mut next_pending = None;

    for (index, candidate) in page.candidates.iter().copied().enumerate() {
        let classified = classify_candidate(mode, progress.side, progress.event_weight, candidate)?;
        let first_action = match (index, pending) {
            (0, Some(expected)) => {
                if classified.expected != expected {
                    return Err("pending Join candidate state changed while resuming".into());
                }
                if classified.actions.len() != 2
                    || classified.actions[0].kind != OutputActionKind::Pair
                    || classified.actions[1].kind != OutputActionKind::CandidateEligibility
                {
                    return Err("pending Join transition no longer follows its pair action".into());
                }
                1
            }
            _ => 0,
        };

        let mut selected_for_candidate = 0_usize;
        for action in classified.actions.iter().copied().skip(first_action) {
            if !action_fits(output_rows, output_bytes, action, budget)? {
                break;
            }
            output_rows = output_rows
                .checked_add(1)
                .ok_or_else(|| "Join output row count overflow".to_string())?;
            output_bytes = output_bytes
                .checked_add(action.row_bytes)
                .ok_or_else(|| "Join output byte count overflow".to_string())?;
            selected_actions.push(action);
            selected_for_candidate += 1;
        }

        let remaining_actions = classified.actions.len() - first_action;
        if selected_for_candidate != remaining_actions {
            all_page_candidates_completed = false;
            if first_action + selected_for_candidate > 0 {
                if first_action + selected_for_candidate != 1
                    || classified.actions.len() != 2
                    || classified.actions[0].kind != OutputActionKind::Pair
                    || classified.actions[1].kind != OutputActionKind::CandidateEligibility
                {
                    return Err("Join action prefix stopped at a non-resumable boundary".into());
                }
                next_pending = Some(classified.expected);
            }
            break;
        }

        if classified.expected.old_counts != classified.expected.new_counts {
            state_changes.push(CandidateStateChange {
                expected: classified.expected,
            });
        }
        progress.complete_candidate(mode, classified.expected)?;
    }

    for action in &selected_actions {
        action.validate()?;
    }
    for change in &state_changes {
        change.validate()?;
    }

    let next = if let Some(candidate) = next_pending {
        validate_pending_candidate(progress, candidate)?;
        JoinContinuation::PendingTransition {
            progress,
            candidate,
        }
    } else if all_page_candidates_completed && page.complete {
        JoinContinuation::Finalize(progress)
    } else {
        JoinContinuation::Probe(progress)
    };

    let usage = WorkUsage {
        input_rows,
        input_bytes,
        output_rows,
        output_bytes,
    };
    usage.validate(budget)?;
    Ok(ActionPlan {
        budget,
        usage,
        actions: selected_actions,
        candidate_changes: state_changes,
        next,
    })
}

fn classify_candidate(
    mode: JoinMode,
    current_side: InputSide,
    event_weight: i64,
    candidate: CandidateProbe,
) -> Result<Classified, String> {
    candidate.validate()?;
    let new_counts = apply_match_delta(mode, candidate.old_counts, candidate.truth, event_weight)?;
    let expected = CandidateExpectation {
        row_id: candidate.row_id,
        multiplicity: candidate.multiplicity,
        truth: candidate.truth,
        old_counts: candidate.old_counts,
        new_counts,
    };
    expected.validate()?;

    let mut actions = Vec::with_capacity(2);
    if candidate.truth == MatchTruth::True
        && matches!(
            mode,
            JoinMode::Inner | JoinMode::Left | JoinMode::Right | JoinMode::Full
        )
    {
        actions.push(OutputAction {
            kind: OutputActionKind::Pair,
            current_side,
            candidate_row_id: Some(candidate.row_id),
            weight: checked_weight_product(event_weight, candidate.multiplicity)?,
            row_bytes: candidate
                .output_bytes
                .pair
                .ok_or_else(|| "Join pair probe omitted projected bytes".to_string())?,
        });
    }

    if candidate_side_is_output(mode, current_side.opposite()) {
        let old_eligible = candidate_eligible(mode, candidate.old_counts);
        let new_eligible = candidate_eligible(mode, new_counts);
        if old_eligible != new_eligible {
            let magnitude = count_to_i64(candidate.multiplicity, "Join transition weight")?;
            actions.push(OutputAction {
                kind: OutputActionKind::CandidateEligibility,
                current_side,
                candidate_row_id: Some(candidate.row_id),
                weight: if new_eligible { magnitude } else { -magnitude },
                row_bytes: candidate.output_bytes.candidate_only.ok_or_else(|| {
                    "Join candidate transition probe omitted projected bytes".to_string()
                })?,
            });
        }
    }

    Ok(Classified { expected, actions })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Classified {
    expected: CandidateExpectation,
    actions: Vec<OutputAction>,
}

pub(super) fn apply_match_delta(
    mode: JoinMode,
    old: MatchCounts,
    truth: MatchTruth,
    event_weight: i64,
) -> Result<MatchCounts, String> {
    old.validate()?;
    let mut new = old;
    match truth {
        MatchTruth::True => {
            new.matched =
                checked_signed_count(old.matched, event_weight, "Join candidate matched count")?;
        }
        MatchTruth::Unknown if mode == JoinMode::NullAwareAnti => {
            new.unknown =
                checked_signed_count(old.unknown, event_weight, "Join candidate unknown count")?;
        }
        MatchTruth::False | MatchTruth::Unknown => {}
    }
    Ok(new)
}

pub(super) fn candidate_side_is_output(mode: JoinMode, candidate_side: InputSide) -> bool {
    match mode {
        JoinMode::Inner => false,
        JoinMode::Left => candidate_side == InputSide::Left,
        JoinMode::Right => candidate_side == InputSide::Right,
        JoinMode::Full => true,
        JoinMode::Semi | JoinMode::Anti | JoinMode::NullAwareAnti => {
            candidate_side == InputSide::Left
        }
    }
}

pub(super) fn candidate_eligible(mode: JoinMode, counts: MatchCounts) -> bool {
    match mode {
        JoinMode::Inner => false,
        JoinMode::Left | JoinMode::Right | JoinMode::Full | JoinMode::Anti => counts.matched == 0,
        JoinMode::Semi => counts.matched > 0,
        JoinMode::NullAwareAnti => counts.matched == 0 && counts.unknown == 0,
    }
}

pub(super) fn validate_probe_page_order(
    progress: InputProgress,
    pending: Option<CandidateExpectation>,
    page: &ProbePage,
) -> Result<(), String> {
    if page.candidates.is_empty() {
        if pending.is_some() {
            return Err("pending Join transition probe returned no candidate".into());
        }
        return if page.complete {
            Ok(())
        } else {
            Err("partial Join probe page is empty".into())
        };
    }

    let mut previous = progress.candidate_after;
    for (index, candidate) in page.candidates.iter().enumerate() {
        candidate.validate()?;
        if index == 0 {
            if let Some(expected) = pending {
                if candidate.row_id != expected.row_id {
                    return Err("pending Join candidate was not the first probe row".into());
                }
                if previous.is_some_and(|row_id| candidate.row_id <= row_id) {
                    return Err("pending Join candidate does not follow the durable cursor".into());
                }
                previous = Some(candidate.row_id);
                continue;
            }
        }
        if previous.is_some_and(|row_id| candidate.row_id <= row_id) {
            return Err("Join probe candidates are not in strict keyset order".into());
        }
        previous = Some(candidate.row_id);
    }
    Ok(())
}

pub(super) fn validate_pending_candidate(
    progress: InputProgress,
    candidate: CandidateExpectation,
) -> Result<(), String> {
    progress.validate()?;
    candidate.validate()?;
    if candidate.truth != MatchTruth::True {
        return Err("pending Join transition is not the second action of a true pair".into());
    }
    let expected_matched = checked_signed_count(
        candidate.old_counts.matched,
        progress.event_weight,
        "pending Join candidate matched count",
    )?;
    if candidate.new_counts.matched != expected_matched
        || candidate.new_counts.unknown != candidate.old_counts.unknown
    {
        return Err("pending Join candidate counts do not match the input event".into());
    }
    if progress
        .candidate_after
        .is_some_and(|row_id| candidate.row_id <= row_id)
    {
        return Err("pending Join candidate does not follow the durable cursor".into());
    }
    Ok(())
}

pub(super) fn action_fits(
    current_rows: u64,
    current_bytes: u64,
    action: OutputAction,
    budget: WorkBudget,
) -> Result<bool, String> {
    action.validate()?;
    let max_rows = usize_to_u64(budget.max_output_rows, "Join output row budget")?;
    let max_bytes = usize_to_u64(budget.max_output_bytes, "Join output byte budget")?;
    let next_rows = current_rows
        .checked_add(1)
        .ok_or_else(|| "Join output row count overflow".to_string())?;
    if next_rows > max_rows {
        return Ok(false);
    }
    let next_bytes = current_bytes
        .checked_add(action.row_bytes)
        .ok_or_else(|| "Join output byte count overflow".to_string())?;
    // The first indivisible output row may exceed the target byte budget.
    Ok(current_rows == 0 || next_bytes <= max_bytes)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnStateProbe {
    Absent {
        output_bytes: u64,
    },
    Present {
        row_id: i64,
        multiplicity: u64,
        counts: MatchCounts,
        output_bytes: u64,
    },
}

impl OwnStateProbe {
    pub(crate) fn absent(output_bytes: u64) -> Result<Self, String> {
        let state = Self::Absent { output_bytes };
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn present(
        row_id: i64,
        multiplicity: u64,
        counts: MatchCounts,
        output_bytes: u64,
    ) -> Result<Self, String> {
        let state = Self::Present {
            row_id,
            multiplicity,
            counts,
            output_bytes,
        };
        state.validate()?;
        Ok(state)
    }

    const fn output_bytes(self) -> u64 {
        match self {
            Self::Absent { output_bytes } | Self::Present { output_bytes, .. } => output_bytes,
        }
    }

    const fn expectation(self) -> OwnExpectation {
        match self {
            Self::Absent { .. } => OwnExpectation::absent(),
            Self::Present {
                row_id,
                multiplicity,
                counts,
                ..
            } => OwnExpectation {
                row_id: Some(row_id),
                multiplicity,
                counts,
            },
        }
    }

    pub(super) fn validate(self) -> Result<(), String> {
        match self {
            Self::Absent { output_bytes } => {
                if output_bytes == 0 {
                    return Err("Join current-row projection has zero measured bytes".into());
                }
            }
            Self::Present {
                row_id,
                multiplicity,
                counts,
                output_bytes,
            } => {
                validate_stable_id(row_id, "Join own arrangement row")?;
                validate_positive_count(multiplicity, "Join own multiplicity")?;
                counts.validate()?;
                if output_bytes == 0 {
                    return Err("Join current-row projection has zero measured bytes".into());
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnStateChangeKind {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OwnStateChange {
    pub(crate) kind: OwnStateChangeKind,
    pub(crate) expected_row_id: Option<i64>,
    pub(crate) expected_multiplicity: u64,
    pub(crate) new_multiplicity: u64,
    pub(crate) counts: MatchCounts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FinalizePlan {
    budget: WorkBudget,
    usage: WorkUsage,
    output: Option<OutputAction>,
    own_change: OwnStateChange,
}

impl FinalizePlan {
    pub(crate) const fn usage(&self) -> WorkUsage {
        self.usage
    }

    pub(crate) const fn output(&self) -> Option<OutputAction> {
        self.output
    }

    pub(crate) const fn own_change(&self) -> OwnStateChange {
        self.own_change
    }

    pub(crate) fn validate_commit(
        &self,
        facts: PrimitiveFacts,
        expected_continuation_rows: u64,
    ) -> Result<(), String> {
        if expected_continuation_rows > 1 {
            return Err("Join finalize expected more than one continuation".into());
        }
        facts.validate_protocol(
            self.budget,
            KernelPhase::Process,
            if expected_continuation_rows == 1 {
                KernelCompletion::Continue
            } else {
                KernelCompletion::Finished
            },
        )?;
        if facts.usage != self.usage {
            return Err("Join finalize commit usage differs from its plan".into());
        }
        if facts.state_rows != 1 {
            return Err("Join finalize commit did not change exactly one own row".into());
        }
        if facts.continuation_rows != expected_continuation_rows {
            return Err("Join finalize commit wrote an unexpected continuation count".into());
        }
        if self.output.is_none() && facts.output != OutputFacts::None {
            return Err("Join finalize commit created an unexpected output chunk".into());
        }
        Ok(())
    }
}

pub(crate) fn plan_finalize(
    mode: JoinMode,
    continuation: &JoinContinuation,
    event: InputEventFacts,
    own: OwnStateProbe,
    budget: WorkBudget,
) -> Result<FinalizePlan, String> {
    continuation.validate_input_resume(event)?;
    own.validate()?;
    let progress = match continuation {
        JoinContinuation::Finalize(progress) => *progress,
        JoinContinuation::Preflight { .. }
        | JoinContinuation::Probe(_)
        | JoinContinuation::PendingTransition { .. } => {
            return Err("Join input cannot finalize before its candidate scan completes".into());
        }
        JoinContinuation::Frontier(_) => {
            return Err("Join frontier cannot use input finalization".into());
        }
    };
    progress.validate()?;
    if own.expectation() != progress.expected_own {
        return Err("Join own arrangement changed while processing its fanout".into());
    }

    let (expected_row_id, old_multiplicity) = match own {
        OwnStateProbe::Absent { .. } => (None, 0),
        OwnStateProbe::Present {
            row_id,
            multiplicity,
            counts,
            ..
        } => {
            if counts != progress.opposite_counts {
                return Err("Join own arrangement counts differ from the completed scan".into());
            }
            (Some(row_id), multiplicity)
        }
    };
    let new_multiplicity = checked_signed_count(
        old_multiplicity,
        progress.event_weight,
        "Join own multiplicity",
    )?;
    let kind = match (expected_row_id, new_multiplicity) {
        (None, 0) => {
            return Err("Join absent own row cannot remain absent after a nonzero event".into());
        }
        (None, _) => OwnStateChangeKind::Insert,
        (Some(_), 0) => OwnStateChangeKind::Delete,
        (Some(_), _) => OwnStateChangeKind::Update,
    };
    let own_change = OwnStateChange {
        kind,
        expected_row_id,
        expected_multiplicity: old_multiplicity,
        new_multiplicity,
        counts: progress.opposite_counts,
    };

    let output = if current_side_is_eligible(mode, progress.side, progress.opposite_counts) {
        Some(OutputAction {
            kind: OutputActionKind::CurrentEligibility,
            current_side: progress.side,
            candidate_row_id: None,
            weight: progress.event_weight,
            row_bytes: own.output_bytes(),
        })
    } else {
        None
    };
    if let Some(action) = output {
        action.validate()?;
    }

    let usage = WorkUsage {
        input_rows: 1,
        input_bytes: progress.event_bytes,
        output_rows: u64::from(output.is_some()),
        output_bytes: output.map_or(0, |action| action.row_bytes),
    };
    usage.validate(budget)?;
    Ok(FinalizePlan {
        budget,
        usage,
        output,
        own_change,
    })
}

pub(super) fn current_side_is_eligible(
    mode: JoinMode,
    current_side: InputSide,
    counts: MatchCounts,
) -> bool {
    match mode {
        JoinMode::Inner => false,
        JoinMode::Left => current_side == InputSide::Left && counts.matched == 0,
        JoinMode::Right => current_side == InputSide::Right && counts.matched == 0,
        JoinMode::Full => counts.matched == 0,
        JoinMode::Semi => current_side == InputSide::Left && counts.matched > 0,
        JoinMode::Anti => current_side == InputSide::Left && counts.matched == 0,
        JoinMode::NullAwareAnti => {
            current_side == InputSide::Left && counts.matched == 0 && counts.unknown == 0
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InputFrontiers {
    pub(crate) left: u64,
    pub(crate) right: u64,
}

impl InputFrontiers {
    pub(crate) const fn get(self, side: InputSide) -> u64 {
        match side {
            InputSide::Left => self.left,
            InputSide::Right => self.right,
        }
    }

    const fn with(self, side: InputSide, frontier: u64) -> Self {
        match side {
            InputSide::Left => Self {
                left: frontier,
                right: self.right,
            },
            InputSide::Right => Self {
                left: self.left,
                right: frontier,
            },
        }
    }

    const fn minimum(self) -> u64 {
        if self.left < self.right {
            self.left
        } else {
            self.right
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrontierState {
    pub(crate) consumed: InputFrontiers,
    pub(crate) published: u64,
    pub(crate) latest_output_data: Option<u64>,
}

impl FrontierState {
    pub(super) fn validate(self) -> Result<(), String> {
        if self.published > self.consumed.minimum() {
            return Err("Join output frontier exceeds a consumed input frontier".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FrontierPlan {
    budget: WorkBudget,
    pub(crate) expected_input: FrontierInputFacts,
    pub(crate) side: InputSide,
    pub(crate) expected_old: InputFrontiers,
    pub(crate) new: InputFrontiers,
    pub(crate) expected_published: u64,
    pub(crate) publish: Option<u64>,
}

impl FrontierPlan {
    pub(crate) fn validate_commit(&self, facts: PrimitiveFacts) -> Result<(), String> {
        facts.validate_protocol(
            self.budget,
            KernelPhase::Frontier,
            KernelCompletion::Finished,
        )?;
        if facts.usage != WorkUsage::default() {
            return Err("Join frontier commit reported data-row work".into());
        }
        if facts.state_rows != 0 {
            return Err("Join frontier commit changed arrangement state".into());
        }
        if facts.continuation_rows != 0 {
            return Err("Join frontier commit left a continuation behind".into());
        }
        match (self.publish, facts.output) {
            (Some(_), OutputFacts::Frontier { .. }) | (None, OutputFacts::None) => Ok(()),
            _ => Err("Join frontier commit output differs from its plan".into()),
        }
    }
}

pub(crate) fn plan_frontier(
    continuation: &JoinContinuation,
    facts: FrontierInputFacts,
    state: FrontierState,
    budget: WorkBudget,
) -> Result<FrontierPlan, String> {
    continuation.validate_frontier_resume(facts)?;
    state.validate()?;
    let old_side = state.consumed.get(facts.side);
    if facts.frontier <= old_side {
        return Err("Join input frontier did not advance".into());
    }
    let new = state.consumed.with(facts.side, facts.frontier);
    let complete_through = new.minimum();
    let publish = if complete_through > state.published
        && state
            .latest_output_data
            .is_none_or(|latest_data| latest_data <= complete_through)
    {
        Some(complete_through)
    } else {
        None
    };
    Ok(FrontierPlan {
        budget,
        expected_input: facts,
        side: facts.side,
        expected_old: state.consumed,
        new,
        expected_published: state.published,
        publish,
    })
}

pub(super) fn validate_input_position(position: InputPosition) -> Result<(), String> {
    if position.stream_id <= 0 || position.chunk_seq <= 0 || position.row_ordinal < 0 {
        return Err("Join continuation contains an invalid input position".into());
    }
    Ok(())
}

pub(super) fn validate_stable_id(value: i64, name: &str) -> Result<(), String> {
    if value <= 0 {
        return Err(format!("{name} ID must be positive"));
    }
    Ok(())
}

pub(super) fn validate_count(value: u64, name: &str) -> Result<(), String> {
    if value > MAX_COUNT {
        return Err(format!("{name} count exceeds PostgreSQL bigint"));
    }
    Ok(())
}

pub(super) fn validate_positive_count(value: u64, name: &str) -> Result<(), String> {
    if value == 0 {
        return Err(format!("{name} must be positive"));
    }
    validate_count(value, name)
}

pub(super) fn checked_count_sum(left: u64, right: u64, name: &str) -> Result<u64, String> {
    let result = left
        .checked_add(right)
        .ok_or_else(|| format!("{name} overflow"))?;
    validate_count(result, name)?;
    Ok(result)
}

pub(super) fn checked_signed_count(base: u64, delta: i64, name: &str) -> Result<u64, String> {
    validate_count(base, name)?;
    let result = i128::from(base) + i128::from(delta);
    if !(0..=i128::from(i64::MAX)).contains(&result) {
        return Err(format!("{name} underflow or overflow"));
    }
    u64::try_from(result).map_err(|_| format!("{name} is not representable"))
}

pub(super) fn checked_weight_product(weight: i64, multiplicity: u64) -> Result<i64, String> {
    validate_positive_count(multiplicity, "Join candidate multiplicity")?;
    let product = i128::from(weight) * i128::from(multiplicity);
    if product == 0 || product < i128::from(i64::MIN) || product > i128::from(i64::MAX) {
        return Err("Join pair weight underflow or overflow".into());
    }
    i64::try_from(product).map_err(|_| "Join pair weight is not representable".into())
}

pub(super) fn count_to_i64(value: u64, name: &str) -> Result<i64, String> {
    validate_count(value, name)?;
    i64::try_from(value).map_err(|_| format!("{name} is not representable"))
}

pub(super) fn usize_to_u64(value: usize, name: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{name} exceeds u64"))
}
