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

use crate::logical::{WorkBudget, WorkQuantum};

use super::{InputPosition, PhaseCode, PrimitiveFacts, WorkUsage};

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

    fn validate(self) -> Result<(), String> {
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

    fn validate(self) -> Result<(), String> {
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

    fn validate(self) -> Result<(), String> {
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

    fn validate(self) -> Result<(), String> {
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

    fn validate(self) -> Result<(), String> {
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

    fn validate_event(self, event_weight: i64) -> Result<(), String> {
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

    fn from_event(event: InputEventFacts, expected_own: OwnExpectation) -> Self {
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

    fn validate(self) -> Result<(), String> {
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

    fn validate_resume(self, event: InputEventFacts) -> Result<(), String> {
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

    fn complete_candidate(
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

    fn validate(self) -> Result<(), String> {
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

    fn from_facts(facts: FrontierInputFacts) -> Self {
        Self {
            positions: facts.positions,
            side: facts.side,
            frontier: facts.frontier,
        }
    }

    fn validate_resume(self, facts: FrontierInputFacts) -> Result<(), String> {
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

    fn validate(self) -> Result<(), String> {
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

    fn validate(self) -> Result<(), String> {
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
    fn validate(self) -> Result<(), String> {
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
    fn validate(self) -> Result<(), String> {
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
        facts.validate(self.budget)?;
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
        if self.actions.is_empty() && facts.output != super::OutputFacts::None {
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

fn apply_match_delta(
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

fn candidate_side_is_output(mode: JoinMode, candidate_side: InputSide) -> bool {
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

fn candidate_eligible(mode: JoinMode, counts: MatchCounts) -> bool {
    match mode {
        JoinMode::Inner => false,
        JoinMode::Left | JoinMode::Right | JoinMode::Full | JoinMode::Anti => counts.matched == 0,
        JoinMode::Semi => counts.matched > 0,
        JoinMode::NullAwareAnti => counts.matched == 0 && counts.unknown == 0,
    }
}

fn validate_probe_page_order(
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

fn validate_pending_candidate(
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

fn action_fits(
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

    fn validate(self) -> Result<(), String> {
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
        facts.validate(self.budget)?;
        if facts.usage != self.usage {
            return Err("Join finalize commit usage differs from its plan".into());
        }
        if facts.state_rows != 1 {
            return Err("Join finalize commit did not change exactly one own row".into());
        }
        if facts.continuation_rows != expected_continuation_rows {
            return Err("Join finalize commit wrote an unexpected continuation count".into());
        }
        if self.output.is_none() && facts.output != super::OutputFacts::None {
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

fn current_side_is_eligible(mode: JoinMode, current_side: InputSide, counts: MatchCounts) -> bool {
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
    fn validate(self) -> Result<(), String> {
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
        facts.validate(self.budget)?;
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
            (Some(_), super::OutputFacts::Frontier { .. }) | (None, super::OutputFacts::None) => {
                Ok(())
            }
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

fn validate_input_position(position: InputPosition) -> Result<(), String> {
    if position.stream_id <= 0 || position.chunk_seq <= 0 || position.row_ordinal < 0 {
        return Err("Join continuation contains an invalid input position".into());
    }
    Ok(())
}

fn validate_stable_id(value: i64, name: &str) -> Result<(), String> {
    if value <= 0 {
        return Err(format!("{name} ID must be positive"));
    }
    Ok(())
}

fn validate_count(value: u64, name: &str) -> Result<(), String> {
    if value > MAX_COUNT {
        return Err(format!("{name} count exceeds PostgreSQL bigint"));
    }
    Ok(())
}

fn validate_positive_count(value: u64, name: &str) -> Result<(), String> {
    if value == 0 {
        return Err(format!("{name} must be positive"));
    }
    validate_count(value, name)
}

fn checked_count_sum(left: u64, right: u64, name: &str) -> Result<u64, String> {
    let result = left
        .checked_add(right)
        .ok_or_else(|| format!("{name} overflow"))?;
    validate_count(result, name)?;
    Ok(result)
}

fn checked_signed_count(base: u64, delta: i64, name: &str) -> Result<u64, String> {
    validate_count(base, name)?;
    let result = i128::from(base) + i128::from(delta);
    if !(0..=i128::from(i64::MAX)).contains(&result) {
        return Err(format!("{name} underflow or overflow"));
    }
    u64::try_from(result).map_err(|_| format!("{name} is not representable"))
}

fn checked_weight_product(weight: i64, multiplicity: u64) -> Result<i64, String> {
    validate_positive_count(multiplicity, "Join candidate multiplicity")?;
    let product = i128::from(weight) * i128::from(multiplicity);
    if product == 0 || product < i128::from(i64::MIN) || product > i128::from(i64::MAX) {
        return Err("Join pair weight underflow or overflow".into());
    }
    i64::try_from(product).map_err(|_| "Join pair weight is not representable".into())
}

fn count_to_i64(value: u64, name: &str) -> Result<i64, String> {
    validate_count(value, name)?;
    i64::try_from(value).map_err(|_| format!("{name} is not representable"))
}

fn usize_to_u64(value: usize, name: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{name} exceeds u64"))
}

#[cfg(feature = "pg17")]
mod execution {
    use pgrx::datum::DatumWithOid;
    use pgrx::prelude::*;

    use crate::kernel::KernelTransition;
    use crate::logical::model::{DataflowPlan, DataflowStage, JoinKind, JoinSpec, OperatorSpec};
    use crate::logical::WorkBudget;
    use crate::postgres::{format_lsn, parse_lsn};
    use crate::scalar_sql::compile_scalar_expression;

    use super::*;
    use crate::kernel::{
        advance_input, append_frontier, canonical_row_key_sql, chunk, compile_named_outputs,
        compile_stage_bindings, lock_continuation, next_chunk, payload_facts,
        replace_continuation_cas, validate_continuation_abi as validate_typed_continuation_abi,
        validate_output_attributes, BindingInput, ChunkKind, ChunkMeta, ContinuationColumn,
        OutputAppendTarget, OutputFacts, PayloadStorage, ProducerKind, RelationRef, StepContext,
        TypeRef,
    };

    const CONTINUATION_COLUMNS: &[ContinuationColumn] = &[
        ContinuationColumn::required("singleton", pg_sys::BOOLOID),
        ContinuationColumn::required("phase", pg_sys::INT2OID),
        ContinuationColumn::required("input_side", pg_sys::INT2OID),
        ContinuationColumn::required("left_stream_id", pg_sys::INT8OID),
        ContinuationColumn::required("left_chunk_seq", pg_sys::INT8OID),
        ContinuationColumn::required("left_row_ordinal", pg_sys::INT8OID),
        ContinuationColumn::required("right_stream_id", pg_sys::INT8OID),
        ContinuationColumn::required("right_chunk_seq", pg_sys::INT8OID),
        ContinuationColumn::required("right_row_ordinal", pg_sys::INT8OID),
        ContinuationColumn::nullable("event_weight", pg_sys::INT8OID),
        ContinuationColumn::nullable("event_bytes", pg_sys::INT8OID),
        ContinuationColumn::nullable("own_row_id", pg_sys::INT8OID),
        ContinuationColumn::nullable("own_multiplicity", pg_sys::INT8OID),
        ContinuationColumn::nullable("own_match_count", pg_sys::INT8OID),
        ContinuationColumn::nullable("own_unknown_count", pg_sys::INT8OID),
        ContinuationColumn::nullable("candidate_after", pg_sys::INT8OID),
        ContinuationColumn::nullable("accumulated_match_count", pg_sys::INT8OID),
        ContinuationColumn::nullable("accumulated_unknown_count", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_row_id", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_multiplicity", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_truth", pg_sys::INT2OID),
        ContinuationColumn::nullable("pending_old_match", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_old_unknown", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_new_match", pg_sys::INT8OID),
        ContinuationColumn::nullable("pending_new_unknown", pg_sys::INT8OID),
        ContinuationColumn::nullable_as("frontier_lsn", pg_sys::PG_LSNOID, "pg_lsn"),
    ];

    struct Layout {
        left_payload: PayloadStorage,
        right_payload: PayloadStorage,
        output_payload: PayloadStorage,
        left_state: RelationRef,
        right_state: RelationRef,
        continuation: RelationRef,
        condition: String,
        outputs: String,
    }

    impl Layout {
        fn input_payload(&self, side: InputSide) -> &PayloadStorage {
            match side {
                InputSide::Left => &self.left_payload,
                InputSide::Right => &self.right_payload,
            }
        }

        fn input_type(&self, side: InputSide) -> &TypeRef {
            &self.input_payload(side).row_type
        }

        fn state(&self, side: InputSide) -> &RelationRef {
            match side {
                InputSide::Left => &self.left_state,
                InputSide::Right => &self.right_state,
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct JoinTransition {
        has_continuation: bool,
        usage: WorkUsage,
        continue_in_transaction: bool,
    }

    impl JoinTransition {
        const fn control(has_continuation: bool) -> Self {
            Self {
                has_continuation,
                usage: WorkUsage {
                    input_rows: 0,
                    input_bytes: 0,
                    output_rows: 0,
                    output_bytes: 0,
                },
                continue_in_transaction: true,
            }
        }

        const fn material(
            has_continuation: bool,
            usage: WorkUsage,
            continue_in_transaction: bool,
        ) -> Self {
            Self {
                has_continuation,
                usage,
                continue_in_transaction,
            }
        }
    }

    pub(crate) fn step(
        transaction: &mut StepContext<'_, '_>,
        plan: &DataflowPlan,
        stage_id: u32,
    ) -> Result<KernelTransition, String> {
        let stage = plan
            .stages
            .get(usize::try_from(stage_id).map_err(|_| "Join stage ID exceeds usize")?)
            .ok_or_else(|| format!("dataflow has no Join stage {stage_id}"))?;
        let OperatorSpec::Join(spec) = &stage.spec else {
            return Err("Join kernel received another operator".into());
        };
        if transaction.inputs().len() != 2
            || transaction.input(0)?.producer != ProducerKind::Operator
            || transaction.input(1)?.producer != ProducerKind::Operator
        {
            return Err("Join requires exactly two operator-stream inputs".into());
        }
        let layout = load_layout(transaction, plan, stage, spec)?;
        let mut continuation = load_continuation(transaction, &layout.continuation)?;
        super::super::validate_continuation_authority(transaction, continuation.is_some())?;
        if continuation.is_none() && spec.kind == JoinKind::Inner {
            if let Some(page) = measure_inner_page(transaction, &layout)? {
                return execute_inner_page(transaction, &layout, page);
            }
        }

        // A quantum publishes at most one immutable output chunk. Clamp the
        // shared budget to that stream's chunk target before any phase runs.
        let mut quantum = WorkQuantum::new(effective_budget(transaction)?, 64);
        let has_continuation = loop {
            let remaining = quantum
                .remaining()
                .ok_or_else(|| "Join quantum exhausted before its first transition".to_string())?;
            transaction.set_transition_budget(remaining);
            let transition = match continuation {
                None => open_next_input(transaction, &layout)?,
                Some(JoinContinuation::Preflight { positions, side }) => {
                    step_preflight(transaction, &layout, positions, side)?
                }
                Some(continuation @ JoinContinuation::Probe(_))
                | Some(continuation @ JoinContinuation::PendingTransition { .. }) => {
                    step_candidates(transaction, &layout, spec, continuation)?
                }
                Some(continuation @ JoinContinuation::Finalize(_)) => {
                    step_finalize(transaction, &layout, spec, continuation)?
                }
                Some(continuation @ JoinContinuation::Frontier(_)) => {
                    step_frontier(transaction, &layout, continuation)?
                }
            };
            quantum.record(transition.usage)?;
            if !transition.continue_in_transaction || quantum.remaining().is_none() {
                break transition.has_continuation;
            }
            continuation = load_continuation(transaction, &layout.continuation)?;
        };
        transaction.transition(has_continuation, quantum.usage())
    }

    fn open_next_input(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
    ) -> Result<JoinTransition, String> {
        let left = next_chunk(transaction, 0)?;
        let right = next_chunk(transaction, 1)?;
        let (side, head) = match (left, right) {
            (Some(left), Some(right)) => {
                if (left.lsn, 0_u16) <= (right.lsn, 1_u16) {
                    (InputSide::Left, left)
                } else {
                    (InputSide::Right, right)
                }
            }
            (Some(left), None) => (InputSide::Left, left),
            (None, Some(right)) => (InputSide::Right, right),
            (None, None) => {
                return Err("runnable Join has neither a continuation nor an input chunk".into());
            }
        };
        let positions = consumer_positions(transaction)?;
        let continuation = match head.kind {
            ChunkKind::Data => {
                payload_facts(transaction, &layout.input_payload(side).relation, &head)?;
                JoinContinuation::start_preflight(positions, side)?
            }
            ChunkKind::Frontier => JoinContinuation::start_frontier(FrontierInputFacts::new(
                side, positions, head.lsn,
            )?)?,
        };
        insert_continuation(transaction, &layout.continuation, &continuation)?;
        Ok(JoinTransition::control(true))
    }

    fn step_preflight(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        positions: InputPositions,
        side: InputSide,
    ) -> Result<JoinTransition, String> {
        validate_positions_for_side(transaction, positions, side)?;
        let expected = JoinContinuation::start_preflight(positions, side)?;
        let (event, _) = load_event(transaction, layout, positions, side)?;
        let own = load_own_expectation(transaction, layout, event)?;
        let next = JoinContinuation::start_input(event, own)?;
        replace_continuation(transaction, &layout.continuation, &expected, Some(&next))?;
        Ok(JoinTransition::control(true))
    }

    fn step_candidates(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        spec: &JoinSpec,
        continuation: JoinContinuation,
    ) -> Result<JoinTransition, String> {
        let progress = continuation
            .input_progress()
            .ok_or_else(|| "Join candidate phase omitted input progress".to_string())?;
        validate_positions_for_side(transaction, progress.positions(), progress.side())?;
        let (event, chunk) =
            load_event(transaction, layout, progress.positions(), progress.side())?;
        continuation.validate_input_resume(event)?;
        let budget = effective_budget(transaction)?;
        let page = probe_candidates(
            transaction,
            layout,
            mode(spec.kind),
            &continuation,
            event,
            budget,
        )?;
        let action = plan_actions(mode(spec.kind), &continuation, event, &page, budget)?;
        let output = append_actions(transaction, layout, &chunk, action.actions(), event)?;
        let changed = apply_candidate_changes(
            transaction,
            layout.state(event.side.opposite()),
            action.candidate_changes(),
        )?;
        replace_continuation(
            transaction,
            &layout.continuation,
            &continuation,
            Some(action.next_continuation()),
        )?;
        action.validate_commit(PrimitiveFacts {
            usage: action.usage(),
            state_rows: changed,
            continuation_rows: 1,
            output,
        })?;
        Ok(if action.usage().is_empty() {
            JoinTransition::control(true)
        } else {
            JoinTransition::material(true, action.usage(), true)
        })
    }

    fn mode(kind: JoinKind) -> JoinMode {
        match kind {
            JoinKind::Inner => JoinMode::Inner,
            JoinKind::Left => JoinMode::Left,
            JoinKind::Right => JoinMode::Right,
            JoinKind::Full => JoinMode::Full,
            JoinKind::Semi => JoinMode::Semi,
            JoinKind::Anti => JoinMode::Anti,
            JoinKind::NullAwareAnti => JoinMode::NullAwareAnti,
        }
    }

    fn load_layout(
        transaction: &mut StepContext<'_, '_>,
        plan: &DataflowPlan,
        stage: &DataflowStage,
        spec: &JoinSpec,
    ) -> Result<Layout, String> {
        let left_input = transaction.input(0)?.clone();
        let right_input = transaction.input(1)?.clone();
        let left_payload = transaction.payload_storage(left_input.stream_id)?;
        let right_payload = transaction.payload_storage(right_input.stream_id)?;
        let output = transaction.output()?.clone();
        let output_payload = transaction.payload_storage(output.stream_id)?;
        let left_state = transaction.state_storage(0)?;
        let right_state = transaction.state_storage(1)?;
        let continuation = transaction.continuation_storage()?;

        validate_state_abi(transaction, &left_state, &left_payload.row_type)?;
        validate_state_abi(transaction, &right_state, &right_payload.row_type)?;
        validate_continuation_abi(transaction, &continuation)?;
        let bindings = compile_stage_bindings(
            transaction,
            plan,
            stage,
            &[
                BindingInput {
                    row_type: &left_payload.row_type,
                    alias: "left_row",
                },
                BindingInput {
                    row_type: &right_payload.row_type,
                    alias: "right_row",
                },
            ],
        )?;
        let output_attributes = transaction.composite_attributes(&output_payload.row_type)?;
        validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
        let outputs =
            compile_named_outputs(&stage.schema.outputs, &spec.outputs, &bindings, "Join")?
                .join(", ");
        Ok(Layout {
            left_payload,
            right_payload,
            output_payload,
            left_state,
            right_state,
            continuation,
            condition: compile_scalar_expression(&spec.condition, &bindings)?,
            outputs,
        })
    }

    fn validate_state_abi(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
        row_type: &TypeRef,
    ) -> Result<(), String> {
        let attributes = transaction.relation_attributes(relation.oid())?;
        let expected = [
            ("row_id", pg_sys::INT8OID),
            ("row_key", pg_sys::BYTEAOID),
            ("row_value", row_type.oid()),
            ("multiplicity", pg_sys::INT8OID),
            ("match_count", pg_sys::INT8OID),
            ("unknown_count", pg_sys::INT8OID),
        ];
        if attributes.len() != expected.len()
            || attributes
                .iter()
                .zip(expected)
                .any(|(attribute, (name, type_oid))| {
                    attribute.name != name || attribute.type_oid != type_oid || !attribute.not_null
                })
        {
            return Err("Join arrangement relation changed its ABI".into());
        }
        let arguments = unsafe { [DatumWithOid::new(relation.oid(), pg_sys::OIDOID)] };
        let indexes = transaction.read(
            r#"
            SELECT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_index AS identity_index
              WHERE identity_index.indrelid = $1
                AND identity_index.indisunique
                AND identity_index.indisvalid
                AND identity_index.indisready
                AND identity_index.indislive
                AND identity_index.indnkeyatts = 1
                AND identity_index.indnatts = 1
                AND identity_index.indkey[0] = 2
                AND identity_index.indexprs IS NULL
                AND identity_index.indpred IS NULL
            )
            "#,
            &arguments,
        )?;
        if !required_table::<bool>(&indexes.first(), 1, "Join arrangement row-key unique index")? {
            return Err("Join arrangement relation lacks its row-key unique index".into());
        }
        Ok(())
    }

    fn validate_continuation_abi(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
    ) -> Result<(), String> {
        validate_typed_continuation_abi(transaction, relation, CONTINUATION_COLUMNS, "Join")
    }

    fn effective_budget(transaction: &StepContext<'_, '_>) -> Result<WorkBudget, String> {
        let budget = transaction.budget();
        let output = transaction.output()?;
        let output_rows =
            usize::try_from(output.target_rows).map_err(|_| "negative Join row target")?;
        let output_bytes =
            usize::try_from(output.target_bytes).map_err(|_| "negative Join byte target")?;
        if output_rows == 0 || output_bytes == 0 {
            return Err("Join output stream has a zero target".into());
        }
        Ok(WorkBudget::new(
            budget.max_input_rows,
            budget.max_input_bytes,
            budget.max_output_rows.min(output_rows),
            budget.max_output_bytes.min(output_bytes),
        ))
    }

    #[derive(Clone, Debug)]
    struct InnerPage {
        side: InputSide,
        chunk: ChunkMeta,
        output_rows: u64,
        output_bytes: u64,
    }

    /// Measures one complete inner-join input chunk. The persisted row cursor
    /// remains the continuation for chunks whose fanout exceeds the quantum.
    fn measure_inner_page(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
    ) -> Result<Option<InnerPage>, String> {
        let left = next_chunk(transaction, 0)?;
        let right = next_chunk(transaction, 1)?;
        let (side, head) = match (left, right) {
            (Some(left), Some(right)) => {
                if (left.lsn, 0_u16) <= (right.lsn, 1_u16) {
                    (InputSide::Left, left)
                } else {
                    (InputSide::Right, right)
                }
            }
            (Some(left), None) => (InputSide::Left, left),
            (None, Some(right)) => (InputSide::Right, right),
            (None, None) => return Ok(None),
        };
        if head.kind != ChunkKind::Data {
            return Ok(None);
        }
        let budget = effective_budget(transaction)?;
        if head.rows > usize_to_u64(budget.max_input_rows, "Join page input row budget")?
            || head.bytes > usize_to_u64(budget.max_input_bytes, "Join page input byte budget")?
        {
            return Ok(None);
        }
        let current_payload = layout.input_payload(side);
        payload_facts(transaction, &current_payload.relation, &head)?;

        let current_alias = side_alias(side);
        let opposite_alias = side_alias(side.opposite());
        let output_row = format!(
            "ROW({})::{}",
            layout.outputs,
            layout.output_payload.row_type.sql()
        );
        let page_predicate =
            format!("{current_alias}.stream_id=$1 AND {current_alias}.chunk_seq=$2");
        let measured = transaction.read(
            &format!(
                r#"
                SELECT count(*)::bigint,
                       coalesce(sum(
                         shiba_internal.effect_row_bytes({output_row})
                       ),0)::bigint
                FROM {current_payload} AS {current_alias}
                JOIN {opposite_state} AS {opposite_alias}
                  ON ({condition}) IS TRUE
                WHERE {page_predicate}
                "#,
                current_payload = current_payload.relation.sql(),
                opposite_state = layout.state(side.opposite()).sql(),
                condition = layout.condition,
            ),
            &unsafe {
                [
                    DatumWithOid::new(head.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(head.sequence, pg_sys::INT8OID),
                ]
            },
        )?;
        if measured.len() != 1 {
            return Err("Join page measurement returned no summary".into());
        }
        let measured = measured.first();
        let output_rows = nonnegative(
            required_table(&measured, 1, "Join page output rows")?,
            "Join page output rows",
        )?;
        let output_bytes = nonnegative(
            required_table(&measured, 2, "Join page output bytes")?,
            "Join page output bytes",
        )?;
        if output_rows > usize_to_u64(budget.max_output_rows, "Join page output row budget")?
            || output_bytes > usize_to_u64(budget.max_output_bytes, "Join page output byte budget")?
        {
            return Ok(None);
        }
        Ok(Some(InnerPage {
            side,
            chunk: head,
            output_rows,
            output_bytes,
        }))
    }

    fn execute_inner_page(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        page: InnerPage,
    ) -> Result<KernelTransition, String> {
        let input = transaction.input(page.side.code() as u16)?.clone();
        let output_facts = append_inner_page(
            transaction,
            layout,
            page.side,
            &page.chunk,
            page.output_rows,
            page.output_bytes,
        )?;
        update_inner_page_candidates(transaction, layout, page.side, &page.chunk)?;
        apply_inner_page_own_state(transaction, layout, page.side, &page.chunk)?;
        advance_input(
            transaction,
            input.port,
            input
                .next_chunk_seq
                .checked_add(1)
                .ok_or_else(|| "Join page input cursor overflow".to_string())?,
            input.consumed_frontier_lsn,
            WorkUsage {
                input_rows: page.chunk.rows,
                input_bytes: page.chunk.bytes,
                ..WorkUsage::default()
            },
        )?;
        if page.output_rows == 0 && !matches!(output_facts, OutputFacts::None) {
            return Err("empty Join page unexpectedly published output".into());
        }
        transaction.transition(
            false,
            WorkUsage {
                input_rows: page.chunk.rows,
                input_bytes: page.chunk.bytes,
                output_rows: page.output_rows,
                output_bytes: page.output_bytes,
            },
        )
    }

    fn append_inner_page(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        side: InputSide,
        chunk: &ChunkMeta,
        expected_rows: u64,
        expected_bytes: u64,
    ) -> Result<OutputFacts, String> {
        if expected_rows == 0 {
            if expected_bytes != 0 {
                return Err("empty Join page measured nonzero output bytes".into());
            }
            return Ok(OutputFacts::None);
        }
        let append_target = transaction.output_append_target(expected_rows, expected_bytes)?;
        let output = transaction.output()?.clone();
        let (target_sequence, row_offset) = match append_target {
            OutputAppendTarget::New { sequence } => (sequence, 0),
            OutputAppendTarget::Extend {
                sequence,
                row_offset,
                ..
            } => (sequence, row_offset),
        };
        let current_alias = side_alias(side);
        let opposite_alias = side_alias(side.opposite());
        let output_row = format!(
            "ROW({})::{}",
            layout.outputs,
            layout.output_payload.row_type.sql()
        );
        let inserted = transaction.write(
            &format!(
                r#"
                WITH joined AS MATERIALIZED (
                  SELECT row_number() OVER (
                           ORDER BY {current_alias}.row_ordinal,
                                    {opposite_alias}.row_id
                         ) - 1 AS page_ordinal,
                         {current_alias}.weight
                           * {opposite_alias}.multiplicity AS weight,
                         {output_row} AS row_value
                  FROM {current_payload} AS {current_alias}
                  JOIN {opposite_state} AS {opposite_alias}
                    ON ({condition}) IS TRUE
                  WHERE {current_alias}.stream_id=$1
                    AND {current_alias}.chunk_seq=$2
                ),
                stored AS (
                  INSERT INTO {output_payload}(
                    stream_id,chunk_seq,row_ordinal,weight,row_value
                  )
                  SELECT $3,$4,$5+page_ordinal,weight,row_value
                  FROM joined
                  ORDER BY page_ordinal
                  RETURNING shiba_internal.effect_row_bytes(row_value) AS row_bytes
                )
                SELECT count(*)::bigint,
                       coalesce(sum(row_bytes),0)::bigint
                FROM stored
                "#,
                current_payload = layout.input_payload(side).relation.sql(),
                opposite_state = layout.state(side.opposite()).sql(),
                condition = layout.condition,
                output_payload = layout.output_payload.relation.sql(),
            ),
            &unsafe {
                [
                    DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
                    DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(target_sequence, pg_sys::INT8OID),
                    DatumWithOid::new(i64_from_u64(row_offset)?, pg_sys::INT8OID),
                ]
            },
        )?;
        if inserted.len() != 1 {
            return Err("Join page append returned no summary".into());
        }
        let inserted = inserted.first();
        if nonnegative(
            required_table(&inserted, 1, "Join page inserted rows")?,
            "Join page inserted rows",
        )? != expected_rows
            || nonnegative(
                required_table(&inserted, 2, "Join page inserted bytes")?,
                "Join page inserted bytes",
            )? != expected_bytes
        {
            return Err("Join page append disagrees with its measurement".into());
        }
        transaction.record_output_append(
            append_target,
            expected_rows,
            expected_bytes,
            chunk.lsn,
        )?;
        Ok(OutputFacts::Data {
            chunk_seq: target_sequence,
        })
    }

    fn update_inner_page_candidates(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        side: InputSide,
        chunk: &ChunkMeta,
    ) -> Result<(), String> {
        let current_alias = side_alias(side);
        let opposite_alias = side_alias(side.opposite());
        let updated = transaction.write(
            &format!(
                r#"
                WITH deltas AS MATERIALIZED (
                  SELECT {opposite_alias}.row_id,
                         coalesce(sum({current_alias}.weight)
                           FILTER (WHERE ({condition}) IS TRUE),0)::bigint
                           AS matched_delta,
                         coalesce(sum({current_alias}.weight)
                           FILTER (WHERE ({condition}) IS NULL),0)::bigint
                           AS unknown_delta
                  FROM {opposite_state} AS {opposite_alias}
                  CROSS JOIN {current_payload} AS {current_alias}
                  WHERE {current_alias}.stream_id=$1
                    AND {current_alias}.chunk_seq=$2
                  GROUP BY {opposite_alias}.row_id
                ),
                changed AS (
                  UPDATE {opposite_state} AS candidate
                  SET match_count=candidate.match_count+deltas.matched_delta,
                      unknown_count=candidate.unknown_count+deltas.unknown_delta
                  FROM deltas
                  WHERE candidate.row_id=deltas.row_id
                    AND candidate.match_count+deltas.matched_delta >= 0
                    AND candidate.unknown_count+deltas.unknown_delta >= 0
                    AND (
                      deltas.matched_delta <> 0
                      OR deltas.unknown_delta <> 0
                    )
                  RETURNING candidate.row_id
                )
                SELECT count(*) FILTER (
                         WHERE matched_delta <> 0 OR unknown_delta <> 0
                       )::bigint,
                       (SELECT count(*)::bigint FROM changed)
                FROM deltas
                "#,
                current_payload = layout.input_payload(side).relation.sql(),
                opposite_state = layout.state(side.opposite()).sql(),
                condition = layout.condition,
            ),
            &unsafe {
                [
                    DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
                ]
            },
        )?;
        if updated.len() != 1 {
            return Err("Join page candidate update returned no summary".into());
        }
        let updated = updated.first();
        let expected = required_table::<i64>(&updated, 1, "Join page candidate changes")?;
        let actual = required_table::<i64>(&updated, 2, "Join page changed candidates")?;
        if expected != actual {
            return Err("Join page candidate counts would underflow".into());
        }
        Ok(())
    }

    fn apply_inner_page_own_state(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        side: InputSide,
        chunk: &ChunkMeta,
    ) -> Result<(), String> {
        let current_alias = side_alias(side);
        let opposite_alias = side_alias(side.opposite());
        let state = layout.state(side);
        let row_key = canonical_row_key_sql("effect.row_value", layout.input_type(side));
        let changed = transaction.write(
            &format!(
                r#"
                WITH incoming AS MATERIALIZED (
                  SELECT effect.row_ordinal,effect.row_value,effect.weight,
                         {row_key} AS row_key,
                         sum(effect.weight) OVER (
                           PARTITION BY {row_key}
                           ORDER BY effect.row_ordinal
                           ROWS UNBOUNDED PRECEDING
                         ) AS prefix
                  FROM {current_payload} AS effect
                  WHERE effect.stream_id=$1 AND effect.chunk_seq=$2
                ),
                collapsed AS MATERIALIZED (
                  SELECT row_key,
                         (array_agg(row_value ORDER BY row_ordinal))[1]
                           AS row_value,
                         sum(weight)::bigint AS net_weight,
                         min(prefix)::bigint AS min_prefix
                  FROM incoming
                  GROUP BY row_key
                ),
                desired AS MATERIALIZED (
                  SELECT collapsed.*,
                         own.row_id,
                         coalesce(own.multiplicity,0)::bigint AS old_multiplicity,
                         coalesce((
                           SELECT sum({opposite_alias}.multiplicity)::bigint
                           FROM {opposite_state} AS {opposite_alias}
                           CROSS JOIN LATERAL (
                             SELECT collapsed.row_value
                           ) AS {current_alias}
                           WHERE ({condition}) IS TRUE
                         ),0)::bigint AS match_count,
                         coalesce((
                           SELECT sum({opposite_alias}.multiplicity)::bigint
                           FROM {opposite_state} AS {opposite_alias}
                           CROSS JOIN LATERAL (
                             SELECT collapsed.row_value
                           ) AS {current_alias}
                           WHERE ({condition}) IS NULL
                         ),0)::bigint AS unknown_count
                  FROM collapsed
                  LEFT JOIN {state} AS own USING(row_key)
                ),
                valid AS MATERIALIZED (
                  SELECT *,old_multiplicity+net_weight AS new_multiplicity
                  FROM desired
                  WHERE old_multiplicity+min_prefix >= 0
                    AND old_multiplicity+net_weight >= 0
                ),
                removed AS (
                  DELETE FROM {state} AS own
                  USING valid
                  WHERE own.row_id=valid.row_id
                    AND valid.new_multiplicity=0
                  RETURNING own.row_id
                ),
                updated AS (
                  UPDATE {state} AS own
                  SET multiplicity=valid.new_multiplicity,
                      match_count=valid.match_count,
                      unknown_count=valid.unknown_count
                  FROM valid
                  WHERE own.row_id=valid.row_id
                    AND valid.new_multiplicity>0
                  RETURNING own.row_id
                ),
                inserted AS (
                  INSERT INTO {state}(
                    row_key,row_value,multiplicity,match_count,unknown_count
                  )
                  SELECT row_key,row_value,new_multiplicity,
                         match_count,unknown_count
                  FROM valid
                  WHERE row_id IS NULL AND new_multiplicity>0
                  ON CONFLICT (row_key) DO NOTHING
                  RETURNING row_id
                )
                SELECT (SELECT count(*)::bigint FROM collapsed),
                       (SELECT count(*)::bigint FROM valid),
                       (SELECT count(*)::bigint FROM removed)
                         +(SELECT count(*)::bigint FROM updated)
                         +(SELECT count(*)::bigint FROM inserted)
                "#,
                current_payload = layout.input_payload(side).relation.sql(),
                opposite_state = layout.state(side.opposite()).sql(),
                condition = layout.condition,
                state = state.sql(),
            ),
            &unsafe {
                [
                    DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
                    DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
                ]
            },
        )?;
        if changed.len() != 1 {
            return Err("Join page own-state mutation returned no summary".into());
        }
        let changed = changed.first();
        let collapsed = required_table::<i64>(&changed, 1, "Join page collapsed rows")?;
        let valid = required_table::<i64>(&changed, 2, "Join page valid rows")?;
        let mutations = required_table::<i64>(&changed, 3, "Join page state mutations")?;
        if collapsed != valid || valid != mutations {
            return Err("Join page own multiplicity would underflow".into());
        }
        Ok(())
    }

    fn load_continuation(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
    ) -> Result<Option<JoinContinuation>, String> {
        lock_continuation(
            transaction,
            relation,
            "phase,input_side,
             left_stream_id,left_chunk_seq,left_row_ordinal,
             right_stream_id,right_chunk_seq,right_row_ordinal,
             event_weight,event_bytes,
             own_row_id,own_multiplicity,
             own_match_count,own_unknown_count,
             candidate_after,
             accumulated_match_count,accumulated_unknown_count,
             pending_row_id,pending_multiplicity,pending_truth,
             pending_old_match,pending_old_unknown,
             pending_new_match,pending_new_unknown,
             frontier_lsn::text",
            "Join",
            |rows| decode_continuation(&rows.first()),
        )
    }

    fn decode_continuation(row: &pgrx::spi::SpiTupleTable<'_>) -> Result<JoinContinuation, String> {
        let phase =
            JoinPhase::from_code(PhaseCode::active(required_table(row, 1, "Join phase")?)?)?;
        let side = InputSide::from_code(required_table(row, 2, "Join input side")?)?;
        let positions = InputPositions::new(
            InputPosition::new(
                required_table(row, 3, "Join left stream")?,
                required_table(row, 4, "Join left chunk")?,
                required_table(row, 5, "Join left row")?,
            )?,
            InputPosition::new(
                required_table(row, 6, "Join right stream")?,
                required_table(row, 7, "Join right chunk")?,
                required_table(row, 8, "Join right row")?,
            )?,
        )?;
        let event_weight = optional_table::<i64>(row, 9)?;
        let event_bytes = optional_nonnegative(row, 10, "Join event bytes")?;
        let own_row_id = optional_table::<i64>(row, 11)?;
        let own_multiplicity = optional_nonnegative(row, 12, "Join own multiplicity")?;
        let own_match = optional_nonnegative(row, 13, "Join own match count")?;
        let own_unknown = optional_nonnegative(row, 14, "Join own unknown count")?;
        let candidate_after = optional_table::<i64>(row, 15)?;
        let accumulated_match = optional_nonnegative(row, 16, "Join accumulated match count")?;
        let accumulated_unknown = optional_nonnegative(row, 17, "Join accumulated unknown count")?;
        let pending_row_id = optional_table::<i64>(row, 18)?;
        let pending_multiplicity = optional_nonnegative(row, 19, "Join pending multiplicity")?;
        let pending_truth = optional_table::<i16>(row, 20)?;
        let pending_old_match = optional_nonnegative(row, 21, "Join pending old match count")?;
        let pending_old_unknown = optional_nonnegative(row, 22, "Join pending old unknown count")?;
        let pending_new_match = optional_nonnegative(row, 23, "Join pending new match count")?;
        let pending_new_unknown = optional_nonnegative(row, 24, "Join pending new unknown count")?;
        let frontier = optional_table::<String>(row, 25)?
            .map(|value| {
                parse_lsn(&value).map_err(|error| format!("invalid Join frontier LSN: {error}"))
            })
            .transpose()?;
        let data_fields = [
            event_weight.is_some(),
            event_bytes.is_some(),
            own_multiplicity.is_some(),
            own_match.is_some(),
            own_unknown.is_some(),
            accumulated_match.is_some(),
            accumulated_unknown.is_some(),
        ];
        let pending_fields = [
            pending_row_id.is_some(),
            pending_multiplicity.is_some(),
            pending_truth.is_some(),
            pending_old_match.is_some(),
            pending_old_unknown.is_some(),
            pending_new_match.is_some(),
            pending_new_unknown.is_some(),
        ];

        match phase {
            JoinPhase::Preflight => {
                if data_fields.into_iter().any(|present| present)
                    || own_row_id.is_some()
                    || candidate_after.is_some()
                    || pending_fields.into_iter().any(|present| present)
                    || frontier.is_some()
                {
                    return Err(
                        "Join Preflight continuation contains phase-incompatible fields".into(),
                    );
                }
                JoinContinuation::start_preflight(positions, side)
            }
            JoinPhase::Frontier => {
                if data_fields.into_iter().any(|present| present)
                    || own_row_id.is_some()
                    || candidate_after.is_some()
                    || pending_fields.into_iter().any(|present| present)
                {
                    return Err("Join Frontier continuation contains data-event fields".into());
                }
                JoinContinuation::start_frontier(FrontierInputFacts::new(
                    side,
                    positions,
                    frontier.ok_or_else(|| {
                        "Join Frontier continuation omitted its frontier".to_string()
                    })?,
                )?)
            }
            JoinPhase::Probe | JoinPhase::PendingTransition | JoinPhase::Finalize => {
                if !data_fields.into_iter().all(|present| present) || frontier.is_some() {
                    return Err("Join data continuation omitted required scalar fields".into());
                }
                let own_counts = MatchCounts::new(
                    own_match.expect("presence was checked"),
                    own_unknown.expect("presence was checked"),
                )?;
                let own = match own_row_id {
                    Some(row_id) => OwnExpectation::present(
                        row_id,
                        own_multiplicity.expect("presence was checked"),
                        own_counts,
                    )?,
                    None => {
                        let absent = OwnExpectation {
                            row_id: None,
                            multiplicity: own_multiplicity.expect("presence was checked"),
                            counts: own_counts,
                        };
                        absent.validate()?;
                        absent
                    }
                };
                let progress = InputProgress::restore(
                    positions,
                    side,
                    event_weight.expect("presence was checked"),
                    event_bytes.expect("presence was checked"),
                    own,
                    candidate_after,
                    MatchCounts::new(
                        accumulated_match.expect("presence was checked"),
                        accumulated_unknown.expect("presence was checked"),
                    )?,
                )?;
                let pending = if pending_fields.into_iter().all(|present| present) {
                    Some(CandidateExpectation::new(
                        pending_row_id.expect("presence was checked"),
                        pending_multiplicity.expect("presence was checked"),
                        MatchTruth::from_code(pending_truth.expect("presence was checked"))?,
                        MatchCounts::new(
                            pending_old_match.expect("presence was checked"),
                            pending_old_unknown.expect("presence was checked"),
                        )?,
                        MatchCounts::new(
                            pending_new_match.expect("presence was checked"),
                            pending_new_unknown.expect("presence was checked"),
                        )?,
                    )?)
                } else if pending_fields.into_iter().any(|present| present) {
                    return Err("Join pending candidate fields are incomplete".into());
                } else {
                    None
                };
                JoinContinuation::restore_input(phase.code(), progress, pending)
            }
        }
    }

    struct JoinFields {
        phase: i16,
        input_side: i16,
        left_stream_id: i64,
        left_chunk_seq: i64,
        left_row_ordinal: i64,
        right_stream_id: i64,
        right_chunk_seq: i64,
        right_row_ordinal: i64,
        event_weight: Option<i64>,
        event_bytes: Option<i64>,
        own_row_id: Option<i64>,
        own_multiplicity: Option<i64>,
        own_match_count: Option<i64>,
        own_unknown_count: Option<i64>,
        candidate_after: Option<i64>,
        accumulated_match_count: Option<i64>,
        accumulated_unknown_count: Option<i64>,
        pending_row_id: Option<i64>,
        pending_multiplicity: Option<i64>,
        pending_truth: Option<i16>,
        pending_old_match: Option<i64>,
        pending_old_unknown: Option<i64>,
        pending_new_match: Option<i64>,
        pending_new_unknown: Option<i64>,
        frontier_lsn: Option<String>,
    }

    fn continuation_fields(continuation: &JoinContinuation) -> Result<JoinFields, String> {
        let (positions, side) = match continuation {
            JoinContinuation::Preflight { positions, side } => (*positions, *side),
            JoinContinuation::Probe(progress)
            | JoinContinuation::PendingTransition { progress, .. }
            | JoinContinuation::Finalize(progress) => (progress.positions(), progress.side()),
            JoinContinuation::Frontier(frontier) => (frontier.positions(), frontier.side()),
        };
        let mut fields = JoinFields {
            phase: continuation.phase().code().value(),
            input_side: side.code(),
            left_stream_id: positions.left.stream_id,
            left_chunk_seq: positions.left.chunk_seq,
            left_row_ordinal: positions.left.row_ordinal,
            right_stream_id: positions.right.stream_id,
            right_chunk_seq: positions.right.chunk_seq,
            right_row_ordinal: positions.right.row_ordinal,
            event_weight: None,
            event_bytes: None,
            own_row_id: None,
            own_multiplicity: None,
            own_match_count: None,
            own_unknown_count: None,
            candidate_after: None,
            accumulated_match_count: None,
            accumulated_unknown_count: None,
            pending_row_id: None,
            pending_multiplicity: None,
            pending_truth: None,
            pending_old_match: None,
            pending_old_unknown: None,
            pending_new_match: None,
            pending_new_unknown: None,
            frontier_lsn: None,
        };
        match continuation {
            JoinContinuation::Preflight { .. } => {}
            JoinContinuation::Frontier(frontier) => {
                fields.frontier_lsn = Some(format_lsn(frontier.frontier()));
            }
            JoinContinuation::Probe(progress) | JoinContinuation::Finalize(progress) => {
                encode_progress(&mut fields, *progress)?;
            }
            JoinContinuation::PendingTransition {
                progress,
                candidate,
            } => {
                encode_progress(&mut fields, *progress)?;
                fields.pending_row_id = Some(candidate.row_id);
                fields.pending_multiplicity =
                    Some(join_i64(candidate.multiplicity, "pending multiplicity")?);
                fields.pending_truth = Some(candidate.truth.code());
                fields.pending_old_match =
                    Some(join_i64(candidate.old_counts.matched, "pending old match")?);
                fields.pending_old_unknown = Some(join_i64(
                    candidate.old_counts.unknown,
                    "pending old unknown",
                )?);
                fields.pending_new_match =
                    Some(join_i64(candidate.new_counts.matched, "pending new match")?);
                fields.pending_new_unknown = Some(join_i64(
                    candidate.new_counts.unknown,
                    "pending new unknown",
                )?);
            }
        }
        Ok(fields)
    }

    fn encode_progress(fields: &mut JoinFields, progress: InputProgress) -> Result<(), String> {
        fields.event_weight = Some(progress.event_weight());
        fields.event_bytes = Some(join_i64(progress.event_bytes(), "event bytes")?);
        let own = progress.expected_own();
        fields.own_row_id = own.row_id;
        fields.own_multiplicity = Some(join_i64(own.multiplicity, "own multiplicity")?);
        fields.own_match_count = Some(join_i64(own.counts.matched, "own match count")?);
        fields.own_unknown_count = Some(join_i64(own.counts.unknown, "own unknown count")?);
        fields.candidate_after = progress.candidate_after();
        fields.accumulated_match_count = Some(join_i64(
            progress.opposite_counts().matched,
            "accumulated match count",
        )?);
        fields.accumulated_unknown_count = Some(join_i64(
            progress.opposite_counts().unknown,
            "accumulated unknown count",
        )?);
        Ok(())
    }

    fn join_i64(value: u64, field: &str) -> Result<i64, String> {
        i64::try_from(value).map_err(|_| format!("Join {field} exceeds bigint"))
    }

    fn continuation_arguments<'a>(fields: &'a JoinFields) -> [DatumWithOid<'a>; 25] {
        unsafe {
            [
                DatumWithOid::new(fields.phase, pg_sys::INT2OID),
                DatumWithOid::new(fields.input_side, pg_sys::INT2OID),
                DatumWithOid::new(fields.left_stream_id, pg_sys::INT8OID),
                DatumWithOid::new(fields.left_chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(fields.left_row_ordinal, pg_sys::INT8OID),
                DatumWithOid::new(fields.right_stream_id, pg_sys::INT8OID),
                DatumWithOid::new(fields.right_chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(fields.right_row_ordinal, pg_sys::INT8OID),
                DatumWithOid::new(fields.event_weight, pg_sys::INT8OID),
                DatumWithOid::new(fields.event_bytes, pg_sys::INT8OID),
                DatumWithOid::new(fields.own_row_id, pg_sys::INT8OID),
                DatumWithOid::new(fields.own_multiplicity, pg_sys::INT8OID),
                DatumWithOid::new(fields.own_match_count, pg_sys::INT8OID),
                DatumWithOid::new(fields.own_unknown_count, pg_sys::INT8OID),
                DatumWithOid::new(fields.candidate_after, pg_sys::INT8OID),
                DatumWithOid::new(fields.accumulated_match_count, pg_sys::INT8OID),
                DatumWithOid::new(fields.accumulated_unknown_count, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_row_id, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_multiplicity, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_truth, pg_sys::INT2OID),
                DatumWithOid::new(fields.pending_old_match, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_old_unknown, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_new_match, pg_sys::INT8OID),
                DatumWithOid::new(fields.pending_new_unknown, pg_sys::INT8OID),
                DatumWithOid::new(fields.frontier_lsn.as_deref(), pg_sys::TEXTOID),
            ]
        }
    }

    fn insert_continuation(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
        continuation: &JoinContinuation,
    ) -> Result<(), String> {
        let fields = continuation_fields(continuation)?;
        let arguments = continuation_arguments(&fields);
        replace_continuation_cas(
            transaction,
            relation,
            CONTINUATION_COLUMNS,
            None,
            Some(&arguments),
            "Join",
        )
    }

    fn replace_continuation(
        transaction: &mut StepContext<'_, '_>,
        relation: &RelationRef,
        expected: &JoinContinuation,
        next: Option<&JoinContinuation>,
    ) -> Result<(), String> {
        let expected_fields = continuation_fields(expected)?;
        let expected_arguments = continuation_arguments(&expected_fields);
        let next_fields = next.map(continuation_fields).transpose()?;
        let next_arguments = next_fields.as_ref().map(continuation_arguments);
        replace_continuation_cas(
            transaction,
            relation,
            CONTINUATION_COLUMNS,
            Some(&expected_arguments),
            next_arguments.as_ref().map(|arguments| &arguments[..]),
            "Join",
        )
    }

    fn consumer_positions(transaction: &StepContext<'_, '_>) -> Result<InputPositions, String> {
        InputPositions::new(
            InputPosition::new(
                transaction.input(0)?.stream_id,
                transaction.input(0)?.next_chunk_seq,
                0,
            )?,
            InputPosition::new(
                transaction.input(1)?.stream_id,
                transaction.input(1)?.next_chunk_seq,
                0,
            )?,
        )
    }

    fn validate_positions_for_side(
        transaction: &StepContext<'_, '_>,
        positions: InputPositions,
        owner: InputSide,
    ) -> Result<(), String> {
        positions.validate()?;
        for side in [InputSide::Left, InputSide::Right] {
            let input = transaction.input(side.code() as u16)?;
            let position = positions.get(side);
            if position.stream_id != input.stream_id
                || position.chunk_seq != input.next_chunk_seq
                || (side != owner && position.row_ordinal != 0)
            {
                return Err("Join continuation is not at its locked consumer positions".into());
            }
        }
        Ok(())
    }

    fn load_event(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        positions: InputPositions,
        side: InputSide,
    ) -> Result<(InputEventFacts, ChunkMeta), String> {
        validate_positions_for_side(transaction, positions, side)?;
        let position = positions.get(side);
        let input = transaction.input(side.code() as u16)?.clone();
        let chunk = chunk(transaction, &input, position.chunk_seq)?
            .ok_or_else(|| "Join continuation references a missing input chunk".to_string())?;
        if chunk.kind != ChunkKind::Data
            || position.row_ordinal < 0
            || u64::try_from(position.row_ordinal).map_err(|_| "negative Join row ordinal")?
                >= chunk.rows
        {
            return Err("Join data continuation references an invalid input row".into());
        }
        let payload = layout.input_payload(side);
        let query = format!(
            r#"
            SELECT effect.weight,
                   shiba_internal.effect_row_bytes(effect.row_value)
            FROM {} AS effect
            WHERE effect.stream_id = $1
              AND effect.chunk_seq = $2
              AND effect.row_ordinal = $3
            "#,
            payload.relation.sql()
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(position.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(position.chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(position.row_ordinal, pg_sys::INT8OID),
            ]
        };
        let table = transaction.read(&query, &arguments)?;
        if table.len() != 1 {
            return Err("Join input position has no unique typed effect row".into());
        }
        let table = table.first();
        let weight = required_table(&table, 1, "Join input weight")?;
        let row_bytes = nonnegative(
            required_table(&table, 2, "Join input bytes")?,
            "Join input bytes",
        )?;
        Ok((
            InputEventFacts::new(side, positions, weight, row_bytes)?,
            chunk,
        ))
    }

    fn load_own_expectation(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        event: InputEventFacts,
    ) -> Result<OwnExpectation, String> {
        let position = event.positions.get(event.side);
        let state = layout.state(event.side);
        let payload = layout.input_payload(event.side);
        let row_key = canonical_row_key_sql("input_row.row_value", layout.input_type(event.side));
        let query = format!(
            r#"
            SELECT own.row_id,own.multiplicity,
                   own.match_count,own.unknown_count
            FROM {} AS own
            JOIN {} AS input_row
              ON own.row_key = {row_key}
            WHERE input_row.stream_id = $1
              AND input_row.chunk_seq = $2
              AND input_row.row_ordinal = $3
            FOR UPDATE OF own
            "#,
            state.sql(),
            payload.relation.sql()
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(position.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(position.chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(position.row_ordinal, pg_sys::INT8OID),
            ]
        };
        let table = transaction.lock(&query, &arguments)?;
        let own = match table.len() {
            0 => OwnExpectation::absent(),
            1 => {
                let row = table.first();
                OwnExpectation::present(
                    required_table(&row, 1, "Join own row ID")?,
                    nonnegative(
                        required_table(&row, 2, "Join own multiplicity")?,
                        "Join own multiplicity",
                    )?,
                    MatchCounts::new(
                        nonnegative(
                            required_table(&row, 3, "Join own match count")?,
                            "Join own match count",
                        )?,
                        nonnegative(
                            required_table(&row, 4, "Join own unknown count")?,
                            "Join own unknown count",
                        )?,
                    )?,
                )?
            }
            count => {
                return Err(format!(
                    "Join own typed row has {count} arrangement identities"
                ));
            }
        };
        own.validate_event(event.weight)?;
        Ok(own)
    }

    fn probe_candidates(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        mode: JoinMode,
        continuation: &JoinContinuation,
        event: InputEventFacts,
        budget: WorkBudget,
    ) -> Result<ProbePage, String> {
        let progress = continuation
            .input_progress()
            .ok_or_else(|| "Join candidate probe has no input progress".to_string())?;
        let current = event.positions.get(event.side);
        let opposite = event.side.opposite();
        let current_alias = side_alias(event.side);
        let candidate_alias = side_alias(opposite);
        let pair_enabled = matches!(
            mode,
            JoinMode::Inner | JoinMode::Left | JoinMode::Right | JoinMode::Full
        );
        let candidate_enabled = candidate_side_is_output(mode, opposite);
        let old_eligible = eligibility_sql(
            mode,
            &format!("{candidate_alias}.match_count"),
            &format!("{candidate_alias}.unknown_count"),
        );
        let new_eligible = eligibility_sql(
            mode,
            &format!("{candidate_alias}.new_match_count"),
            &format!("{candidate_alias}.new_unknown_count"),
        );
        let pair_bytes = if pair_enabled {
            format!(
                "CASE WHEN {candidate_alias}.truth = 1 THEN
                   shiba_internal.effect_row_bytes(
                     ROW({})::{}
                   )
                 END",
                layout.outputs,
                layout.output_payload.row_type.sql()
            )
        } else {
            "NULL::bigint".into()
        };
        let candidate_bytes = if candidate_enabled {
            format!(
                "CASE WHEN ({old_eligible}) IS DISTINCT FROM ({new_eligible})
                   THEN (
                     SELECT shiba_internal.effect_row_bytes(
                              ROW({})::{}
                            )
                     FROM (
                       SELECT NULL::{} AS row_value
                     ) AS {}
                   )
                 END",
                layout.outputs,
                layout.output_payload.row_type.sql(),
                layout.input_type(event.side).sql(),
                current_alias,
            )
        } else {
            "NULL::bigint".into()
        };
        let unknown_delta = if mode == JoinMode::NullAwareAnti {
            "CASE WHEN truth_rows.truth = -1 THEN $7::numeric ELSE 0::numeric END"
        } else {
            "0::numeric"
        };
        let query = format!(
            r#"
            WITH candidate_source AS MATERIALIZED (
              SELECT candidate.row_id,candidate.row_value,
                     candidate.multiplicity,candidate.match_count,
                     candidate.unknown_count,
                     shiba_internal.effect_row_bytes(candidate.row_value)
                       AS row_bytes
              FROM {candidate_state} AS candidate
              WHERE candidate.row_id > $4
              ORDER BY candidate.row_id
              LIMIT $5
            ),
            measured AS MATERIALIZED (
              SELECT candidate_source.*,
                     row_number() OVER (ORDER BY row_id) AS running_rows,
                     sum(row_bytes) OVER (
                       ORDER BY row_id ROWS UNBOUNDED PRECEDING
                     ) AS running_bytes
              FROM candidate_source
            ),
            bounded AS MATERIALIZED (
              SELECT *
              FROM measured
              WHERE running_rows = 1 OR running_bytes <= $6
            ),
            current_input AS MATERIALIZED (
              SELECT input_row.row_value
              FROM {current_payload} AS input_row
              WHERE input_row.stream_id = $1
                AND input_row.chunk_seq = $2
                AND input_row.row_ordinal = $3
            ),
            truth_rows AS MATERIALIZED (
              SELECT {candidate_alias}.*,
                     CASE
                       WHEN ({condition}) IS TRUE THEN 1::smallint
                       WHEN ({condition}) IS NULL THEN -1::smallint
                       ELSE 0::smallint
                     END AS truth
              FROM bounded AS {candidate_alias}
              CROSS JOIN current_input AS {current_alias}
            ),
            counted AS MATERIALIZED (
              SELECT truth_rows.*,
                     (
                       truth_rows.match_count::numeric
                       + CASE WHEN truth_rows.truth = 1
                           THEN $7::numeric ELSE 0::numeric END
                     )::bigint AS new_match_count,
                     (
                       truth_rows.unknown_count::numeric
                       + {unknown_delta}
                     )::bigint AS new_unknown_count
              FROM truth_rows
            )
            SELECT {candidate_alias}.row_id,{candidate_alias}.multiplicity,
                   {candidate_alias}.truth,
                   {candidate_alias}.match_count,
                   {candidate_alias}.unknown_count,
                   {candidate_alias}.row_bytes,
                   {pair_bytes} AS pair_bytes,
                   {candidate_bytes} AS candidate_bytes
            FROM counted AS {candidate_alias}
            CROSS JOIN current_input AS {current_alias}
            ORDER BY {candidate_alias}.row_id
            "#,
            candidate_state = layout.state(opposite).sql(),
            current_payload = layout.input_payload(event.side).relation.sql(),
            condition = layout.condition,
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(current.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(current.chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(current.row_ordinal, pg_sys::INT8OID),
                DatumWithOid::new(progress.candidate_after().unwrap_or(0), pg_sys::INT8OID),
                DatumWithOid::new(i64_from_usize(budget.max_input_rows)?, pg_sys::INT8OID),
                DatumWithOid::new(i64_from_usize(budget.max_input_bytes)?, pg_sys::INT8OID),
                DatumWithOid::new(event.weight, pg_sys::INT8OID),
            ]
        };
        let rows = transaction.read(&query, &arguments)?;
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            candidates.push(CandidateProbe::new(
                required_row(&row, 1, "Join candidate row ID")?,
                nonnegative(
                    required_row(&row, 2, "Join candidate multiplicity")?,
                    "Join candidate multiplicity",
                )?,
                MatchTruth::from_code(required_row(&row, 3, "Join candidate truth")?)?,
                MatchCounts::new(
                    nonnegative(
                        required_row(&row, 4, "Join candidate match count")?,
                        "Join candidate match count",
                    )?,
                    nonnegative(
                        required_row(&row, 5, "Join candidate unknown count")?,
                        "Join candidate unknown count",
                    )?,
                )?,
                nonnegative(
                    required_row(&row, 6, "Join candidate bytes")?,
                    "Join candidate bytes",
                )?,
                ProjectionBytes::new(
                    optional_nonnegative_row(&row, 7, "Join pair bytes")?,
                    optional_nonnegative_row(&row, 8, "Join transition bytes")?,
                )?,
            )?);
        }
        let after = candidates
            .last()
            .map_or(progress.candidate_after().unwrap_or(0), |candidate| {
                candidate.row_id
            });
        let complete_query = format!(
            "SELECT NOT EXISTS (SELECT 1 FROM {} WHERE row_id > $1)",
            layout.state(opposite).sql()
        );
        let complete_arguments = unsafe { [DatumWithOid::new(after, pg_sys::INT8OID)] };
        let complete = transaction
            .read(&complete_query, &complete_arguments)?
            .first()
            .get_one::<bool>()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Join candidate completion probe returned NULL".to_string())?;
        ProbePage::new(candidates, complete)
    }

    fn side_alias(side: InputSide) -> &'static str {
        match side {
            InputSide::Left => "left_row",
            InputSide::Right => "right_row",
        }
    }

    fn eligibility_sql(mode: JoinMode, matched: &str, unknown: &str) -> String {
        match mode {
            JoinMode::Inner => "false".into(),
            JoinMode::Left | JoinMode::Right | JoinMode::Full | JoinMode::Anti => {
                format!("{matched} = 0")
            }
            JoinMode::Semi => format!("{matched} > 0"),
            JoinMode::NullAwareAnti => format!("{matched} = 0 AND {unknown} = 0"),
        }
    }

    fn append_actions(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        chunk: &ChunkMeta,
        actions: &[OutputAction],
        event: InputEventFacts,
    ) -> Result<OutputFacts, String> {
        if actions.is_empty() {
            return Ok(OutputFacts::None);
        }
        let current_position = event.positions.get(event.side);
        let current_alias = side_alias(event.side);
        let opposite_alias = side_alias(event.side.opposite());
        let mut selects = Vec::with_capacity(actions.len());
        for (ordinal, action) in actions.iter().copied().enumerate() {
            action.validate()?;
            if action.current_side != event.side {
                return Err("Join output action changed its current input side".into());
            }
            let row = format!(
                "ROW({})::{}",
                layout.outputs,
                layout.output_payload.row_type.sql()
            );
            let select = match action.kind {
                OutputActionKind::Pair => format!(
                    r#"
                    SELECT {ordinal}::bigint AS action_ordinal,
                           {weight}::bigint AS weight,
                           {row} AS row_value
                    FROM {current_payload} AS {current_alias}
                    JOIN {candidate_state} AS {opposite_alias}
                      ON {opposite_alias}.row_id = {candidate_id}
                    WHERE {current_alias}.stream_id = {stream_id}
                      AND {current_alias}.chunk_seq = {chunk_seq}
                      AND {current_alias}.row_ordinal = {row_ordinal}
                    "#,
                    ordinal = ordinal,
                    weight = action.weight,
                    candidate_id = action
                        .candidate_row_id
                        .ok_or_else(|| "Join pair action omitted its candidate".to_string())?,
                    current_payload = layout.input_payload(event.side).relation.sql(),
                    candidate_state = layout.state(event.side.opposite()).sql(),
                    stream_id = current_position.stream_id,
                    chunk_seq = current_position.chunk_seq,
                    row_ordinal = current_position.row_ordinal,
                ),
                OutputActionKind::CandidateEligibility => format!(
                    r#"
                    SELECT {ordinal}::bigint AS action_ordinal,
                           {weight}::bigint AS weight,
                           {row} AS row_value
                    FROM {candidate_state} AS {opposite_alias}
                    CROSS JOIN (
                      SELECT NULL::{current_type} AS row_value
                    ) AS {current_alias}
                    WHERE {opposite_alias}.row_id = {candidate_id}
                    "#,
                    ordinal = ordinal,
                    weight = action.weight,
                    candidate_id = action.candidate_row_id.ok_or_else(|| {
                        "Join transition action omitted its candidate".to_string()
                    })?,
                    candidate_state = layout.state(event.side.opposite()).sql(),
                    current_type = layout.input_type(event.side).sql(),
                ),
                OutputActionKind::CurrentEligibility => format!(
                    r#"
                    SELECT {ordinal}::bigint AS action_ordinal,
                           {weight}::bigint AS weight,
                           {row} AS row_value
                    FROM {current_payload} AS {current_alias}
                    CROSS JOIN (
                      SELECT NULL::{opposite_type} AS row_value
                    ) AS {opposite_alias}
                    WHERE {current_alias}.stream_id = {stream_id}
                      AND {current_alias}.chunk_seq = {chunk_seq}
                      AND {current_alias}.row_ordinal = {row_ordinal}
                    "#,
                    ordinal = ordinal,
                    weight = action.weight,
                    current_payload = layout.input_payload(event.side).relation.sql(),
                    opposite_type = layout.input_type(event.side.opposite()).sql(),
                    stream_id = current_position.stream_id,
                    chunk_seq = current_position.chunk_seq,
                    row_ordinal = current_position.row_ordinal,
                ),
            };
            selects.push(select);
        }
        let expected_rows = usize_to_u64(actions.len(), "Join output action count")?;
        let expected_bytes = actions.iter().try_fold(0_u64, |sum, action| {
            sum.checked_add(action.row_bytes)
                .ok_or_else(|| "Join output action bytes overflow".to_string())
        })?;
        let append_target = transaction.output_append_target(expected_rows, expected_bytes)?;
        let output = transaction.output()?.clone();
        let (target_sequence, row_offset) = match append_target {
            OutputAppendTarget::New { sequence } => (sequence, 0),
            OutputAppendTarget::Extend {
                sequence,
                row_offset,
                ..
            } => (sequence, row_offset),
        };
        let query = format!(
            r#"
            WITH action_rows AS MATERIALIZED (
              {action_rows}
            ),
            measured AS MATERIALIZED (
              SELECT action_rows.*,
                     shiba_internal.effect_row_bytes(row_value) AS row_bytes
              FROM action_rows
            ),
            stats AS MATERIALIZED (
              SELECT count(*)::bigint AS row_count,
                     coalesce(sum(row_bytes),0)::bigint AS payload_bytes,
                     min(action_ordinal)::bigint AS first_ordinal,
                     max(action_ordinal)::bigint AS last_ordinal
              FROM measured
            ),
            validated AS MATERIALIZED (
              SELECT *
              FROM stats
              WHERE row_count = $3
                AND payload_bytes = $4
                AND first_ordinal = 0
                AND last_ordinal = $3 - 1
            ),
            inserted AS (
              INSERT INTO {output_relation}(
                stream_id,chunk_seq,row_ordinal,weight,row_value
              )
              SELECT $1,$2,$5 + measured.action_ordinal,
                     measured.weight,measured.row_value
              FROM measured
              CROSS JOIN validated
              ORDER BY measured.action_ordinal
              RETURNING shiba_internal.effect_row_bytes(row_value)
                AS stored_bytes
            )
            SELECT stats.row_count,stats.payload_bytes,
                   (SELECT count(*)::bigint FROM inserted),
                   (
                     SELECT coalesce(sum(stored_bytes),0)::bigint
                     FROM inserted
            )
            FROM stats
            "#,
            action_rows = selects.join(" UNION ALL "),
            output_relation = layout.output_payload.relation.sql(),
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(target_sequence, pg_sys::INT8OID),
                DatumWithOid::new(i64_from_u64(expected_rows)?, pg_sys::INT8OID),
                DatumWithOid::new(i64_from_u64(expected_bytes)?, pg_sys::INT8OID),
                DatumWithOid::new(i64_from_u64(row_offset)?, pg_sys::INT8OID),
            ]
        };
        let table = transaction.write(&query, &arguments)?;
        if table.len() != 1 {
            return Err("Join output primitive returned no summary row".into());
        }
        let table = table.first();
        let rows = nonnegative(
            required_table(&table, 1, "Join evaluated output rows")?,
            "Join evaluated output rows",
        )?;
        let bytes = nonnegative(
            required_table(&table, 2, "Join evaluated output bytes")?,
            "Join evaluated output bytes",
        )?;
        let inserted = nonnegative(
            required_table(&table, 3, "Join inserted output rows")?,
            "Join inserted output rows",
        )?;
        let stored_bytes = nonnegative(
            required_table(&table, 4, "Join stored output bytes")?,
            "Join stored output bytes",
        )?;
        if rows != expected_rows || bytes != expected_bytes {
            return Err("Join output projection changed after its bounded probe".into());
        }
        if inserted != expected_rows || stored_bytes != expected_bytes {
            return Err("Join output staging returned inconsistent payload facts".into());
        }
        transaction.record_output_append(
            append_target,
            expected_rows,
            expected_bytes,
            chunk.lsn,
        )?;
        Ok(OutputFacts::Data {
            chunk_seq: target_sequence,
        })
    }

    fn apply_candidate_changes(
        transaction: &mut StepContext<'_, '_>,
        state: &RelationRef,
        changes: &[CandidateStateChange],
    ) -> Result<u64, String> {
        if changes.is_empty() {
            return Ok(0);
        }
        let values = changes
            .iter()
            .map(|change| {
                let expected = change.expected;
                format!(
                    "({},{},{},{},{},{})",
                    expected.row_id,
                    expected.multiplicity,
                    expected.old_counts.matched,
                    expected.old_counts.unknown,
                    expected.new_counts.matched,
                    expected.new_counts.unknown
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            r#"
            WITH expected(
              row_id,multiplicity,old_match,old_unknown,new_match,new_unknown
            ) AS (VALUES {values}),
            updated AS (
              UPDATE {state} AS arrangement
              SET match_count = expected.new_match,
                  unknown_count = expected.new_unknown
              FROM expected
              WHERE arrangement.row_id = expected.row_id
                AND arrangement.multiplicity = expected.multiplicity
                AND arrangement.match_count = expected.old_match
                AND arrangement.unknown_count = expected.old_unknown
              RETURNING arrangement.row_id
            )
            SELECT count(*)::bigint FROM updated
            "#,
            state = state.sql(),
        );
        let updated = transaction
            .write(&query, &[])?
            .first()
            .get_one::<i64>()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Join candidate update count returned NULL".to_string())?;
        let updated = nonnegative(updated, "Join candidate update count")?;
        if updated != usize_to_u64(changes.len(), "Join candidate update count")? {
            return Err("Join candidate compare-and-set changed an unexpected row count".into());
        }
        Ok(updated)
    }

    fn step_finalize(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        spec: &JoinSpec,
        continuation: JoinContinuation,
    ) -> Result<JoinTransition, String> {
        let progress = continuation
            .input_progress()
            .ok_or_else(|| "Join Finalize phase omitted input progress".to_string())?;
        validate_positions_for_side(transaction, progress.positions(), progress.side())?;
        let (event, chunk) =
            load_event(transaction, layout, progress.positions(), progress.side())?;
        continuation.validate_input_resume(event)?;
        let actual_own = load_own_expectation(transaction, layout, event)?;
        if actual_own != progress.expected_own() {
            return Err("Join own arrangement changed during its fanout".into());
        }
        let output_required =
            current_side_is_eligible(mode(spec.kind), event.side, progress.opposite_counts());
        let output_bytes = if output_required {
            measure_current_output(transaction, layout, event)?
        } else {
            1
        };
        let own_probe = match actual_own.row_id {
            None => OwnStateProbe::absent(output_bytes)?,
            Some(row_id) => OwnStateProbe::present(
                row_id,
                actual_own.multiplicity,
                actual_own.counts,
                output_bytes,
            )?,
        };
        let budget = effective_budget(transaction)?;
        let plan = plan_finalize(mode(spec.kind), &continuation, event, own_probe, budget)?;
        let actions = plan.output().into_iter().collect::<Vec<_>>();
        let output = append_actions(transaction, layout, &chunk, &actions, event)?;
        apply_own_change(transaction, layout, event, plan.own_change())?;

        let position = progress.positions().get(progress.side());
        let next_ordinal = position
            .row_ordinal
            .checked_add(1)
            .ok_or_else(|| "Join input row ordinal overflow".to_string())?;
        let next =
            if u64::try_from(next_ordinal).map_err(|_| "negative Join row ordinal")? < chunk.rows {
                let mut positions = progress.positions();
                match progress.side() {
                    InputSide::Left => positions.left.row_ordinal = next_ordinal,
                    InputSide::Right => positions.right.row_ordinal = next_ordinal,
                }
                Some(JoinContinuation::start_preflight(
                    positions,
                    progress.side(),
                )?)
            } else if u64::try_from(next_ordinal).map_err(|_| "negative Join row ordinal")?
                == chunk.rows
            {
                let input = transaction.input(progress.side().code() as u16)?.clone();
                advance_input(
                    transaction,
                    input.port,
                    input
                        .next_chunk_seq
                        .checked_add(1)
                        .ok_or_else(|| "Join input chunk cursor overflow".to_string())?,
                    input.consumed_frontier_lsn,
                    WorkUsage {
                        input_rows: chunk.rows,
                        input_bytes: chunk.bytes,
                        ..WorkUsage::default()
                    },
                )?;
                None
            } else {
                return Err("Join Finalize advanced beyond its immutable input chunk".into());
            };
        replace_continuation(
            transaction,
            &layout.continuation,
            &continuation,
            next.as_ref(),
        )?;
        let has_continuation = next.is_some();
        let expected_continuation_rows = u64::from(has_continuation);
        plan.validate_commit(
            PrimitiveFacts {
                usage: plan.usage(),
                state_rows: 1,
                continuation_rows: expected_continuation_rows,
                output,
            },
            expected_continuation_rows,
        )?;
        Ok(JoinTransition::material(
            has_continuation,
            plan.usage(),
            has_continuation,
        ))
    }

    fn measure_current_output(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        event: InputEventFacts,
    ) -> Result<u64, String> {
        let current = event.positions.get(event.side);
        let current_alias = side_alias(event.side);
        let opposite_alias = side_alias(event.side.opposite());
        let query = format!(
            r#"
            SELECT shiba_internal.effect_row_bytes(
                     ROW({})::{}
                   )
            FROM {} AS {}
            CROSS JOIN (
              SELECT NULL::{} AS row_value
            ) AS {}
            WHERE {}.stream_id = $1
              AND {}.chunk_seq = $2
              AND {}.row_ordinal = $3
            "#,
            layout.outputs,
            layout.output_payload.row_type.sql(),
            layout.input_payload(event.side).relation.sql(),
            current_alias,
            layout.input_type(event.side.opposite()).sql(),
            opposite_alias,
            current_alias,
            current_alias,
            current_alias,
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(current.stream_id, pg_sys::INT8OID),
                DatumWithOid::new(current.chunk_seq, pg_sys::INT8OID),
                DatumWithOid::new(current.row_ordinal, pg_sys::INT8OID),
            ]
        };
        let table = transaction.read(&query, &arguments)?;
        if table.len() != 1 {
            return Err("Join current-row projection returned no unique row".into());
        }
        nonnegative(
            required_table(&table.first(), 1, "Join current output bytes")?,
            "Join current output bytes",
        )
    }

    fn apply_own_change(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        event: InputEventFacts,
        change: OwnStateChange,
    ) -> Result<(), String> {
        let current = event.positions.get(event.side);
        let state = layout.state(event.side);
        let payload = layout.input_payload(event.side);
        let row_key = canonical_row_key_sql("input_row.row_value", layout.input_type(event.side));
        let query = match change.kind {
            OwnStateChangeKind::Insert => format!(
                r#"
                INSERT INTO {}(
                  row_key,row_value,multiplicity,match_count,unknown_count
                )
                SELECT {row_key},input_row.row_value,{},{},{}
                FROM {} AS input_row
                WHERE input_row.stream_id = {}
                  AND input_row.chunk_seq = {}
                  AND input_row.row_ordinal = {}
                ON CONFLICT (row_key) DO NOTHING
                RETURNING row_id
                "#,
                state.sql(),
                change.new_multiplicity,
                change.counts.matched,
                change.counts.unknown,
                payload.relation.sql(),
                current.stream_id,
                current.chunk_seq,
                current.row_ordinal,
            ),
            OwnStateChangeKind::Update => format!(
                r#"
                UPDATE {} SET multiplicity = {}
                WHERE row_id = {}
                  AND multiplicity = {}
                  AND match_count = {}
                  AND unknown_count = {}
                RETURNING row_id
                "#,
                state.sql(),
                change.new_multiplicity,
                change
                    .expected_row_id
                    .ok_or_else(|| "Join own update omitted its row ID".to_string())?,
                change.expected_multiplicity,
                change.counts.matched,
                change.counts.unknown,
            ),
            OwnStateChangeKind::Delete => format!(
                r#"
                DELETE FROM {}
                WHERE row_id = {}
                  AND multiplicity = {}
                  AND match_count = {}
                  AND unknown_count = {}
                RETURNING row_id
                "#,
                state.sql(),
                change
                    .expected_row_id
                    .ok_or_else(|| "Join own delete omitted its row ID".to_string())?,
                change.expected_multiplicity,
                change.counts.matched,
                change.counts.unknown,
            ),
        };
        if transaction.write(&query, &[])?.len() != 1 {
            return Err("Join own arrangement compare-and-set did not affect one row".into());
        }
        Ok(())
    }

    fn step_frontier(
        transaction: &mut StepContext<'_, '_>,
        layout: &Layout,
        continuation: JoinContinuation,
    ) -> Result<JoinTransition, String> {
        let JoinContinuation::Frontier(frontier) = &continuation else {
            return Err("Join frontier executor received another phase".into());
        };
        let facts =
            FrontierInputFacts::new(frontier.side(), frontier.positions(), frontier.frontier())?;
        continuation.validate_frontier_resume(facts)?;
        validate_positions_for_side(transaction, facts.positions, facts.side)?;
        let position = facts.positions.get(facts.side);
        if position.row_ordinal != 0 {
            return Err("Join frontier continuation has a data row ordinal".into());
        }
        let input = transaction.input(facts.side.code() as u16)?.clone();
        let head = chunk(transaction, &input, position.chunk_seq)?
            .ok_or_else(|| "Join frontier continuation references a missing chunk".to_string())?;
        if head.kind != ChunkKind::Frontier
            || head.rows != 0
            || head.bytes != 0
            || head.lsn != facts.frontier
        {
            return Err("Join frontier continuation changed its immutable chunk".into());
        }
        let output = transaction.output()?.clone();
        let published = output.published_frontier_lsn.unwrap_or(0);
        let state = FrontierState {
            consumed: InputFrontiers {
                left: transaction.input(0)?.consumed_frontier_lsn,
                right: transaction.input(1)?.consumed_frontier_lsn,
            },
            published,
            latest_output_data: output.latest_data_lsn,
        };
        let plan = plan_frontier(&continuation, facts, state, transaction.budget())?;
        let output_facts = if let Some(publish) = plan.publish {
            append_frontier(transaction, publish)?
        } else {
            OutputFacts::None
        };
        advance_input(
            transaction,
            input.port,
            input
                .next_chunk_seq
                .checked_add(1)
                .ok_or_else(|| "Join frontier chunk cursor overflow".to_string())?,
            facts.frontier,
            WorkUsage::default(),
        )?;
        replace_continuation(transaction, &layout.continuation, &continuation, None)?;
        plan.validate_commit(PrimitiveFacts {
            usage: WorkUsage::default(),
            state_rows: 0,
            continuation_rows: 0,
            output: output_facts,
        })?;
        Ok(JoinTransition::material(false, WorkUsage::default(), false))
    }

    fn required_table<T: FromDatum + IntoDatum>(
        table: &pgrx::spi::SpiTupleTable<'_>,
        ordinal: usize,
        name: &str,
    ) -> Result<T, String> {
        table
            .get::<T>(ordinal)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("database returned NULL {name}"))
    }

    fn optional_table<T: FromDatum + IntoDatum>(
        table: &pgrx::spi::SpiTupleTable<'_>,
        ordinal: usize,
    ) -> Result<Option<T>, String> {
        table.get::<T>(ordinal).map_err(|error| error.to_string())
    }

    fn required_row<T: FromDatum + IntoDatum>(
        row: &pgrx::spi::SpiHeapTupleData<'_>,
        ordinal: usize,
        name: &str,
    ) -> Result<T, String> {
        row.get::<T>(ordinal)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("database returned NULL {name}"))
    }

    fn optional_nonnegative(
        table: &pgrx::spi::SpiTupleTable<'_>,
        ordinal: usize,
        name: &str,
    ) -> Result<Option<u64>, String> {
        optional_table::<i64>(table, ordinal)?
            .map(|value| nonnegative(value, name))
            .transpose()
    }

    fn optional_nonnegative_row(
        row: &pgrx::spi::SpiHeapTupleData<'_>,
        ordinal: usize,
        name: &str,
    ) -> Result<Option<u64>, String> {
        row.get::<i64>(ordinal)
            .map_err(|error| error.to_string())?
            .map(|value| nonnegative(value, name))
            .transpose()
    }

    fn nonnegative(value: i64, name: &str) -> Result<u64, String> {
        u64::try_from(value).map_err(|_| format!("{name} is negative"))
    }

    fn i64_from_u64(value: u64) -> Result<i64, String> {
        i64::try_from(value).map_err(|_| "Join resource count exceeds bigint".into())
    }

    fn i64_from_usize(value: usize) -> Result<i64, String> {
        i64::try_from(value).map_err(|_| "Join resource budget exceeds bigint".into())
    }
}

#[cfg(feature = "pg17")]
pub(crate) use execution::step;

pub(crate) const KERNEL: super::KernelFn = super::KernelFn::new(
    super::KernelContract::new(
        &[
            super::InputContract::Operator,
            super::InputContract::Operator,
        ],
        super::OutputContract::EffectStream,
    ),
    step,
);

#[cfg(feature = "pg17")]
pub(crate) fn provision(
    client: &mut pgrx::spi::SpiClient<'_>,
    result_oid: pgrx::pg_sys::Oid,
    stage_id: i32,
    stage: &crate::logical::model::DataflowStage,
    input_streams: &[i64],
    output_stream: i64,
) -> Result<(), String> {
    use crate::logical::model::OperatorSpec;

    use super::register::{
        catalog_continuation, catalog_state, column_sql, qualified_internal, resolve_relation_oid,
        type_sql,
    };
    use super::storage;

    if result_oid == pgrx::pg_sys::InvalidOid {
        return Err("Join provisioning received an invalid result OID".into());
    }
    if stage_id < 0 {
        return Err("Join provisioning received a negative stage ID".into());
    }
    let OperatorSpec::Join(spec) = &stage.spec else {
        return Err("Join provisioning received another operator".into());
    };
    if stage.inputs.len() != 2 || input_streams.len() != 2 {
        return Err(format!(
            "Join stage {stage_id} does not have exactly two durable inputs"
        ));
    }
    if spec.outputs.len() != stage.schema.outputs.len() {
        return Err(format!(
            "Join stage {stage_id} output expressions do not match its schema"
        ));
    }

    for input in &stage.schema.inputs {
        column_sql(client, &input.type_)?;
    }
    for output in &stage.schema.outputs {
        type_sql(client, &output.type_)?;
    }

    let left_payload = storage::payload(client, input_streams[0])?;
    let right_payload = storage::payload(client, input_streams[1])?;
    let output_payload = storage::payload(client, output_stream)?;
    let output_attributes = storage::composite_attributes(client, &output_payload.row_type)?;
    if output_attributes.len() != stage.schema.outputs.len()
        || output_attributes
            .iter()
            .zip(&stage.schema.outputs)
            .any(|(attribute, output)| {
                attribute.type_oid.to_u32() != output.type_.type_oid
                    || attribute.typmod != output.type_.typmod
                    || attribute.collation_oid.to_u32() != output.type_.collation_oid
            })
    {
        return Err(format!(
            "Join stage {stage_id} output payload changed its plan schema"
        ));
    }

    let result_id = result_oid.to_u32();
    let left_state = qualified_internal(&format!("join_left_state_r{result_id}_s{stage_id}"));
    let right_state = qualified_internal(&format!("join_right_state_r{result_id}_s{stage_id}"));
    create_join_state(
        client,
        stage_id,
        "left",
        &left_state,
        left_payload.row_type.sql(),
    )?;
    create_join_state(
        client,
        stage_id,
        "right",
        &right_state,
        right_payload.row_type.sql(),
    )?;

    let continuation = qualified_internal(&format!("join_continuation_r{result_id}_s{stage_id}"));
    client
        .update(
            &format!(
                r#"
                CREATE TABLE {continuation}(
                  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                  phase smallint NOT NULL CHECK(phase BETWEEN 1 AND 5),
                  input_side smallint NOT NULL CHECK(input_side IN (0,1)),
                  left_stream_id bigint NOT NULL CHECK(left_stream_id > 0),
                  left_chunk_seq bigint NOT NULL CHECK(left_chunk_seq > 0),
                  left_row_ordinal bigint NOT NULL CHECK(left_row_ordinal >= 0),
                  right_stream_id bigint NOT NULL CHECK(right_stream_id > 0),
                  right_chunk_seq bigint NOT NULL CHECK(right_chunk_seq > 0),
                  right_row_ordinal bigint NOT NULL CHECK(right_row_ordinal >= 0),
                  event_weight bigint,
                  event_bytes bigint,
                  own_row_id bigint,
                  own_multiplicity bigint,
                  own_match_count bigint,
                  own_unknown_count bigint,
                  candidate_after bigint,
                  accumulated_match_count bigint,
                  accumulated_unknown_count bigint,
                  pending_row_id bigint,
                  pending_multiplicity bigint,
                  pending_truth smallint,
                  pending_old_match bigint,
                  pending_old_unknown bigint,
                  pending_new_match bigint,
                  pending_new_unknown bigint,
                  frontier_lsn pg_lsn,
                  CHECK(
                    (input_side = 0 AND right_row_ordinal = 0)
                    OR (input_side = 1 AND left_row_ordinal = 0)
                  ),
                  CHECK(
                    (
                      phase = 1
                      AND event_weight IS NULL
                      AND event_bytes IS NULL
                      AND own_row_id IS NULL
                      AND own_multiplicity IS NULL
                      AND own_match_count IS NULL
                      AND own_unknown_count IS NULL
                      AND candidate_after IS NULL
                      AND accumulated_match_count IS NULL
                      AND accumulated_unknown_count IS NULL
                      AND pending_row_id IS NULL
                      AND pending_multiplicity IS NULL
                      AND pending_truth IS NULL
                      AND pending_old_match IS NULL
                      AND pending_old_unknown IS NULL
                      AND pending_new_match IS NULL
                      AND pending_new_unknown IS NULL
                      AND frontier_lsn IS NULL
                    )
                    OR (
                      phase IN (2,3,4)
                      AND event_weight IS NOT NULL
                      AND event_weight <> 0
                      AND event_bytes IS NOT NULL
                      AND event_bytes > 0
                      AND own_multiplicity IS NOT NULL
                      AND own_match_count IS NOT NULL
                      AND own_match_count >= 0
                      AND own_unknown_count IS NOT NULL
                      AND own_unknown_count >= 0
                      AND (
                        (
                          own_row_id IS NULL
                          AND own_multiplicity = 0
                          AND own_match_count = 0
                          AND own_unknown_count = 0
                          AND event_weight > 0
                        )
                        OR (
                          own_row_id > 0
                          AND own_multiplicity > 0
                          AND own_multiplicity::numeric
                                + event_weight::numeric
                              BETWEEN 0 AND 9223372036854775807::numeric
                        )
                      )
                      AND (candidate_after IS NULL OR candidate_after > 0)
                      AND accumulated_match_count IS NOT NULL
                      AND accumulated_match_count >= 0
                      AND accumulated_unknown_count IS NOT NULL
                      AND accumulated_unknown_count >= 0
                      AND frontier_lsn IS NULL
                      AND (
                        (
                          phase IN (2,4)
                          AND pending_row_id IS NULL
                          AND pending_multiplicity IS NULL
                          AND pending_truth IS NULL
                          AND pending_old_match IS NULL
                          AND pending_old_unknown IS NULL
                          AND pending_new_match IS NULL
                          AND pending_new_unknown IS NULL
                        )
                        OR (
                          phase = 3
                          AND pending_row_id IS NOT NULL
                          AND pending_row_id > coalesce(candidate_after,0)
                          AND pending_multiplicity IS NOT NULL
                          AND pending_multiplicity > 0
                          AND pending_truth = 1
                          AND pending_old_match IS NOT NULL
                          AND pending_old_match >= 0
                          AND pending_old_unknown IS NOT NULL
                          AND pending_old_unknown >= 0
                          AND pending_new_match IS NOT NULL
                          AND pending_new_match >= 0
                          AND pending_new_unknown = pending_old_unknown
                          AND pending_new_match::numeric
                                = pending_old_match::numeric
                                  + event_weight::numeric
                        )
                      )
                    )
                    OR (
                      phase = 5
                      AND event_weight IS NULL
                      AND event_bytes IS NULL
                      AND own_row_id IS NULL
                      AND own_multiplicity IS NULL
                      AND own_match_count IS NULL
                      AND own_unknown_count IS NULL
                      AND candidate_after IS NULL
                      AND accumulated_match_count IS NULL
                      AND accumulated_unknown_count IS NULL
                      AND pending_row_id IS NULL
                      AND pending_multiplicity IS NULL
                      AND pending_truth IS NULL
                      AND pending_old_match IS NULL
                      AND pending_old_unknown IS NULL
                      AND pending_new_match IS NULL
                      AND pending_new_unknown IS NULL
                      AND frontier_lsn IS NOT NULL
                    )
                  )
                )
                "#
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Join stage {stage_id} continuation: {error}"))?;
    protect_join_relation(client, stage_id, "continuation", &continuation)?;

    let left_oid = resolve_relation_oid(client, &left_state)?;
    let right_oid = resolve_relation_oid(client, &right_state)?;
    let continuation_oid = resolve_relation_oid(client, &continuation)?;
    catalog_state(client, result_oid, stage_id, 0, left_oid)?;
    catalog_state(client, result_oid, stage_id, 1, right_oid)?;
    catalog_continuation(client, result_oid, stage_id, continuation_oid)
}

#[cfg(feature = "pg17")]
fn create_join_state(
    client: &mut pgrx::spi::SpiClient<'_>,
    stage_id: i32,
    side: &str,
    relation: &str,
    row_type: &str,
) -> Result<(), String> {
    client
        .update(
            &format!(
                r#"
                CREATE TABLE {relation}(
                  row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                  row_key bytea NOT NULL UNIQUE,
                  row_value {row_type} NOT NULL,
                  multiplicity bigint NOT NULL CHECK(multiplicity > 0),
                  match_count bigint NOT NULL CHECK(match_count >= 0),
                  unknown_count bigint NOT NULL CHECK(unknown_count >= 0)
                )
                "#
            ),
            None,
            &[],
        )
        .map_err(|error| {
            format!("could not create Join stage {stage_id} {side} arrangement: {error}")
        })?;
    protect_join_relation(client, stage_id, side, relation)
}

#[cfg(feature = "pg17")]
fn protect_join_relation(
    client: &mut pgrx::spi::SpiClient<'_>,
    stage_id: i32,
    label: &str,
    relation: &str,
) -> Result<(), String> {
    client
        .update(
            &format!("REVOKE ALL ON TABLE {relation} FROM PUBLIC"),
            None,
            &[],
        )
        .map_err(|error| {
            format!("could not protect Join stage {stage_id} {label} storage: {error}")
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
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
            continuation_rows: 1,
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
            .validate_input_resume(
                InputEventFacts::new(InputSide::Left, positions(), 3, 7).unwrap()
            )
            .is_err());
        assert!(continuation
            .validate_input_resume(
                InputEventFacts::new(InputSide::Left, positions(), 2, 8).unwrap()
            )
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
                    continuation_rows: 0,
                    output: super::super::OutputFacts::Data { chunk_seq: 10 },
                },
                0,
            )
            .unwrap();

        let left_mode =
            plan_finalize(JoinMode::Left, &right_ready, right, own, budget(1, 1, 100)).unwrap();
        assert!(left_mode.output().is_none());

        let full =
            plan_finalize(JoinMode::Full, &right_ready, right, own, budget(1, 1, 100)).unwrap();
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
        let page =
            ProbePage::new(vec![candidate(1, 2, MatchTruth::True, 0, 0, 3, 3)], true).unwrap();
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
        facts.continuation_rows = 0;
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

        let page =
            ProbePage::new(vec![candidate(5, 2, MatchTruth::True, 0, 0, 3, 3)], true).unwrap();
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
}
