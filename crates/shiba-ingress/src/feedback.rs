use crate::IngressError;

#[derive(Clone, Copy)]
enum PendingFeedback {
    Applied(u64),
    Empty { lsn: u64, authorization: u64 },
    Aborted(u64),
}

/// In-memory capability state; `PostgreSQL` remains the durable slot authority.
pub(crate) struct FeedbackState {
    last_acknowledged_lsn: u64,
    pending: Option<PendingFeedback>,
}

impl FeedbackState {
    pub(crate) const fn new(last_acknowledged_lsn: u64) -> Self {
        Self {
            last_acknowledged_lsn,
            pending: None,
        }
    }

    pub(crate) const fn last_acknowledged_lsn(&self) -> u64 {
        self.last_acknowledged_lsn
    }

    #[cfg(test)]
    pub(crate) const fn pending_lsn(&self) -> Option<u64> {
        match self.pending {
            Some(
                PendingFeedback::Applied(lsn)
                | PendingFeedback::Empty { lsn, .. }
                | PendingFeedback::Aborted(lsn),
            ) => Some(lsn),
            None => None,
        }
    }

    pub(crate) const fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub(crate) fn mark_applied(&mut self, lsn: u64) {
        self.pending = Some(PendingFeedback::Applied(lsn));
    }

    pub(crate) fn mark_empty(&mut self, lsn: u64, authorization: u64) {
        self.pending = Some(PendingFeedback::Empty { lsn, authorization });
    }

    pub(crate) fn mark_aborted(&mut self, lsn: u64) {
        self.pending = Some(PendingFeedback::Aborted(lsn));
    }

    pub(crate) fn require_applied(&self, lsn: u64) -> Result<(), IngressError> {
        matches!(self.pending, Some(PendingFeedback::Applied(value)) if value == lsn)
            .then_some(())
            .ok_or(IngressError::FeedbackMismatch)
    }

    pub(crate) fn require_empty(&self, lsn: u64, authorization: u64) -> Result<(), IngressError> {
        matches!(
            self.pending,
            Some(PendingFeedback::Empty { lsn: value, authorization: expected })
                if value == lsn && expected == authorization
        )
        .then_some(())
        .ok_or(IngressError::FeedbackMismatch)
    }

    pub(crate) fn require_aborted(&self, lsn: u64) -> Result<(), IngressError> {
        matches!(self.pending, Some(PendingFeedback::Aborted(value)) if value == lsn)
            .then_some(())
            .ok_or(IngressError::FeedbackMismatch)
    }

    pub(crate) fn complete(&mut self, lsn: u64) {
        self.last_acknowledged_lsn = lsn;
        self.pending = None;
    }
}
