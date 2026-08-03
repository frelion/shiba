use crate::{
    IngressError,
    receiver::SourceReceiver,
    tokens::{AbortedTransaction, BootstrapFence, DurableTransaction, EmptyCommitted},
};

impl SourceReceiver {
    /// Acknowledges one exact durably applied transaction.
    ///
    /// # Errors
    /// Rejects the wrong token kind/coordinate or feedback transport failure.
    pub fn acknowledge(&mut self, token: &DurableTransaction) -> Result<(), IngressError> {
        self.require_ack_ready()?;
        self.feedback
            .require_applied(token.end_lsn(), token.authorization())?;
        self.send_ack(token.end_lsn())
    }

    /// Acknowledges one exact abort without invoking Runtime.
    ///
    /// # Errors
    /// Rejects the wrong token kind/coordinate or feedback transport failure.
    pub fn acknowledge_abort(&mut self, token: &AbortedTransaction) -> Result<(), IngressError> {
        self.require_ack_ready()?;
        self.feedback
            .require_aborted(token.acknowledgment_lsn(), token.authorization())?;
        self.send_ack(token.acknowledgment_lsn())
    }

    /// Acknowledges one exact empty commit without invoking Runtime.
    ///
    /// # Errors
    /// Rejects the wrong token kind/coordinate or feedback transport failure.
    pub fn acknowledge_empty(&mut self, token: &EmptyCommitted) -> Result<(), IngressError> {
        self.require_ack_ready()?;
        self.feedback
            .require_empty(token.end_lsn(), token.authorization())?;
        self.send_ack(token.end_lsn())
    }

    pub(crate) fn acknowledge_fence(&mut self, token: &BootstrapFence) -> Result<(), IngressError> {
        self.require_ack_ready()?;
        self.feedback
            .require_fence(token.end_lsn(), token.authorization())?;
        self.send_ack(token.end_lsn())
    }

    fn require_ack_ready(&self) -> Result<(), IngressError> {
        if self.failed {
            return Err(IngressError::ReceiverFailed);
        }
        if self.outstanding_lsn.is_some() {
            return Err(IngressError::FeedbackMismatch);
        }
        Ok(())
    }

    fn send_ack(&mut self, lsn: u64) -> Result<(), IngressError> {
        self.transport.send_feedback(lsn)?;
        self.feedback.complete(lsn);
        Ok(())
    }
}
