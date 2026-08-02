use shiba_runtime::activate_bootstrap;

use crate::{BootstrapCatchupProgress, IngressError, bootstrap_catchup::BootstrapCatchupSession};

impl BootstrapCatchupSession {
    pub(crate) fn retry_durable_ack(&mut self) -> Result<BootstrapCatchupProgress, IngressError> {
        self.config.revalidate(&mut self.apply, true)?;
        let token = self
            .pending_durable
            .take()
            .ok_or(IngressError::FeedbackMismatch)?;
        if let Err(error) = self.receiver_mut()?.acknowledge(&token) {
            self.pending_durable = Some(token);
            return Err(error);
        }
        Ok(BootstrapCatchupProgress::TransactionApplied)
    }

    pub(crate) fn retry_fence_activation(
        &mut self,
    ) -> Result<BootstrapCatchupProgress, IngressError> {
        self.config.revalidate(&mut self.apply, true)?;
        let token = self
            .pending_fence
            .take()
            .ok_or(IngressError::FeedbackMismatch)?;
        if token.source_id() != self.spec.source_id
            || token.bootstrap_id() != self.spec.bootstrap_id
        {
            return Err(IngressError::FeedbackMismatch);
        }
        activate_bootstrap(
            &mut self.apply,
            self.spec.source_id,
            self.spec.bootstrap_id,
            token.message_lsn(),
            token.end_lsn(),
        )?;
        self.config.revalidate(&mut self.apply, true)?;
        if let Err(error) = self.receiver_mut()?.acknowledge_fence(&token) {
            self.pending_fence = Some(token);
            return Err(error);
        }
        self.active = true;
        Ok(BootstrapCatchupProgress::Active)
    }
}
