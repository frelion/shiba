use crate::{
    GovernedGraphSession, IngressError, ReplicationMode,
    bootstrap_catchup::BootstrapCatchupSession, governed::AttachOptions,
};

impl BootstrapCatchupSession {
    /// Converts an activated bootstrap into the ordinary governed M10 session.
    ///
    /// # Errors
    /// Fails if activation has not durably committed or advisory ownership was
    /// lost before the normal receiver reattaches.
    pub fn into_live(self) -> Result<GovernedGraphSession, IngressError> {
        if !self.active {
            return Err(IngressError::Governance("bootstrap is not active"));
        }
        let Self {
            receiver,
            mut apply,
            spec,
            options,
            apply_conninfo,
            replication_conninfo,
            advisory_key,
            permit,
            ..
        } = self;
        drop(receiver);
        let released: bool = apply
            .query_one("SELECT pg_catalog.pg_advisory_unlock($1)", &[&advisory_key])?
            .get(0);
        if !released {
            return Err(IngressError::Governance(
                "source advisory lock was not held",
            ));
        }
        drop(apply);
        drop(permit);
        GovernedGraphSession::attach(
            &apply_conninfo,
            &replication_conninfo,
            spec.graph_id,
            spec.slot_generation,
            AttachOptions::new(ReplicationMode::Committed, options.statement_timeout())?,
        )
    }
}
