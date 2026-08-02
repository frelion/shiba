use std::sync::atomic::{AtomicUsize, Ordering};

use crate::IngressError;

pub const MAX_ACTIVE_SOURCES: usize = 32;
pub const CONNECTIONS_PER_SOURCE: usize = 2;
pub const MAX_ACTIVE_CONNECTIONS: usize = MAX_ACTIVE_SOURCES * CONNECTIONS_PER_SOURCE;

static ACTIVE_SOURCES: AtomicUsize = AtomicUsize::new(0);

/// Process-local admission only; it owns no durable source or slot state.
pub(crate) struct ActivePermit;

impl ActivePermit {
    pub(crate) fn acquire() -> Result<Self, IngressError> {
        ACTIVE_SOURCES
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < MAX_ACTIVE_SOURCES).then_some(active + 1)
            })
            .map_err(|_| IngressError::Governance("active source limit reached"))?;
        Ok(Self)
    }
}

impl Drop for ActivePermit {
    fn drop(&mut self) {
        let previous = ACTIVE_SOURCES.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "active source permit underflow");
    }
}
