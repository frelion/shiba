use std::sync::atomic::{AtomicUsize, Ordering};

use crate::IngressError;

pub const MAX_ACTIVE_GRAPHS: usize = 32;
pub const CONNECTIONS_PER_GRAPH: usize = 2;
pub const BOOTSTRAP_CONNECTIONS_PER_GRAPH: usize = 3;
pub const MAX_ACTIVE_CONNECTIONS: usize = MAX_ACTIVE_GRAPHS * CONNECTIONS_PER_GRAPH;

static ACTIVE_GRAPHS: AtomicUsize = AtomicUsize::new(0);

/// Process-local admission only; it owns no durable source or slot state.
pub(crate) struct ActivePermit;

impl ActivePermit {
    pub(crate) fn acquire() -> Result<Self, IngressError> {
        ACTIVE_GRAPHS
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < MAX_ACTIVE_GRAPHS).then_some(active + 1)
            })
            .map_err(|_| IngressError::Governance("active graph limit reached"))?;
        Ok(Self)
    }
}

impl Drop for ActivePermit {
    fn drop(&mut self) {
        let previous = ACTIVE_GRAPHS.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "active graph permit underflow");
    }
}
