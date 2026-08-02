use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Cloneable signal for interrupting a session that is waiting for WAL.
#[derive(Clone, Debug)]
pub struct ShutdownHandle {
    requested: Arc<AtomicBool>,
}

impl ShutdownHandle {
    pub(crate) fn new() -> Self {
        Self {
            requested: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Requests cooperative shutdown without acknowledging received WAL.
    pub fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::ShutdownHandle;

    #[test]
    fn cloned_handle_shares_one_monotonic_signal() {
        let handle = ShutdownHandle::new();
        let clone = handle.clone();
        assert!(!handle.is_requested());
        clone.request();
        assert!(handle.is_requested());
    }
}
