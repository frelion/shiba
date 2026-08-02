use crate::{
    IngressError,
    frame::{FrameStatus, frame_status},
};

const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Eq, PartialEq)]
pub struct AssembledTransaction {
    pub bytes: Vec<u8>,
    pub end_lsn: u64,
}

pub struct CommittedAssembler {
    pending: Vec<u8>,
    scanned: usize,
    open: bool,
}

impl CommittedAssembler {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pending: Vec::new(),
            scanned: 0,
            open: false,
        }
    }

    /// # Errors
    /// Rejects invalid framing/order or more than 16 MiB of pending input.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Option<AssembledTransaction>, IngressError> {
        let Some(next_len) = self.pending.len().checked_add(bytes.len()) else {
            return self.fail(IngressError::LimitExceeded);
        };
        if next_len > MAX_TRANSACTION_BYTES {
            return self.fail(IngressError::LimitExceeded);
        }
        self.pending.extend_from_slice(bytes);
        loop {
            let status = match frame_status(&self.pending[self.scanned..]) {
                Ok(status) => status,
                Err(error) => return self.fail(error),
            };
            let FrameStatus::Complete {
                len,
                tag,
                terminal_end_lsn,
            } = status
            else {
                return Ok(None);
            };
            if !self.open {
                if tag != b'B' {
                    return self.fail(IngressError::MessageOrder);
                }
                self.open = true;
            } else if tag == b'B' {
                return self.fail(IngressError::MessageOrder);
            }
            self.scanned = self
                .scanned
                .checked_add(len)
                .ok_or(IngressError::LimitExceeded)?;
            if let Some(end_lsn) = terminal_end_lsn {
                let remainder = self.pending.split_off(self.scanned);
                let transaction = std::mem::replace(&mut self.pending, remainder);
                self.scanned = 0;
                self.open = false;
                return Ok(Some(AssembledTransaction {
                    bytes: transaction,
                    end_lsn,
                }));
            }
        }
    }

    fn fail<T>(&mut self, error: IngressError) -> Result<T, IngressError> {
        self.pending.clear();
        self.scanned = 0;
        self.open = false;
        Err(error)
    }
}

impl Default for CommittedAssembler {
    fn default() -> Self {
        Self::new()
    }
}
