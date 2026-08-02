use crate::{
    AssembledTransaction, IngressError,
    assembler::MAX_TRANSACTION_BYTES,
    frame::{StreamCommit, StreamFrameStatus, stream_frame_status},
};

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum StreamTerminal {
    Committed(AssembledTransaction),
    EmptyCommitted {
        xid: u32,
        commit_lsn: u64,
        end_lsn: u64,
        segment_count: usize,
    },
    Aborted {
        acknowledgment_lsn: u64,
    },
}

#[derive(Clone, Copy)]
enum StreamState {
    Start,
    Segment {
        xid: u32,
        empty: bool,
        segment_count: usize,
    },
    Between {
        xid: u32,
        empty: bool,
        segment_count: usize,
    },
}

/// Bounded protocol-v2 framing state. Semantic decoding remains in Runtime.
pub(crate) struct StreamedAssembler {
    pending: Vec<u8>,
    scanned: usize,
    state: StreamState,
    frame_data_start: Option<u64>,
    latest_data_start: Option<u64>,
}

impl StreamedAssembler {
    pub(crate) const fn new() -> Self {
        Self {
            pending: Vec::new(),
            scanned: 0,
            state: StreamState::Start,
            frame_data_start: None,
            latest_data_start: None,
        }
    }

    pub(crate) fn poll(&mut self) -> Result<Option<StreamTerminal>, IngressError> {
        self.scan()
    }

    /// Appends one outer `XLogData` payload and retains the `dataStart` of the
    /// chunk containing each frame's first byte.
    pub(crate) fn push(
        &mut self,
        data_start: u64,
        bytes: &[u8],
    ) -> Result<Option<StreamTerminal>, IngressError> {
        let old_len = self.pending.len();
        let Some(next_len) = old_len.checked_add(bytes.len()) else {
            return self.fail(IngressError::LimitExceeded);
        };
        if bytes.is_empty() || next_len > MAX_TRANSACTION_BYTES {
            return self.fail(IngressError::LimitExceeded);
        }
        self.pending.extend_from_slice(bytes);
        self.latest_data_start = Some(data_start);
        if self.frame_data_start.is_none() && self.scanned >= old_len {
            self.frame_data_start = Some(data_start);
        }
        self.scan()
    }

    fn scan(&mut self) -> Result<Option<StreamTerminal>, IngressError> {
        loop {
            let status = match stream_frame_status(&self.pending[self.scanned..]) {
                Ok(status) => status,
                Err(error) => return self.fail(error),
            };
            let StreamFrameStatus::Complete {
                len,
                tag,
                xid,
                first_segment,
                commit,
            } = status
            else {
                return Ok(None);
            };
            let frame_start = self.frame_data_start.ok_or(IngressError::MessageOrder)?;
            let empty_commit =
                matches!(self.state, StreamState::Between { empty: true, .. }) && tag == b'c';
            let segment_count = match self.state {
                StreamState::Between { segment_count, .. } => segment_count,
                StreamState::Start | StreamState::Segment { .. } => 0,
            };
            if let Err(error) = self.advance(tag, xid, first_segment) {
                return self.fail(error);
            }
            self.scanned = self
                .scanned
                .checked_add(len)
                .ok_or(IngressError::LimitExceeded)?;

            if tag == b'c' {
                let commit = commit.ok_or(IngressError::InvalidFrame)?;
                if empty_commit {
                    return self.finish_empty(xid, commit, segment_count);
                }
                return self.finish_commit(commit.end_lsn);
            }
            if tag == b'A' {
                self.finish_terminal();
                return Ok(Some(StreamTerminal::Aborted {
                    acknowledgment_lsn: frame_start,
                }));
            }
            // Eager scanning always stops only at an incomplete current frame
            // or a terminal. Therefore, once that incomplete frame completes,
            // any following frame starts in the newest pushed chunk. This is
            // why one retained origin is sufficient without an unbounded span
            // queue, even when the current frame crossed chunk boundaries.
            self.frame_data_start = (self.scanned < self.pending.len())
                .then_some(self.latest_data_start)
                .flatten();
        }
    }

    fn advance(
        &mut self,
        tag: u8,
        xid: Option<u32>,
        first_segment: Option<bool>,
    ) -> Result<(), IngressError> {
        self.state = match (self.state, tag) {
            (StreamState::Start, b'S') if first_segment == Some(true) => StreamState::Segment {
                xid: valid_xid(xid)?,
                empty: true,
                segment_count: 1,
            },
            (
                StreamState::Segment {
                    xid: expected,
                    segment_count,
                    ..
                },
                b'R' | b'I',
            ) if xid == Some(expected) => StreamState::Segment {
                xid: expected,
                empty: false,
                segment_count,
            },
            (
                StreamState::Segment {
                    xid,
                    empty,
                    segment_count,
                },
                b'E',
            ) => StreamState::Between {
                xid,
                empty,
                segment_count,
            },
            (
                StreamState::Between {
                    xid: expected,
                    empty,
                    segment_count,
                },
                b'S',
            ) if xid == Some(expected) && first_segment == Some(false) => StreamState::Segment {
                xid: expected,
                empty,
                segment_count: segment_count
                    .checked_add(1)
                    .ok_or(IngressError::LimitExceeded)?,
            },
            (StreamState::Between { xid: expected, .. }, b'c' | b'A') if xid == Some(expected) => {
                StreamState::Start
            }
            _ => return Err(IngressError::MessageOrder),
        };
        Ok(())
    }

    fn finish_commit(&mut self, end_lsn: u64) -> Result<Option<StreamTerminal>, IngressError> {
        if end_lsn == 0 {
            return self.fail(IngressError::InvalidFrame);
        }
        let remainder = self.pending.split_off(self.scanned);
        let transaction = std::mem::replace(&mut self.pending, remainder);
        self.reset_after_terminal();
        Ok(Some(StreamTerminal::Committed(AssembledTransaction {
            bytes: transaction,
            end_lsn,
        })))
    }

    fn finish_empty(
        &mut self,
        xid: Option<u32>,
        commit: StreamCommit,
        segment_count: usize,
    ) -> Result<Option<StreamTerminal>, IngressError> {
        let xid = valid_xid(xid)?;
        if commit.flags != 0
            || commit.commit_lsn == 0
            || commit.end_lsn < commit.commit_lsn
            || self.scanned != self.pending.len()
        {
            return self.fail(IngressError::InvalidFrame);
        }
        self.pending.clear();
        self.reset_after_terminal();
        Ok(Some(StreamTerminal::EmptyCommitted {
            xid,
            commit_lsn: commit.commit_lsn,
            end_lsn: commit.end_lsn,
            segment_count,
        }))
    }

    fn finish_terminal(&mut self) {
        self.pending = self.pending.split_off(self.scanned);
        self.reset_after_terminal();
    }

    fn reset_after_terminal(&mut self) {
        self.scanned = 0;
        self.state = StreamState::Start;
        self.frame_data_start = (!self.pending.is_empty())
            .then_some(self.latest_data_start)
            .flatten();
    }

    fn fail<T>(&mut self, error: IngressError) -> Result<T, IngressError> {
        self.pending.clear();
        self.scanned = 0;
        self.state = StreamState::Start;
        self.frame_data_start = None;
        Err(error)
    }
}

fn valid_xid(xid: Option<u32>) -> Result<u32, IngressError> {
    xid.filter(|value| *value != 0)
        .ok_or(IngressError::MessageOrder)
}
