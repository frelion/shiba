//! Bounded pgoutput-ingress state machine.
//!
//! Replication I/O happens only outside SPI transactions.  This module turns
//! complete CopyData frames into one bounded, single-source-transaction batch;
//! the Runtime persists that batch in a separate short transaction.

use crate::pgoutput::{self, Message, ParseContext, Tuple};
use crate::replication::{
    CopyDataPoll, ReplicationError, ReplicationMessage, ReplicationTransport,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IngressEvent {
    pub change_lsn: u64,
    pub change_ordinal: u64,
    pub image_ordinal: u32,
    pub source_subxid: u32,
    pub source_oid: u32,
    pub weight: i64,
    pub payload: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum IngressBoundary {
    Commit { commit_lsn: u64, end_lsn: u64 },
    AbortTransaction { abort_lsn: u64 },
    AbortSubtransaction { source_subxid: u32 },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IngressBatch {
    pub source_xid: u32,
    pub transaction_start_lsn: u64,
    pub decode_end_lsn: u64,
    pub events: Vec<IngressEvent>,
    pub boundary: Option<IngressBoundary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IngressBudget {
    pub max_events: usize,
    pub max_wire_bytes: usize,
    pub max_poll_time: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum IngressPoll {
    Batch(IngressBatch),
    Yield { reply_requested: bool },
    Pending { reply_requested: bool },
    End,
}

#[derive(Debug)]
pub(crate) enum IngressError {
    Replication(ReplicationError),
    Protocol(&'static str),
    State(String),
    CounterOverflow(&'static str),
}

impl fmt::Display for IngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Replication(error) => error.fmt(formatter),
            Self::Protocol(error) => write!(formatter, "invalid pgoutput message: {error}"),
            Self::State(error) => write!(formatter, "invalid pgoutput stream state: {error}"),
            Self::CounterOverflow(counter) => {
                write!(formatter, "ingress {counter} counter overflow")
            }
        }
    }
}

impl Error for IngressError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Replication(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ReplicationError> for IngressError {
    fn from(error: ReplicationError) -> Self {
        Self::Replication(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveTransaction {
    xid: u32,
    start_lsn: u64,
    expected_commit_lsn: Option<u64>,
}

struct PendingBatch {
    source_xid: u32,
    transaction_start_lsn: u64,
    decode_end_lsn: u64,
    wire_bytes: u64,
    events: Vec<IngressEvent>,
    boundary: Option<IngressBoundary>,
}

impl PendingBatch {
    fn new(transaction: ActiveTransaction) -> Self {
        Self {
            source_xid: transaction.xid,
            transaction_start_lsn: transaction.start_lsn,
            // This is the greatest wire position actually observed in this
            // batch. Begin.final_lsn identifies the whole transaction and
            // must never be treated as durable progress for a prefix batch.
            decode_end_lsn: 0,
            wire_bytes: 0,
            events: Vec::new(),
            boundary: None,
        }
    }

    fn observe_frame(&mut self, wal_start: u64, payload: &[u8]) -> Result<(), IngressError> {
        self.decode_end_lsn = self.decode_end_lsn.max(wal_start);
        self.wire_bytes = self
            .wire_bytes
            .checked_add(
                u64::try_from(payload.len())
                    .map_err(|_| IngressError::CounterOverflow("wire-byte"))?,
            )
            .ok_or(IngressError::CounterOverflow("wire-byte"))?;
        Ok(())
    }

    fn finish(self) -> IngressBatch {
        IngressBatch {
            source_xid: self.source_xid,
            transaction_start_lsn: self.transaction_start_lsn,
            decode_end_lsn: self.decode_end_lsn,
            events: self.events,
            boundary: self.boundary,
        }
    }
}

pub(crate) struct ReplicationIngress {
    transport: ReplicationTransport,
    relations: HashMap<u32, Vec<String>>,
    max_cached_relations: usize,
    active_transaction: Option<ActiveTransaction>,
    streamed_transaction: Option<ActiveTransaction>,
    last_change_lsn: HashMap<u32, (u64, u64)>,
    pending: Option<PendingBatch>,
    reply_requested: bool,
}

impl ReplicationIngress {
    pub(crate) fn new(transport: ReplicationTransport, max_cached_relations: usize) -> Self {
        assert!(max_cached_relations > 0);
        Self {
            transport,
            relations: HashMap::new(),
            max_cached_relations,
            active_transaction: None,
            streamed_transaction: None,
            last_change_lsn: HashMap::new(),
            pending: None,
            reply_requested: false,
        }
    }

    pub(crate) fn transport_mut(&mut self) -> &mut ReplicationTransport {
        &mut self.transport
    }

    pub(crate) fn poll_batch(
        &mut self,
        budget: IngressBudget,
    ) -> Result<IngressPoll, IngressError> {
        if budget.max_events == 0 || budget.max_wire_bytes == 0 || budget.max_poll_time.is_zero() {
            return Err(IngressError::State(
                "ingress budgets must be greater than zero".into(),
            ));
        }

        let started = Instant::now();
        loop {
            match self.transport.poll_copy_data()? {
                CopyDataPoll::Pending => {
                    if let Some(batch) = self.pending.take() {
                        return Ok(IngressPoll::Batch(batch.finish()));
                    }
                    let reply_requested = std::mem::take(&mut self.reply_requested);
                    return Ok(IngressPoll::Pending { reply_requested });
                }
                CopyDataPoll::End => {
                    if let Some(batch) = self.pending.take() {
                        return Ok(IngressPoll::Batch(batch.finish()));
                    }
                    return Ok(IngressPoll::End);
                }
                CopyDataPoll::Message(ReplicationMessage::PrimaryKeepalive {
                    reply_requested,
                    ..
                }) => {
                    self.reply_requested |= reply_requested;
                    if self.pending.is_none() && self.reply_requested {
                        return Ok(IngressPoll::Pending {
                            reply_requested: std::mem::take(&mut self.reply_requested),
                        });
                    }
                }
                CopyDataPoll::Message(ReplicationMessage::XLogData {
                    wal_start,
                    pgoutput,
                    ..
                }) => {
                    let boundary = self.process_pgoutput(wal_start, &pgoutput)?;
                    if self.pending.is_none() {
                        // Non-transactional logical messages are deliberately
                        // outside Shiba's source-row contract.  They acquire no
                        // ingress transaction and are acknowledged only when a
                        // later durable source transaction advances feedback.
                        continue;
                    }
                    let pending = self.pending.as_ref().ok_or_else(|| {
                        IngressError::State(
                            "pgoutput frame did not acquire a source transaction".into(),
                        )
                    })?;
                    if boundary
                        || pending.events.len() >= budget.max_events
                        || pending.wire_bytes as usize >= budget.max_wire_bytes
                    {
                        return Ok(IngressPoll::Batch(
                            self.pending
                                .take()
                                .expect("pending batch checked above")
                                .finish(),
                        ));
                    }
                    if started.elapsed() >= budget.max_poll_time {
                        // The batch stays in memory so a busy stream does not
                        // turn every scheduler quantum into a tiny SPI write.
                        // Yield is still work: Runtime must poll again instead
                        // of sleeping without a replication-socket latch.
                        return Ok(IngressPoll::Yield {
                            reply_requested: std::mem::take(&mut self.reply_requested),
                        });
                    }
                }
            }
        }
    }

    fn process_pgoutput(&mut self, wal_start: u64, payload: &[u8]) -> Result<bool, IngressError> {
        let context = if self
            .active_transaction
            .is_some_and(|transaction| transaction.expected_commit_lsn.is_none())
        {
            ParseContext::Streaming
        } else {
            ParseContext::NonStreaming
        };
        let message =
            pgoutput::parse_with_context(payload, context).map_err(IngressError::Protocol)?;

        let mut boundary = false;
        match message {
            Message::Begin { final_lsn, xid } => {
                if self.active_transaction.is_some() {
                    return Err(IngressError::State(
                        "BEGIN arrived inside an active transaction".into(),
                    ));
                }
                let transaction = ActiveTransaction {
                    xid,
                    start_lsn: wal_start,
                    expected_commit_lsn: Some(final_lsn),
                };
                self.active_transaction = Some(transaction);
                self.ensure_pending(transaction)?;
            }
            Message::StreamStart { xid, first_segment } => {
                if self.active_transaction.is_some() {
                    return Err(IngressError::State(
                        "StreamStart arrived inside another transaction segment".into(),
                    ));
                }
                let transaction = if first_segment {
                    if self.streamed_transaction.is_some() {
                        return Err(IngressError::State(
                            "first StreamStart repeated for an open transaction".into(),
                        ));
                    }
                    ActiveTransaction {
                        xid,
                        start_lsn: wal_start,
                        expected_commit_lsn: None,
                    }
                } else {
                    let transaction = self.streamed_transaction.ok_or_else(|| {
                        IngressError::State("later StreamStart has no first segment".into())
                    })?;
                    if transaction.xid != xid {
                        return Err(IngressError::State(
                            "later StreamStart changed transaction xid".into(),
                        ));
                    }
                    transaction
                };
                self.streamed_transaction = Some(transaction);
                self.active_transaction = Some(transaction);
                self.ensure_pending(transaction)?;
            }
            Message::StreamStop => {
                let transaction = self.require_active_transaction("StreamStop")?;
                if transaction.expected_commit_lsn.is_some()
                    || self.streamed_transaction != Some(transaction)
                {
                    return Err(IngressError::State(
                        "StreamStop has no matching streamed transaction".into(),
                    ));
                }
                self.active_transaction = None;
                boundary = true;
            }
            Message::StreamCommit {
                xid,
                commit_lsn,
                end_lsn,
                ..
            } => {
                if self.active_transaction.is_some() {
                    return Err(IngressError::State(
                        "StreamCommit arrived inside a transaction segment".into(),
                    ));
                }
                let transaction = self.streamed_transaction.take().ok_or_else(|| {
                    IngressError::State("StreamCommit has no matching transaction".into())
                })?;
                if transaction.xid != xid {
                    return Err(IngressError::State(
                        "StreamCommit changed transaction xid".into(),
                    ));
                }
                let pending = self.ensure_pending(transaction)?;
                pending.decode_end_lsn = end_lsn;
                pending.boundary = Some(IngressBoundary::Commit {
                    commit_lsn,
                    end_lsn,
                });
                self.last_change_lsn.remove(&xid);
                boundary = true;
            }
            Message::StreamAbort { xid, subxid } => {
                if self.active_transaction.is_some() {
                    return Err(IngressError::State(
                        "StreamAbort arrived inside a transaction segment".into(),
                    ));
                }
                let transaction = self.streamed_transaction.ok_or_else(|| {
                    IngressError::State("StreamAbort has no matching transaction".into())
                })?;
                if transaction.xid != xid {
                    return Err(IngressError::State(
                        "StreamAbort changed transaction xid".into(),
                    ));
                }
                self.ensure_pending(transaction)?.boundary = if xid == subxid {
                    self.streamed_transaction = None;
                    self.last_change_lsn.remove(&xid);
                    Some(IngressBoundary::AbortTransaction {
                        abort_lsn: wal_start,
                    })
                } else {
                    Some(IngressBoundary::AbortSubtransaction {
                        source_subxid: subxid,
                    })
                };
                boundary = true;
            }
            Message::Relation {
                source_xid,
                relid,
                columns,
            } => {
                let transaction = self.require_active_transaction("Relation")?;
                self.validate_message_xid(source_xid, "Relation")?;
                self.ensure_pending(transaction)?;
                if !self.relations.contains_key(&relid)
                    && self.relations.len() >= self.max_cached_relations
                {
                    return Err(IngressError::State(format!(
                        "relation descriptor cache reached shiba.max_cached_relations ({})",
                        self.max_cached_relations
                    )));
                }
                self.relations.insert(relid, columns);
            }
            Message::Type { source_xid, .. } => {
                let transaction = self.require_active_transaction("Type")?;
                self.validate_message_xid(source_xid, "Type")?;
                self.ensure_pending(transaction)?;
            }
            Message::Origin { .. } => {
                let transaction = self.require_active_transaction("Origin")?;
                self.ensure_pending(transaction)?;
            }
            Message::Logical {
                source_xid,
                transactional,
                ..
            } => {
                if transactional {
                    let transaction =
                        self.require_active_transaction("transactional logical Message")?;
                    self.validate_message_xid(source_xid, "logical Message")?;
                    self.ensure_pending(transaction)?;
                } else if self.active_transaction.is_some() {
                    return Err(IngressError::State(
                        "non-transactional logical Message arrived inside a source transaction"
                            .into(),
                    ));
                }
            }
            Message::Insert {
                source_xid,
                relid,
                row,
            } => {
                let source_subxid = self.validate_message_xid(source_xid, "Insert")?;
                self.push_change(wal_start, source_subxid, relid, &[(0, 1, row)])?;
            }
            Message::Update {
                source_xid,
                relid,
                old,
                new,
            } => {
                let source_subxid = self.validate_message_xid(source_xid, "Update")?;
                self.push_change(
                    wal_start,
                    source_subxid,
                    relid,
                    &[(0, -1, old), (1, 1, new)],
                )?;
            }
            Message::Delete {
                source_xid,
                relid,
                old,
            } => {
                let source_subxid = self.validate_message_xid(source_xid, "Delete")?;
                self.push_change(wal_start, source_subxid, relid, &[(0, -1, old)])?;
            }
            Message::Truncate { .. } => {
                return Err(IngressError::State(
                    "TRUNCATE is outside the DAG delta contract; remove it from the publication"
                        .into(),
                ));
            }
            Message::Commit {
                commit_lsn,
                end_lsn,
            } => {
                let transaction = self.require_active_transaction("COMMIT")?;
                if transaction.expected_commit_lsn != Some(commit_lsn) {
                    return Err(IngressError::State(
                        "Commit LSN differs from Begin final LSN".into(),
                    ));
                }
                let pending = self.ensure_pending(transaction)?;
                pending.decode_end_lsn = end_lsn;
                pending.boundary = Some(IngressBoundary::Commit {
                    commit_lsn,
                    end_lsn,
                });
                self.active_transaction = None;
                self.last_change_lsn.remove(&transaction.xid);
                boundary = true;
            }
        }

        let transaction = self.pending.as_ref().map(|pending| ActiveTransaction {
            xid: pending.source_xid,
            start_lsn: pending.transaction_start_lsn,
            expected_commit_lsn: None,
        });
        if let Some(transaction) = transaction {
            let pending = self.ensure_pending(transaction)?;
            pending.observe_frame(wal_start, payload)?;
        }
        Ok(boundary)
    }

    fn push_change(
        &mut self,
        wal_start: u64,
        source_subxid: u32,
        relid: u32,
        images: &[(u32, i64, Tuple)],
    ) -> Result<(), IngressError> {
        let transaction = self.require_active_transaction("row change")?;
        let xid = transaction.xid;
        let change_ordinal = match self.last_change_lsn.get_mut(&xid) {
            Some((last_lsn, next_ordinal)) if *last_lsn == wal_start => {
                let ordinal = *next_ordinal;
                *next_ordinal = next_ordinal
                    .checked_add(1)
                    .ok_or(IngressError::CounterOverflow("change-ordinal"))?;
                ordinal
            }
            _ => {
                self.last_change_lsn.insert(xid, (wal_start, 1));
                0
            }
        };
        let columns = self.relations.get(&relid).ok_or_else(|| {
            IngressError::State(format!("row change references unknown relation {relid}"))
        })?;
        let mut converted = Vec::with_capacity(images.len());
        for (image_ordinal, weight, tuple) in images {
            converted.push(IngressEvent {
                change_lsn: wal_start,
                change_ordinal,
                image_ordinal: *image_ordinal,
                source_subxid,
                source_oid: relid,
                weight: *weight,
                payload: tuple_to_json(tuple, columns)?,
            });
        }
        self.ensure_pending(transaction)?.events.extend(converted);
        Ok(())
    }

    fn validate_message_xid(
        &self,
        source_xid: Option<u32>,
        message: &'static str,
    ) -> Result<u32, IngressError> {
        let transaction = self.require_active_transaction(message)?;
        match (transaction.expected_commit_lsn, source_xid) {
            (Some(_), None) => Ok(transaction.xid),
            (None, Some(subxid)) => Ok(subxid),
            (Some(_), Some(_)) => Err(IngressError::State(format!(
                "{message} carried a streamed xid outside a stream segment"
            ))),
            (None, None) => Err(IngressError::State(format!(
                "{message} omitted its xid inside a stream segment"
            ))),
        }
    }

    fn ensure_pending(
        &mut self,
        transaction: ActiveTransaction,
    ) -> Result<&mut PendingBatch, IngressError> {
        if let Some(pending) = self.pending.as_ref() {
            if pending.source_xid != transaction.xid
                || pending.transaction_start_lsn != transaction.start_lsn
            {
                return Err(IngressError::State(
                    "one ingress batch crossed source transactions".into(),
                ));
            }
        } else {
            self.pending = Some(PendingBatch::new(transaction));
        }
        Ok(self.pending.as_mut().expect("pending batch initialized"))
    }

    fn require_active_transaction(
        &self,
        message: &'static str,
    ) -> Result<ActiveTransaction, IngressError> {
        self.active_transaction.ok_or_else(|| {
            IngressError::State(format!("{message} arrived outside a source transaction"))
        })
    }
}

fn tuple_to_json(tuple: &Tuple, columns: &[String]) -> Result<Value, IngressError> {
    if tuple.len() != columns.len() {
        return Err(IngressError::State(format!(
            "tuple has {} values but relation metadata has {} columns",
            tuple.len(),
            columns.len()
        )));
    }
    let mut object = Map::new();
    for (column, value) in columns.iter().zip(tuple) {
        object.insert(
            column.clone(),
            value
                .as_ref()
                .map_or(Value::Null, |value| Value::String(value.clone())),
        );
    }
    Ok(Value::Object(object))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tuple_conversion_is_bounded_to_one_complete_tuple() {
        assert_eq!(
            tuple_to_json(
                &vec![Some("1".into()), None],
                &["id".into(), "value".into()]
            )
            .unwrap(),
            serde_json::json!({"id": "1", "value": null})
        );
        assert!(tuple_to_json(&vec![Some("1".into())], &[]).is_err());
    }
}
