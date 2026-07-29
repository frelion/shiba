//! Bounded pgoutput-v2 ingress state machine.
//!
//! Replication I/O happens only outside SPI transactions.  This module turns
//! complete CopyData frames into one bounded, single-source-transaction batch;
//! the Runtime persists that batch in a separate short transaction.

use crate::pgoutput::{self, Message, ParseContext, Tuple};
use crate::replication::{
    CopyDataPoll, ReplicationError, ReplicationMessage, ReplicationTransport,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

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
pub(crate) enum IngressFinalization {
    Commit { commit_lsn: u64, end_lsn: u64 },
    Abort { end_lsn: u64 },
    SubtransactionAbort { subxid: u32, control_lsn: u64 },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IngressBatch {
    pub source_xid: u32,
    pub streamed: bool,
    pub identity_lsn: u64,
    pub decode_end_lsn: u64,
    pub digest: [u8; 32],
    pub message_count: u64,
    pub wire_bytes: u64,
    pub events: Vec<IngressEvent>,
    pub finalization: Option<IngressFinalization>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IngressBudget {
    pub max_events: usize,
    pub max_wire_bytes: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum IngressPoll {
    Batch(IngressBatch),
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
                write!(formatter, "v2 ingress {counter} counter overflow")
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
enum ActiveSegment {
    Ordinary { xid: u32, first_lsn: u64 },
    Streaming { xid: u32, first_lsn: u64 },
}

impl ActiveSegment {
    fn xid(self) -> u32 {
        match self {
            Self::Ordinary { xid, .. } | Self::Streaming { xid, .. } => xid,
        }
    }

    fn first_lsn(self) -> u64 {
        match self {
            Self::Ordinary { first_lsn, .. } | Self::Streaming { first_lsn, .. } => first_lsn,
        }
    }

    fn is_streamed(self) -> bool {
        matches!(self, Self::Streaming { .. })
    }
}

struct PendingBatch {
    source_xid: u32,
    streamed: bool,
    identity_lsn: u64,
    decode_end_lsn: u64,
    hasher: Sha256,
    message_count: u64,
    wire_bytes: u64,
    events: Vec<IngressEvent>,
    finalization: Option<IngressFinalization>,
}

impl PendingBatch {
    fn new(segment: ActiveSegment) -> Self {
        Self {
            source_xid: segment.xid(),
            streamed: segment.is_streamed(),
            identity_lsn: segment.first_lsn(),
            decode_end_lsn: segment.first_lsn(),
            hasher: Sha256::new(),
            message_count: 0,
            wire_bytes: 0,
            events: Vec::new(),
            finalization: None,
        }
    }

    fn observe_frame(&mut self, wal_start: u64, payload: &[u8]) -> Result<(), IngressError> {
        self.decode_end_lsn = self.decode_end_lsn.max(wal_start);
        self.message_count = self
            .message_count
            .checked_add(1)
            .ok_or(IngressError::CounterOverflow("message"))?;
        self.wire_bytes = self
            .wire_bytes
            .checked_add(
                u64::try_from(payload.len())
                    .map_err(|_| IngressError::CounterOverflow("wire-byte"))?,
            )
            .ok_or(IngressError::CounterOverflow("wire-byte"))?;
        self.hasher.update(wal_start.to_be_bytes());
        self.hasher.update(
            u64::try_from(payload.len())
                .map_err(|_| IngressError::CounterOverflow("wire-byte"))?
                .to_be_bytes(),
        );
        self.hasher.update(payload);
        Ok(())
    }

    fn finish(self) -> IngressBatch {
        IngressBatch {
            source_xid: self.source_xid,
            streamed: self.streamed,
            identity_lsn: self.identity_lsn,
            decode_end_lsn: self.decode_end_lsn,
            digest: self.hasher.finalize().into(),
            message_count: self.message_count,
            wire_bytes: self.wire_bytes,
            events: self.events,
            finalization: self.finalization,
        }
    }
}

pub(crate) struct ReplicationIngress {
    transport: ReplicationTransport,
    relations: HashMap<u32, Vec<String>>,
    open_streams: HashMap<u32, u64>,
    active_segment: Option<ActiveSegment>,
    last_change_lsn: HashMap<u32, (u64, u64)>,
    pending: Option<PendingBatch>,
    reply_requested: bool,
}

impl ReplicationIngress {
    pub(crate) fn new(
        transport: ReplicationTransport,
        open_streams: impl IntoIterator<Item = (u32, u64)>,
    ) -> Self {
        Self {
            transport,
            relations: HashMap::new(),
            open_streams: open_streams.into_iter().collect(),
            active_segment: None,
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
        if budget.max_events == 0 || budget.max_wire_bytes == 0 {
            return Err(IngressError::State(
                "ingress budgets must be greater than zero".into(),
            ));
        }

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
                }
            }
        }
    }

    fn process_pgoutput(&mut self, wal_start: u64, payload: &[u8]) -> Result<bool, IngressError> {
        let context = match self.active_segment {
            Some(ActiveSegment::Streaming { .. }) => ParseContext::Streaming,
            _ => ParseContext::NonStreaming,
        };
        let message =
            pgoutput::parse_with_context(payload, context).map_err(IngressError::Protocol)?;

        let mut boundary = false;
        match message {
            Message::Begin { final_lsn, xid } => {
                if self.active_segment.is_some() {
                    return Err(IngressError::State(
                        "ordinary BEGIN arrived inside an active segment".into(),
                    ));
                }
                let segment = ActiveSegment::Ordinary {
                    xid,
                    // Begin.final_lsn is the stable transaction identity.
                    // XLogData.wal_start is only an envelope position.
                    first_lsn: final_lsn,
                };
                self.active_segment = Some(segment);
                self.ensure_pending(segment)?;
            }
            Message::StreamStart { xid, first_segment } => {
                if self.active_segment.is_some() {
                    return Err(IngressError::State(
                        "Stream Start arrived inside an active segment".into(),
                    ));
                }
                let first_lsn = if first_segment {
                    // "first" is scoped to the current decoding session.  A
                    // reconnect after a durably persisted mid-transaction
                    // position can emit another first segment for an already
                    // open durable transaction; preserve its original key.
                    *self.open_streams.entry(xid).or_insert(wal_start)
                } else {
                    *self.open_streams.get(&xid).ok_or_else(|| {
                        IngressError::State(format!("later stream segment for unknown xid {xid}"))
                    })?
                };
                let segment = ActiveSegment::Streaming { xid, first_lsn };
                self.active_segment = Some(segment);
                self.ensure_pending(segment)?;
            }
            Message::Relation {
                source_xid,
                relid,
                columns,
            } => {
                let segment = self.require_active_segment("Relation")?;
                self.validate_message_xid(segment, source_xid, "Relation")?;
                self.ensure_pending(segment)?;
                self.relations.insert(relid, columns);
            }
            Message::Type { source_xid, .. } => {
                let segment = self.require_active_segment("Type")?;
                self.validate_message_xid(segment, source_xid, "Type")?;
                self.ensure_pending(segment)?;
            }
            Message::Origin { .. } => {
                let segment = self.require_ordinary_segment("Origin")?;
                self.ensure_pending(segment)?;
            }
            Message::LogicalMessage {
                source_xid,
                transactional,
                ..
            } => {
                if transactional {
                    let segment = self.require_active_segment("transactional logical Message")?;
                    self.validate_message_xid(segment, source_xid, "logical Message")?;
                    self.ensure_pending(segment)?;
                } else if self.active_segment.is_some() {
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
                let source_subxid = self.source_subxid(source_xid, "Insert")?;
                self.push_change(wal_start, source_subxid, relid, &[(0, 1, row)])?;
            }
            Message::Update {
                source_xid,
                relid,
                old,
                new,
            } => {
                let source_subxid = self.source_subxid(source_xid, "Update")?;
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
                let source_subxid = self.source_subxid(source_xid, "Delete")?;
                self.push_change(wal_start, source_subxid, relid, &[(0, -1, old)])?;
            }
            Message::Truncate { .. } => {
                return Err(IngressError::State(
                    "TRUNCATE is outside the v2 DAG delta contract; remove it from the publication"
                        .into(),
                ));
            }
            Message::StreamStop => {
                let segment = self.require_streaming_segment("Stream Stop")?;
                self.ensure_pending(segment)?;
                self.active_segment = None;
                boundary = true;
            }
            Message::Commit {
                commit_lsn,
                end_lsn,
            } => {
                let segment = self.require_ordinary_segment("COMMIT")?;
                let pending = self.ensure_pending(segment)?;
                pending.decode_end_lsn = end_lsn;
                pending.finalization = Some(IngressFinalization::Commit {
                    commit_lsn,
                    end_lsn,
                });
                self.active_segment = None;
                self.last_change_lsn.remove(&segment.xid());
                boundary = true;
            }
            Message::StreamCommit {
                xid,
                commit_lsn,
                end_lsn,
                ..
            } => {
                if self.active_segment.is_some() {
                    return Err(IngressError::State(
                        "Stream Commit arrived inside an active segment".into(),
                    ));
                }
                let first_lsn = *self.open_streams.get(&xid).ok_or_else(|| {
                    IngressError::State(format!("Stream Commit for unknown xid {xid}"))
                })?;
                let segment = ActiveSegment::Streaming { xid, first_lsn };
                let pending = self.ensure_pending(segment)?;
                pending.decode_end_lsn = end_lsn;
                pending.finalization = Some(IngressFinalization::Commit {
                    commit_lsn,
                    end_lsn,
                });
                self.open_streams.remove(&xid);
                self.last_change_lsn.remove(&xid);
                boundary = true;
            }
            Message::StreamAbort { xid, subxid } => {
                if self.active_segment.is_some() {
                    return Err(IngressError::State(
                        "Stream Abort arrived inside an active segment".into(),
                    ));
                }
                let first_lsn = *self.open_streams.get(&xid).ok_or_else(|| {
                    IngressError::State(format!("Stream Abort for unknown xid {xid}"))
                })?;
                let segment = ActiveSegment::Streaming { xid, first_lsn };
                let pending = self.ensure_pending(segment)?;
                if xid == subxid {
                    pending.finalization = Some(IngressFinalization::Abort { end_lsn: wal_start });
                    self.open_streams.remove(&xid);
                    self.last_change_lsn.remove(&xid);
                } else {
                    pending.finalization = Some(IngressFinalization::SubtransactionAbort {
                        subxid,
                        control_lsn: wal_start,
                    });
                }
                boundary = true;
            }
        }

        let segment = self
            .pending
            .as_ref()
            .map(|pending| ActiveSegment::Streaming {
                xid: pending.source_xid,
                first_lsn: pending.identity_lsn,
            });
        if let Some(segment) = segment {
            let pending = self.ensure_pending(segment)?;
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
        let segment = self.require_active_segment("row change")?;
        let xid = segment.xid();
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
        self.ensure_pending(segment)?.events.extend(converted);
        Ok(())
    }

    fn source_subxid(
        &self,
        source_xid: Option<u32>,
        message: &'static str,
    ) -> Result<u32, IngressError> {
        let segment = self.require_active_segment(message)?;
        self.validate_message_xid(segment, source_xid, message)?;
        Ok(source_xid.unwrap_or_else(|| segment.xid()))
    }

    fn validate_message_xid(
        &self,
        segment: ActiveSegment,
        source_xid: Option<u32>,
        message: &'static str,
    ) -> Result<(), IngressError> {
        match (segment, source_xid) {
            (ActiveSegment::Ordinary { .. }, None) | (ActiveSegment::Streaming { .. }, Some(_)) => {
                Ok(())
            }
            (ActiveSegment::Ordinary { .. }, Some(_)) => Err(IngressError::State(format!(
                "{message} used a streamed xid inside an ordinary transaction"
            ))),
            (ActiveSegment::Streaming { .. }, None) => Err(IngressError::State(format!(
                "{message} omitted its xid inside a streamed transaction"
            ))),
        }
    }

    fn ensure_pending(
        &mut self,
        segment: ActiveSegment,
    ) -> Result<&mut PendingBatch, IngressError> {
        if let Some(pending) = self.pending.as_ref() {
            if pending.source_xid != segment.xid() || pending.identity_lsn != segment.first_lsn() {
                return Err(IngressError::State(
                    "one ingress batch crossed source transactions".into(),
                ));
            }
        } else {
            self.pending = Some(PendingBatch::new(segment));
        }
        Ok(self.pending.as_mut().expect("pending batch initialized"))
    }

    fn require_active_segment(&self, message: &'static str) -> Result<ActiveSegment, IngressError> {
        self.active_segment.ok_or_else(|| {
            IngressError::State(format!("{message} arrived outside a source transaction"))
        })
    }

    fn require_streaming_segment(
        &self,
        message: &'static str,
    ) -> Result<ActiveSegment, IngressError> {
        match self.require_active_segment(message)? {
            segment @ ActiveSegment::Streaming { .. } => Ok(segment),
            ActiveSegment::Ordinary { .. } => Err(IngressError::State(format!(
                "{message} arrived inside an ordinary transaction"
            ))),
        }
    }

    fn require_ordinary_segment(
        &self,
        message: &'static str,
    ) -> Result<ActiveSegment, IngressError> {
        match self.require_active_segment(message)? {
            segment @ ActiveSegment::Ordinary { .. } => Ok(segment),
            ActiveSegment::Streaming { .. } => Err(IngressError::State(format!(
                "{message} arrived inside a streamed segment"
            ))),
        }
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

    #[test]
    fn pending_batch_digest_includes_wal_position_and_frame_boundary() {
        let segment = ActiveSegment::Streaming {
            xid: 7,
            first_lsn: 10,
        };
        let mut first = PendingBatch::new(segment);
        first.observe_frame(10, b"ab").unwrap();
        let mut second = PendingBatch::new(segment);
        second.observe_frame(11, b"ab").unwrap();
        let mut third = PendingBatch::new(segment);
        third.observe_frame(10, b"a").unwrap();
        third.observe_frame(10, b"b").unwrap();
        assert_ne!(first.finish().digest, second.finish().digest);
        assert_ne!(
            PendingBatch::new(segment).finish().digest,
            third.finish().digest
        );
    }
}
