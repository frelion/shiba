use crate::IngressError;

use std::time::{SystemTime, UNIX_EPOCH};

const XLOG_DATA_HEADER_BYTES: usize = 25;
const KEEPALIVE_BYTES: usize = 18;
const FEEDBACK_BYTES: usize = 34;
const POSTGRES_EPOCH_UNIX_SECONDS: u64 = 946_684_800;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationMessage<'a> {
    XLogData {
        wal_start: u64,
        wal_end: u64,
        server_time_micros: u64,
        data: &'a [u8],
    },
    Keepalive {
        wal_end: u64,
        server_time_micros: u64,
        reply_requested: bool,
    },
}

/// Decodes one complete `CopyData` payload returned by libpq.
///
/// # Errors
/// Rejects truncated frames, unknown tags, empty `XLogData` payloads, and invalid
/// keepalive reply flags.
pub fn parse_replication_message(payload: &[u8]) -> Result<ReplicationMessage<'_>, IngressError> {
    match payload.first() {
        Some(b'w') if payload.len() > XLOG_DATA_HEADER_BYTES => Ok(ReplicationMessage::XLogData {
            wal_start: read_u64(payload, 1)?,
            wal_end: read_u64(payload, 9)?,
            server_time_micros: read_u64(payload, 17)?,
            data: &payload[XLOG_DATA_HEADER_BYTES..],
        }),
        Some(b'w') => Err(IngressError::InvalidEnvelope("truncated XLogData")),
        Some(b'k') if payload.len() == KEEPALIVE_BYTES => {
            let reply_requested = match payload[17] {
                0 => false,
                1 => true,
                _ => {
                    return Err(IngressError::InvalidEnvelope(
                        "invalid keepalive reply flag",
                    ));
                }
            };
            Ok(ReplicationMessage::Keepalive {
                wal_end: read_u64(payload, 1)?,
                server_time_micros: read_u64(payload, 9)?,
                reply_requested,
            })
        }
        Some(b'k') => Err(IngressError::InvalidEnvelope("invalid keepalive length")),
        Some(_) => Err(IngressError::InvalidEnvelope("unknown CopyData tag")),
        None => Err(IngressError::InvalidEnvelope("empty CopyData payload")),
    }
}

/// Encodes one `PostgreSQL` standby-status update for an already durable LSN.
///
/// # Errors
/// Rejects timestamps before the `PostgreSQL` epoch or outside its signed
/// microsecond representation.
pub fn encode_feedback(
    durable_lsn: u64,
    now: SystemTime,
) -> Result<[u8; FEEDBACK_BYTES], IngressError> {
    let unix_duration = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| IngressError::InvalidEnvelope("feedback time predates Unix epoch"))?;
    let postgres_duration = unix_duration
        .checked_sub(std::time::Duration::from_secs(POSTGRES_EPOCH_UNIX_SECONDS))
        .ok_or(IngressError::InvalidEnvelope(
            "feedback time predates PostgreSQL epoch",
        ))?;
    let postgres_micros = i64::try_from(postgres_duration.as_micros()).map_err(|_| {
        IngressError::InvalidEnvelope("feedback time exceeds PostgreSQL timestamp range")
    })?;

    let mut feedback = [0_u8; FEEDBACK_BYTES];
    feedback[0] = b'r';
    feedback[1..9].copy_from_slice(&durable_lsn.to_be_bytes());
    feedback[9..17].copy_from_slice(&durable_lsn.to_be_bytes());
    feedback[17..25].copy_from_slice(&durable_lsn.to_be_bytes());
    feedback[25..33].copy_from_slice(&postgres_micros.to_be_bytes());
    feedback[33] = 0;
    Ok(feedback)
}

fn read_u64(bytes: &[u8], at: usize) -> Result<u64, IngressError> {
    let value = bytes
        .get(at..at + 8)
        .ok_or(IngressError::InvalidEnvelope("truncated integer"))?;
    Ok(u64::from_be_bytes(value.try_into().map_err(|_| {
        IngressError::InvalidEnvelope("invalid integer width")
    })?))
}
