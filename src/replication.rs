//! Synchronous libpq transport for PostgreSQL 17 logical replication.
//!
//! Connection setup and `START_REPLICATION` are synchronous. Once streaming
//! starts, CopyData reads and standby-status writes are non-blocking. This
//! module parses only the replication envelope; `pgoutput` payload bytes are
//! deliberately left opaque for the caller.

use std::error::Error;
use std::ffi::{CStr, CString};
use std::fmt;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::{self, NonNull};

const PGRES_COPY_BOTH: c_int = 8;
const CONNECTION_OK: c_int = 0;
const POSTGRES_EPOCH_UNIX_SECONDS: i64 = 946_684_800;
const MICROS_PER_SECOND: i64 = 1_000_000;

#[repr(C)]
struct PGconn {
    _private: [u8; 0],
}

#[repr(C)]
struct PGresult {
    _private: [u8; 0],
}

#[link(name = "pq")]
unsafe extern "C" {
    fn PQconnectdbParams(
        keywords: *const *const c_char,
        values: *const *const c_char,
        expand_dbname: c_int,
    ) -> *mut PGconn;
    fn PQstatus(conn: *const PGconn) -> c_int;
    fn PQerrorMessage(conn: *const PGconn) -> *mut c_char;
    fn PQsetnonblocking(conn: *mut PGconn, arg: c_int) -> c_int;
    fn PQsocket(conn: *const PGconn) -> c_int;
    fn PQexec(conn: *mut PGconn, query: *const c_char) -> *mut PGresult;
    fn PQresultStatus(result: *const PGresult) -> c_int;
    fn PQresultErrorMessage(result: *const PGresult) -> *mut c_char;
    fn PQclear(result: *mut PGresult);
    fn PQconsumeInput(conn: *mut PGconn) -> c_int;
    fn PQgetCopyData(conn: *mut PGconn, buffer: *mut *mut c_char, async_: c_int) -> c_int;
    fn PQputCopyData(conn: *mut PGconn, buffer: *const c_char, nbytes: c_int) -> c_int;
    fn PQflush(conn: *mut PGconn) -> c_int;
    fn PQescapeLiteral(conn: *mut PGconn, value: *const c_char, length: usize) -> *mut c_char;
    fn PQfreemem(pointer: *mut c_void);
    fn PQfinish(conn: *mut PGconn);
}

/// Options passed to PostgreSQL's `START_REPLICATION` command.
#[derive(Debug, Clone, Copy)]
pub struct StartReplicationOptions<'a> {
    pub slot: &'a str,
    pub start_lsn: u64,
    pub publication_names: &'a [&'a str],
}

/// A CopyData message after removing only the physical replication envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMessage {
    XLogData {
        wal_start: u64,
        wal_end: u64,
        server_time: i64,
        pgoutput: Vec<u8>,
    },
    PrimaryKeepalive {
        wal_end: u64,
        server_time: i64,
        reply_requested: bool,
    },
}

/// Result of a non-blocking CopyData poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyDataPoll {
    Message(ReplicationMessage),
    Pending,
    End,
}

/// Result of queuing or flushing a non-blocking standby status update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStatus {
    /// The complete message reached the operating-system socket.
    Flushed,
    /// libpq did not queue the message; retry `send_standby_status`.
    WouldBlock,
    /// libpq queued the message; call `flush` until it returns `Flushed`.
    PendingFlush,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    EmptyEnvelope,
    UnknownEnvelope(u8),
    TruncatedEnvelope {
        kind: &'static str,
        expected_at_least: usize,
        actual: usize,
    },
    InvalidKeepaliveLength(usize),
    InvalidReplyRequested(u8),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyEnvelope => formatter.write_str("empty replication CopyData envelope"),
            Self::UnknownEnvelope(tag) => {
                write!(formatter, "unknown replication CopyData tag 0x{tag:02x}")
            }
            Self::TruncatedEnvelope {
                kind,
                expected_at_least,
                actual,
            } => write!(
                formatter,
                "truncated {kind} envelope: expected at least {expected_at_least} bytes, got {actual}"
            ),
            Self::InvalidKeepaliveLength(actual) => write!(
                formatter,
                "invalid primary keepalive envelope: expected 18 bytes, got {actual}"
            ),
            Self::InvalidReplyRequested(value) => {
                write!(formatter, "invalid reply-requested byte {value}")
            }
        }
    }
}

impl Error for ProtocolError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationError {
    InteriorNul { field: &'static str },
    InvalidSlotName(String),
    MissingPublication,
    InvalidPublicationName,
    Connect(String),
    StartReplication(String),
    ConfigureNonblocking(String),
    InvalidState(&'static str),
    ConsumeInput(String),
    ReceiveCopyData(String),
    CopyDataTooLarge(usize),
    SendStatus(String),
    Flush(String),
    EscapePublicationNames(String),
    Protocol(ProtocolError),
}

impl fmt::Display for ReplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InteriorNul { field } => write!(formatter, "{field} contains a NUL byte"),
            Self::InvalidSlotName(slot) => {
                write!(
                    formatter,
                    "invalid PostgreSQL replication slot name {slot:?}"
                )
            }
            Self::MissingPublication => formatter.write_str("at least one publication is required"),
            Self::InvalidPublicationName => {
                formatter.write_str("publication names must be non-empty and contain no NUL bytes")
            }
            Self::Connect(message) => write!(formatter, "replication connection failed: {message}"),
            Self::StartReplication(message) => {
                write!(formatter, "START_REPLICATION failed: {message}")
            }
            Self::ConfigureNonblocking(message) => {
                write!(
                    formatter,
                    "failed to enable non-blocking libpq mode: {message}"
                )
            }
            Self::InvalidState(message) => {
                write!(formatter, "invalid replication state: {message}")
            }
            Self::ConsumeInput(message) => write!(formatter, "failed to consume input: {message}"),
            Self::ReceiveCopyData(message) => {
                write!(formatter, "failed to receive CopyData: {message}")
            }
            Self::CopyDataTooLarge(length) => {
                write!(
                    formatter,
                    "CopyData frame length {length} exceeds libpq's int limit"
                )
            }
            Self::SendStatus(message) => {
                write!(formatter, "failed to send standby status: {message}")
            }
            Self::Flush(message) => write!(formatter, "failed to flush libpq output: {message}"),
            Self::EscapePublicationNames(message) => {
                write!(formatter, "failed to escape publication names: {message}")
            }
            Self::Protocol(error) => error.fmt(formatter),
        }
    }
}

impl Error for ReplicationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ProtocolError> for ReplicationError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

/// An owned libpq replication connection.
///
/// `Drop` always calls `PQfinish`, including after a failed
/// `START_REPLICATION` attempt.
pub struct ReplicationTransport {
    conn: NonNull<PGconn>,
    streaming: bool,
}

impl ReplicationTransport {
    /// Connect using a libpq conninfo string while forcing
    /// `replication=database`.
    pub fn connect(conninfo: &str) -> Result<Self, ReplicationError> {
        let conninfo = cstring(conninfo, "conninfo")?;
        let dbname = c"dbname";
        let replication = c"replication";
        let application_name = c"application_name";
        let connect_timeout = c"connect_timeout";
        let database = c"database";
        let shiba = c"shiba";
        let five_seconds = c"5";
        let keywords = [
            dbname.as_ptr(),
            replication.as_ptr(),
            application_name.as_ptr(),
            connect_timeout.as_ptr(),
            ptr::null(),
        ];
        let values = [
            conninfo.as_ptr(),
            database.as_ptr(),
            shiba.as_ptr(),
            five_seconds.as_ptr(),
            ptr::null(),
        ];

        let conn =
            NonNull::new(unsafe { PQconnectdbParams(keywords.as_ptr(), values.as_ptr(), 1) })
                .ok_or_else(|| ReplicationError::Connect("libpq returned a null PGconn".into()))?;

        let transport = Self {
            conn,
            streaming: false,
        };
        if unsafe { PQstatus(transport.conn.as_ptr()) } != CONNECTION_OK {
            return Err(ReplicationError::Connect(transport.connection_error()));
        }
        Ok(transport)
    }

    /// Start pgoutput protocol version 2 with in-progress transaction
    /// streaming enabled.
    pub fn start_replication(
        &mut self,
        options: StartReplicationOptions<'_>,
    ) -> Result<(), ReplicationError> {
        if self.streaming {
            return Err(ReplicationError::InvalidState(
                "START_REPLICATION has already succeeded",
            ));
        }
        validate_slot_name(options.slot)?;
        if options.publication_names.is_empty() {
            return Err(ReplicationError::MissingPublication);
        }
        if options
            .publication_names
            .iter()
            .any(|name| name.is_empty() || name.as_bytes().contains(&0))
        {
            return Err(ReplicationError::InvalidPublicationName);
        }

        let publication_list = options
            .publication_names
            .iter()
            .map(|name| quote_identifier(name))
            .collect::<Vec<_>>()
            .join(", ");
        let publication_list =
            CString::new(publication_list).map_err(|_| ReplicationError::InvalidPublicationName)?;
        let escaped = unsafe {
            PQescapeLiteral(
                self.conn.as_ptr(),
                publication_list.as_ptr(),
                publication_list.as_bytes().len(),
            )
        };
        let escaped = NonNull::new(escaped)
            .ok_or_else(|| ReplicationError::EscapePublicationNames(self.connection_error()))?;
        let escaped_publications = unsafe { CStr::from_ptr(escaped.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        unsafe { PQfreemem(escaped.as_ptr().cast()) };

        let command = format!(
            "START_REPLICATION SLOT {} LOGICAL {} \
             (proto_version '2', publication_names {}, streaming 'on')",
            options.slot,
            format_lsn(options.start_lsn),
            escaped_publications
        );
        let command = cstring(&command, "START_REPLICATION command")?;
        let result = NonNull::new(unsafe { PQexec(self.conn.as_ptr(), command.as_ptr()) })
            .ok_or_else(|| ReplicationError::StartReplication(self.connection_error()))?;
        let status = unsafe { PQresultStatus(result.as_ptr()) };
        if status != PGRES_COPY_BOTH {
            let message = result_error(result);
            unsafe { PQclear(result.as_ptr()) };
            return Err(ReplicationError::StartReplication(message));
        }
        unsafe { PQclear(result.as_ptr()) };

        if unsafe { PQsetnonblocking(self.conn.as_ptr(), 1) } != 0 {
            return Err(ReplicationError::ConfigureNonblocking(
                self.connection_error(),
            ));
        }
        self.streaming = true;
        Ok(())
    }

    /// Return the libpq socket for integration with a scheduler's poll set.
    pub fn socket(&self) -> Result<c_int, ReplicationError> {
        let socket = unsafe { PQsocket(self.conn.as_ptr()) };
        if socket < 0 {
            Err(ReplicationError::InvalidState(
                "libpq connection has no open socket",
            ))
        } else {
            Ok(socket)
        }
    }

    /// Consume currently available input and retrieve at most one CopyData
    /// frame without blocking.
    pub fn poll_copy_data(&mut self) -> Result<CopyDataPoll, ReplicationError> {
        self.require_streaming()?;
        if unsafe { PQconsumeInput(self.conn.as_ptr()) } == 0 {
            return Err(ReplicationError::ConsumeInput(self.connection_error()));
        }

        let mut buffer = ptr::null_mut();
        let length = unsafe { PQgetCopyData(self.conn.as_ptr(), &mut buffer, 1) };
        match length {
            0 => Ok(CopyDataPoll::Pending),
            -1 => Ok(CopyDataPoll::End),
            -2 => Err(ReplicationError::ReceiveCopyData(self.connection_error())),
            length if length > 0 => {
                let length = usize::try_from(length)
                    .map_err(|_| ReplicationError::CopyDataTooLarge(length as usize))?;
                let buffer = NonNull::new(buffer).ok_or_else(|| {
                    ReplicationError::ReceiveCopyData(
                        "libpq returned a positive length with a null buffer".into(),
                    )
                })?;
                let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast(), length) };
                let parsed = parse_copy_data(bytes);
                unsafe { PQfreemem(buffer.as_ptr().cast()) };
                parsed.map(CopyDataPoll::Message).map_err(Into::into)
            }
            _ => Err(ReplicationError::ReceiveCopyData(
                "libpq returned an undocumented CopyData status".into(),
            )),
        }
    }

    /// Queue a PostgreSQL standby status update (`CopyData('r', ...)`) without
    /// blocking.
    pub fn send_standby_status(
        &mut self,
        write_lsn: u64,
        flush_lsn: u64,
        apply_lsn: u64,
        reply_requested: bool,
    ) -> Result<WriteStatus, ReplicationError> {
        self.require_streaming()?;
        let frame = encode_standby_status(
            write_lsn,
            flush_lsn,
            apply_lsn,
            postgres_timestamp_now(),
            reply_requested,
        );
        let length = c_int::try_from(frame.len())
            .map_err(|_| ReplicationError::CopyDataTooLarge(frame.len()))?;
        match unsafe { PQputCopyData(self.conn.as_ptr(), frame.as_ptr().cast::<c_char>(), length) }
        {
            1 => self.flush(),
            0 => Ok(WriteStatus::WouldBlock),
            -1 => Err(ReplicationError::SendStatus(self.connection_error())),
            _ => Err(ReplicationError::SendStatus(
                "libpq returned an undocumented PQputCopyData status".into(),
            )),
        }
    }

    /// Flush data already queued in libpq without blocking.
    pub fn flush(&mut self) -> Result<WriteStatus, ReplicationError> {
        self.require_streaming()?;
        match unsafe { PQflush(self.conn.as_ptr()) } {
            0 => Ok(WriteStatus::Flushed),
            1 => Ok(WriteStatus::PendingFlush),
            -1 => Err(ReplicationError::Flush(self.connection_error())),
            _ => Err(ReplicationError::Flush(
                "libpq returned an undocumented PQflush status".into(),
            )),
        }
    }

    fn require_streaming(&self) -> Result<(), ReplicationError> {
        if self.streaming {
            Ok(())
        } else {
            Err(ReplicationError::InvalidState(
                "logical replication has not been started",
            ))
        }
    }

    fn connection_error(&self) -> String {
        unsafe { copy_error(PQerrorMessage(self.conn.as_ptr())) }
    }
}

impl Drop for ReplicationTransport {
    fn drop(&mut self) {
        unsafe { PQfinish(self.conn.as_ptr()) };
    }
}

/// Parse an XLogData (`w`) or primary keepalive (`k`) CopyData payload.
pub fn parse_copy_data(input: &[u8]) -> Result<ReplicationMessage, ProtocolError> {
    match input.first().copied() {
        None => Err(ProtocolError::EmptyEnvelope),
        Some(b'w') => {
            require_length(input, 25, "XLogData")?;
            Ok(ReplicationMessage::XLogData {
                wal_start: read_u64(input, 1, "XLogData")?,
                wal_end: read_u64(input, 9, "XLogData")?,
                server_time: read_i64(input, 17, "XLogData")?,
                pgoutput: input[25..].to_vec(),
            })
        }
        Some(b'k') => {
            require_length(input, 18, "primary keepalive")?;
            if input.len() != 18 {
                return Err(ProtocolError::InvalidKeepaliveLength(input.len()));
            }
            let reply_requested = match input[17] {
                0 => false,
                1 => true,
                value => return Err(ProtocolError::InvalidReplyRequested(value)),
            };
            Ok(ReplicationMessage::PrimaryKeepalive {
                wal_end: read_u64(input, 1, "primary keepalive")?,
                server_time: read_i64(input, 9, "primary keepalive")?,
                reply_requested,
            })
        }
        Some(tag) => Err(ProtocolError::UnknownEnvelope(tag)),
    }
}

/// Encode the payload of a standby status update CopyData message.
pub fn encode_standby_status(
    write_lsn: u64,
    flush_lsn: u64,
    apply_lsn: u64,
    client_time: i64,
    reply_requested: bool,
) -> [u8; 34] {
    let mut frame = [0; 34];
    frame[0] = b'r';
    frame[1..9].copy_from_slice(&write_lsn.to_be_bytes());
    frame[9..17].copy_from_slice(&flush_lsn.to_be_bytes());
    frame[17..25].copy_from_slice(&apply_lsn.to_be_bytes());
    frame[25..33].copy_from_slice(&client_time.to_be_bytes());
    frame[33] = u8::from(reply_requested);
    frame
}

fn validate_slot_name(slot: &str) -> Result<(), ReplicationError> {
    if slot.is_empty()
        || slot.len() > 63
        || !slot
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ReplicationError::InvalidSlotName(slot.to_owned()));
    }
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:08X}", lsn >> 32, lsn as u32)
}

fn postgres_timestamp_now() -> i64 {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = i64::try_from(unix.as_secs()).unwrap_or(i64::MAX);
    let micros = i64::from(unix.subsec_micros());
    seconds
        .saturating_sub(POSTGRES_EPOCH_UNIX_SECONDS)
        .saturating_mul(MICROS_PER_SECOND)
        .saturating_add(micros)
}

fn require_length(
    input: &[u8],
    expected_at_least: usize,
    kind: &'static str,
) -> Result<(), ProtocolError> {
    if input.len() < expected_at_least {
        Err(ProtocolError::TruncatedEnvelope {
            kind,
            expected_at_least,
            actual: input.len(),
        })
    } else {
        Ok(())
    }
}

fn read_u64(input: &[u8], offset: usize, kind: &'static str) -> Result<u64, ProtocolError> {
    require_length(input, offset + 8, kind)?;
    Ok(u64::from_be_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("length checked above"),
    ))
}

fn read_i64(input: &[u8], offset: usize, kind: &'static str) -> Result<i64, ProtocolError> {
    require_length(input, offset + 8, kind)?;
    Ok(i64::from_be_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("length checked above"),
    ))
}

fn cstring(value: &str, field: &'static str) -> Result<CString, ReplicationError> {
    CString::new(value).map_err(|_| ReplicationError::InteriorNul { field })
}

fn result_error(result: NonNull<PGresult>) -> String {
    unsafe { copy_error(PQresultErrorMessage(result.as_ptr())) }
}

unsafe fn copy_error(pointer: *const c_char) -> String {
    if pointer.is_null() {
        return "libpq returned no error message".into();
    }
    unsafe { CStr::from_ptr(pointer) }
        .to_string_lossy()
        .trim()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xlog_data(wal_start: u64, wal_end: u64, time: i64, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![b'w'];
        frame.extend_from_slice(&wal_start.to_be_bytes());
        frame.extend_from_slice(&wal_end.to_be_bytes());
        frame.extend_from_slice(&time.to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    fn keepalive(wal_end: u64, time: i64, reply: u8) -> Vec<u8> {
        let mut frame = vec![b'k'];
        frame.extend_from_slice(&wal_end.to_be_bytes());
        frame.extend_from_slice(&time.to_be_bytes());
        frame.push(reply);
        frame
    }

    #[test]
    fn parses_xlog_data_big_endian_and_preserves_pgoutput() {
        let payload = [b'S', 0, 0, 0, 7, 1, b'I', 0xff];
        assert_eq!(
            parse_copy_data(&xlog_data(
                0x0102_0304_0506_0708,
                0x1112_1314_1516_1718,
                -0x0102_0304_0506_0708,
                &payload,
            )),
            Ok(ReplicationMessage::XLogData {
                wal_start: 0x0102_0304_0506_0708,
                wal_end: 0x1112_1314_1516_1718,
                server_time: -0x0102_0304_0506_0708,
                pgoutput: payload.to_vec(),
            })
        );
    }

    #[test]
    fn parses_both_keepalive_reply_values() {
        assert_eq!(
            parse_copy_data(&keepalive(42, -7, 0)),
            Ok(ReplicationMessage::PrimaryKeepalive {
                wal_end: 42,
                server_time: -7,
                reply_requested: false,
            })
        );
        assert_eq!(
            parse_copy_data(&keepalive(u64::MAX, i64::MAX, 1)),
            Ok(ReplicationMessage::PrimaryKeepalive {
                wal_end: u64::MAX,
                server_time: i64::MAX,
                reply_requested: true,
            })
        );
    }

    #[test]
    fn rejects_truncated_and_malformed_envelopes() {
        assert_eq!(parse_copy_data(&[]), Err(ProtocolError::EmptyEnvelope));
        assert_eq!(
            parse_copy_data(b"?"),
            Err(ProtocolError::UnknownEnvelope(b'?'))
        );
        for length in 1..25 {
            assert!(matches!(
                parse_copy_data(&vec![b'w'; length]),
                Err(ProtocolError::TruncatedEnvelope {
                    kind: "XLogData",
                    expected_at_least: 25,
                    actual,
                }) if actual == length
            ));
        }
        for length in 1..18 {
            assert!(matches!(
                parse_copy_data(&vec![b'k'; length]),
                Err(ProtocolError::TruncatedEnvelope {
                    kind: "primary keepalive",
                    expected_at_least: 18,
                    actual,
                }) if actual == length
            ));
        }
        assert_eq!(
            parse_copy_data(&keepalive(0, 0, 2)),
            Err(ProtocolError::InvalidReplyRequested(2))
        );
        let mut oversized = keepalive(0, 0, 0);
        oversized.push(0);
        assert_eq!(
            parse_copy_data(&oversized),
            Err(ProtocolError::InvalidKeepaliveLength(19))
        );
    }

    #[test]
    fn encodes_standby_status_in_network_byte_order() {
        assert_eq!(
            encode_standby_status(
                0x0102_0304_0506_0708,
                0x1112_1314_1516_1718,
                0x2122_2324_2526_2728,
                -0x0102_0304_0506_0708,
                true,
            ),
            [
                b'r', 1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 19, 20, 21, 22, 23, 24, 33, 34, 35, 36, 37,
                38, 39, 40, 254, 253, 252, 251, 250, 249, 248, 248, 1,
            ]
        );
    }

    #[test]
    fn validates_slot_names_and_formats_lsn() {
        assert!(validate_slot_name("shiba_slot_17").is_ok());
        for invalid in ["", "UPPER", "has-dash", "has space"] {
            assert!(matches!(
                validate_slot_name(invalid),
                Err(ReplicationError::InvalidSlotName(_))
            ));
        }
        assert!(validate_slot_name(&"a".repeat(63)).is_ok());
        assert!(validate_slot_name(&"a".repeat(64)).is_err());
        assert_eq!(format_lsn(0), "0/00000000");
        assert_eq!(format_lsn(0x0123_4567_89ab_cdef), "1234567/89ABCDEF");
    }

    #[test]
    fn quotes_publication_identifiers_without_parsing_payloads() {
        assert_eq!(quote_identifier("ordinary"), "\"ordinary\"");
        assert_eq!(quote_identifier("odd\"name"), "\"odd\"\"name\"");
    }
}
