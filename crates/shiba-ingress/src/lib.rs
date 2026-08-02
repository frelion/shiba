//! Production logical-replication transport with bounded transaction assembly.

#![forbid(unsafe_code)]

use core::fmt;

mod assembler;
mod envelope;
mod feedback;
#[cfg(test)]
mod feedback_tests;
mod frame;
mod governance;
mod governed;
mod limits;
mod publication;
mod receive_loop;
mod receiver;
mod source_shape;
mod streamed;
#[cfg(test)]
mod streamed_tests;
mod tokens;
mod transport;

pub use assembler::{AssembledTransaction, CommittedAssembler};
pub use envelope::{ReplicationMessage, encode_feedback, parse_replication_message};
pub use governed::{AttachOptions, GovernedSourceSession};
pub use limits::{CONNECTIONS_PER_SOURCE, MAX_ACTIVE_CONNECTIONS, MAX_ACTIVE_SOURCES};
pub(crate) use receiver::SourceReceiver;
pub use tokens::{
    AbortedTransaction, DurableTransaction, EmptyCommitted, ReceivedInput, StreamedInput,
};
pub use transport::ReplicationMode;
pub(crate) use transport::ReplicationTransport;

#[derive(Debug)]
pub enum IngressError {
    InvalidEnvelope(&'static str),
    InvalidIdentifier(&'static str),
    InvalidFrame,
    MessageOrder,
    LimitExceeded,
    FeedbackPending,
    FeedbackMismatch,
    ReceiverFailed,
    Governance(&'static str),
    Database(postgres::Error),
    Libpq(libpq::errors::Error),
    UnexpectedStatus(libpq::Status),
    Decode(shiba_runtime::PgoutputError),
    Runtime(shiba_runtime::M2Error),
}

impl fmt::Display for IngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEnvelope(reason) => {
                write!(formatter, "invalid replication envelope: {reason}")
            }
            Self::InvalidIdentifier(kind) => write!(formatter, "invalid replication {kind}"),
            Self::InvalidFrame => formatter.write_str("invalid pgoutput frame"),
            Self::MessageOrder => formatter.write_str("invalid pgoutput transaction order"),
            Self::LimitExceeded => formatter.write_str("ingress transaction limit exceeded"),
            Self::FeedbackPending => {
                formatter.write_str("durable replication feedback is still pending")
            }
            Self::FeedbackMismatch => {
                formatter.write_str("replication receiver state does not match the token")
            }
            Self::ReceiverFailed => {
                formatter.write_str("replication receiver failed closed and must restart")
            }
            Self::Governance(reason) => write!(formatter, "source governance failed: {reason}"),
            Self::Database(error) => write!(formatter, "governance database failed: {error}"),
            Self::Libpq(error) => {
                write!(formatter, "logical replication transport failed: {error}")
            }
            Self::UnexpectedStatus(status) => {
                write!(formatter, "expected COPY BOTH, got {status:?}")
            }
            Self::Decode(error) => write!(formatter, "pgoutput decode failed: {error}"),
            Self::Runtime(error) => write!(formatter, "runtime apply failed: {error}"),
        }
    }
}

impl std::error::Error for IngressError {}

impl From<libpq::errors::Error> for IngressError {
    fn from(value: libpq::errors::Error) -> Self {
        Self::Libpq(value)
    }
}

impl From<shiba_runtime::PgoutputError> for IngressError {
    fn from(value: shiba_runtime::PgoutputError) -> Self {
        Self::Decode(value)
    }
}

impl From<shiba_runtime::M2Error> for IngressError {
    fn from(value: shiba_runtime::M2Error) -> Self {
        Self::Runtime(value)
    }
}

impl From<postgres::Error> for IngressError {
    fn from(value: postgres::Error) -> Self {
        Self::Database(value)
    }
}
