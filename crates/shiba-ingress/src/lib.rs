//! Production logical-replication transport with bounded transaction assembly.

#![forbid(unsafe_code)]

use core::fmt;

mod assembler;
mod envelope;
mod frame;
mod receiver;
mod transport;

pub use assembler::{AssembledTransaction, CommittedAssembler};
pub use envelope::{ReplicationMessage, parse_replication_message};
pub use receiver::{ReceivedTransaction, SourceReceiver};
pub(crate) use transport::ReplicationTransport;

#[derive(Debug)]
pub enum IngressError {
    InvalidEnvelope(&'static str),
    InvalidIdentifier(&'static str),
    InvalidFrame,
    MessageOrder,
    LimitExceeded,
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
