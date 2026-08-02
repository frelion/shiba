//! Minimal, database-independent contracts shared by clean-room V2 components.
//!
//! This crate contains identities and wire values only. It deliberately has no
//! database access, clock, process-global state, operator protocol, or runtime
//! authority.

#![forbid(unsafe_code)]

mod digest;
mod error;
mod identity;
mod lsn;
mod version;
mod wire;

pub use digest::{WIRE_DIGEST_DOMAIN, WireDigest};
pub use error::{ProtocolError, ScopeMismatch};
pub use identity::{
    CauseId, CommitFrontier, IngressTransactionId, InputSequence, SlotGeneration, SourceId,
    SourceTransactionId,
};
pub use lsn::{ParsePostgresLsnError, PostgresLsn};
pub use version::{CatalogVersion, ProtocolVersion};
pub use wire::{WireEnvelope, WireMessage};
