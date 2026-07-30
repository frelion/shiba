//! PostgreSQL logical-replication transport and pgoutput decoding.
//!
//! The transport owns the libpq CopyData envelope. The pgoutput module owns
//! the payload protocol. Ingress consumes both through this boundary and owns
//! only source-transaction admission semantics.

pub(crate) mod pgoutput;
mod transport;

pub(crate) use transport::*;
