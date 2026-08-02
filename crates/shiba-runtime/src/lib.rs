//! Clean-room single-source transactional walking skeleton through M4.1.
//! Strict decoding feeds the sole writer and `PostgreSQL` transaction owner.

#![forbid(unsafe_code)]

mod count;
mod error;
mod pgoutput;
mod pgoutput_wire;
mod processor;
mod transaction;

pub use error::M2Error;
pub use pgoutput::{PgoutputError, PgoutputSource, decode_committed_insert};
pub use processor::{ProcessOutcome, process};
pub use transaction::{SourceInsert, SourcePayload, SourceTransaction};
