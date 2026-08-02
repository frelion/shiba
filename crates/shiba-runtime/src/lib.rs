//! Clean-room single-source transaction pipeline through M4.4; strict pgoutput
//! decoding feeds the sole writer and `PostgreSQL` transaction owner.

#![forbid(unsafe_code)]

mod count;
mod error;
mod pgoutput;
mod pgoutput_source;
mod pgoutput_wire;
mod processor;
mod transaction;

pub use error::M2Error;
pub use pgoutput::{PgoutputError, decode_committed_changes};
pub use pgoutput_source::PgoutputSource;
pub use processor::{ProcessOutcome, process};
pub use transaction::{SourceChange, SourceInsert, SourcePayload, SourceTransaction, SourceUpdate};
