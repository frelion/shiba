//! Clean-room multi-source transaction pipeline; bounded strict pgoutput decoding
//! feeds the sole writer and `PostgreSQL` transaction owner.

#![forbid(unsafe_code)]

mod error;
mod operator_execution;
mod pgoutput;
mod pgoutput_source;
mod pgoutput_tuple;
mod pgoutput_wire;
mod processor;
mod registration;
mod source_apply;
mod source_preflight;
mod streamed_pgoutput;
mod transaction;

pub use error::M2Error;
pub use pgoutput::{PgoutputError, decode_committed_changes};
pub use pgoutput_source::PgoutputSource;
pub use processor::{ProcessOutcome, process};
pub use registration::{RegistrationError, compile_and_register};
pub use streamed_pgoutput::decode_streamed_changes;
pub use transaction::{
    SourceChange, SourceInsert, SourcePayload, SourceTransaction, SourceUpdate, SourceUpdatePayload,
};
