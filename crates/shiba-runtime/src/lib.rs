//! M2's single-source, INSERT-only transactional walking skeleton.
//!
//! The caller supplies an ingress-independent committed source transaction.
//! [`process`] is the sole production entry point and owns the complete
//! `PostgreSQL` transaction. There is no decoder, fallback, worker, or dynamic
//! SQL surface in this crate.

#![forbid(unsafe_code)]

mod count;
mod error;
mod processor;
mod transaction;

pub use error::M2Error;
pub use processor::{ProcessOutcome, process};
pub use transaction::{SourceInsert, SourceTransaction};
