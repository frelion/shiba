//! Rust-owned control flow for bounded dataflow stages.
//!
//! PostgreSQL relations remain authoritative for typed rows, arrangements,
//! aggregate state, ordering keys, and pending output. This module owns only
//! scalar control facts: phase, stable row IDs, stream positions, budgets,
//! and transaction outcomes.

mod aggregate;
mod aggregate_capability;
mod bindings;
mod btree;
mod continuation;
mod contract;
pub(crate) mod core;
mod dispatcher;
mod distinct;
mod join;
mod linear;
pub(crate) mod register;
mod runner;
mod sink;
mod step;
mod storage;
mod stream;
mod topn;
mod window;

pub(crate) use core::*;
