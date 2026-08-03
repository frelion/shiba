//! Clean-room multi-source transaction pipeline; bounded strict pgoutput decoding
//! feeds the sole writer and `PostgreSQL` transaction owner.

#![forbid(unsafe_code)]

mod bootstrap;
mod bootstrap_activation;
mod bootstrap_model;
mod error;
mod keyed_state;
mod operator_execution;
mod pgoutput;
mod pgoutput_source;
mod pgoutput_tuple;
mod pgoutput_wire;
mod processor;
mod registration;
mod registration_descriptor;
mod result_sink;
mod source_apply;
mod source_batch;
mod source_preflight;
mod streamed_pgoutput;
mod transaction;

pub use bootstrap::{
    BootstrapProcessOutcome, process_bootstrap_batch, reset_abandoned_bootstrap_state,
};
pub use bootstrap_activation::{
    BootstrapTransitionOutcome, activate_bootstrap, complete_bootstrap_scan,
};
pub use bootstrap_model::{BootstrapBatch, MAX_BOOTSTRAP_BATCH_ROWS, SnapshotRow};
pub use error::M2Error;
pub use pgoutput::{
    PgoutputError, PgoutputRelationState, decode_committed_changes,
    decode_committed_changes_in_session,
};
pub use pgoutput_source::{PgoutputGraph, PgoutputSource};
pub use processor::{ProcessOutcome, process};
pub use registration::{
    GraphResultContract, RebuildGraphArtifact, RebuildSourceTarget, RegistrationError,
    compile_and_register, compile_and_register_in_transaction, compile_rebuild_graph,
};
pub use streamed_pgoutput::{decode_streamed_changes, decode_streamed_changes_in_session};
pub use transaction::{
    GraphSourceChange, GraphTransaction, SourceChange, SourceInsert, SourcePayload, SourceUpdate,
    SourceUpdatePayload,
};
