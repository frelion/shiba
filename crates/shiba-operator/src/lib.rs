//! Pure, database-independent operator contracts and evaluation.

#![forbid(unsafe_code)]

mod expression;
mod graph;
mod graph_budget;
mod graph_eval;
mod graph_topology;
mod graph_transition;
mod graph_validation;
mod grouped;
mod grouped_plan;
mod grouped_state;
mod join;
mod join_codec;
mod join_plan;
mod join_transition;
mod kernel;
mod materialize;
mod model;
mod plan;
mod result_delta;
mod result_row;
mod result_schema;
mod scalar;
mod state;
mod typed;

pub use expression::{Expression, ExpressionError};
pub use graph::{
    ColumnBinding, GraphError, NodeId, NodeInput, OperatorGraph, OperatorNode, OperatorNodeKind,
    SourcePort,
};
pub use graph_eval::apply_graph;
pub use graph_transition::{
    DeltaBatch, GraphEffectOrigin, GraphTransition, MultiInputBatch, RowDelta, SourceDeltaBatch,
};
pub use graph_validation::source_typed_layout;
pub use kernel::{KernelError, apply_graph_plan, graph_state_read_set};
pub use model::{EffectOrigin, EncodedOperatorState, ObjectAddress};
pub use plan::{OutputContract, StateContract};
pub use result_delta::{ResultDelta, ResultMutation, ResultRowKey};
pub use result_row::{MAX_RESULT_ROW_BYTES, RESULT_ROW_FORMAT_VERSION, TypedResultRowV1};
pub use result_schema::{
    MAX_RESULT_FIELDS, MAX_RESULT_SCHEMA_BYTES, RESULT_SCHEMA_FORMAT_VERSION, ResultError,
    ResultField, ResultSchemaV1,
};
pub use state::{
    StateDelta, StateEntry, StateError, StateKey, StateMutation, StatePartition, StateReadSet,
    StateSnapshot,
};
pub use typed::{TypedError, TypedLayout, TypedRow, TypedValue, ValueType};
