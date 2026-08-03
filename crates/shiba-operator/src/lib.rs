//! Pure, database-independent operator contracts and evaluation.

#![forbid(unsafe_code)]

mod expression;
mod graph;
mod graph_budget;
mod graph_eval;
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
mod scalar;
mod state;
mod typed;

pub use expression::{Expression, ExpressionError};
pub use graph::{
    ColumnBinding, DeltaBatch, GraphEffectOrigin, GraphError, GraphTransition, MultiInputBatch,
    NodeId, NodeInput, OperatorGraph, OperatorNode, OperatorNodeKind, ResultDelta, ResultMutation,
    RowDelta, SourceDeltaBatch, SourcePort,
};
pub use graph_eval::apply_graph;
pub use graph_validation::source_typed_layout;
pub use kernel::{KernelError, apply_graph_plan, graph_state_read_set};
pub use model::{EffectOrigin, EncodedOperatorState, ObjectAddress};
pub use plan::{OutputContract, StateContract};
pub use state::{
    StateDelta, StateEntry, StateError, StateKey, StateMutation, StatePartition, StateReadSet,
    StateSnapshot,
};
pub use typed::{TypedError, TypedLayout, TypedRow, TypedValue, ValueType};
