//! Pure, database-independent operator contracts and evaluation.

#![forbid(unsafe_code)]

mod expression;
mod graph;
mod graph_eval;
mod graph_validation;
mod kernel;
mod materialize;
mod model;
mod plan;
mod typed;

pub use expression::{Expression, ExpressionError};
pub use graph::{
    ColumnBinding, DeltaBatch, GraphError, GraphTransition, NodeId, NodeInput, OperatorGraph,
    OperatorNode, OperatorNodeKind, ResultDelta, ResultMutation, RowDelta, StateDelta,
};
pub use graph_eval::apply_graph;
pub use graph_validation::source_typed_layout;
pub use kernel::{KernelError, apply_plan, decode_state, initial_state, initial_transition};
pub use model::{
    EffectOrigin, EncodedOperatorState, KeyedMutation, ObjectAddress, OperatorId,
    OperatorTransition, OutputDelta,
};
pub use plan::{
    CompiledPlan, InputBinding, InputRole, OutputContract, PlanError, PlanImplementation,
    StateContract,
};
pub use typed::{TypedError, TypedLayout, TypedRow, TypedValue, ValueType};
