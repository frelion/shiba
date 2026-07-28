//! Persisted logical-plan model.
//!
//! These types are the stable JSON contract stored in PostgreSQL. Runtime-only
//! validation and execution details deliberately live in sibling modules.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const LOGICAL_PLAN_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogicalPlan {
    pub version: u32,
    pub nodes: Vec<LogicalNode>,
    pub edges: Vec<LogicalEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogicalNode {
    pub id: String,
    pub operator: OperatorKind,
    pub config: Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperatorKind {
    Scan,
    Filter,
    Project,
    InnerJoin,
    LeftJoin,
    RightJoin,
    FullJoin,
    SemiJoin,
    AntiJoin,
    NullAwareAntiJoin,
    Distinct,
    Aggregate,
    Having,
    Window,
    TopN,
    Sink,
}

impl OperatorKind {
    pub(super) fn is_join(self) -> bool {
        matches!(
            self,
            Self::InnerJoin
                | Self::LeftJoin
                | Self::RightJoin
                | Self::FullJoin
                | Self::SemiJoin
                | Self::AntiJoin
                | Self::NullAwareAntiJoin
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogicalEdge {
    pub from: String,
    pub to: String,
    pub input: u16,
}
