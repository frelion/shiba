//! The one persisted and executable relational-DAG model.
//!
//! Lowering emits stages in topological order. Registration validates and
//! stores this exact structure, and Runtime executes it directly.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DataflowPlan {
    /// Settings required to decode text-format PostgreSQL constants exactly.
    pub(crate) execution_settings: ExecutionSettings,
    pub(crate) stages: Vec<DataflowStage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionSettings {
    pub(crate) timezone: String,
    pub(crate) datestyle: String,
    pub(crate) intervalstyle: String,
    pub(crate) extra_float_digits: i32,
    pub(crate) bytea_output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DataflowStage {
    pub(crate) spec: OperatorSpec,
    pub(crate) schema: StageSchema,
    /// Array position is the input port.
    pub(crate) inputs: Vec<DataflowInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DataflowInput {
    pub(crate) upstream_stage_id: u32,
    pub(crate) bindings: Vec<SlotBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct StageSchema {
    pub(crate) inputs: Vec<InputSlot>,
    pub(crate) outputs: Vec<OutputSlot>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperatorKind {
    Scan,
    Filter,
    Project,
    Join,
    Distinct,
    Aggregate,
    Window,
    TopN,
    Sink,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    NullAwareAnti,
}

impl OperatorKind {
    pub(crate) fn input_count(self) -> u16 {
        if self == Self::Scan {
            0
        } else if self == Self::Join {
            2
        } else {
            1
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(
    tag = "operator",
    content = "config",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum OperatorSpec {
    Scan(ScanSpec),
    Filter(FilterSpec),
    Project(ProjectSpec),
    Join(JoinSpec),
    Distinct(DistinctSpec),
    Aggregate(AggregateSpec),
    Window(WindowSpec),
    #[serde(rename = "topn")]
    TopN(TopNSpec),
    Sink,
}

impl OperatorSpec {
    pub(crate) fn kind(&self) -> OperatorKind {
        match self {
            Self::Scan(_) => OperatorKind::Scan,
            Self::Filter(_) => OperatorKind::Filter,
            Self::Project(_) => OperatorKind::Project,
            Self::Join(_) => OperatorKind::Join,
            Self::Distinct(_) => OperatorKind::Distinct,
            Self::Aggregate(_) => OperatorKind::Aggregate,
            Self::Window(_) => OperatorKind::Window,
            Self::TopN(_) => OperatorKind::TopN,
            Self::Sink => OperatorKind::Sink,
        }
    }

    pub(crate) fn output_slots(&self) -> Vec<SlotId> {
        match self {
            Self::Scan(spec) => spec.columns.iter().map(|column| column.output).collect(),
            Self::Filter(spec) => spec.outputs.iter().map(|output| output.output).collect(),
            Self::Project(spec) => spec
                .expressions
                .iter()
                .map(|output| output.output)
                .collect(),
            Self::Join(spec) => spec.outputs.iter().map(|output| output.output).collect(),
            Self::Distinct(spec) => spec.outputs.iter().map(|output| output.output).collect(),
            Self::Aggregate(spec) => spec
                .groups
                .iter()
                .map(|group| group.output)
                .chain(spec.aggregates.iter().map(|aggregate| aggregate.output))
                .collect(),
            Self::Window(spec) => spec
                .outputs
                .iter()
                .map(|output| output.output)
                .chain(spec.functions.iter().map(|function| function.output))
                .collect(),
            Self::TopN(spec) => spec.outputs.iter().map(|output| output.output).collect(),
            Self::Sink => Vec::new(),
        }
    }

    pub(crate) fn expressions(&self) -> Vec<&ScalarExpr> {
        let mut expressions = Vec::new();
        match self {
            Self::Scan(_) | Self::Sink => {}
            Self::Filter(spec) => {
                expressions.push(&spec.predicate);
                expressions.extend(spec.outputs.iter().map(|output| &output.expr));
            }
            Self::Project(spec) => {
                expressions.extend(spec.expressions.iter().map(|output| &output.expr));
            }
            Self::Join(spec) => {
                expressions.push(&spec.condition);
                expressions.extend(spec.outputs.iter().map(|output| &output.expr));
            }
            Self::Distinct(spec) => {
                expressions.extend(spec.keys.iter().map(|key| &key.expr));
                expressions.extend(spec.outputs.iter().map(|output| &output.expr));
            }
            Self::Aggregate(spec) => {
                expressions.extend(spec.groups.iter().map(|group| &group.key.expr));
                for aggregate in &spec.aggregates {
                    expressions.extend(&aggregate.args);
                    expressions.extend(&aggregate.direct_args);
                    expressions.extend(aggregate.distinct.iter().map(|distinct| &distinct.expr));
                    expressions.extend(aggregate.filter.as_ref());
                    expressions.extend(aggregate.order_by.iter().map(|sort| &sort.expr));
                }
            }
            Self::Window(spec) => {
                expressions.extend(spec.partition_by.iter().map(|key| &key.expr));
                expressions.extend(spec.order_by.iter().map(|sort| &sort.expr));
                expressions.extend(spec.frame.start_offset.as_ref());
                expressions.extend(spec.frame.end_offset.as_ref());
                for function in &spec.functions {
                    expressions.extend(&function.args);
                    expressions.extend(function.filter.as_ref());
                }
                expressions.extend(spec.outputs.iter().map(|output| &output.expr));
            }
            Self::TopN(spec) => {
                expressions.extend(spec.order_by.iter().map(|sort| &sort.expr));
                expressions.extend(spec.outputs.iter().map(|output| &output.expr));
            }
        }
        expressions
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScanSpec {
    pub(crate) source_oid: u32,
    pub(crate) columns: Vec<ScanColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScanColumn {
    pub(crate) output: SlotId,
    pub(crate) attnum: i16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FilterSpec {
    pub(crate) predicate: ScalarExpr,
    pub(crate) outputs: Vec<NamedExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectSpec {
    pub(crate) expressions: Vec<NamedExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct JoinSpec {
    pub(crate) kind: JoinKind,
    /// Cross join is represented as an inner join with a constant true condition.
    pub(crate) condition: ScalarExpr,
    pub(crate) outputs: Vec<NamedExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DistinctSpec {
    pub(crate) keys: Vec<SortGroupExpr>,
    pub(crate) outputs: Vec<NamedExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AggregateSpec {
    pub(crate) groups: Vec<GroupExpr>,
    pub(crate) aggregates: Vec<AggregateExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupExpr {
    pub(crate) output: SlotId,
    pub(crate) name: Option<String>,
    pub(crate) key: SortGroupExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AggregateExpr {
    pub(crate) ref_id: u32,
    pub(crate) output: SlotId,
    pub(crate) function_oid: u32,
    pub(crate) input_collation_oid: u32,
    pub(crate) args: Vec<ScalarExpr>,
    pub(crate) direct_args: Vec<ScalarExpr>,
    pub(crate) distinct: Vec<SortGroupExpr>,
    pub(crate) filter: Option<ScalarExpr>,
    pub(crate) order_by: Vec<SortGroupExpr>,
    pub(crate) type_: SlotType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WindowSpec {
    pub(crate) partition_by: Vec<SortGroupExpr>,
    pub(crate) order_by: Vec<SortGroupExpr>,
    pub(crate) frame: WindowFrame,
    pub(crate) functions: Vec<WindowExpr>,
    pub(crate) outputs: Vec<NamedExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WindowExpr {
    pub(crate) ref_id: u32,
    pub(crate) output: SlotId,
    pub(crate) function_oid: u32,
    pub(crate) input_collation_oid: u32,
    pub(crate) args: Vec<ScalarExpr>,
    pub(crate) filter: Option<ScalarExpr>,
    pub(crate) star: bool,
    pub(crate) aggregate: bool,
    pub(crate) type_: SlotType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WindowFrame {
    /// PostgreSQL `FRAMEOPTION_*` bits captured from the analyzed query.
    pub(crate) options: u32,
    pub(crate) start_offset: Option<ScalarExpr>,
    pub(crate) end_offset: Option<ScalarExpr>,
    pub(crate) start_in_range_function_oid: Option<u32>,
    pub(crate) end_in_range_function_oid: Option<u32>,
    pub(crate) in_range_collation_oid: u32,
    pub(crate) in_range_ascending: bool,
    pub(crate) in_range_nulls_first: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct TopNSpec {
    pub(crate) order_by: Vec<SortGroupExpr>,
    pub(crate) limit: u64,
    pub(crate) offset: u64,
    pub(crate) with_ties: bool,
    pub(crate) outputs: Vec<NamedExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamedExpr {
    pub(crate) output: SlotId,
    pub(crate) name: Option<String>,
    pub(crate) expr: ScalarExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SortGroupExpr {
    pub(crate) expr: ScalarExpr,
    pub(crate) type_: SlotType,
    pub(crate) equality_operator_oid: u32,
    pub(crate) sort_operator_oid: u32,
    pub(crate) nulls_first: bool,
    pub(crate) hashable: bool,
}

/// A node-local ID used by scalar expressions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub(crate) struct BindingId(pub(crate) u32);

/// An ID in one node's output row.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub(crate) struct SlotId(pub(crate) u32);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SlotType {
    pub(crate) type_oid: u32,
    pub(crate) typmod: i32,
    pub(crate) collation_oid: u32,
    pub(crate) nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct InputSlot {
    pub(crate) binding: BindingId,
    pub(crate) input: u16,
    pub(crate) type_: SlotType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputSlot {
    pub(crate) slot: SlotId,
    pub(crate) type_: SlotType,
}

/// Maps one upstream output slot to one downstream, node-local binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SlotBinding {
    pub(crate) source_slot: SlotId,
    pub(crate) target_binding: BindingId,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BoolExprKind {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BooleanTestKind {
    True,
    NotTrue,
    False,
    NotFalse,
    Unknown,
    NotUnknown,
}

/// Reversible datum representation.
///
/// Text values use PostgreSQL's output/input functions and must be decoded
/// under the plan's captured [`ExecutionSettings`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "format",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum DatumRepr {
    Text(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaseWhen {
    pub(crate) when: ScalarExpr,
    pub(crate) then: ScalarExpr,
}

/// Catalog-resolved scalar expression.
///
/// `Input` addresses a typed binding, never a generated SQL alias. Aggregate
/// and window functions are owned by their operator specs and expose slots
/// directly, so scalar expressions have no second reference mechanism.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ScalarExpr {
    Input {
        binding: BindingId,
    },
    /// `value: None` is SQL NULL.
    Constant {
        type_: SlotType,
        value: Option<DatumRepr>,
    },
    Call {
        function_oid: u32,
        args: Vec<ScalarExpr>,
        type_: SlotType,
    },
    Operator {
        operator_oid: u32,
        args: Vec<ScalarExpr>,
        type_: SlotType,
    },
    Distinct {
        operator_oid: u32,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
        type_: SlotType,
    },
    NullIf {
        operator_oid: u32,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
        type_: SlotType,
    },
    ScalarArrayOperator {
        operator_oid: u32,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
        use_or: bool,
        type_: SlotType,
    },
    Bool {
        op: BoolExprKind,
        args: Vec<ScalarExpr>,
    },
    BooleanTest {
        arg: Box<ScalarExpr>,
        test: BooleanTestKind,
    },
    NullTest {
        arg: Box<ScalarExpr>,
        is_not: bool,
    },
    Coalesce {
        args: Vec<ScalarExpr>,
        type_: SlotType,
    },
    Case {
        operand: Option<Box<ScalarExpr>>,
        arms: Vec<CaseWhen>,
        else_expr: Box<ScalarExpr>,
        type_: SlotType,
    },
    /// PostgreSQL's analyzed placeholder for the operand of a simple CASE.
    ///
    /// It is valid only inside a `when` expression belonging to a `Case`
    /// whose `operand` is present.
    CaseTest {
        type_: SlotType,
    },
    Relabel {
        arg: Box<ScalarExpr>,
        type_: SlotType,
    },
    CoerceViaIo {
        arg: Box<ScalarExpr>,
        type_: SlotType,
    },
    CoerceToDomain {
        arg: Box<ScalarExpr>,
        type_: SlotType,
    },
    Collate {
        arg: Box<ScalarExpr>,
        collation_oid: u32,
        type_: SlotType,
    },
}

impl ScalarExpr {
    pub(crate) fn visit(&self, visitor: &mut impl FnMut(&ScalarExpr)) {
        visitor(self);
        match self {
            Self::Input { .. } | Self::Constant { .. } | Self::CaseTest { .. } => {}
            Self::Call { args, .. }
            | Self::Operator { args, .. }
            | Self::Bool { args, .. }
            | Self::Coalesce { args, .. } => {
                for arg in args {
                    arg.visit(visitor);
                }
            }
            Self::ScalarArrayOperator { left, right, .. } => {
                left.visit(visitor);
                right.visit(visitor);
            }
            Self::Distinct { left, right, .. } | Self::NullIf { left, right, .. } => {
                left.visit(visitor);
                right.visit(visitor);
            }
            Self::BooleanTest { arg, .. }
            | Self::NullTest { arg, .. }
            | Self::Relabel { arg, .. }
            | Self::CoerceViaIo { arg, .. }
            | Self::CoerceToDomain { arg, .. }
            | Self::Collate { arg, .. } => arg.visit(visitor),
            Self::Case {
                operand,
                arms,
                else_expr,
                ..
            } => {
                if let Some(operand) = operand {
                    operand.visit(visitor);
                }
                for arm in arms {
                    arm.when.visit(visitor);
                    arm.then.visit(visitor);
                }
                else_expr.visit(visitor);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer_type() -> SlotType {
        SlotType {
            type_oid: 23,
            typmod: -1,
            collation_oid: 0,
            nullable: false,
        }
    }

    #[test]
    fn aggregate_distinct_has_one_exact_json_contract() {
        let aggregate = AggregateExpr {
            ref_id: 1,
            output: SlotId(2),
            function_oid: 2_146,
            input_collation_oid: 0,
            args: Vec::new(),
            direct_args: Vec::new(),
            distinct: vec![SortGroupExpr {
                expr: ScalarExpr::Constant {
                    type_: integer_type(),
                    value: Some(DatumRepr::Text("1".into())),
                },
                type_: integer_type(),
                equality_operator_oid: 96,
                sort_operator_oid: 97,
                nulls_first: false,
                hashable: true,
            }],
            filter: None,
            order_by: Vec::new(),
            type_: integer_type(),
        };
        let json = serde_json::to_value(&aggregate).unwrap();
        assert_eq!(json["input_collation_oid"], 0);
        assert_eq!(
            serde_json::from_value::<AggregateExpr>(json.clone()).unwrap(),
            aggregate
        );

        let mut removed_contract = json;
        removed_contract["distinct"] = serde_json::json!(true);
        assert!(serde_json::from_value::<AggregateExpr>(removed_contract).is_err());
    }
}
