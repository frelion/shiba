//! Lowering from PostgreSQL's analyzed `Query` to the one relational-DAG model.
//!
//! PostgreSQL names columns in expressions with `(varno, varattno,
//! varlevelsup)`. Shiba resolves that address exactly once, at registration,
//! into a typed node-local [`BindingId`]. No generated SQL aliases or
//! query-family classifier survive this boundary.

use std::collections::{BTreeSet, HashMap};
use std::ffi::CStr;

use pgrx::pg_sys;

use crate::planner::model::{
    AggregateExpr, AggregateSpec, BindingId, BoolExprKind, BooleanTestKind, CaseWhen,
    DataflowInput, DataflowPlan, DataflowStage, DatumRepr, DistinctSpec, ExecutionSettings,
    FilterSpec, GroupExpr, InputSlot, JoinKind, JoinSpec, NamedExpr, OperatorSpec, OutputSlot,
    ProjectSpec, ScalarExpr, ScanColumn, ScanSpec, SlotBinding, SlotId, SlotType, SortGroupExpr,
    StageSchema, TopNSpec, WindowExpr, WindowFrame, WindowSpec,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoweringError {
    capability: &'static str,
    node: Option<String>,
    detail: String,
}

impl LoweringError {
    fn unsupported(
        capability: &'static str,
        node: *mut pg_sys::Node,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            capability,
            node: (!node.is_null()).then(|| unsafe { format!("{:?}", (*node).type_) }),
            detail: detail.into(),
        }
    }

    fn invalid(capability: &'static str, detail: impl Into<String>) -> Self {
        Self {
            capability,
            node: None,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for LoweringError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.capability, self.detail)?;
        if let Some(node) = &self.node {
            write!(formatter, " (PostgreSQL node {node})")?;
        }
        Ok(())
    }
}

impl std::error::Error for LoweringError {}

/// A lowered query body. The sink is attached after PostgreSQL creates the CTAS
/// result and assigns its relation OID.
pub(crate) struct LoweredQuery {
    plan: DataflowPlan,
    sources: BTreeSet<u32>,
    output: Relation,
    next_binding: u32,
}

impl LoweredQuery {
    pub(crate) fn source_oids(&self) -> Vec<u32> {
        self.sources.iter().copied().collect()
    }

    pub(crate) fn finish(mut self) -> DataflowPlan {
        let (inputs, bindings, _) = bind_relation(&self.output, 0, &mut self.next_binding);
        self.plan.stages.push(DataflowStage {
            spec: OperatorSpec::Sink,
            schema: StageSchema {
                inputs,
                outputs: Vec::new(),
            },
            inputs: vec![DataflowInput {
                upstream_stage_id: self.output.stage_id,
                bindings,
            }],
        });
        self.plan
    }
}

#[derive(Debug, Clone)]
struct Column {
    slot: SlotId,
    type_: SlotType,
    name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct VarKey {
    rte: i32,
    attnum: i16,
}

#[derive(Debug, Clone)]
struct Relation {
    stage_id: u32,
    columns: Vec<Column>,
    vars: HashMap<VarKey, usize>,
    groups: Vec<(*mut pg_sys::Node, usize)>,
    aggregates: HashMap<usize, usize>,
    windows: HashMap<usize, usize>,
    target_refs: HashMap<u32, usize>,
}

impl Relation {
    fn empty() -> Self {
        Self {
            stage_id: 0,
            columns: Vec::new(),
            vars: HashMap::new(),
            groups: Vec::new(),
            aggregates: HashMap::new(),
            windows: HashMap::new(),
            target_refs: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct Builder {
    stages: Vec<DataflowStage>,
    sources: BTreeSet<u32>,
    next_slot: u32,
    next_binding: u32,
    next_aggregate: u32,
    next_window: u32,
}

impl Builder {
    fn next_stage_id(&self) -> u32 {
        u32::try_from(self.stages.len()).expect("dataflow has more than u32::MAX stages")
    }

    fn slot(&mut self) -> SlotId {
        let slot = SlotId(self.next_slot);
        self.next_slot += 1;
        slot
    }

    fn aggregate_ref(&mut self) -> u32 {
        self.next_aggregate += 1;
        self.next_aggregate
    }

    fn window_ref(&mut self) -> u32 {
        self.next_window += 1;
        self.next_window
    }

    fn add_stage(
        &mut self,
        spec: OperatorSpec,
        schema_inputs: Vec<InputSlot>,
        outputs: Vec<Column>,
        inputs: Vec<DataflowInput>,
    ) -> Relation {
        let stage_id = self.next_stage_id();
        self.stages.push(DataflowStage {
            spec,
            schema: StageSchema {
                inputs: schema_inputs,
                outputs: outputs
                    .iter()
                    .map(|column| OutputSlot {
                        slot: column.slot,
                        type_: column.type_.clone(),
                    })
                    .collect(),
            },
            inputs,
        });
        Relation {
            stage_id,
            columns: outputs,
            vars: HashMap::new(),
            groups: Vec::new(),
            aggregates: HashMap::new(),
            windows: HashMap::new(),
            target_refs: HashMap::new(),
        }
    }

    fn bind_input(&mut self, source: &Relation, input: u16) -> BoundInput {
        let (inputs, bindings, columns) = bind_relation(source, input, &mut self.next_binding);
        BoundInput {
            input: DataflowInput {
                upstream_stage_id: source.stage_id,
                bindings,
            },
            schema_inputs: inputs,
            columns,
        }
    }

    fn passthrough_outputs(
        &mut self,
        source: &Relation,
        bindings: &[BindingId],
        additional: usize,
    ) -> (Vec<Column>, Vec<NamedExpr>) {
        assert_eq!(source.columns.len(), bindings.len());
        let capacity = source.columns.len() + additional;
        let mut columns = Vec::with_capacity(capacity);
        let mut expressions = Vec::with_capacity(capacity);
        for (column, binding) in source.columns.iter().zip(bindings) {
            let output = self.slot();
            columns.push(Column {
                slot: output,
                type_: column.type_.clone(),
                name: column.name.clone(),
            });
            expressions.push(NamedExpr {
                output,
                name: column.name.clone(),
                expr: ScalarExpr::Input { binding: *binding },
            });
        }
        (columns, expressions)
    }
}

struct BoundInput {
    input: DataflowInput,
    schema_inputs: Vec<InputSlot>,
    columns: Vec<BindingId>,
}

fn bind_relation(
    relation: &Relation,
    input: u16,
    next_binding: &mut u32,
) -> (Vec<InputSlot>, Vec<SlotBinding>, Vec<BindingId>) {
    let mut inputs = Vec::with_capacity(relation.columns.len());
    let mut bindings = Vec::with_capacity(relation.columns.len());
    let mut columns = Vec::with_capacity(relation.columns.len());
    for column in &relation.columns {
        let binding = BindingId(*next_binding);
        *next_binding += 1;
        inputs.push(InputSlot {
            binding,
            input,
            type_: column.type_.clone(),
        });
        bindings.push(SlotBinding {
            source_slot: column.slot,
            target_binding: binding,
        });
        columns.push(binding);
    }
    (inputs, bindings, columns)
}

fn typed_schema(inputs: Vec<InputSlot>, outputs: &[Column]) -> StageSchema {
    StageSchema {
        inputs,
        outputs: outputs
            .iter()
            .map(|column| OutputSlot {
                slot: column.slot,
                type_: column.type_.clone(),
            })
            .collect(),
    }
}

fn remap_relation(source: &Relation, stage_id: u32, columns: Vec<Column>) -> Relation {
    debug_assert_eq!(source.columns.len(), columns.len());
    Relation {
        stage_id,
        columns,
        vars: source.vars.clone(),
        groups: source.groups.clone(),
        aggregates: source.aggregates.clone(),
        windows: source.windows.clone(),
        target_refs: source.target_refs.clone(),
    }
}

struct ScalarScope<'a> {
    relation: &'a Relation,
    bindings: &'a [BindingId],
}

struct ScalarContext<'a> {
    scopes: Vec<ScalarScope<'a>>,
}

impl ScalarContext<'_> {
    unsafe fn input(&self, variable: *mut pg_sys::Var) -> Result<ScalarExpr, LoweringError> {
        let level = (*variable).varlevelsup as usize;
        let Some(scope) = self.scopes.get(level) else {
            return Err(LoweringError::unsupported(
                "scalar.binding",
                variable.cast(),
                format!("varlevelsup {level} has no lowering scope"),
            ));
        };
        let key = VarKey {
            rte: (*variable).varno,
            attnum: (*variable).varattno,
        };
        let Some(column) = scope.relation.vars.get(&key).copied() else {
            return Err(LoweringError::unsupported(
                "scalar.binding",
                variable.cast(),
                format!(
                    "cannot resolve Var({}, {}, {}) to a child output slot",
                    (*variable).varno,
                    (*variable).varattno,
                    (*variable).varlevelsup
                ),
            ));
        };
        let Some(binding) = scope.bindings.get(column).copied() else {
            return Err(LoweringError::invalid(
                "scalar.binding",
                "resolved column is absent from the node input schema",
            ));
        };
        Ok(ScalarExpr::Input { binding })
    }
}

/// # Safety
/// `list` must be null or a live PostgreSQL list.
unsafe fn list_items(list: *mut pg_sys::List) -> impl Iterator<Item = *mut std::ffi::c_void> {
    let length = pg_sys::list_length(list);
    (0..length).map(move |index| pg_sys::list_nth(list, index))
}

unsafe fn c_string(pointer: *const std::ffi::c_char) -> Option<String> {
    (!pointer.is_null()).then(|| CStr::from_ptr(pointer).to_string_lossy().into_owned())
}

unsafe fn slot_type(node: *mut pg_sys::Node) -> SlotType {
    SlotType {
        type_oid: pg_sys::exprType(node).to_u32(),
        typmod: pg_sys::exprTypmod(node),
        collation_oid: pg_sys::exprCollation(node).to_u32(),
        nullable: true,
    }
}

unsafe fn immutable_function(
    function_oid: pg_sys::Oid,
    node: *mut pg_sys::Node,
    capability: &'static str,
) -> Result<(), LoweringError> {
    if function_oid == pg_sys::InvalidOid {
        return Err(LoweringError::unsupported(
            capability,
            node,
            "PostgreSQL did not resolve a function OID",
        ));
    }
    if pg_sys::get_func_namespace(function_oid).to_u32() != pg_sys::PG_CATALOG_NAMESPACE {
        let name = c_string(pg_sys::get_func_name(function_oid))
            .unwrap_or_else(|| function_oid.to_u32().to_string());
        return Err(LoweringError::unsupported(
            capability,
            node,
            format!("function {name} is outside the trusted pg_catalog namespace"),
        ));
    }
    if pg_sys::func_volatile(function_oid) as u8 != pg_sys::PROVOLATILE_IMMUTABLE {
        let name = c_string(pg_sys::get_func_name(function_oid))
            .unwrap_or_else(|| function_oid.to_u32().to_string());
        return Err(LoweringError::unsupported(
            capability,
            node,
            format!("function {name} is not immutable"),
        ));
    }
    Ok(())
}

unsafe fn scalar(
    node: *mut pg_sys::Node,
    context: &ScalarContext<'_>,
    capability: &'static str,
) -> Result<ScalarExpr, LoweringError> {
    if node.is_null() {
        return Err(LoweringError::invalid(
            capability,
            "PostgreSQL supplied a NULL expression node",
        ));
    }
    if let Some(scope) = context.scopes.first() {
        if let Some((_, column)) = scope.relation.groups.iter().find(|(group, _)| {
            pg_sys::equal(
                node.cast::<std::ffi::c_void>(),
                (*group).cast::<std::ffi::c_void>(),
            )
        }) {
            let binding = scope.bindings.get(*column).copied().ok_or_else(|| {
                LoweringError::invalid(
                    capability,
                    "group expression is absent from the node input schema",
                )
            })?;
            return Ok(ScalarExpr::Input { binding });
        }
    }
    match (*node).type_ {
        pg_sys::NodeTag::T_Var => context.input(node.cast()),
        pg_sys::NodeTag::T_Const => {
            let constant = node.cast::<pg_sys::Const>();
            let type_ = slot_type(node);
            if (*constant).constisnull {
                return Ok(ScalarExpr::Constant { type_, value: None });
            }
            let mut output_function = pg_sys::InvalidOid;
            let mut is_varlena = false;
            pg_sys::getTypeOutputInfo((*constant).consttype, &mut output_function, &mut is_varlena);
            let output = pg_sys::OidOutputFunctionCall(output_function, (*constant).constvalue);
            let value = c_string(output).ok_or_else(|| {
                LoweringError::unsupported(
                    capability,
                    node,
                    "type output function returned NULL for a non-NULL constant",
                )
            })?;
            Ok(ScalarExpr::Constant {
                type_,
                value: Some(DatumRepr::Text(value)),
            })
        }
        pg_sys::NodeTag::T_FuncExpr => {
            let function = node.cast::<pg_sys::FuncExpr>();
            if (*function).funcretset {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "set-returning scalar functions are not supported",
                ));
            }
            if (*function).funcvariadic {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "explicit VARIADIC scalar calls are not supported",
                ));
            }
            immutable_function((*function).funcid, node, capability)?;
            Ok(ScalarExpr::Call {
                function_oid: (*function).funcid.to_u32(),
                args: list_items((*function).args)
                    .map(|argument| scalar(argument.cast(), context, capability))
                    .collect::<Result<_, _>>()?,
                type_: slot_type(node),
            })
        }
        pg_sys::NodeTag::T_OpExpr => {
            let operation = node.cast::<pg_sys::OpExpr>();
            if (*operation).opretset {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "set-returning operators are not supported",
                ));
            }
            immutable_function((*operation).opfuncid, node, capability)?;
            Ok(ScalarExpr::Operator {
                operator_oid: (*operation).opno.to_u32(),
                args: list_items((*operation).args)
                    .map(|argument| scalar(argument.cast(), context, capability))
                    .collect::<Result<_, _>>()?,
                type_: slot_type(node),
            })
        }
        pg_sys::NodeTag::T_DistinctExpr | pg_sys::NodeTag::T_NullIfExpr => {
            let operation = node.cast::<pg_sys::OpExpr>();
            if (*operation).opretset {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "set-returning operators are not supported",
                ));
            }
            immutable_function((*operation).opfuncid, node, capability)?;
            let arguments = list_items((*operation).args)
                .map(|argument| scalar(argument.cast(), context, capability))
                .collect::<Result<Vec<_>, _>>()?;
            let [left, right] = arguments.as_slice() else {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "DISTINCT/NULLIF operator does not have two arguments",
                ));
            };
            if (*node).type_ == pg_sys::NodeTag::T_DistinctExpr {
                Ok(ScalarExpr::Distinct {
                    operator_oid: (*operation).opno.to_u32(),
                    left: Box::new(left.clone()),
                    right: Box::new(right.clone()),
                    type_: slot_type(node),
                })
            } else {
                Ok(ScalarExpr::NullIf {
                    operator_oid: (*operation).opno.to_u32(),
                    left: Box::new(left.clone()),
                    right: Box::new(right.clone()),
                    type_: slot_type(node),
                })
            }
        }
        pg_sys::NodeTag::T_ScalarArrayOpExpr => {
            let operation = node.cast::<pg_sys::ScalarArrayOpExpr>();
            immutable_function((*operation).opfuncid, node, capability)?;
            let arguments = list_items((*operation).args)
                .map(|argument| scalar(argument.cast(), context, capability))
                .collect::<Result<Vec<_>, _>>()?;
            let [left, right] = arguments.as_slice() else {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "scalar-array operator does not have two arguments",
                ));
            };
            Ok(ScalarExpr::ScalarArrayOperator {
                operator_oid: (*operation).opno.to_u32(),
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
                use_or: (*operation).useOr,
                type_: slot_type(node),
            })
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let boolean = node.cast::<pg_sys::BoolExpr>();
            let op = match (*boolean).boolop {
                pg_sys::BoolExprType::AND_EXPR => BoolExprKind::And,
                pg_sys::BoolExprType::OR_EXPR => BoolExprKind::Or,
                pg_sys::BoolExprType::NOT_EXPR => BoolExprKind::Not,
                _ => {
                    return Err(LoweringError::unsupported(
                        capability,
                        node,
                        "unknown PostgreSQL boolean operation",
                    ));
                }
            };
            let args = list_items((*boolean).args)
                .map(|argument| scalar(argument.cast(), context, capability))
                .collect::<Result<Vec<_>, _>>()?;
            if (op == BoolExprKind::Not && args.len() != 1)
                || (op != BoolExprKind::Not && args.is_empty())
            {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "boolean expression has invalid arity",
                ));
            }
            Ok(ScalarExpr::Bool { op, args })
        }
        pg_sys::NodeTag::T_NullTest => {
            let test = node.cast::<pg_sys::NullTest>();
            if (*test).argisrow {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "row-valued NULL tests are not supported",
                ));
            }
            let is_not = match (*test).nulltesttype {
                pg_sys::NullTestType::IS_NULL => false,
                pg_sys::NullTestType::IS_NOT_NULL => true,
                _ => {
                    return Err(LoweringError::unsupported(
                        capability,
                        node,
                        "unknown PostgreSQL NULL test",
                    ));
                }
            };
            Ok(ScalarExpr::NullTest {
                arg: Box::new(scalar((*test).arg.cast(), context, capability)?),
                is_not,
            })
        }
        pg_sys::NodeTag::T_BooleanTest => {
            let test = node.cast::<pg_sys::BooleanTest>();
            let test_kind = match (*test).booltesttype {
                pg_sys::BoolTestType::IS_TRUE => BooleanTestKind::True,
                pg_sys::BoolTestType::IS_NOT_TRUE => BooleanTestKind::NotTrue,
                pg_sys::BoolTestType::IS_FALSE => BooleanTestKind::False,
                pg_sys::BoolTestType::IS_NOT_FALSE => BooleanTestKind::NotFalse,
                pg_sys::BoolTestType::IS_UNKNOWN => BooleanTestKind::Unknown,
                pg_sys::BoolTestType::IS_NOT_UNKNOWN => BooleanTestKind::NotUnknown,
                _ => {
                    return Err(LoweringError::unsupported(
                        capability,
                        node,
                        "unknown PostgreSQL boolean test",
                    ));
                }
            };
            Ok(ScalarExpr::BooleanTest {
                arg: Box::new(scalar((*test).arg.cast(), context, capability)?),
                test: test_kind,
            })
        }
        pg_sys::NodeTag::T_CoalesceExpr => {
            let coalesce = node.cast::<pg_sys::CoalesceExpr>();
            Ok(ScalarExpr::Coalesce {
                args: list_items((*coalesce).args)
                    .map(|argument| scalar(argument.cast(), context, capability))
                    .collect::<Result<_, _>>()?,
                type_: slot_type(node),
            })
        }
        pg_sys::NodeTag::T_CaseExpr => {
            let case = node.cast::<pg_sys::CaseExpr>();
            let operand = (!(*case).arg.is_null())
                .then(|| scalar((*case).arg.cast(), context, capability))
                .transpose()?
                .map(Box::new);
            let arms = list_items((*case).args)
                .map(|item| {
                    let arm = item.cast::<pg_sys::CaseWhen>();
                    Ok(CaseWhen {
                        when: scalar((*arm).expr.cast(), context, capability)?,
                        then: scalar((*arm).result.cast(), context, capability)?,
                    })
                })
                .collect::<Result<_, LoweringError>>()?;
            if (*case).defresult.is_null() {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "CASE expression has no analyzed ELSE result",
                ));
            }
            Ok(ScalarExpr::Case {
                operand,
                arms,
                else_expr: Box::new(scalar((*case).defresult.cast(), context, capability)?),
                type_: slot_type(node),
            })
        }
        pg_sys::NodeTag::T_CaseTestExpr => Ok(ScalarExpr::CaseTest {
            type_: slot_type(node),
        }),
        pg_sys::NodeTag::T_RelabelType => {
            let relabel = node.cast::<pg_sys::RelabelType>();
            Ok(ScalarExpr::Relabel {
                arg: Box::new(scalar((*relabel).arg.cast(), context, capability)?),
                type_: slot_type(node),
            })
        }
        pg_sys::NodeTag::T_CoerceViaIO => {
            let coercion = node.cast::<pg_sys::CoerceViaIO>();
            Ok(ScalarExpr::CoerceViaIo {
                arg: Box::new(scalar((*coercion).arg.cast(), context, capability)?),
                type_: slot_type(node),
            })
        }
        pg_sys::NodeTag::T_CoerceToDomain => {
            let coercion = node.cast::<pg_sys::CoerceToDomain>();
            Ok(ScalarExpr::CoerceToDomain {
                arg: Box::new(scalar((*coercion).arg.cast(), context, capability)?),
                type_: slot_type(node),
            })
        }
        pg_sys::NodeTag::T_CollateExpr => {
            let collate = node.cast::<pg_sys::CollateExpr>();
            Ok(ScalarExpr::Collate {
                arg: Box::new(scalar((*collate).arg.cast(), context, capability)?),
                collation_oid: (*collate).collOid.to_u32(),
                type_: slot_type(node),
            })
        }
        pg_sys::NodeTag::T_Aggref => {
            let Some(scope) = context.scopes.first() else {
                return Err(LoweringError::invalid(
                    capability,
                    "aggregate reference has no current input",
                ));
            };
            let key = node as usize;
            let Some(column) = scope.relation.aggregates.get(&key).copied() else {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "aggregate reference is outside its aggregate node",
                ));
            };
            Ok(ScalarExpr::Input {
                binding: scope.bindings[column],
            })
        }
        pg_sys::NodeTag::T_WindowFunc => {
            let Some(scope) = context.scopes.first() else {
                return Err(LoweringError::invalid(
                    capability,
                    "window reference has no current input",
                ));
            };
            let key = node as usize;
            let Some(column) = scope.relation.windows.get(&key).copied() else {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "window reference is outside its window node",
                ));
            };
            Ok(ScalarExpr::Input {
                binding: scope.bindings[column],
            })
        }
        pg_sys::NodeTag::T_SubLink => Err(LoweringError::unsupported(
            capability,
            node,
            "scalar subqueries are only supported as top-level WHERE conjuncts",
        )),
        _ => Err(LoweringError::unsupported(
            capability,
            node,
            "this concrete scalar expression is not supported",
        )),
    }
}

fn current_settings() -> ExecutionSettings {
    unsafe fn setting(name: &CStr) -> String {
        let value = pg_sys::GetConfigOption(name.as_ptr(), false, false);
        c_string(value).unwrap_or_default()
    }
    ExecutionSettings {
        timezone: unsafe { setting(c"TimeZone") },
        datestyle: unsafe { setting(c"DateStyle") },
        intervalstyle: unsafe { setting(c"IntervalStyle") },
        extra_float_digits: unsafe { setting(c"extra_float_digits") }
            .parse()
            .unwrap_or_default(),
        bytea_output: unsafe { setting(c"bytea_output") },
    }
}

unsafe fn range_table_entry(
    query: *mut pg_sys::Query,
    rtindex: i32,
) -> Result<*mut pg_sys::RangeTblEntry, LoweringError> {
    let length = pg_sys::list_length((*query).rtable);
    if rtindex <= 0 || rtindex > length {
        return Err(LoweringError::invalid(
            "from.binding",
            format!("range-table index {rtindex} is outside 1..={length}"),
        ));
    }
    let entry: *mut pg_sys::RangeTblEntry = pg_sys::list_nth((*query).rtable, rtindex - 1).cast();
    if entry.is_null() {
        return Err(LoweringError::invalid(
            "from.binding",
            format!("range-table entry {rtindex} is NULL"),
        ));
    }
    Ok(entry)
}

unsafe fn alias_column_names(entry: *mut pg_sys::RangeTblEntry) -> Vec<Option<String>> {
    if (*entry).eref.is_null() {
        return Vec::new();
    }
    list_items((*(*entry).eref).colnames)
        .map(|item| {
            let string = item.cast::<pg_sys::String>();
            (!string.is_null())
                .then(|| c_string((*string).sval))
                .flatten()
        })
        .collect()
}

unsafe fn lower_scan(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    rtindex: i32,
    entry: *mut pg_sys::RangeTblEntry,
) -> Result<Relation, LoweringError> {
    if (*entry).relid == pg_sys::InvalidOid {
        return Err(LoweringError::invalid(
            "scan.relation",
            format!("RTE {rtindex} has no relation OID"),
        ));
    }
    let source_oid = (*entry).relid.to_u32();
    builder.sources.insert(source_oid);
    let names = alias_column_names(entry);
    let mut columns = Vec::new();
    let mut scan_columns = Vec::new();
    let mut vars = HashMap::new();
    for (offset, name) in names.into_iter().enumerate() {
        let attnum = i16::try_from(offset + 1).map_err(|_| {
            LoweringError::invalid("scan.schema", "relation has too many user columns")
        })?;
        let mut type_oid = pg_sys::InvalidOid;
        let mut typmod = -1;
        let mut collation_oid = pg_sys::InvalidOid;
        pg_sys::get_atttypetypmodcoll(
            (*entry).relid,
            attnum,
            &mut type_oid,
            &mut typmod,
            &mut collation_oid,
        );
        // Dropped attributes retain a position in an RTE but cannot be read by
        // an analyzed Var. Keeping them out gives scans a real executable row.
        if type_oid == pg_sys::InvalidOid {
            continue;
        }
        let slot = builder.slot();
        let column_index = columns.len();
        columns.push(Column {
            slot,
            type_: SlotType {
                type_oid: type_oid.to_u32(),
                typmod,
                collation_oid: collation_oid.to_u32(),
                nullable: true,
            },
            name,
        });
        scan_columns.push(ScanColumn {
            output: slot,
            attnum,
        });
        vars.insert(
            VarKey {
                rte: rtindex,
                attnum,
            },
            column_index,
        );
    }
    if columns.is_empty() {
        return Err(LoweringError::unsupported(
            "scan.schema",
            query.cast(),
            format!("relation OID {source_oid} has no readable user columns"),
        ));
    }
    let mut relation = builder.add_stage(
        OperatorSpec::Scan(ScanSpec {
            source_oid,
            columns: scan_columns,
        }),
        Vec::new(),
        columns,
        Vec::new(),
    );
    relation.vars = vars;
    Ok(relation)
}

unsafe fn lower_range_ref(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    reference: *mut pg_sys::RangeTblRef,
) -> Result<Relation, LoweringError> {
    let rtindex = (*reference).rtindex;
    let entry = range_table_entry(query, rtindex)?;
    match (*entry).rtekind {
        pg_sys::RTEKind::RTE_RELATION => lower_scan(builder, query, rtindex, entry),
        pg_sys::RTEKind::RTE_SUBQUERY => {
            if (*entry).lateral {
                return Err(LoweringError::unsupported(
                    "from.lateral",
                    reference.cast(),
                    "LATERAL subqueries require a correlated apply operator",
                ));
            }
            if (*entry).subquery.is_null() {
                return Err(LoweringError::invalid(
                    "from.subquery",
                    format!("RTE {rtindex} has no analyzed subquery"),
                ));
            }
            let lowered = lower_query_body(builder, (*entry).subquery)?;
            let names = alias_column_names(entry);
            if names.len() != lowered.columns.len() {
                return Err(LoweringError::invalid(
                    "from.subquery",
                    format!(
                        "RTE {rtindex} exposes {} columns but its subquery lowered {}",
                        names.len(),
                        lowered.columns.len()
                    ),
                ));
            }
            let mut relation = lowered;
            relation.vars.clear();
            relation.groups.clear();
            relation.aggregates.clear();
            relation.windows.clear();
            for (index, (column, name)) in relation.columns.iter_mut().zip(names).enumerate() {
                column.name = name;
                relation.vars.insert(
                    VarKey {
                        rte: rtindex,
                        attnum: i16::try_from(index + 1).map_err(|_| {
                            LoweringError::invalid(
                                "from.subquery",
                                "subquery has too many output columns",
                            )
                        })?,
                    },
                    index,
                );
            }
            Ok(relation)
        }
        other => Err(LoweringError::unsupported(
            "from.range_table",
            reference.cast(),
            format!("RTE kind {other:?} is not a relation or subquery"),
        )),
    }
}

fn combine_relations(left: &Relation, right: &Relation) -> Relation {
    let mut relation = Relation::empty();
    relation.columns = left.columns.iter().chain(&right.columns).cloned().collect();
    relation.vars = left.vars.clone();
    let offset = left.columns.len();
    relation.vars.extend(
        right
            .vars
            .iter()
            .map(|(key, column)| (*key, column + offset)),
    );
    relation.groups = left.groups.clone();
    relation.groups.extend(
        right
            .groups
            .iter()
            .map(|(node, column)| (*node, column + offset)),
    );
    relation.aggregates = left.aggregates.clone();
    relation.aggregates.extend(
        right
            .aggregates
            .iter()
            .map(|(key, column)| (*key, column + offset)),
    );
    relation.windows = left.windows.clone();
    relation.windows.extend(
        right
            .windows
            .iter()
            .map(|(key, column)| (*key, column + offset)),
    );
    relation.target_refs = left.target_refs.clone();
    relation.target_refs.extend(
        right
            .target_refs
            .iter()
            .map(|(key, column)| (*key, column + offset)),
    );
    relation
}

fn true_constant() -> ScalarExpr {
    ScalarExpr::Constant {
        type_: SlotType {
            type_oid: pg_sys::BOOLOID.to_u32(),
            typmod: -1,
            collation_oid: pg_sys::InvalidOid.to_u32(),
            nullable: false,
        },
        value: Some(DatumRepr::Text("t".into())),
    }
}

fn join_kind(kind: pg_sys::JoinType::Type) -> Result<JoinKind, LoweringError> {
    match kind {
        pg_sys::JoinType::JOIN_INNER => Ok(JoinKind::Inner),
        pg_sys::JoinType::JOIN_LEFT => Ok(JoinKind::Left),
        pg_sys::JoinType::JOIN_RIGHT => Ok(JoinKind::Right),
        pg_sys::JoinType::JOIN_FULL => Ok(JoinKind::Full),
        pg_sys::JoinType::JOIN_SEMI => Ok(JoinKind::Semi),
        pg_sys::JoinType::JOIN_ANTI => Ok(JoinKind::Anti),
        other => Err(LoweringError::invalid(
            "join.kind",
            format!("PostgreSQL join kind {other:?} is not executable"),
        )),
    }
}

unsafe fn build_join(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    left: Relation,
    right: Relation,
    kind: JoinKind,
    qualification: *mut pg_sys::Node,
    join_rtindex: Option<i32>,
) -> Result<Relation, LoweringError> {
    let stage_id = builder.next_stage_id();
    let left_bound = builder.bind_input(&left, 0);
    let right_bound = builder.bind_input(&right, 1);
    let combined = combine_relations(&left, &right);
    let bindings = left_bound
        .columns
        .iter()
        .chain(&right_bound.columns)
        .copied()
        .collect::<Vec<_>>();
    let context = ScalarContext {
        scopes: vec![ScalarScope {
            relation: &combined,
            bindings: &bindings,
        }],
    };
    let condition = if qualification.is_null() {
        true_constant()
    } else {
        scalar(qualification, &context, "join.condition")?
    };
    let output_count = if matches!(
        kind,
        JoinKind::Semi | JoinKind::Anti | JoinKind::NullAwareAnti
    ) {
        left.columns.len()
    } else {
        combined.columns.len()
    };
    let mut outputs = Vec::with_capacity(output_count);
    let mut expressions = Vec::with_capacity(output_count);
    for (index, (column, binding)) in combined
        .columns
        .iter()
        .zip(&bindings)
        .take(output_count)
        .enumerate()
    {
        let mut type_ = column.type_.clone();
        let from_left = index < left.columns.len();
        if matches!(kind, JoinKind::Left) && !from_left
            || matches!(kind, JoinKind::Right) && from_left
            || matches!(kind, JoinKind::Full)
        {
            type_.nullable = true;
        }
        let output = builder.slot();
        outputs.push(Column {
            slot: output,
            type_,
            name: column.name.clone(),
        });
        expressions.push(NamedExpr {
            output,
            name: column.name.clone(),
            expr: ScalarExpr::Input { binding: *binding },
        });
    }
    let mut relation = Relation {
        stage_id,
        columns: outputs,
        vars: combined
            .vars
            .iter()
            .filter(|(_, column)| **column < output_count)
            .map(|(key, column)| (*key, *column))
            .collect(),
        groups: Vec::new(),
        aggregates: HashMap::new(),
        windows: HashMap::new(),
        target_refs: HashMap::new(),
    };
    let mut schema_inputs = left_bound.schema_inputs;
    schema_inputs.extend(right_bound.schema_inputs);
    builder.stages.push(DataflowStage {
        spec: OperatorSpec::Join(JoinSpec {
            kind,
            condition,
            outputs: expressions,
        }),
        schema: typed_schema(schema_inputs, &relation.columns),
        inputs: vec![left_bound.input, right_bound.input],
    });
    if let Some(rtindex) = join_rtindex {
        relation = lower_join_aliases(builder, query, relation, rtindex)?;
    }
    Ok(relation)
}

unsafe fn lower_join_aliases(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    joined: Relation,
    rtindex: i32,
) -> Result<Relation, LoweringError> {
    if rtindex <= 0 {
        return Ok(joined);
    }
    let entry = range_table_entry(query, rtindex)?;
    if (*entry).rtekind != pg_sys::RTEKind::RTE_JOIN {
        return Err(LoweringError::invalid(
            "join.alias",
            format!("JoinExpr rtindex {rtindex} does not address RTE_JOIN"),
        ));
    }
    let names = alias_column_names(entry);
    enum AliasOutput {
        Existing(usize),
        Computed {
            expression: *mut pg_sys::Node,
            name: Option<String>,
        },
    }
    let mut aliases = Vec::new();
    for (offset, alias) in list_items((*entry).joinaliasvars).enumerate() {
        let alias = alias.cast::<pg_sys::Node>();
        if alias.is_null() {
            return Err(LoweringError::unsupported(
                "join.alias",
                query.cast(),
                "RTE_JOIN contains an empty alias expression",
            ));
        }
        let name = names.get(offset).cloned().flatten();
        let key = VarKey {
            rte: rtindex,
            attnum: i16::try_from(offset + 1).map_err(|_| {
                LoweringError::invalid("join.alias", "join exposes too many output columns")
            })?,
        };
        let existing = if (*alias).type_ == pg_sys::NodeTag::T_Var {
            let variable = alias.cast::<pg_sys::Var>();
            ((*variable).varlevelsup == 0)
                .then(|| {
                    joined
                        .vars
                        .get(&VarKey {
                            rte: (*variable).varno,
                            attnum: (*variable).varattno,
                        })
                        .copied()
                })
                .flatten()
        } else {
            None
        };
        aliases.push((
            key,
            existing.map_or(
                AliasOutput::Computed {
                    expression: alias,
                    name,
                },
                AliasOutput::Existing,
            ),
        ));
    }
    if aliases
        .iter()
        .all(|(_, alias)| matches!(alias, AliasOutput::Existing(_)))
    {
        let mut relation = joined;
        for (key, alias) in aliases {
            let AliasOutput::Existing(column) = alias else {
                unreachable!();
            };
            relation.vars.insert(key, column);
        }
        return Ok(relation);
    }

    let stage_id = builder.next_stage_id();
    let bound = builder.bind_input(&joined, 0);
    let context = ScalarContext {
        scopes: vec![ScalarScope {
            relation: &joined,
            bindings: &bound.columns,
        }],
    };
    let computed_count = aliases
        .iter()
        .filter(|(_, alias)| matches!(alias, AliasOutput::Computed { .. }))
        .count();
    let (mut columns, mut expressions) =
        builder.passthrough_outputs(&joined, &bound.columns, computed_count);
    let mut join_vars = HashMap::new();
    for (key, alias) in aliases {
        match alias {
            AliasOutput::Existing(column) => {
                join_vars.insert(key, column);
            }
            AliasOutput::Computed { expression, name } => {
                let output = builder.slot();
                let column = columns.len();
                columns.push(Column {
                    slot: output,
                    type_: slot_type(expression),
                    name: name.clone(),
                });
                expressions.push(NamedExpr {
                    output,
                    name,
                    expr: scalar(expression, &context, "join.alias")?,
                });
                join_vars.insert(key, column);
            }
        }
    }
    let mut relation = Relation {
        stage_id,
        columns,
        vars: joined.vars.clone(),
        groups: joined.groups.clone(),
        aggregates: joined.aggregates.clone(),
        windows: joined.windows.clone(),
        target_refs: joined.target_refs.clone(),
    };
    relation.vars.extend(join_vars);
    builder.stages.push(DataflowStage {
        spec: OperatorSpec::Project(ProjectSpec { expressions }),
        schema: typed_schema(bound.schema_inputs, &relation.columns),
        inputs: vec![bound.input],
    });
    Ok(relation)
}

unsafe fn lower_from_item(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    node: *mut pg_sys::Node,
) -> Result<Relation, LoweringError> {
    if node.is_null() {
        return Err(LoweringError::invalid("from.item", "NULL FROM item"));
    }
    match (*node).type_ {
        pg_sys::NodeTag::T_RangeTblRef => lower_range_ref(builder, query, node.cast()),
        pg_sys::NodeTag::T_JoinExpr => {
            let join = node.cast::<pg_sys::JoinExpr>();
            let left = lower_from_item(builder, query, (*join).larg)?;
            let right = lower_from_item(builder, query, (*join).rarg)?;
            build_join(
                builder,
                query,
                left,
                right,
                join_kind((*join).jointype)?,
                (*join).quals,
                ((*join).rtindex > 0).then_some((*join).rtindex),
            )
        }
        _ => Err(LoweringError::unsupported(
            "from.item",
            node,
            "FROM accepts RangeTblRef and JoinExpr nodes",
        )),
    }
}

unsafe fn lower_from(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
) -> Result<Relation, LoweringError> {
    if (*query).jointree.is_null() {
        return Err(LoweringError::unsupported(
            "from",
            query.cast(),
            "SELECT without a FROM relation is not a maintained dataflow",
        ));
    }
    let mut items = list_items((*(*query).jointree).fromlist);
    let Some(first) = items.next() else {
        return Err(LoweringError::unsupported(
            "from",
            (*query).jointree.cast(),
            "SELECT without a FROM relation is not a maintained dataflow",
        ));
    };
    let mut relation = lower_from_item(builder, query, first.cast())?;
    for item in items {
        let right = lower_from_item(builder, query, item.cast())?;
        relation = build_join(
            builder,
            query,
            relation,
            right,
            JoinKind::Inner,
            std::ptr::null_mut(),
            None,
        )?;
    }
    Ok(relation)
}

unsafe fn lower_filter(
    builder: &mut Builder,
    source: Relation,
    predicate: *mut pg_sys::Node,
    capability: &'static str,
) -> Result<Relation, LoweringError> {
    let stage_id = builder.next_stage_id();
    let bound = builder.bind_input(&source, 0);
    let context = ScalarContext {
        scopes: vec![ScalarScope {
            relation: &source,
            bindings: &bound.columns,
        }],
    };
    let predicate = scalar(predicate, &context, capability)?;
    let (outputs, expressions) = builder.passthrough_outputs(&source, &bound.columns, 0);
    builder.stages.push(DataflowStage {
        spec: OperatorSpec::Filter(FilterSpec {
            predicate,
            outputs: expressions,
        }),
        schema: typed_schema(bound.schema_inputs, &outputs),
        inputs: vec![bound.input],
    });
    Ok(remap_relation(&source, stage_id, outputs))
}

unsafe fn conjuncts(node: *mut pg_sys::Node) -> Vec<*mut pg_sys::Node> {
    if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_BoolExpr {
        return vec![node];
    }
    let boolean = node.cast::<pg_sys::BoolExpr>();
    if (*boolean).boolop != pg_sys::BoolExprType::AND_EXPR {
        return vec![node];
    }
    list_items((*boolean).args)
        .flat_map(|argument| conjuncts(argument.cast()))
        .collect()
}

unsafe fn collect_var_levels(node: *mut pg_sys::Node, levels: &mut BTreeSet<u32>) {
    if node.is_null() {
        return;
    }
    match (*node).type_ {
        pg_sys::NodeTag::T_Var => {
            levels.insert((*node.cast::<pg_sys::Var>()).varlevelsup);
        }
        pg_sys::NodeTag::T_OpExpr
        | pg_sys::NodeTag::T_DistinctExpr
        | pg_sys::NodeTag::T_NullIfExpr => {
            for argument in list_items((*node.cast::<pg_sys::OpExpr>()).args) {
                collect_var_levels(argument.cast(), levels);
            }
        }
        pg_sys::NodeTag::T_FuncExpr => {
            for argument in list_items((*node.cast::<pg_sys::FuncExpr>()).args) {
                collect_var_levels(argument.cast(), levels);
            }
        }
        pg_sys::NodeTag::T_ScalarArrayOpExpr => {
            for argument in list_items((*node.cast::<pg_sys::ScalarArrayOpExpr>()).args) {
                collect_var_levels(argument.cast(), levels);
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            for argument in list_items((*node.cast::<pg_sys::BoolExpr>()).args) {
                collect_var_levels(argument.cast(), levels);
            }
        }
        pg_sys::NodeTag::T_NullTest => {
            collect_var_levels((*node.cast::<pg_sys::NullTest>()).arg.cast(), levels);
        }
        pg_sys::NodeTag::T_BooleanTest => {
            collect_var_levels((*node.cast::<pg_sys::BooleanTest>()).arg.cast(), levels);
        }
        pg_sys::NodeTag::T_CoalesceExpr => {
            for argument in list_items((*node.cast::<pg_sys::CoalesceExpr>()).args) {
                collect_var_levels(argument.cast(), levels);
            }
        }
        pg_sys::NodeTag::T_RelabelType => {
            collect_var_levels((*node.cast::<pg_sys::RelabelType>()).arg.cast(), levels);
        }
        pg_sys::NodeTag::T_CoerceViaIO => {
            collect_var_levels((*node.cast::<pg_sys::CoerceViaIO>()).arg.cast(), levels);
        }
        pg_sys::NodeTag::T_CoerceToDomain => {
            collect_var_levels((*node.cast::<pg_sys::CoerceToDomain>()).arg.cast(), levels);
        }
        pg_sys::NodeTag::T_CollateExpr => {
            collect_var_levels((*node.cast::<pg_sys::CollateExpr>()).arg.cast(), levels);
        }
        pg_sys::NodeTag::T_CaseExpr => {
            let case = node.cast::<pg_sys::CaseExpr>();
            collect_var_levels((*case).arg.cast(), levels);
            for item in list_items((*case).args) {
                let arm = item.cast::<pg_sys::CaseWhen>();
                collect_var_levels((*arm).expr.cast(), levels);
                collect_var_levels((*arm).result.cast(), levels);
            }
            collect_var_levels((*case).defresult.cast(), levels);
        }
        _ => {}
    }
}

enum SemiCondition {
    Correlated(Vec<*mut pg_sys::Node>),
    In {
        outer: *mut pg_sys::Node,
        outer_on_left: bool,
        operator_oid: u32,
        operator_function_oid: pg_sys::Oid,
    },
    True,
}

unsafe fn build_semi_join(
    builder: &mut Builder,
    outer: Relation,
    inner: Relation,
    kind: JoinKind,
    condition: SemiCondition,
) -> Result<Relation, LoweringError> {
    let stage_id = builder.next_stage_id();
    let outer_bound = builder.bind_input(&outer, 0);
    let inner_bound = builder.bind_input(&inner, 1);
    let condition = match condition {
        SemiCondition::True => true_constant(),
        SemiCondition::Correlated(expressions) => {
            let context = ScalarContext {
                // The expression belongs to the inner Query: level 0 is its
                // relation, level 1 is the outer relation.
                scopes: vec![
                    ScalarScope {
                        relation: &inner,
                        bindings: &inner_bound.columns,
                    },
                    ScalarScope {
                        relation: &outer,
                        bindings: &outer_bound.columns,
                    },
                ],
            };
            let mut lowered = expressions
                .into_iter()
                .map(|expression| scalar(expression, &context, "subquery.correlation"))
                .collect::<Result<Vec<_>, _>>()?;
            if lowered.len() == 1 {
                lowered.pop().expect("checked one expression")
            } else {
                ScalarExpr::Bool {
                    op: BoolExprKind::And,
                    args: lowered,
                }
            }
        }
        SemiCondition::In {
            outer: expression,
            outer_on_left,
            operator_oid,
            operator_function_oid,
        } => {
            immutable_function(operator_function_oid, expression, "subquery.in")?;
            let outer_context = ScalarContext {
                scopes: vec![ScalarScope {
                    relation: &outer,
                    bindings: &outer_bound.columns,
                }],
            };
            let left = scalar(expression, &outer_context, "subquery.in")?;
            let Some(right_binding) = inner_bound.columns.first().copied() else {
                return Err(LoweringError::invalid(
                    "subquery.in",
                    "IN subquery has no output column",
                ));
            };
            let inner = ScalarExpr::Input {
                binding: right_binding,
            };
            ScalarExpr::Operator {
                operator_oid,
                args: if outer_on_left {
                    vec![left, inner]
                } else {
                    vec![inner, left]
                },
                type_: SlotType {
                    type_oid: pg_sys::BOOLOID.to_u32(),
                    typmod: -1,
                    collation_oid: pg_sys::InvalidOid.to_u32(),
                    nullable: true,
                },
            }
        }
    };
    let (columns, expressions) = builder.passthrough_outputs(&outer, &outer_bound.columns, 0);
    let mut schema_inputs = outer_bound.schema_inputs;
    schema_inputs.extend(inner_bound.schema_inputs);
    builder.stages.push(DataflowStage {
        spec: OperatorSpec::Join(JoinSpec {
            kind,
            condition,
            outputs: expressions,
        }),
        schema: typed_schema(schema_inputs, &columns),
        inputs: vec![outer_bound.input, inner_bound.input],
    });
    Ok(remap_relation(&outer, stage_id, columns))
}

unsafe fn lower_exists(
    builder: &mut Builder,
    outer: Relation,
    link: *mut pg_sys::SubLink,
    negated: bool,
) -> Result<Relation, LoweringError> {
    if (*link).subselect.is_null() || (*(*link).subselect).type_ != pg_sys::NodeTag::T_Query {
        return Err(LoweringError::unsupported(
            "subquery.exists",
            link.cast(),
            "EXISTS does not contain an analyzed Query",
        ));
    }
    let inner_query = (*link).subselect.cast::<pg_sys::Query>();
    if (*inner_query).hasAggs
        || (*inner_query).hasWindowFuncs
        || (*inner_query).hasSubLinks
        || !(*inner_query).groupClause.is_null()
        || !(*inner_query).havingQual.is_null()
        || !(*inner_query).limitCount.is_null()
        || !(*inner_query).limitOffset.is_null()
        || !(*inner_query).setOperations.is_null()
    {
        return Err(LoweringError::unsupported(
            "subquery.exists",
            inner_query.cast(),
            "correlated EXISTS currently supports a relational FROM/WHERE body",
        ));
    }
    let mut inner = lower_from(builder, inner_query)?;
    let mut correlated = Vec::new();
    if !(*inner_query).jointree.is_null() {
        for predicate in conjuncts((*(*inner_query).jointree).quals) {
            if predicate.is_null() {
                continue;
            }
            let mut levels = BTreeSet::new();
            collect_var_levels(predicate, &mut levels);
            if levels.iter().any(|level| *level > 1) {
                return Err(LoweringError::unsupported(
                    "subquery.correlation",
                    predicate,
                    "correlation deeper than one query level is not supported",
                ));
            }
            if levels.contains(&1) {
                if !levels.contains(&0) {
                    return Err(LoweringError::unsupported(
                        "subquery.correlation",
                        predicate,
                        "correlated predicate must also reference the inner input",
                    ));
                }
                correlated.push(predicate);
            } else {
                inner = lower_filter(builder, inner, predicate, "subquery.filter")?;
            }
        }
    }
    let condition = match correlated.as_slice() {
        [] => SemiCondition::True,
        _ => SemiCondition::Correlated(correlated),
    };
    build_semi_join(
        builder,
        outer,
        inner,
        if negated {
            JoinKind::Anti
        } else {
            JoinKind::Semi
        },
        condition,
    )
}

unsafe fn lower_in(
    builder: &mut Builder,
    outer: Relation,
    link: *mut pg_sys::SubLink,
    negated: bool,
) -> Result<Relation, LoweringError> {
    if (*link).subselect.is_null() || (*(*link).subselect).type_ != pg_sys::NodeTag::T_Query {
        return Err(LoweringError::unsupported(
            "subquery.in",
            link.cast(),
            "IN does not contain an analyzed Query",
        ));
    }
    if (*link).testexpr.is_null() || (*(*link).testexpr).type_ != pg_sys::NodeTag::T_OpExpr {
        return Err(LoweringError::unsupported(
            "subquery.in",
            (*link).testexpr,
            "IN requires one analyzed operator expression",
        ));
    }
    let operation = (*link).testexpr.cast::<pg_sys::OpExpr>();
    let arguments = list_items((*operation).args)
        .map(|argument| argument.cast::<pg_sys::Node>())
        .collect::<Vec<_>>();
    let [left, right] = arguments.as_slice() else {
        return Err(LoweringError::unsupported(
            "subquery.in",
            (*link).testexpr,
            "IN operator does not have two arguments",
        ));
    };
    unsafe fn sublink_param(node: *mut pg_sys::Node) -> bool {
        if node.is_null() {
            return false;
        }
        match (*node).type_ {
            pg_sys::NodeTag::T_Param => {
                (*node.cast::<pg_sys::Param>()).paramkind == pg_sys::ParamKind::PARAM_SUBLINK
            }
            pg_sys::NodeTag::T_RelabelType => {
                sublink_param((*node.cast::<pg_sys::RelabelType>()).arg.cast())
            }
            _ => false,
        }
    }
    let (outer_expression, outer_on_left) = match (sublink_param(*left), sublink_param(*right)) {
        (false, true) => (*left, true),
        (true, false) => (*right, false),
        _ => {
            return Err(LoweringError::unsupported(
                "subquery.in",
                (*link).testexpr,
                "IN must compare one outer expression with one subquery parameter",
            ));
        }
    };
    let inner_query = (*link).subselect.cast::<pg_sys::Query>();
    let inner = lower_query_body(builder, inner_query)?;
    if inner.columns.len() != 1 {
        return Err(LoweringError::unsupported(
            "subquery.in",
            inner_query.cast(),
            format!(
                "IN subquery must produce one column, found {}",
                inner.columns.len()
            ),
        ));
    }
    build_semi_join(
        builder,
        outer,
        inner,
        if negated {
            JoinKind::NullAwareAnti
        } else {
            JoinKind::Semi
        },
        SemiCondition::In {
            outer: outer_expression,
            outer_on_left,
            operator_oid: (*operation).opno.to_u32(),
            operator_function_oid: (*operation).opfuncid,
        },
    )
}

unsafe fn lower_sublink(
    builder: &mut Builder,
    _query: *mut pg_sys::Query,
    outer: Relation,
    link: *mut pg_sys::SubLink,
    negated: bool,
) -> Result<Relation, LoweringError> {
    match (*link).subLinkType {
        pg_sys::SubLinkType::EXISTS_SUBLINK => lower_exists(builder, outer, link, negated),
        pg_sys::SubLinkType::ANY_SUBLINK => lower_in(builder, outer, link, negated),
        other => Err(LoweringError::unsupported(
            "subquery.kind",
            link.cast(),
            format!("SubLink type {other:?} is not a semi/anti join capability"),
        )),
    }
}

unsafe fn lower_where(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    mut relation: Relation,
) -> Result<Relation, LoweringError> {
    if (*query).jointree.is_null() || (*(*query).jointree).quals.is_null() {
        return Ok(relation);
    }
    for predicate in conjuncts((*(*query).jointree).quals) {
        if (*predicate).type_ == pg_sys::NodeTag::T_SubLink {
            relation = lower_sublink(builder, query, relation, predicate.cast(), false)?;
            continue;
        }
        if (*predicate).type_ == pg_sys::NodeTag::T_BoolExpr {
            let boolean = predicate.cast::<pg_sys::BoolExpr>();
            let arguments = list_items((*boolean).args).collect::<Vec<_>>();
            if (*boolean).boolop == pg_sys::BoolExprType::NOT_EXPR
                && arguments.len() == 1
                && (*arguments[0].cast::<pg_sys::Node>()).type_ == pg_sys::NodeTag::T_SubLink
            {
                relation = lower_sublink(builder, query, relation, arguments[0].cast(), true)?;
                continue;
            }
        }
        relation = lower_filter(builder, relation, predicate, "where.predicate")?;
    }
    Ok(relation)
}

unsafe fn target_entries(query: *mut pg_sys::Query) -> Vec<*mut pg_sys::TargetEntry> {
    list_items((*query).targetList)
        .map(|item| item.cast())
        .collect()
}

unsafe fn target_for_sort_ref(
    query: *mut pg_sys::Query,
    reference: u32,
    capability: &'static str,
) -> Result<*mut pg_sys::TargetEntry, LoweringError> {
    target_entries(query)
        .into_iter()
        .find(|target| (**target).ressortgroupref == reference)
        .ok_or_else(|| {
            LoweringError::invalid(
                capability,
                format!("sort/group reference {reference} has no TargetEntry"),
            )
        })
}

unsafe fn walk_expression(node: *mut pg_sys::Node, visitor: &mut impl FnMut(*mut pg_sys::Node)) {
    if node.is_null() {
        return;
    }
    visitor(node);
    match (*node).type_ {
        pg_sys::NodeTag::T_Aggref => {
            let aggregate = node.cast::<pg_sys::Aggref>();
            for argument in list_items((*aggregate).aggdirectargs) {
                walk_expression(argument.cast(), visitor);
            }
            for item in list_items((*aggregate).args) {
                let target = item.cast::<pg_sys::TargetEntry>();
                walk_expression((*target).expr.cast(), visitor);
            }
            walk_expression((*aggregate).aggfilter.cast(), visitor);
        }
        pg_sys::NodeTag::T_WindowFunc => {
            let function = node.cast::<pg_sys::WindowFunc>();
            for argument in list_items((*function).args) {
                walk_expression(argument.cast(), visitor);
            }
            walk_expression((*function).aggfilter.cast(), visitor);
        }
        pg_sys::NodeTag::T_OpExpr
        | pg_sys::NodeTag::T_DistinctExpr
        | pg_sys::NodeTag::T_NullIfExpr => {
            for argument in list_items((*node.cast::<pg_sys::OpExpr>()).args) {
                walk_expression(argument.cast(), visitor);
            }
        }
        pg_sys::NodeTag::T_FuncExpr => {
            for argument in list_items((*node.cast::<pg_sys::FuncExpr>()).args) {
                walk_expression(argument.cast(), visitor);
            }
        }
        pg_sys::NodeTag::T_ScalarArrayOpExpr => {
            for argument in list_items((*node.cast::<pg_sys::ScalarArrayOpExpr>()).args) {
                walk_expression(argument.cast(), visitor);
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            for argument in list_items((*node.cast::<pg_sys::BoolExpr>()).args) {
                walk_expression(argument.cast(), visitor);
            }
        }
        pg_sys::NodeTag::T_NullTest => {
            walk_expression((*node.cast::<pg_sys::NullTest>()).arg.cast(), visitor);
        }
        pg_sys::NodeTag::T_BooleanTest => {
            walk_expression((*node.cast::<pg_sys::BooleanTest>()).arg.cast(), visitor);
        }
        pg_sys::NodeTag::T_CoalesceExpr => {
            for argument in list_items((*node.cast::<pg_sys::CoalesceExpr>()).args) {
                walk_expression(argument.cast(), visitor);
            }
        }
        pg_sys::NodeTag::T_RelabelType => {
            walk_expression((*node.cast::<pg_sys::RelabelType>()).arg.cast(), visitor);
        }
        pg_sys::NodeTag::T_CoerceViaIO => {
            walk_expression((*node.cast::<pg_sys::CoerceViaIO>()).arg.cast(), visitor);
        }
        pg_sys::NodeTag::T_CoerceToDomain => {
            walk_expression((*node.cast::<pg_sys::CoerceToDomain>()).arg.cast(), visitor);
        }
        pg_sys::NodeTag::T_CollateExpr => {
            walk_expression((*node.cast::<pg_sys::CollateExpr>()).arg.cast(), visitor);
        }
        pg_sys::NodeTag::T_CaseExpr => {
            let case = node.cast::<pg_sys::CaseExpr>();
            walk_expression((*case).arg.cast(), visitor);
            for item in list_items((*case).args) {
                let arm = item.cast::<pg_sys::CaseWhen>();
                walk_expression((*arm).expr.cast(), visitor);
                walk_expression((*arm).result.cast(), visitor);
            }
            walk_expression((*case).defresult.cast(), visitor);
        }
        // A SubLink owns another Query scope and must not be traversed as a
        // scalar child of this query.
        pg_sys::NodeTag::T_SubLink
        | pg_sys::NodeTag::T_Var
        | pg_sys::NodeTag::T_Const
        | pg_sys::NodeTag::T_Param => {}
        _ => {}
    }
}

unsafe fn collect_aggregate_nodes(
    query: *mut pg_sys::Query,
) -> Result<Vec<*mut pg_sys::Aggref>, LoweringError> {
    let mut aggregates: Vec<*mut pg_sys::Aggref> = Vec::new();
    for target in target_entries(query) {
        walk_expression((*target).expr.cast(), &mut |node| {
            if (*node).type_ == pg_sys::NodeTag::T_Aggref {
                aggregates.push(node.cast());
            }
        });
    }
    walk_expression((*query).havingQual, &mut |node| {
        if (*node).type_ == pg_sys::NodeTag::T_Aggref {
            aggregates.push(node.cast());
        }
    });
    aggregates.sort_by_key(|aggregate| *aggregate as usize);
    aggregates.dedup_by_key(|aggregate| *aggregate as usize);
    if let Some(aggregate) = aggregates
        .iter()
        .copied()
        .find(|aggregate| (**aggregate).agglevelsup != 0)
    {
        return Err(LoweringError::unsupported(
            "aggregate.scope",
            aggregate.cast(),
            "outer-level aggregate references are not supported",
        ));
    }
    Ok(aggregates)
}

unsafe fn sort_group_expression(
    clause: *mut pg_sys::SortGroupClause,
    target: *mut pg_sys::TargetEntry,
    context: &ScalarContext<'_>,
    capability: &'static str,
) -> Result<SortGroupExpr, LoweringError> {
    if (*clause).eqop == pg_sys::InvalidOid || (*clause).sortop == pg_sys::InvalidOid {
        return Err(LoweringError::unsupported(
            capability,
            clause.cast(),
            "PostgreSQL did not resolve equality and sort operators",
        ));
    }
    let expression = (*target).expr.cast();
    Ok(SortGroupExpr {
        expr: scalar(expression, context, capability)?,
        type_: slot_type(expression),
        equality_operator_oid: (*clause).eqop.to_u32(),
        sort_operator_oid: (*clause).sortop.to_u32(),
        nulls_first: (*clause).nulls_first,
        hashable: (*clause).hashable,
    })
}

unsafe fn sort_group_expressions(
    clauses: *mut pg_sys::List,
    targets: *mut pg_sys::List,
    context: &ScalarContext<'_>,
    capability: &'static str,
) -> Result<Vec<SortGroupExpr>, LoweringError> {
    let targets = list_items(targets)
        .map(|item| item.cast::<pg_sys::TargetEntry>())
        .collect::<Vec<_>>();
    list_items(clauses)
        .map(|item| {
            let clause = item.cast::<pg_sys::SortGroupClause>();
            let target = targets
                .iter()
                .copied()
                .find(|target| (**target).ressortgroupref == (*clause).tleSortGroupRef)
                .ok_or_else(|| {
                    LoweringError::invalid(
                        capability,
                        format!(
                            "sort reference {} has no expression",
                            (*clause).tleSortGroupRef
                        ),
                    )
                })?;
            sort_group_expression(clause, target, context, capability)
        })
        .collect()
}

unsafe fn direct_group_var(node: *mut pg_sys::Node) -> Option<VarKey> {
    if node.is_null() {
        return None;
    }
    let node = match (*node).type_ {
        pg_sys::NodeTag::T_RelabelType => (*node.cast::<pg_sys::RelabelType>()).arg.cast(),
        _ => node,
    };
    if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let variable = node.cast::<pg_sys::Var>();
    ((*variable).varlevelsup == 0).then_some(VarKey {
        rte: (*variable).varno,
        attnum: (*variable).varattno,
    })
}

unsafe fn lower_aggregate(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    source: Relation,
) -> Result<Relation, LoweringError> {
    if !(*query).groupingSets.is_null() {
        return Err(LoweringError::unsupported(
            "aggregate.grouping_sets",
            (*query).groupingSets.cast(),
            "GROUPING SETS, ROLLUP, and CUBE need a grouping-set operator",
        ));
    }
    if (*query).groupDistinct {
        return Err(LoweringError::unsupported(
            "aggregate.group_distinct",
            query.cast(),
            "GROUP BY DISTINCT is not supported",
        ));
    }
    let aggregate_nodes = collect_aggregate_nodes(query)?;
    if aggregate_nodes.is_empty() && (*query).groupClause.is_null() {
        return Ok(source);
    }
    let stage_id = builder.next_stage_id();
    let bound = builder.bind_input(&source, 0);
    let context = ScalarContext {
        scopes: vec![ScalarScope {
            relation: &source,
            bindings: &bound.columns,
        }],
    };
    let mut groups = Vec::new();
    let mut group_outputs = Vec::new();
    let mut columns = Vec::new();
    let mut vars = HashMap::new();
    for item in list_items((*query).groupClause) {
        let clause = item.cast::<pg_sys::SortGroupClause>();
        let target = target_for_sort_ref(query, (*clause).tleSortGroupRef, "aggregate.group_by")?;
        let expression = (*target).expr.cast::<pg_sys::Node>();
        let key = sort_group_expression(clause, target, &context, "aggregate.group_by")?;
        let output = builder.slot();
        let index = columns.len();
        let name = c_string((*target).resname);
        columns.push(Column {
            slot: output,
            type_: key.type_.clone(),
            name: name.clone(),
        });
        groups.push(GroupExpr { output, name, key });
        group_outputs.push((expression, index));
        if let Some(key) = direct_group_var(expression) {
            vars.insert(key, index);
        }
    }
    let target_names = target_entries(query)
        .into_iter()
        .map(|target| ((*target).expr as usize, c_string((*target).resname)))
        .collect::<HashMap<_, _>>();
    let mut aggregates = Vec::with_capacity(aggregate_nodes.len());
    let mut aggregate_outputs = HashMap::new();
    for aggregate in aggregate_nodes {
        if (*aggregate).aggfnoid == pg_sys::InvalidOid {
            return Err(LoweringError::unsupported(
                "aggregate.function",
                aggregate.cast(),
                "PostgreSQL did not resolve the aggregate function",
            ));
        }
        if (*aggregate).aggkind != b'n' as std::ffi::c_char {
            return Err(LoweringError::unsupported(
                "aggregate.kind",
                aggregate.cast(),
                "ordered-set and hypothetical-set aggregates need a dedicated capability",
            ));
        }
        let ref_id = builder.aggregate_ref();
        let output = builder.slot();
        let type_ = slot_type(aggregate.cast());
        let index = columns.len();
        let name = target_names.get(&(aggregate as usize)).cloned().flatten();
        columns.push(Column {
            slot: output,
            type_: type_.clone(),
            name,
        });
        aggregate_outputs.insert(aggregate as usize, index);
        let args = list_items((*aggregate).args)
            .filter_map(|item| {
                let target = item.cast::<pg_sys::TargetEntry>();
                (!(*target).resjunk).then_some(target)
            })
            .map(|target| scalar((*target).expr.cast(), &context, "aggregate.argument"))
            .collect::<Result<_, _>>()?;
        let direct_args = list_items((*aggregate).aggdirectargs)
            .map(|argument| scalar(argument.cast(), &context, "aggregate.direct_argument"))
            .collect::<Result<_, _>>()?;
        let filter = (!(*aggregate).aggfilter.is_null())
            .then(|| scalar((*aggregate).aggfilter.cast(), &context, "aggregate.filter"))
            .transpose()?;
        aggregates.push(AggregateExpr {
            ref_id,
            output,
            function_oid: (*aggregate).aggfnoid.to_u32(),
            input_collation_oid: (*aggregate).inputcollid.to_u32(),
            args,
            direct_args,
            distinct: sort_group_expressions(
                (*aggregate).aggdistinct,
                (*aggregate).args,
                &context,
                "aggregate.distinct",
            )?,
            filter,
            order_by: sort_group_expressions(
                (*aggregate).aggorder,
                (*aggregate).args,
                &context,
                "aggregate.order_by",
            )?,
            type_,
        });
    }
    builder.stages.push(DataflowStage {
        spec: OperatorSpec::Aggregate(AggregateSpec { groups, aggregates }),
        schema: typed_schema(bound.schema_inputs, &columns),
        inputs: vec![bound.input],
    });
    Ok(Relation {
        stage_id,
        columns,
        vars,
        groups: group_outputs,
        aggregates: aggregate_outputs,
        windows: HashMap::new(),
        target_refs: HashMap::new(),
    })
}

unsafe fn collect_window_nodes(query: *mut pg_sys::Query) -> Vec<*mut pg_sys::WindowFunc> {
    let mut windows = Vec::new();
    for target in target_entries(query) {
        walk_expression((*target).expr.cast(), &mut |node| {
            if (*node).type_ == pg_sys::NodeTag::T_WindowFunc {
                windows.push(node.cast());
            }
        });
    }
    windows.sort_by_key(|window| *window as usize);
    windows.dedup_by_key(|window| *window as usize);
    windows
}

unsafe fn lower_windows(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    mut source: Relation,
) -> Result<Relation, LoweringError> {
    let window_nodes = collect_window_nodes(query);
    if window_nodes.is_empty() {
        return Ok(source);
    }
    let target_names = target_entries(query)
        .into_iter()
        .map(|target| ((*target).expr as usize, c_string((*target).resname)))
        .collect::<HashMap<_, _>>();
    let mut lowered = BTreeSet::new();
    for item in list_items((*query).windowClause) {
        let clause = item.cast::<pg_sys::WindowClause>();
        let functions = window_nodes
            .iter()
            .copied()
            .filter(|function| (**function).winref == (*clause).winref)
            .collect::<Vec<_>>();
        if functions.is_empty() {
            continue;
        }
        let stage_id = builder.next_stage_id();
        let bound = builder.bind_input(&source, 0);
        let context = ScalarContext {
            scopes: vec![ScalarScope {
                relation: &source,
                bindings: &bound.columns,
            }],
        };
        let partition_by = sort_group_expressions(
            (*clause).partitionClause,
            (*query).targetList,
            &context,
            "window.partition_by",
        )?;
        let order_by = sort_group_expressions(
            (*clause).orderClause,
            (*query).targetList,
            &context,
            "window.order_by",
        )?;
        let start_offset = (!(*clause).startOffset.is_null())
            .then(|| scalar((*clause).startOffset, &context, "window.frame_start"))
            .transpose()?;
        let end_offset = (!(*clause).endOffset.is_null())
            .then(|| scalar((*clause).endOffset, &context, "window.frame_end"))
            .transpose()?;
        let (mut columns, outputs) =
            builder.passthrough_outputs(&source, &bound.columns, functions.len());
        let mut function_specs = Vec::with_capacity(functions.len());
        let mut current_windows = HashMap::new();
        for function in functions {
            if (*function).winfnoid == pg_sys::InvalidOid {
                return Err(LoweringError::unsupported(
                    "window.function",
                    function.cast(),
                    "PostgreSQL did not resolve the window function",
                ));
            }
            let ref_id = builder.window_ref();
            let output = builder.slot();
            let type_ = slot_type(function.cast());
            let name = target_names.get(&(function as usize)).cloned().flatten();
            current_windows.insert(function as usize, columns.len());
            columns.push(Column {
                slot: output,
                type_: type_.clone(),
                name: name.clone(),
            });
            let filter = (!(*function).aggfilter.is_null())
                .then(|| scalar((*function).aggfilter.cast(), &context, "window.filter"))
                .transpose()?;
            function_specs.push(WindowExpr {
                ref_id,
                output,
                function_oid: (*function).winfnoid.to_u32(),
                input_collation_oid: (*function).inputcollid.to_u32(),
                args: list_items((*function).args)
                    .map(|argument| scalar(argument.cast(), &context, "window.argument"))
                    .collect::<Result<_, _>>()?,
                filter,
                star: (*function).winstar,
                aggregate: (*function).winagg,
                type_,
            });
            lowered.insert(function as usize);
        }
        builder.stages.push(DataflowStage {
            spec: OperatorSpec::Window(WindowSpec {
                partition_by,
                order_by,
                frame: WindowFrame {
                    options: (*clause).frameOptions as u32,
                    start_offset,
                    end_offset,
                    start_in_range_function_oid: ((*clause).startInRangeFunc != pg_sys::InvalidOid)
                        .then(|| (*clause).startInRangeFunc.to_u32()),
                    end_in_range_function_oid: ((*clause).endInRangeFunc != pg_sys::InvalidOid)
                        .then(|| (*clause).endInRangeFunc.to_u32()),
                    in_range_collation_oid: (*clause).inRangeColl.to_u32(),
                    in_range_ascending: (*clause).inRangeAsc,
                    in_range_nulls_first: (*clause).inRangeNullsFirst,
                },
                functions: function_specs,
                outputs,
            }),
            schema: typed_schema(bound.schema_inputs, &columns),
            inputs: vec![bound.input],
        });
        let mut relation = Relation {
            stage_id,
            columns,
            vars: source.vars,
            groups: source.groups,
            aggregates: source.aggregates,
            windows: source.windows,
            target_refs: source.target_refs,
        };
        relation.windows.extend(current_windows);
        debug_assert!(relation
            .windows
            .values()
            .all(|column| *column < relation.columns.len()));
        source = relation;
    }
    if let Some(window) = window_nodes
        .iter()
        .find(|window| !lowered.contains(&(**window as usize)))
    {
        return Err(LoweringError::unsupported(
            "window.specification",
            (*window).cast(),
            format!("window reference {} has no WindowClause", (**window).winref),
        ));
    }
    Ok(source)
}

unsafe fn lower_project(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    source: Relation,
    include_junk: bool,
) -> Result<Relation, LoweringError> {
    let targets = target_entries(query)
        .into_iter()
        .filter(|target| include_junk || !(**target).resjunk)
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Err(LoweringError::unsupported(
            "project.target_list",
            query.cast(),
            "query has no visible target expressions",
        ));
    }
    let stage_id = builder.next_stage_id();
    let bound = builder.bind_input(&source, 0);
    let context = ScalarContext {
        scopes: vec![ScalarScope {
            relation: &source,
            bindings: &bound.columns,
        }],
    };
    let mut columns = Vec::with_capacity(targets.len());
    let mut expressions = Vec::with_capacity(targets.len());
    let mut target_refs = HashMap::new();
    for target in targets {
        let expression = (*target).expr.cast::<pg_sys::Node>();
        let output = builder.slot();
        let name = c_string((*target).resname);
        let index = columns.len();
        columns.push(Column {
            slot: output,
            type_: slot_type(expression),
            name: name.clone(),
        });
        expressions.push(NamedExpr {
            output,
            name,
            expr: scalar(expression, &context, "project.expression")?,
        });
        if (*target).ressortgroupref != 0 {
            target_refs.insert((*target).ressortgroupref, index);
        }
    }
    builder.stages.push(DataflowStage {
        spec: OperatorSpec::Project(ProjectSpec { expressions }),
        schema: typed_schema(bound.schema_inputs, &columns),
        inputs: vec![bound.input],
    });
    Ok(Relation {
        stage_id,
        columns,
        vars: HashMap::new(),
        groups: Vec::new(),
        aggregates: HashMap::new(),
        windows: HashMap::new(),
        target_refs,
    })
}

unsafe fn lower_visible_project(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    source: Relation,
) -> Result<Relation, LoweringError> {
    let visible = target_entries(query)
        .into_iter()
        .enumerate()
        .filter(|(_, target)| !(**target).resjunk)
        .collect::<Vec<_>>();
    if visible.len() == source.columns.len() {
        return Ok(source);
    }
    let stage_id = builder.next_stage_id();
    let bound = builder.bind_input(&source, 0);
    let mut columns = Vec::with_capacity(visible.len());
    let mut expressions = Vec::with_capacity(visible.len());
    for (index, target) in visible {
        if index >= source.columns.len() {
            return Err(LoweringError::invalid(
                "project.visible",
                "projected target position is absent from the input row",
            ));
        }
        let output = builder.slot();
        let name = c_string((*target).resname);
        columns.push(Column {
            slot: output,
            type_: source.columns[index].type_.clone(),
            name: name.clone(),
        });
        expressions.push(NamedExpr {
            output,
            name,
            expr: ScalarExpr::Input {
                binding: bound.columns[index],
            },
        });
    }
    builder.stages.push(DataflowStage {
        spec: OperatorSpec::Project(ProjectSpec { expressions }),
        schema: typed_schema(bound.schema_inputs, &columns),
        inputs: vec![bound.input],
    });
    Ok(Relation {
        stage_id,
        columns,
        vars: HashMap::new(),
        groups: Vec::new(),
        aggregates: HashMap::new(),
        windows: HashMap::new(),
        target_refs: HashMap::new(),
    })
}

unsafe fn lower_distinct(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    source: Relation,
) -> Result<Relation, LoweringError> {
    if (*query).distinctClause.is_null() {
        return Ok(source);
    }
    if (*query).hasDistinctOn {
        return Err(LoweringError::unsupported(
            "distinct.on",
            query.cast(),
            "DISTINCT ON needs ordered first-row state, distinct keys alone are insufficient",
        ));
    }
    let stage_id = builder.next_stage_id();
    let bound = builder.bind_input(&source, 0);
    let mut keys = Vec::new();
    for item in list_items((*query).distinctClause) {
        let clause = item.cast::<pg_sys::SortGroupClause>();
        let Some(column) = source.target_refs.get(&(*clause).tleSortGroupRef).copied() else {
            return Err(LoweringError::invalid(
                "distinct.key",
                format!(
                    "DISTINCT reference {} is absent from the projected row",
                    (*clause).tleSortGroupRef
                ),
            ));
        };
        if (*clause).eqop == pg_sys::InvalidOid || (*clause).sortop == pg_sys::InvalidOid {
            return Err(LoweringError::unsupported(
                "distinct.key",
                clause.cast(),
                "PostgreSQL did not resolve equality and sort operators",
            ));
        }
        keys.push(SortGroupExpr {
            expr: ScalarExpr::Input {
                binding: bound.columns[column],
            },
            type_: source.columns[column].type_.clone(),
            equality_operator_oid: (*clause).eqop.to_u32(),
            sort_operator_oid: (*clause).sortop.to_u32(),
            nulls_first: (*clause).nulls_first,
            hashable: (*clause).hashable,
        });
    }
    let (columns, outputs) = builder.passthrough_outputs(&source, &bound.columns, 0);
    builder.stages.push(DataflowStage {
        spec: OperatorSpec::Distinct(DistinctSpec { keys, outputs }),
        schema: typed_schema(bound.schema_inputs, &columns),
        inputs: vec![bound.input],
    });
    Ok(remap_relation(&source, stage_id, columns))
}

unsafe fn integer_constant(
    node: *mut pg_sys::Node,
    capability: &'static str,
) -> Result<Option<u64>, LoweringError> {
    if node.is_null() {
        return Ok(None);
    }
    match (*node).type_ {
        pg_sys::NodeTag::T_RelabelType => {
            return integer_constant((*node.cast::<pg_sys::RelabelType>()).arg.cast(), capability);
        }
        pg_sys::NodeTag::T_CoerceViaIO => {
            return integer_constant((*node.cast::<pg_sys::CoerceViaIO>()).arg.cast(), capability);
        }
        pg_sys::NodeTag::T_CoerceToDomain => {
            return integer_constant(
                (*node.cast::<pg_sys::CoerceToDomain>()).arg.cast(),
                capability,
            );
        }
        pg_sys::NodeTag::T_FuncExpr => {
            let function = node.cast::<pg_sys::FuncExpr>();
            if !matches!(
                (*function).funcformat,
                pg_sys::CoercionForm::COERCE_EXPLICIT_CAST
                    | pg_sys::CoercionForm::COERCE_IMPLICIT_CAST
            ) {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "LIMIT/OFFSET function is not a cast",
                ));
            }
            let arguments = list_items((*function).args).collect::<Vec<_>>();
            let [argument] = arguments.as_slice() else {
                return Err(LoweringError::unsupported(
                    capability,
                    node,
                    "LIMIT/OFFSET cast does not have one argument",
                ));
            };
            return integer_constant((*argument).cast(), capability);
        }
        pg_sys::NodeTag::T_Const => {}
        _ => {
            return Err(LoweringError::unsupported(
                capability,
                node,
                "LIMIT/OFFSET must be a nonnegative integer constant",
            ));
        }
    }
    let constant = node.cast::<pg_sys::Const>();
    if (*constant).constisnull {
        return Ok(None);
    }
    let mut output_function = pg_sys::InvalidOid;
    let mut is_varlena = false;
    pg_sys::getTypeOutputInfo((*constant).consttype, &mut output_function, &mut is_varlena);
    let output = pg_sys::OidOutputFunctionCall(output_function, (*constant).constvalue);
    let value = c_string(output).ok_or_else(|| {
        LoweringError::unsupported(
            capability,
            node,
            "integer type output function returned NULL",
        )
    })?;
    value.parse::<u64>().map(Some).map_err(|_| {
        LoweringError::unsupported(
            capability,
            node,
            format!("LIMIT/OFFSET value {value:?} is not a nonnegative integer"),
        )
    })
}

unsafe fn lower_topn(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
    source: Relation,
) -> Result<Relation, LoweringError> {
    let has_order = !(*query).sortClause.is_null();
    let limit = integer_constant((*query).limitCount, "topn.limit")?;
    let offset = integer_constant((*query).limitOffset, "topn.offset")?.unwrap_or(0);
    if !has_order && limit.is_none() && offset == 0 {
        return Ok(source);
    }
    if !has_order {
        return Err(LoweringError::unsupported(
            "topn.order_by",
            query.cast(),
            "LIMIT/OFFSET without ORDER BY has no deterministic maintained row set",
        ));
    }
    let Some(limit) = limit else {
        return Err(LoweringError::unsupported(
            "topn.limit",
            (*query).sortClause.cast(),
            "ORDER BY without a finite LIMIT has no relational effect in a sink table",
        ));
    };
    let stage_id = builder.next_stage_id();
    let bound = builder.bind_input(&source, 0);
    let mut order_by = Vec::new();
    for item in list_items((*query).sortClause) {
        let clause = item.cast::<pg_sys::SortGroupClause>();
        let Some(column) = source.target_refs.get(&(*clause).tleSortGroupRef).copied() else {
            return Err(LoweringError::invalid(
                "topn.order_by",
                format!(
                    "ORDER BY reference {} is absent from the projected row",
                    (*clause).tleSortGroupRef
                ),
            ));
        };
        if (*clause).eqop == pg_sys::InvalidOid || (*clause).sortop == pg_sys::InvalidOid {
            return Err(LoweringError::unsupported(
                "topn.order_by",
                clause.cast(),
                "PostgreSQL did not resolve equality and sort operators",
            ));
        }
        order_by.push(SortGroupExpr {
            expr: ScalarExpr::Input {
                binding: bound.columns[column],
            },
            type_: source.columns[column].type_.clone(),
            equality_operator_oid: (*clause).eqop.to_u32(),
            sort_operator_oid: (*clause).sortop.to_u32(),
            nulls_first: (*clause).nulls_first,
            hashable: (*clause).hashable,
        });
    }
    let (columns, outputs) = builder.passthrough_outputs(&source, &bound.columns, 0);
    builder.stages.push(DataflowStage {
        spec: OperatorSpec::TopN(TopNSpec {
            order_by,
            limit,
            offset,
            with_ties: (*query).limitOption == pg_sys::LimitOption::LIMIT_OPTION_WITH_TIES,
            outputs,
        }),
        schema: typed_schema(bound.schema_inputs, &columns),
        inputs: vec![bound.input],
    });
    Ok(remap_relation(&source, stage_id, columns))
}

unsafe fn validate_query_capabilities(query: *mut pg_sys::Query) -> Result<(), LoweringError> {
    if (*query).commandType != pg_sys::CmdType::CMD_SELECT {
        return Err(LoweringError::unsupported(
            "query.command",
            query.cast(),
            "only SELECT queries lower to a maintained dataflow",
        ));
    }
    if !(*query).setOperations.is_null() {
        return Err(LoweringError::unsupported(
            "query.set_operation",
            (*query).setOperations,
            "UNION, INTERSECT, and EXCEPT need set-operation nodes",
        ));
    }
    if !(*query).cteList.is_null() || (*query).hasRecursive {
        return Err(LoweringError::unsupported(
            "query.cte",
            query.cast(),
            "CTEs need explicit reusable-subgraph lowering",
        ));
    }
    if (*query).hasTargetSRFs {
        return Err(LoweringError::unsupported(
            "project.set_returning_function",
            query.cast(),
            "set-returning target expressions need a flat-map operator",
        ));
    }
    if (*query).hasForUpdate || !(*query).rowMarks.is_null() {
        return Err(LoweringError::unsupported(
            "query.row_lock",
            query.cast(),
            "row-lock clauses do not belong in a maintained view",
        ));
    }
    Ok(())
}

unsafe fn lower_query_body(
    builder: &mut Builder,
    query: *mut pg_sys::Query,
) -> Result<Relation, LoweringError> {
    validate_query_capabilities(query)?;
    let relation = lower_from(builder, query)?;
    let relation = lower_where(builder, query, relation)?;
    let relation = lower_aggregate(builder, query, relation)?;
    let relation = if (*query).havingQual.is_null() {
        relation
    } else {
        lower_filter(builder, relation, (*query).havingQual, "having.predicate")?
    };
    let relation = lower_windows(builder, query, relation)?;
    let relation = lower_project(builder, query, relation, true)?;
    let relation = lower_distinct(builder, query, relation)?;
    let relation = lower_topn(builder, query, relation)?;
    lower_visible_project(builder, query, relation)
}

/// Lower an analyzed PostgreSQL query into one typed relational DAG.
///
/// # Safety
/// `query` must be non-NULL and remain live in PostgreSQL's current memory
/// context for this call.
pub(crate) unsafe fn lower(query: *mut pg_sys::Query) -> Result<LoweredQuery, LoweringError> {
    if query.is_null() {
        return Err(LoweringError::invalid(
            "query",
            "cannot lower a NULL PostgreSQL Query",
        ));
    }
    let mut builder = Builder::default();
    let output = lower_query_body(&mut builder, query)?;
    Ok(LoweredQuery {
        plan: DataflowPlan {
            execution_settings: current_settings(),
            stages: builder.stages,
        },
        sources: builder.sources,
        output,
        next_binding: builder.next_binding,
    })
}

#[cfg(any(test, feature = "pg_test"))]
pub(crate) unsafe fn lower_select_for_test(sql: &str) -> Result<DataflowPlan, LoweringError> {
    let sql = std::ffi::CString::new(sql)
        .map_err(|_| LoweringError::invalid("query.test", "SQL contains a NUL byte"))?;
    let raw_statements = pg_sys::pg_parse_query(sql.as_ptr());
    if pg_sys::list_length(raw_statements) != 1 {
        return Err(LoweringError::invalid(
            "query.test",
            "test SQL must contain exactly one statement",
        ));
    }
    let raw: *mut pg_sys::RawStmt = pg_sys::list_nth(raw_statements, 0).cast();
    let analyzed = pg_sys::pg_analyze_and_rewrite_fixedparams(
        raw,
        sql.as_ptr(),
        std::ptr::null(),
        0,
        std::ptr::null_mut(),
    );
    if pg_sys::list_length(analyzed) != 1 {
        return Err(LoweringError::invalid(
            "query.test",
            "test SQL must analyze to exactly one Query",
        ));
    }
    let query: *mut pg_sys::Query = pg_sys::list_nth(analyzed, 0).cast();
    if query.is_null() || (*query).type_ != pg_sys::NodeTag::T_Query {
        return Err(LoweringError::invalid(
            "query.test",
            "PostgreSQL did not produce an analyzed Query",
        ));
    }
    Ok(lower(query)?.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_errors_name_the_exact_layer_and_node() {
        let error = LoweringError::unsupported(
            "project.expression",
            std::ptr::null_mut(),
            "unsupported test expression",
        );
        assert_eq!(
            error.to_string(),
            "project.expression: unsupported test expression"
        );
    }

    #[test]
    fn relation_remap_preserves_slot_provenance() {
        let source = Relation {
            stage_id: 0,
            columns: vec![Column {
                slot: SlotId(1),
                type_: SlotType {
                    type_oid: 23,
                    typmod: -1,
                    collation_oid: 0,
                    nullable: false,
                },
                name: Some("id".into()),
            }],
            vars: HashMap::from([(VarKey { rte: 3, attnum: 1 }, 0)]),
            groups: Vec::new(),
            aggregates: HashMap::new(),
            windows: HashMap::new(),
            target_refs: HashMap::new(),
        };
        let remapped = remap_relation(
            &source,
            1,
            vec![Column {
                slot: SlotId(2),
                ..source.columns[0].clone()
            }],
        );
        assert_eq!(remapped.vars[&VarKey { rte: 3, attnum: 1 }], 0);
        assert_eq!(remapped.columns[0].slot, SlotId(2));
    }
}
