//! Validation for the one persisted and executable dataflow plan.

use std::collections::{HashMap, HashSet};

use super::model::{
    BindingId, BoolExprKind, DataflowPlan, DataflowStage, JoinKind, OperatorKind, OperatorSpec,
    ScalarExpr, SlotId, SlotType,
};

impl DataflowPlan {
    pub(crate) fn validate(&self) -> Result<(), String> {
        validate_execution_settings(&self.execution_settings)?;
        if self.stages.is_empty() {
            return Err("dataflow plan has no stages".into());
        }

        let mut consumers = vec![HashSet::new(); self.stages.len()];
        let mut scan_count = 0;
        let mut sink_count = 0;
        for (stage_id, stage) in self.stages.iter().enumerate() {
            let label = format!("stage {stage_id}");
            let expected_inputs = usize::from(stage.spec.kind().input_count());
            if stage.inputs.len() != expected_inputs {
                return Err(format!(
                    "{label} ({:?}) requires {expected_inputs} inputs, found {}",
                    stage.spec.kind(),
                    stage.inputs.len()
                ));
            }
            for input in &stage.inputs {
                let upstream = usize::try_from(input.upstream_stage_id)
                    .map_err(|_| format!("{label} has an invalid upstream stage ID"))?;
                if upstream >= stage_id {
                    return Err(format!("{label} has a non-upstream input"));
                }
                consumers[upstream].insert(stage_id);
            }
            validate_stage(stage, &label)?;
            match &stage.spec {
                OperatorSpec::Scan(spec) => {
                    if spec.source_oid == 0 {
                        return Err(format!("{label} has an invalid source OID"));
                    }
                    scan_count += 1;
                }
                OperatorSpec::Sink => {
                    sink_count += 1;
                }
                _ => {}
            }
        }
        if scan_count == 0 {
            return Err("dataflow plan has no Scan stage".into());
        }
        if sink_count != 1 {
            return Err(format!(
                "dataflow plan must contain exactly one Sink, found {sink_count}"
            ));
        }
        for (stage_id, stage) in self.stages.iter().enumerate() {
            if stage.spec.kind() == OperatorKind::Sink {
                if !consumers[stage_id].is_empty() {
                    return Err(format!("Sink stage {stage_id} has a downstream consumer"));
                }
            } else if consumers[stage_id].is_empty() {
                return Err(format!("stage {stage_id} has no durable path to the Sink"));
            }
        }
        validate_slot_bindings(&self.stages)
    }
}

pub(crate) fn validate_execution_settings(
    settings: &super::model::ExecutionSettings,
) -> Result<(), String> {
    if settings.timezone.is_empty()
        || settings.datestyle.is_empty()
        || settings.intervalstyle.is_empty()
        || !(-15..=3).contains(&settings.extra_float_digits)
        || !matches!(settings.bytea_output.as_str(), "hex" | "escape")
    {
        return Err("dataflow plan has invalid captured execution settings".into());
    }
    Ok(())
}

fn validate_stage(stage: &DataflowStage, label: &str) -> Result<(), String> {
    let inputs = &stage.schema.inputs;
    let outputs = &stage.schema.outputs;
    let input_count = stage.spec.kind().input_count();
    let mut bindings = HashMap::with_capacity(inputs.len());
    for input in inputs {
        if input.input >= input_count {
            return Err(format!(
                "{label} declares binding {:?} on invalid input port {}",
                input.binding, input.input
            ));
        }
        validate_slot_type(&input.type_, label)?;
        if bindings.insert(input.binding, &input.type_).is_some() {
            return Err(format!(
                "{label} declares duplicate input binding {:?}",
                input.binding
            ));
        }
    }

    let mut slots = HashSet::with_capacity(outputs.len());
    for output in outputs {
        validate_slot_type(&output.type_, label)?;
        if !slots.insert(output.slot) {
            return Err(format!(
                "{label} declares duplicate output slot {:?}",
                output.slot
            ));
        }
    }

    let spec_outputs = stage.spec.output_slots();
    let schema_outputs: Vec<_> = outputs.iter().map(|output| output.slot).collect();
    if spec_outputs != schema_outputs {
        return Err(format!("{label} operator outputs do not match its schema"));
    }
    validate_operator_spec(stage, label, &bindings)
}

fn validate_operator_spec(
    stage: &DataflowStage,
    label: &str,
    bindings: &HashMap<BindingId, &SlotType>,
) -> Result<(), String> {
    match &stage.spec {
        OperatorSpec::Scan(spec) => {
            if spec.source_oid == 0
                || spec.columns.iter().any(|column| column.attnum == 0)
                || has_duplicates(spec.columns.iter().map(|column| column.output))
            {
                return Err(format!("{label} has an invalid Scan specification"));
            }
        }
        OperatorSpec::Distinct(spec) => {
            for key in &spec.keys {
                validate_sort_group_expression(key, bindings, label, "DISTINCT key")?;
            }
        }
        OperatorSpec::Join(spec) => {
            if spec.equi_keys.iter().any(|key| {
                bindings.get(&key.left_binding).is_none()
                    || bindings.get(&key.right_binding).is_none()
                    || bindings[&key.left_binding] != bindings[&key.right_binding]
                    || stage
                        .schema
                        .inputs
                        .iter()
                        .find(|input| input.binding == key.left_binding)
                        .is_none_or(|input| input.input != 0)
                    || stage
                        .schema
                        .inputs
                        .iter()
                        .find(|input| input.binding == key.right_binding)
                        .is_none_or(|input| input.input != 1)
            }) {
                return Err(format!("{label} has invalid Join equality keys"));
            }
            if spec.kind == JoinKind::NullAwareAnti && !spec.equi_keys.is_empty() {
                return Err(format!(
                    "{label} NullAwareAnti Join cannot use equality key arrangements"
                ));
            }
        }
        OperatorSpec::Aggregate(spec) => {
            if has_duplicates(spec.aggregates.iter().map(|aggregate| aggregate.ref_id))
                || spec
                    .aggregates
                    .iter()
                    .any(|aggregate| aggregate.ref_id == 0 || aggregate.function_oid == 0)
            {
                return Err(format!("{label} has an invalid aggregate reference"));
            }
            for group in &spec.groups {
                validate_sort_group_expression(&group.key, bindings, label, "GROUP BY key")?;
            }
            for aggregate in &spec.aggregates {
                validate_slot_type(&aggregate.type_, label)?;
                for distinct in &aggregate.distinct {
                    validate_sort_group_expression(
                        distinct,
                        bindings,
                        label,
                        "aggregate DISTINCT key",
                    )?;
                }
                for order in &aggregate.order_by {
                    validate_sort_group_expression(
                        order,
                        bindings,
                        label,
                        "aggregate ORDER BY key",
                    )?;
                }
            }
        }
        OperatorSpec::Window(spec) => {
            if has_duplicates(spec.functions.iter().map(|function| function.ref_id))
                || spec
                    .functions
                    .iter()
                    .any(|function| function.ref_id == 0 || function.function_oid == 0)
                || spec.frame.start_in_range_function_oid == Some(0)
                || spec.frame.end_in_range_function_oid == Some(0)
            {
                return Err(format!("{label} has an invalid Window specification"));
            }
            for function in &spec.functions {
                validate_slot_type(&function.type_, label)?;
            }
            for partition in &spec.partition_by {
                validate_sort_group_expression(
                    partition,
                    bindings,
                    label,
                    "window PARTITION BY key",
                )?;
            }
            for order in &spec.order_by {
                validate_sort_group_expression(order, bindings, label, "window ORDER BY key")?;
            }
        }
        OperatorSpec::TopN(spec) if spec.order_by.is_empty() => {
            return Err(format!("{label} has an invalid TopN ordering"));
        }
        _ => {}
    }
    if let OperatorSpec::TopN(spec) = &stage.spec {
        for order in &spec.order_by {
            validate_sort_group_expression(order, bindings, label, "TopN ORDER BY key")?;
        }
    }

    for expression in stage.spec.expressions() {
        validate_scalar_expr(expression, bindings, label)?;
    }
    Ok(())
}

fn validate_sort_group_expression(
    sort: &super::model::SortGroupExpr,
    bindings: &HashMap<BindingId, &SlotType>,
    label: &str,
    role: &str,
) -> Result<(), String> {
    if sort.equality_operator_oid == 0 || sort.sort_operator_oid == 0 {
        return Err(format!("{label} has an invalid {role} operator"));
    }
    validate_slot_type(&sort.type_, label)?;
    validate_expression_type(&sort.expr, &sort.type_, bindings, label, role)
}

fn validate_expression_type(
    expression: &ScalarExpr,
    declared: &SlotType,
    bindings: &HashMap<BindingId, &SlotType>,
    label: &str,
    role: &str,
) -> Result<(), String> {
    let resolved = expression_type(expression, bindings)
        .ok_or_else(|| format!("{label} has a {role} expression with no resolved type"))?;
    if resolved != *declared {
        return Err(format!(
            "{label} has a {role} expression whose declared type does not match its expression"
        ));
    }
    Ok(())
}

fn validate_scalar_expr(
    expression: &ScalarExpr,
    bindings: &HashMap<BindingId, &SlotType>,
    node_id: &str,
) -> Result<(), String> {
    let mut error = None;
    expression.visit(&mut |part| {
        if error.is_some() {
            return;
        }
        error = match part {
            ScalarExpr::Input { binding } if !bindings.contains_key(binding) => Some(format!(
                "node {node_id} expression references unknown input binding {binding:?}"
            )),
            ScalarExpr::Constant { type_, .. } if type_.type_oid == 0 => {
                Some(format!("node {node_id} has an invalid constant"))
            }
            ScalarExpr::Call {
                function_oid,
                type_,
                ..
            } if *function_oid == 0 || type_.type_oid == 0 => {
                Some(format!("node {node_id} has an invalid function call"))
            }
            ScalarExpr::Operator {
                operator_oid,
                type_,
                ..
            }
            | ScalarExpr::Distinct {
                operator_oid,
                type_,
                ..
            }
            | ScalarExpr::NullIf {
                operator_oid,
                type_,
                ..
            }
            | ScalarExpr::ScalarArrayOperator {
                operator_oid,
                type_,
                ..
            } if *operator_oid == 0 || type_.type_oid == 0 => {
                Some(format!("node {node_id} has an invalid operator call"))
            }
            ScalarExpr::Bool { op, args }
                if args.is_empty() || (*op == BoolExprKind::Not && args.len() != 1) =>
            {
                Some(format!("node {node_id} has an invalid boolean expression"))
            }
            ScalarExpr::Coalesce { args, type_ } if args.is_empty() || type_.type_oid == 0 => {
                Some(format!("node {node_id} has an invalid coalesce expression"))
            }
            ScalarExpr::Case { arms, type_, .. } if arms.is_empty() || type_.type_oid == 0 => {
                Some(format!("node {node_id} has an invalid case expression"))
            }
            ScalarExpr::CaseTest { type_ } if type_.type_oid == 0 => Some(format!(
                "node {node_id} has an invalid CASE operand placeholder"
            )),
            ScalarExpr::Relabel { type_, .. }
            | ScalarExpr::CoerceViaIo { type_, .. }
            | ScalarExpr::CoerceToDomain { type_, .. }
            | ScalarExpr::Collate { type_, .. }
                if type_.type_oid == 0 =>
            {
                Some(format!("node {node_id} has an invalid coercion"))
            }
            _ => None,
        };
    });
    if error.is_none() {
        error = validate_case_test_scope(expression, &[], bindings, node_id).err();
    }
    error.map_or(Ok(()), Err)
}

fn validate_case_test_scope(
    expression: &ScalarExpr,
    case_operands: &[SlotType],
    bindings: &HashMap<BindingId, &SlotType>,
    node_id: &str,
) -> Result<(), String> {
    match expression {
        ScalarExpr::CaseTest { type_ } => {
            let Some(operand_type) = case_operands.last() else {
                return Err(format!(
                    "node {node_id} has a CASE operand placeholder outside a simple CASE arm"
                ));
            };
            if type_.type_oid != operand_type.type_oid
                || type_.typmod != operand_type.typmod
                || type_.collation_oid != operand_type.collation_oid
            {
                return Err(format!(
                    "node {node_id} has a CASE operand placeholder with the wrong type"
                ));
            }
        }
        ScalarExpr::Input { .. } | ScalarExpr::Constant { .. } => {}
        ScalarExpr::Call { args, .. }
        | ScalarExpr::Operator { args, .. }
        | ScalarExpr::Bool { args, .. }
        | ScalarExpr::Coalesce { args, .. } => {
            for argument in args {
                validate_case_test_scope(argument, case_operands, bindings, node_id)?;
            }
        }
        ScalarExpr::Distinct { left, right, .. }
        | ScalarExpr::NullIf { left, right, .. }
        | ScalarExpr::ScalarArrayOperator { left, right, .. } => {
            validate_case_test_scope(left, case_operands, bindings, node_id)?;
            validate_case_test_scope(right, case_operands, bindings, node_id)?;
        }
        ScalarExpr::BooleanTest { arg, .. }
        | ScalarExpr::NullTest { arg, .. }
        | ScalarExpr::Relabel { arg, .. }
        | ScalarExpr::CoerceViaIo { arg, .. }
        | ScalarExpr::CoerceToDomain { arg, .. }
        | ScalarExpr::Collate { arg, .. } => {
            validate_case_test_scope(arg, case_operands, bindings, node_id)?;
        }
        ScalarExpr::Case {
            operand,
            arms,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                validate_case_test_scope(operand, case_operands, bindings, node_id)?;
            }
            let mut arm_operands = case_operands.to_vec();
            if let Some(operand) = operand {
                arm_operands.push(expression_type(operand, bindings).ok_or_else(|| {
                    format!("node {node_id} has a simple CASE operand with no type")
                })?);
            }
            for arm in arms {
                validate_case_test_scope(&arm.when, &arm_operands, bindings, node_id)?;
                validate_case_test_scope(&arm.then, case_operands, bindings, node_id)?;
            }
            validate_case_test_scope(else_expr, case_operands, bindings, node_id)?;
        }
    }
    Ok(())
}

fn expression_type(
    expression: &ScalarExpr,
    bindings: &HashMap<BindingId, &SlotType>,
) -> Option<SlotType> {
    Some(match expression {
        ScalarExpr::Input { binding } => (*bindings.get(binding)?).clone(),
        ScalarExpr::Constant { type_, .. }
        | ScalarExpr::Call { type_, .. }
        | ScalarExpr::Operator { type_, .. }
        | ScalarExpr::Distinct { type_, .. }
        | ScalarExpr::NullIf { type_, .. }
        | ScalarExpr::ScalarArrayOperator { type_, .. }
        | ScalarExpr::Coalesce { type_, .. }
        | ScalarExpr::Case { type_, .. }
        | ScalarExpr::CaseTest { type_ }
        | ScalarExpr::Relabel { type_, .. }
        | ScalarExpr::CoerceViaIo { type_, .. }
        | ScalarExpr::CoerceToDomain { type_, .. }
        | ScalarExpr::Collate { type_, .. } => type_.clone(),
        ScalarExpr::Bool { .. } | ScalarExpr::BooleanTest { .. } | ScalarExpr::NullTest { .. } => {
            SlotType {
                type_oid: 16,
                typmod: -1,
                collation_oid: 0,
                nullable: true,
            }
        }
    })
}

fn validate_slot_type(type_: &SlotType, node_id: &str) -> Result<(), String> {
    if type_.type_oid == 0 {
        Err(format!(
            "node {node_id} declares a slot with invalid type OID"
        ))
    } else {
        Ok(())
    }
}

fn validate_slot_bindings(stages: &[DataflowStage]) -> Result<(), String> {
    let mut bound = vec![HashSet::<BindingId>::new(); stages.len()];
    for (stage_id, stage) in stages.iter().enumerate() {
        let downstream: HashMap<_, _> = stage
            .schema
            .inputs
            .iter()
            .map(|input| (input.binding, (input.input, &input.type_)))
            .collect();
        for (input_port, input) in stage.inputs.iter().enumerate() {
            let upstream_outputs = &stages[input.upstream_stage_id as usize].schema.outputs;
            let upstream: HashMap<SlotId, &SlotType> = upstream_outputs
                .iter()
                .map(|output| (output.slot, &output.type_))
                .collect();
            for binding in &input.bindings {
                let source = upstream.get(&binding.source_slot).ok_or_else(|| {
                    format!(
                        "input {} -> {stage_id} references an unknown output slot",
                        input.upstream_stage_id
                    )
                })?;
                let (port, target) = downstream.get(&binding.target_binding).ok_or_else(|| {
                    format!(
                        "input {} -> {stage_id} references an unknown input binding",
                        input.upstream_stage_id
                    )
                })?;
                if usize::from(*port) != input_port
                    || *source != *target
                    || !bound[stage_id].insert(binding.target_binding)
                {
                    return Err(format!(
                        "input {} -> {stage_id} has an invalid slot binding",
                        input.upstream_stage_id
                    ));
                }
            }
        }
        if downstream
            .keys()
            .any(|binding| !bound[stage_id].contains(binding))
        {
            return Err(format!("stage {stage_id} has an unbound input"));
        }
    }
    Ok(())
}

fn has_duplicates<T: Eq + std::hash::Hash>(values: impl IntoIterator<Item = T>) -> bool {
    let mut seen = HashSet::new();
    values.into_iter().any(|value| !seen.insert(value))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::model::{
        DataflowInput, DataflowPlan, DataflowStage, DatumRepr, ExecutionSettings, FilterSpec,
        InputSlot, JoinKind, JoinSpec, NamedExpr, OutputSlot, ProjectSpec, ScanColumn, ScanSpec,
        SlotBinding, StageSchema,
    };

    fn int_type() -> SlotType {
        SlotType {
            type_oid: 23,
            typmod: -1,
            collation_oid: 0,
            nullable: false,
        }
    }

    fn bool_constant() -> ScalarExpr {
        ScalarExpr::Constant {
            type_: SlotType {
                type_oid: 16,
                typmod: -1,
                collation_oid: 0,
                nullable: false,
            },
            value: Some(DatumRepr::Text("t".into())),
        }
    }

    fn settings() -> ExecutionSettings {
        ExecutionSettings {
            timezone: "UTC".into(),
            datestyle: "ISO, MDY".into(),
            intervalstyle: "postgres".into(),
            extra_float_digits: 1,
            bytea_output: "hex".into(),
        }
    }

    fn schema(inputs: &[(u32, u16)], outputs: &[u32]) -> StageSchema {
        StageSchema {
            inputs: inputs
                .iter()
                .map(|(binding, input)| InputSlot {
                    binding: BindingId(*binding),
                    input: *input,
                    type_: int_type(),
                })
                .collect(),
            outputs: outputs
                .iter()
                .map(|slot| OutputSlot {
                    slot: SlotId(*slot),
                    type_: int_type(),
                })
                .collect(),
        }
    }

    fn input(upstream_stage_id: u32, source_slot: u32, target_binding: u32) -> DataflowInput {
        DataflowInput {
            upstream_stage_id,
            bindings: vec![SlotBinding {
                source_slot: SlotId(source_slot),
                target_binding: BindingId(target_binding),
            }],
        }
    }

    fn branch_and_fanin_plan() -> DataflowPlan {
        DataflowPlan {
            execution_settings: settings(),
            stages: vec![
                DataflowStage {
                    spec: OperatorSpec::Scan(ScanSpec {
                        source_oid: 41,
                        columns: vec![ScanColumn {
                            output: SlotId(1),
                            attnum: 1,
                        }],
                    }),
                    schema: schema(&[], &[1]),
                    inputs: vec![],
                },
                DataflowStage {
                    spec: OperatorSpec::Filter(FilterSpec {
                        predicate: bool_constant(),
                        outputs: vec![NamedExpr {
                            output: SlotId(2),
                            name: None,
                            expr: ScalarExpr::Input {
                                binding: BindingId(10),
                            },
                        }],
                    }),
                    schema: schema(&[(10, 0)], &[2]),
                    inputs: vec![input(0, 1, 10)],
                },
                DataflowStage {
                    spec: OperatorSpec::Project(ProjectSpec {
                        expressions: vec![NamedExpr {
                            output: SlotId(3),
                            name: None,
                            expr: ScalarExpr::Input {
                                binding: BindingId(20),
                            },
                        }],
                    }),
                    schema: schema(&[(20, 0)], &[3]),
                    inputs: vec![input(0, 1, 20)],
                },
                DataflowStage {
                    spec: OperatorSpec::Join(JoinSpec {
                        kind: JoinKind::Inner,
                        condition: bool_constant(),
                        equi_keys: Vec::new(),
                        outputs: vec![NamedExpr {
                            output: SlotId(4),
                            name: None,
                            expr: ScalarExpr::Input {
                                binding: BindingId(30),
                            },
                        }],
                    }),
                    schema: schema(&[(30, 0), (31, 1)], &[4]),
                    inputs: vec![input(1, 2, 30), input(2, 3, 31)],
                },
                DataflowStage {
                    spec: OperatorSpec::Sink,
                    schema: schema(&[(40, 0)], &[]),
                    inputs: vec![input(3, 4, 40)],
                },
            ],
        }
    }

    #[test]
    fn accepts_topological_branch_and_fanin() {
        let plan = branch_and_fanin_plan();
        plan.validate().unwrap();
        assert_eq!(
            plan.stages
                .iter()
                .filter(|stage| stage.spec.kind() == OperatorKind::Join)
                .count(),
            1
        );
        assert_eq!(
            plan.stages
                .iter()
                .filter(|stage| {
                    stage
                        .inputs
                        .iter()
                        .any(|input| input.upstream_stage_id == 0)
                })
                .count(),
            2
        );
    }

    #[test]
    fn persisted_plan_rejects_unknown_fields_at_every_level() {
        let plan = branch_and_fanin_plan();
        let mut json = serde_json::to_value(&plan).unwrap();
        json["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<DataflowPlan>(json).is_err());

        let mut json = serde_json::to_value(&plan).unwrap();
        json["stages"][0]["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<DataflowPlan>(json).is_err());

        let mut json = serde_json::to_value(&plan).unwrap();
        json["stages"][0]["schema"]["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<DataflowPlan>(json).is_err());
    }

    #[test]
    fn rejects_non_upstream_input() {
        let mut plan = branch_and_fanin_plan();
        plan.stages[1].inputs[0].upstream_stage_id = 1;
        assert!(plan.validate().unwrap_err().contains("non-upstream"));
    }

    #[test]
    fn rejects_unknown_output_slot_and_input_binding() {
        let mut unknown_slot = branch_and_fanin_plan();
        unknown_slot.stages[1].inputs[0].bindings[0].source_slot = SlotId(999);
        assert!(unknown_slot
            .validate()
            .unwrap_err()
            .contains("unknown output slot"));

        let mut unknown_binding = branch_and_fanin_plan();
        unknown_binding.stages[1].inputs[0].bindings[0].target_binding = BindingId(999);
        assert!(unknown_binding
            .validate()
            .unwrap_err()
            .contains("unknown input binding"));
    }

    #[test]
    fn rejects_scalar_reference_to_unknown_binding() {
        let mut plan = branch_and_fanin_plan();
        let OperatorSpec::Filter(spec) = &mut plan.stages[1].spec else {
            unreachable!()
        };
        spec.predicate = ScalarExpr::Input {
            binding: BindingId(999),
        };
        assert!(plan
            .validate()
            .unwrap_err()
            .contains("expression references unknown input binding"));
    }
}
