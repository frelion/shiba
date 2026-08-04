use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectNamePart,
    SelectItem, Spanned,
};

use crate::bounds::Budget;
use crate::expression::lower_expression;
use crate::relation::{LoweringContext, identifier};
use crate::{
    Aggregate, ErrorCode, FrontendError, Join, SelectExpression, Span, UnboundExpression,
    UnboundSelectItem,
};

pub(crate) fn lower_projection(
    items: &[SelectItem],
    context: &LoweringContext<'_>,
    budget: &mut Budget,
) -> Result<Vec<UnboundSelectItem>, FrontendError> {
    let projection = items
        .iter()
        .map(|item| lower_item(item, context, budget))
        .collect::<Result<Vec<_>, _>>()?;
    reject_duplicate_aliases(&projection)?;
    Ok(projection)
}

fn lower_item(
    item: &SelectItem,
    context: &LoweringContext<'_>,
    budget: &mut Budget,
) -> Result<UnboundSelectItem, FrontendError> {
    let span = context.map.span(item.span());
    budget.ast(span)?;
    let (expression, alias) = match item {
        SelectItem::UnnamedExpr(expression) => {
            (lower_select_expression(expression, context, budget)?, None)
        }
        SelectItem::ExprWithAlias { expr, alias } => (
            lower_select_expression(expr, context, budget)?,
            Some(identifier(alias, context.map)?),
        ),
        _ => {
            return Err(FrontendError::unsupported(
                ErrorCode::UnsupportedSyntax,
                span,
            ));
        }
    };
    Ok(UnboundSelectItem {
        expression,
        presentation_alias: alias,
        span,
    })
}

fn lower_select_expression(
    expression: &Expr,
    context: &LoweringContext<'_>,
    budget: &mut Budget,
) -> Result<SelectExpression, FrontendError> {
    if let Expr::Function(function) = expression {
        return lower_aggregate(function, context, budget);
    }
    lower_expression(expression, context, budget, 1).map(SelectExpression::Expression)
}

fn lower_aggregate(
    function: &Function,
    context: &LoweringContext<'_>,
    budget: &mut Budget,
) -> Result<SelectExpression, FrontendError> {
    let span = context.map.span(function.span());
    budget.expression(1, span)?;
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    }
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    let name = identifier(name, context.map)?;
    if name.quoted {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    if arguments.duplicate_treatment.is_some()
        || !arguments.clauses.is_empty()
        || arguments.args.len() != 1
    {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    }
    let FunctionArg::Unnamed(argument) = &arguments.args[0] else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    let argument = match (name.value.as_str(), argument) {
        ("count", FunctionArgExpr::Wildcard) => crate::AggregateArgument::Star,
        ("count" | "sum" | "min" | "max", FunctionArgExpr::Expr(expression)) => {
            let input = lower_expression(expression, context, budget, 2)?;
            if !matches!(input, UnboundExpression::Column(_)) {
                return Err(FrontendError::unsupported(
                    ErrorCode::UnsupportedSyntax,
                    span,
                ));
            }
            crate::AggregateArgument::Expression(input)
        }
        _ => {
            return Err(FrontendError::unsupported(
                ErrorCode::UnsupportedSyntax,
                span,
            ));
        }
    };
    Ok(SelectExpression::Aggregate(Aggregate {
        function: name.value,
        argument,
        span,
    }))
}

pub(crate) fn lower_aggregate_for_having(
    function: &Function,
    context: &LoweringContext<'_>,
    budget: &mut Budget,
) -> Result<Aggregate, FrontendError> {
    match lower_aggregate(function, context, budget)? {
        SelectExpression::Aggregate(aggregate) => Ok(aggregate),
        SelectExpression::Expression(_) => Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            context.map.span(function.span()),
        )),
    }
}

pub(crate) fn lower_group_by(
    group_by: &GroupByExpr,
    context: &LoweringContext<'_>,
    budget: &mut Budget,
) -> Result<Option<UnboundExpression>, FrontendError> {
    match group_by {
        GroupByExpr::Expressions(values, modifiers)
            if values.is_empty() && modifiers.is_empty() =>
        {
            Ok(None)
        }
        GroupByExpr::Expressions(values, modifiers)
            if values.len() == 1 && modifiers.is_empty() =>
        {
            let value = lower_expression(&values[0], context, budget, 1)?;
            if matches!(value, UnboundExpression::Column(_)) {
                Ok(Some(value))
            } else {
                Err(FrontendError::unsupported(
                    ErrorCode::UnsupportedSyntax,
                    value.span(),
                ))
            }
        }
        _ => Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            context.map.span(group_by.span()),
        )),
    }
}

pub(crate) fn validate_shape(
    projection: &[UnboundSelectItem],
    group: Option<&UnboundExpression>,
    join: Option<&Join>,
    having: Option<&crate::UnboundHavingExpression>,
    span: Span,
) -> Result<(), FrontendError> {
    let aggregates = projection
        .iter()
        .filter(|item| matches!(item.expression, SelectExpression::Aggregate(_)))
        .count();
    let valid = if join.is_some() {
        group.is_none()
            && having.is_none()
            && aggregates == 0
            && projection.len() == 2
            && matches!(&projection[0].expression, SelectExpression::Expression(UnboundExpression::Column(column)) if column.source == 0)
            && matches!(&projection[1].expression, SelectExpression::Expression(UnboundExpression::Column(column)) if column.source == 1)
    } else if let Some(group) = group {
        projection.len() >= 2
            && aggregates == projection.len() - 1
            && matches!(&projection[0].expression, SelectExpression::Expression(value) if same_expression(value, group))
    } else if aggregates > 0 {
        projection.len() == aggregates && aggregates > 0
    } else {
        projection.len() == 2
    };
    if valid {
        Ok(())
    } else {
        Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ))
    }
}

fn reject_duplicate_aliases(items: &[UnboundSelectItem]) -> Result<(), FrontendError> {
    for (index, item) in items.iter().enumerate() {
        if let Some(alias) = &item.presentation_alias
            && items[..index]
                .iter()
                .filter_map(|item| item.presentation_alias.as_ref())
                .any(|other| other.value == alias.value)
        {
            return Err(FrontendError::unsupported(
                ErrorCode::DuplicateAlias,
                alias.span,
            ));
        }
    }
    Ok(())
}

fn same_expression(left: &UnboundExpression, right: &UnboundExpression) -> bool {
    // Quoting is durable rebinding semantics. Before Catalog binding proves an
    // exact column identity, `id` and `"id"` deliberately remain distinct.
    matches!((left, right), (UnboundExpression::Column(left), UnboundExpression::Column(right))
        if left.source == right.source && left.name.value == right.name.value && left.name.quoted == right.name.quoted)
}
