use sqlparser::ast::{
    BinaryOperator as SqlBinary, Expr, Query, Select, SelectFlavor, SelectItem, Spanned, Statement,
    UnaryOperator as SqlUnary, Value,
};

use crate::bounds::{Budget, MAX_PLAIN_PROJECTION, MAX_PROJECTION, MAX_SOURCES};
use crate::expression::lower_expression;
use crate::parser::SourceMap;
use crate::relation::{LoweringContext, lower_join, sources};
use crate::select_lower::{lower_group_by, lower_projection, validate_shape};
use crate::{
    BinaryOperator, ErrorCode, FrontendError, Span, UnaryOperator, UnboundHavingExpression,
    UnboundQuery,
};

pub(crate) fn lower(
    statement: Statement,
    map: &SourceMap<'_>,
) -> Result<UnboundQuery, FrontendError> {
    let Statement::Query(query) = statement else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            Span { start: 0, end: 0 },
        ));
    };
    lower_query(&query, map)
}

fn lower_query(query: &Query, map: &SourceMap<'_>) -> Result<UnboundQuery, FrontendError> {
    let span = map.span(query.span());
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    }
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    lower_select(select, map, span)
}

fn lower_select(
    select: &Select,
    map: &SourceMap<'_>,
    query_span: Span,
) -> Result<UnboundQuery, FrontendError> {
    reject_extensions(select, map)?;
    if select.from.len() != 1 || select.projection.is_empty() {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            map.span(select.span()),
        ));
    }
    let has_aggregate = select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(Expr::Function(_))
                | SelectItem::ExprWithAlias {
                    expr: Expr::Function(_),
                    ..
                }
        )
    });
    if select.projection.len() > MAX_PROJECTION
        || (select.projection.len() > MAX_PLAIN_PROJECTION && !has_aggregate)
    {
        return Err(FrontendError::limit(
            ErrorCode::QueryTooComplex,
            map.span(select.span()),
        ));
    }
    let mut budget = Budget::default();
    budget.ast(query_span)?;
    let (sources, join_ast) = sources(&select.from[0], map, &mut budget)?;
    if sources.len() > MAX_SOURCES {
        return Err(FrontendError::limit(ErrorCode::QueryTooComplex, query_span));
    }
    let context = LoweringContext::new(map, &sources);
    let join = join_ast
        .map(|value| lower_join(value, &context, &mut budget))
        .transpose()?;
    let projection = lower_projection(&select.projection, &context, &mut budget)?;
    let selection = select
        .selection
        .as_ref()
        .map(|value| lower_expression(value, &context, &mut budget, 1))
        .transpose()?;
    let group_by = lower_group_by(&select.group_by, &context, &mut budget)?;
    let having = select
        .having
        .as_ref()
        .map(|value| lower_having(value, &context, &mut budget, 0))
        .transpose()?;
    validate_shape(
        &projection,
        group_by.as_ref(),
        join.as_ref(),
        having.as_ref(),
        query_span,
    )?;
    Ok(UnboundQuery {
        sources,
        join,
        projection,
        selection,
        group_by,
        having,
        span: query_span,
    })
}

fn reject_extensions(select: &Select, map: &SourceMap<'_>) -> Result<(), FrontendError> {
    let unsupported = !select.optimizer_hints.is_empty()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.top_before_distinct
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.window_before_qualify
        || select.value_table_mode.is_some()
        || select.flavor != SelectFlavor::Standard;
    if unsupported {
        Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            map.span(select.span()),
        ))
    } else {
        Ok(())
    }
}

fn lower_having(
    expression: &Expr,
    context: &LoweringContext<'_>,
    budget: &mut Budget,
    depth: usize,
) -> Result<UnboundHavingExpression, FrontendError> {
    let span = context.map.span(expression.span());
    if let Expr::Nested(input) = expression {
        return lower_having(input, context, budget, depth + 1);
    }
    let boolean = matches!(
        expression,
        Expr::BinaryOp { .. } | Expr::UnaryOp { .. } | Expr::IsNull(_) | Expr::IsNotNull(_)
    );
    budget.having(depth, boolean, span)?;
    budget.expression(1, span)?;
    match expression {
        Expr::Function(function) => Ok(UnboundHavingExpression::Aggregate(
            crate::select_lower::lower_aggregate_for_having(function, context, budget)?,
        )),
        Expr::Value(value) => match &value.value {
            Value::Number(value, false) => value
                .parse::<i64>()
                .map(|value| UnboundHavingExpression::Int8(value, span))
                .map_err(|_| FrontendError::unsupported(ErrorCode::UnsupportedSyntax, span)),
            Value::Null => Ok(UnboundHavingExpression::Null(span)),
            _ => Err(FrontendError::unsupported(
                ErrorCode::UnsupportedSyntax,
                span,
            )),
        },
        Expr::UnaryOp {
            op: SqlUnary::Not,
            expr,
        } => Ok(UnboundHavingExpression::Unary {
            operator: UnaryOperator::Not,
            input: Box::new(lower_having(expr, context, budget, depth + 1)?),
            span,
        }),
        Expr::IsNull(input) | Expr::IsNotNull(input) => Ok(UnboundHavingExpression::Unary {
            operator: if matches!(expression, Expr::IsNull(_)) {
                UnaryOperator::IsNull
            } else {
                UnaryOperator::IsNotNull
            },
            input: Box::new(lower_having(input, context, budget, depth + 1)?),
            span,
        }),
        Expr::BinaryOp { left, op, right } => {
            let operator = match op {
                SqlBinary::Eq => BinaryOperator::Equal,
                SqlBinary::NotEq => BinaryOperator::NotEqual,
                SqlBinary::Lt => BinaryOperator::Less,
                SqlBinary::LtEq => BinaryOperator::LessEqual,
                SqlBinary::Gt => BinaryOperator::Greater,
                SqlBinary::GtEq => BinaryOperator::GreaterEqual,
                SqlBinary::And => BinaryOperator::And,
                SqlBinary::Or => BinaryOperator::Or,
                _ => {
                    return Err(FrontendError::unsupported(
                        ErrorCode::UnsupportedSyntax,
                        span,
                    ));
                }
            };
            Ok(UnboundHavingExpression::Binary {
                operator,
                left: Box::new(lower_having(left, context, budget, depth + 1)?),
                right: Box::new(lower_having(right, context, budget, depth + 1)?),
                span,
            })
        }
        _ => Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        )),
    }
}
