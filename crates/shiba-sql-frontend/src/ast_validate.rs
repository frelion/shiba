use crate::bounds::{
    MAX_AST_NODES, MAX_BOOLEAN_TERMS, MAX_EXPRESSION_DEPTH, MAX_EXPRESSION_NODES, MAX_PROJECTION,
    MAX_SOURCES,
};
use crate::{
    AggregateArgument, BinaryOperator, ColumnRef, ErrorCode, FrontendError, Identifier,
    SelectExpression, Span, UnaryOperator, UnboundExpression, UnboundQuery,
};

pub(crate) fn validate(query: &UnboundQuery) -> Result<(), FrontendError> {
    if query.sources.is_empty()
        || query.sources.len() > MAX_SOURCES
        || query.projection.is_empty()
        || query.projection.len() > MAX_PROJECTION
        || (query.sources.len() == 2) != query.join.is_some()
    {
        return Err(canonical(query.span));
    }
    let mut ast_nodes = 1usize;
    let mut identifiers = Vec::new();
    for source in &query.sources {
        ast_nodes = checked_ast(ast_nodes, 1, source.span)?;
        identifiers.extend([&source.schema, &source.relation]);
        if let Some(alias) = &source.alias {
            identifiers.push(alias);
        }
    }
    reject_duplicate_aliases(query)?;

    let mut expressions = Vec::new();
    for item in &query.projection {
        ast_nodes = checked_ast(ast_nodes, 1, item.span)?;
        if let Some(alias) = &item.presentation_alias {
            identifiers.push(alias);
        }
        match &item.expression {
            SelectExpression::Expression(expression) => expressions.push((expression, 1usize)),
            SelectExpression::Aggregate(aggregate) => {
                if aggregate.function.is_empty()
                    || aggregate.function.len() > 63
                    || aggregate
                        .function
                        .bytes()
                        .any(|byte| byte.is_ascii_uppercase())
                {
                    return Err(canonical(aggregate.span));
                }
                if let AggregateArgument::Expression(input) = &aggregate.argument {
                    expressions.push((input, 1));
                }
            }
        }
    }
    if let Some(selection) = &query.selection {
        expressions.push((selection, 1));
    }
    if let Some(group) = &query.group_by {
        expressions.push((group, 1));
    }
    if let Some(having) = &query.having {
        validate_having(
            having,
            query.sources.len(),
            &mut identifiers,
            &mut expressions,
        )?;
    }
    if let Some(join) = &query.join {
        ast_nodes = checked_ast(ast_nodes, 1, join.span)?;
        if join.left.source != 0 || join.right.source != 1 {
            return Err(canonical(join.span));
        }
        validate_column(&join.left, query.sources.len(), &mut identifiers)?;
        validate_column(&join.right, query.sources.len(), &mut identifiers)?;
    }
    for identifier in identifiers {
        validate_identifier(identifier)?;
    }
    validate_expressions(query.sources.len(), expressions, ast_nodes)
}

fn validate_having<'a>(
    having: &'a crate::UnboundHavingExpression,
    source_count: usize,
    identifiers: &mut Vec<&'a Identifier>,
    expressions: &mut Vec<(&'a UnboundExpression, usize)>,
) -> Result<(), FrontendError> {
    let mut nodes = 0;
    let mut boolean_terms = 0;
    validate_having_inner(
        having,
        source_count,
        identifiers,
        expressions,
        0,
        &mut nodes,
        &mut boolean_terms,
    )
}

fn validate_having_inner<'a>(
    having: &'a crate::UnboundHavingExpression,
    source_count: usize,
    identifiers: &mut Vec<&'a Identifier>,
    expressions: &mut Vec<(&'a UnboundExpression, usize)>,
    depth: usize,
    nodes: &mut usize,
    boolean_terms: &mut usize,
) -> Result<(), FrontendError> {
    use crate::UnboundHavingExpression as H;
    *nodes = nodes.checked_add(1).ok_or_else(|| limit(having.span()))?;
    if depth > crate::bounds::MAX_HAVING_DEPTH || *nodes > crate::bounds::MAX_HAVING_NODES {
        return Err(limit(having.span()));
    }
    let is_boolean = matches!(having, H::Binary { .. } | H::Unary { .. });
    if is_boolean {
        *boolean_terms = boolean_terms
            .checked_add(1)
            .ok_or_else(|| limit(having.span()))?;
        if *boolean_terms > crate::bounds::MAX_HAVING_BOOLEAN_TERMS {
            return Err(limit(having.span()));
        }
    }
    match having {
        H::Aggregate(aggregate) => {
            if aggregate.function.is_empty()
                || aggregate.function.len() > 63
                || aggregate
                    .function
                    .bytes()
                    .any(|byte| byte.is_ascii_uppercase())
            {
                return Err(canonical(aggregate.span));
            }
            if let AggregateArgument::Expression(input) = &aggregate.argument {
                expressions.push((input, 1));
            }
        }
        H::Int8(..) | H::Null(_) => {}
        H::Binary { left, right, .. } => {
            validate_having_inner(
                left,
                source_count,
                identifiers,
                expressions,
                depth + 1,
                nodes,
                boolean_terms,
            )?;
            validate_having_inner(
                right,
                source_count,
                identifiers,
                expressions,
                depth + 1,
                nodes,
                boolean_terms,
            )?;
        }
        H::Unary { input, .. } => validate_having_inner(
            input,
            source_count,
            identifiers,
            expressions,
            depth + 1,
            nodes,
            boolean_terms,
        )?,
    }
    if let H::Aggregate(aggregate) = having
        && let AggregateArgument::Expression(UnboundExpression::Column(column)) =
            &aggregate.argument
    {
        validate_column(column, source_count, identifiers)?;
    }
    Ok(())
}

fn validate_expressions(
    source_count: usize,
    mut stack: Vec<(&UnboundExpression, usize)>,
    mut ast_nodes: usize,
) -> Result<(), FrontendError> {
    let mut expression_nodes = 0usize;
    let mut boolean_terms = 0usize;
    while let Some((expression, depth)) = stack.pop() {
        let span = expression.span();
        ast_nodes = checked_ast(ast_nodes, 1, span)?;
        expression_nodes = expression_nodes.checked_add(1).ok_or_else(|| limit(span))?;
        if expression_nodes > MAX_EXPRESSION_NODES || depth > MAX_EXPRESSION_DEPTH {
            return Err(limit(span));
        }
        match expression {
            UnboundExpression::Column(column) => {
                let mut identifiers = Vec::new();
                validate_column(column, source_count, &mut identifiers)?;
                validate_identifier(&column.name)?;
            }
            UnboundExpression::Int8(..) | UnboundExpression::Null(_) => {}
            UnboundExpression::Binary {
                operator,
                left,
                right,
                ..
            } => {
                if !matches!(operator, BinaryOperator::Add | BinaryOperator::Subtract) {
                    boolean_terms = checked_boolean(boolean_terms, span)?;
                }
                stack.push((right, depth + 1));
                stack.push((left, depth + 1));
            }
            UnboundExpression::Unary {
                operator, input, ..
            } => {
                if matches!(
                    operator,
                    UnaryOperator::IsNull | UnaryOperator::IsNotNull | UnaryOperator::Not
                ) {
                    boolean_terms = checked_boolean(boolean_terms, span)?;
                }
                stack.push((input, depth + 1));
            }
        }
    }
    Ok(())
}

fn validate_column<'a>(
    column: &'a ColumnRef,
    source_count: usize,
    identifiers: &mut Vec<&'a Identifier>,
) -> Result<(), FrontendError> {
    if usize::from(column.source) >= source_count {
        return Err(canonical(column.span));
    }
    identifiers.push(&column.name);
    Ok(())
}

fn validate_identifier(identifier: &Identifier) -> Result<(), FrontendError> {
    let valid_unquoted = identifier.value.bytes().enumerate().all(|(index, byte)| {
        if index == 0 {
            byte.is_ascii_lowercase() || byte == b'_'
        } else {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'$')
        }
    });
    if identifier.value.is_empty()
        || identifier.value.len() > 63
        || identifier.value.contains('\0')
        || (!identifier.quoted && !valid_unquoted)
    {
        return Err(FrontendError::unsupported(
            ErrorCode::InvalidIdentifier,
            identifier.span,
        ));
    }
    Ok(())
}

fn reject_duplicate_aliases(query: &UnboundQuery) -> Result<(), FrontendError> {
    if query.sources.len() == 2 {
        let left = query.sources[0]
            .alias
            .as_ref()
            .ok_or_else(|| canonical(query.sources[0].span))?;
        let right = query.sources[1]
            .alias
            .as_ref()
            .ok_or_else(|| canonical(query.sources[1].span))?;
        if left.value == right.value {
            return Err(canonical(right.span));
        }
    }
    for (index, item) in query.projection.iter().enumerate() {
        if let Some(alias) = &item.presentation_alias
            && query.projection[..index]
                .iter()
                .filter_map(|item| item.presentation_alias.as_ref())
                .any(|other| other.value == alias.value)
        {
            return Err(canonical(alias.span));
        }
    }
    Ok(())
}

fn checked_ast(current: usize, added: usize, span: Span) -> Result<usize, FrontendError> {
    let value = current.checked_add(added).ok_or_else(|| limit(span))?;
    if value > MAX_AST_NODES {
        Err(limit(span))
    } else {
        Ok(value)
    }
}

fn checked_boolean(current: usize, span: Span) -> Result<usize, FrontendError> {
    let value = current.checked_add(1).ok_or_else(|| limit(span))?;
    if value > MAX_BOOLEAN_TERMS {
        Err(limit(span))
    } else {
        Ok(value)
    }
}

fn limit(span: Span) -> FrontendError {
    FrontendError::limit(ErrorCode::QueryTooComplex, span)
}

fn canonical(span: Span) -> FrontendError {
    FrontendError::unsupported(ErrorCode::CanonicalizationFailed, span)
}
