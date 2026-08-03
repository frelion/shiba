use sqlparser::ast::{
    Ident, JoinConstraint, JoinOperator, ObjectNamePart, Spanned, TableFactor, TableWithJoins,
};

use crate::bounds::Budget;
use crate::expression::lower_expression;
use crate::parser::SourceMap;
use crate::{
    BinaryOperator, ColumnRef, ErrorCode, FrontendError, Identifier, Join, QualifiedRelation,
    UnboundExpression,
};

pub(crate) struct LoweringContext<'a> {
    pub(crate) map: &'a SourceMap<'a>,
    sources: &'a [QualifiedRelation],
    require_qualified: bool,
}

impl<'a> LoweringContext<'a> {
    pub(crate) fn new(map: &'a SourceMap<'a>, sources: &'a [QualifiedRelation]) -> Self {
        Self {
            map,
            sources,
            require_qualified: sources.len() == 2,
        }
    }

    pub(crate) fn column(
        &self,
        qualifier: Option<&Ident>,
        name: &Ident,
    ) -> Result<ColumnRef, FrontendError> {
        let name = identifier(name, self.map)?;
        let source = match qualifier {
            Some(qualifier) => {
                let qualifier = identifier(qualifier, self.map)?;
                self.sources
                    .iter()
                    .position(|source| source_qualifier(source) == qualifier.value)
                    .ok_or_else(|| {
                        FrontendError::unsupported(ErrorCode::AmbiguousColumn, qualifier.span)
                    })?
            }
            None if !self.require_qualified => 0,
            None => {
                return Err(FrontendError::unsupported(
                    ErrorCode::AmbiguousColumn,
                    name.span,
                ));
            }
        };
        let source = u8::try_from(source).map_err(|_| {
            FrontendError::unsupported(ErrorCode::CanonicalizationFailed, name.span)
        })?;
        Ok(ColumnRef {
            source,
            span: name.span,
            name,
        })
    }
}

pub(crate) fn sources<'a>(
    from: &'a TableWithJoins,
    map: &SourceMap<'_>,
    budget: &mut Budget,
) -> Result<(Vec<QualifiedRelation>, Option<&'a sqlparser::ast::Join>), FrontendError> {
    if from.joins.len() > 1 {
        return Err(FrontendError::limit(
            ErrorCode::QueryTooComplex,
            map.span(from.span()),
        ));
    }
    let mut values = vec![relation(&from.relation, map, budget)?];
    let join = from.joins.first();
    if let Some(join) = join {
        values.push(relation(&join.relation, map, budget)?);
    }
    if join.is_some() && values.iter().any(|value| value.alias.is_none()) {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            map.span(from.span()),
        ));
    }
    if values.len() == 2 && source_qualifier(&values[0]) == source_qualifier(&values[1]) {
        return Err(FrontendError::unsupported(
            ErrorCode::DuplicateAlias,
            values[1].span,
        ));
    }
    Ok((values, join))
}

fn relation(
    factor: &TableFactor,
    map: &SourceMap<'_>,
    budget: &mut Budget,
) -> Result<QualifiedRelation, FrontendError> {
    let span = map.span(factor.span());
    budget.ast(span)?;
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = factor
    else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    }
    let [
        ObjectNamePart::Identifier(schema),
        ObjectNamePart::Identifier(relation),
    ] = name.0.as_slice()
    else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    let alias = alias
        .as_ref()
        .map(|alias| {
            if !alias.columns.is_empty() || alias.at.is_some() {
                return Err(FrontendError::unsupported(
                    ErrorCode::UnsupportedSyntax,
                    map.span(alias.name.span),
                ));
            }
            identifier(&alias.name, map)
        })
        .transpose()?;
    Ok(QualifiedRelation {
        schema: identifier(schema, map)?,
        relation: identifier(relation, map)?,
        alias,
        span,
    })
}

pub(crate) fn lower_join(
    join: &sqlparser::ast::Join,
    context: &LoweringContext<'_>,
    budget: &mut Budget,
) -> Result<Join, FrontendError> {
    let span = context.map.span(join.span());
    budget.ast(span)?;
    if join.global {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    }
    let (JoinOperator::Join(constraint) | JoinOperator::Inner(constraint)) = &join.join_operator
    else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    let JoinConstraint::On(on) = constraint else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    let expression = lower_expression(on, context, budget, 1)?;
    let UnboundExpression::Binary {
        operator: BinaryOperator::Equal,
        left,
        right,
        ..
    } = expression
    else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    let (UnboundExpression::Column(mut left), UnboundExpression::Column(mut right)) =
        (*left, *right)
    else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    if left.source == 1 && right.source == 0 {
        core::mem::swap(&mut left, &mut right);
    }
    if left.source != 0 || right.source != 1 {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    }
    Ok(Join { left, right, span })
}

pub(crate) fn identifier(value: &Ident, map: &SourceMap<'_>) -> Result<Identifier, FrontendError> {
    let span = map.span(value.span);
    let quoted = match value.quote_style {
        None => false,
        Some('"') => true,
        _ => {
            return Err(FrontendError::unsupported(
                ErrorCode::InvalidIdentifier,
                span,
            ));
        }
    };
    let normalized = if quoted {
        value.value.clone()
    } else {
        value.value.to_ascii_lowercase()
    };
    let valid_unquoted = normalized.bytes().enumerate().all(|(index, byte)| {
        if index == 0 {
            byte.is_ascii_alphabetic() || byte == b'_'
        } else {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
        }
    });
    if normalized.is_empty()
        || normalized.len() > 63
        || normalized.contains('\0')
        || (!quoted && !valid_unquoted)
    {
        return Err(FrontendError::unsupported(
            ErrorCode::InvalidIdentifier,
            span,
        ));
    }
    Ok(Identifier {
        value: normalized,
        quoted,
        span,
    })
}

fn source_qualifier(source: &QualifiedRelation) -> &str {
    let value = source.alias.as_ref().unwrap_or(&source.relation);
    &value.value
}
