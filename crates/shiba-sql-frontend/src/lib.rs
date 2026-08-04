//! Bounded, database-independent SQL declaration frontend.

#![forbid(unsafe_code)]

mod ast;
mod ast_validate;
mod bind;
mod bind_aggregate;
mod bind_aggregate_nodes;
mod bind_aggregate_support;
mod bind_expression;
mod bind_join;
mod bounds;
mod error;
mod expression;
mod lowering;
mod parser;
mod relation;
mod select_lower;

pub use ast::{
    Aggregate, AggregateArgument, BinaryOperator, ColumnRef, Identifier, Join, QualifiedRelation,
    SelectExpression, UnaryOperator, UnboundExpression, UnboundQuery, UnboundSelectItem,
};
pub use bind::{ResolvedSource, bind_query};
pub use error::{ErrorClass, ErrorCode, FrontendError, Span};
pub use parser::parse_sql;
