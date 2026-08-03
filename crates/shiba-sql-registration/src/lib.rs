//! Short `PostgreSQL` control-plane adapter for bounded SQL declarations.

#![forbid(unsafe_code)]

mod catalog;
mod catalog_descriptor;
mod error;

use postgres::Client;
use shiba_operator::OperatorGraph;
use shiba_protocol::GraphId;
use shiba_sql_frontend::{ErrorCode, ResolvedSource, bind_query, parse_sql};

pub use error::SqlRegistrationError;

/// Parses before opening a database transaction, then binds and atomically
/// installs one canonical query declaration and bound graph.
///
/// # Errors
///
/// Returns a stable SQL diagnostic for parse, binding, Catalog, compilation or
/// registration failure. No raw SQL or parser AST is persisted.
pub fn compile_sql_and_register(
    client: &mut Client,
    graph_id: GraphId,
    sql: &str,
) -> Result<OperatorGraph, SqlRegistrationError> {
    let query = parse_sql(sql)?;
    let mut transaction = client.transaction().map_err(|error| {
        SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, query.span, error)
    })?;
    let sources = catalog::resolve_sources(&mut transaction, graph_id, &query)?
        .into_iter()
        .map(|source| ResolvedSource {
            descriptor: source.descriptor,
            identity: source.identity,
        })
        .collect::<Vec<_>>();
    let spec = bind_query(graph_id, &query, &sources)?;
    let graph = shiba_runtime::compile_and_register_in_transaction(&mut transaction, &spec)
        .map_err(|error| {
            let code = if matches!(
                &error,
                shiba_runtime::RegistrationError::Runtime(shiba_runtime::M2Error::Postgres(pg))
                    if pg.code() == Some(&postgres::error::SqlState::UNIQUE_VIOLATION)
            ) {
                ErrorCode::GraphConflict
            } else {
                ErrorCode::RegistrationFailed
            };
            SqlRegistrationError::runtime(code, query.span, error)
        })?;
    transaction.commit().map_err(|error| {
        SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, query.span, error)
    })?;
    Ok(graph)
}
