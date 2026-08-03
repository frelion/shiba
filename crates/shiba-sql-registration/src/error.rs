use core::fmt;

use shiba_sql_frontend::{ErrorClass, ErrorCode, FrontendError, Span};

#[derive(Debug)]
pub enum SqlRegistrationError {
    Frontend(FrontendError),
    Catalog {
        code: ErrorCode,
        span: Span,
    },
    Postgres {
        code: ErrorCode,
        span: Span,
        source: postgres::Error,
    },
    Runtime {
        code: ErrorCode,
        span: Span,
        source: shiba_runtime::RegistrationError,
    },
}

impl SqlRegistrationError {
    pub(crate) const fn catalog(code: ErrorCode, span: Span) -> Self {
        Self::Catalog { code, span }
    }

    pub(crate) fn postgres(code: ErrorCode, span: Span, source: postgres::Error) -> Self {
        Self::Postgres { code, span, source }
    }

    pub(crate) fn runtime(
        code: ErrorCode,
        span: Span,
        source: shiba_runtime::RegistrationError,
    ) -> Self {
        Self::Runtime { code, span, source }
    }

    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::Frontend(error) => error.code,
            Self::Catalog { code, .. }
            | Self::Postgres { code, .. }
            | Self::Runtime { code, .. } => *code,
        }
    }

    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        match self {
            Self::Frontend(error) => error.class,
            Self::Catalog { .. } | Self::Postgres { .. } | Self::Runtime { .. } => {
                ErrorClass::Binding
            }
        }
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Frontend(error) => error.span,
            Self::Catalog { span, .. }
            | Self::Postgres { span, .. }
            | Self::Runtime { span, .. } => *span,
        }
    }
}

impl From<FrontendError> for SqlRegistrationError {
    fn from(error: FrontendError) -> Self {
        Self::Frontend(error)
    }
}

impl fmt::Display for SqlRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "SQL registration rejected: {} at {}..{}",
            self.code(),
            self.span().start,
            self.span().end
        )
    }
}

impl std::error::Error for SqlRegistrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Frontend(error) => Some(error),
            Self::Postgres { source, .. } => Some(source),
            Self::Runtime { source, .. } => Some(source),
            Self::Catalog { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_diagnostics_keep_stable_code_and_span() {
        let span = Span { start: 7, end: 21 };
        let error = SqlRegistrationError::catalog(ErrorCode::DdlDrift, span);
        assert_eq!(error.code(), ErrorCode::DdlDrift);
        assert_eq!(error.class(), ErrorClass::Binding);
        assert_eq!(error.span(), span);
        assert_eq!(
            error.to_string(),
            "SQL registration rejected: ddl_drift at 7..21"
        );
    }

    #[test]
    fn parser_diagnostics_are_preserved_without_remapping() {
        let frontend = shiba_sql_frontend::parse_sql("DELETE FROM app.events")
            .expect_err("mutation is outside the SQL declaration grammar");
        let expected = (frontend.class, frontend.code, frontend.span);
        let registration = SqlRegistrationError::from(frontend);
        assert_eq!(
            (
                registration.class(),
                registration.code(),
                registration.span()
            ),
            expected
        );
    }
}
