use core::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub(crate) fn whole(input: &str) -> Self {
        Self {
            start: 0,
            end: input.len(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClass {
    Parser,
    Unsupported,
    Limit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    InputTooLarge,
    TokenLimit,
    ParseError,
    MultipleStatements,
    UnsupportedSyntax,
    InvalidIdentifier,
    DuplicateAlias,
    AmbiguousColumn,
    UnknownRelation,
    UnknownColumn,
    SourceNotRegistered,
    TypeMismatch,
    IdentityMismatch,
    QueryTooComplex,
    DdlDrift,
    GraphConflict,
    CanonicalizationFailed,
    RegistrationFailed,
}

impl ErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InputTooLarge => "input_too_large",
            Self::TokenLimit => "token_limit",
            Self::ParseError => "parse_error",
            Self::MultipleStatements => "multiple_statements",
            Self::UnsupportedSyntax => "unsupported_syntax",
            Self::InvalidIdentifier => "invalid_identifier",
            Self::DuplicateAlias => "duplicate_alias",
            Self::AmbiguousColumn => "ambiguous_column",
            Self::UnknownRelation => "unknown_relation",
            Self::UnknownColumn => "unknown_column",
            Self::SourceNotRegistered => "source_not_registered",
            Self::TypeMismatch => "type_mismatch",
            Self::IdentityMismatch => "identity_mismatch",
            Self::QueryTooComplex => "query_too_complex",
            Self::DdlDrift => "ddl_drift",
            Self::GraphConflict => "graph_conflict",
            Self::CanonicalizationFailed => "canonicalization_failed",
            Self::RegistrationFailed => "registration_failed",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontendError {
    pub class: ErrorClass,
    pub code: ErrorCode,
    pub span: Span,
}

impl FrontendError {
    pub(crate) const fn parser(code: ErrorCode, span: Span) -> Self {
        Self {
            class: ErrorClass::Parser,
            code,
            span,
        }
    }

    pub(crate) const fn unsupported(code: ErrorCode, span: Span) -> Self {
        Self {
            class: ErrorClass::Unsupported,
            code,
            span,
        }
    }

    pub(crate) const fn limit(code: ErrorCode, span: Span) -> Self {
        Self {
            class: ErrorClass::Limit,
            code,
            span,
        }
    }
}

impl fmt::Display for FrontendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "SQL frontend rejected: {} at {}..{}",
            self.code, self.span.start, self.span.end
        )
    }
}

impl std::error::Error for FrontendError {}
