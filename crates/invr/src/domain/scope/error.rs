use std::{error::Error, fmt};

use crate::scope::Scope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeError {
    UnknownScope { input: String },

    MissingColumnName { input: String },

    UnexpectedScope { expected: Scope, found: Scope },
}

impl ScopeError {
    pub fn unknown(input: impl Into<String>) -> Self {
        Self::UnknownScope {
            input: input.into(),
        }
    }

    pub fn missing_column(input: impl Into<String>) -> Self {
        Self::MissingColumnName {
            input: input.into(),
        }
    }

    pub fn unexpected(expected: Scope, found: Scope) -> Self {
        Self::UnexpectedScope { expected, found }
    }
}

impl fmt::Display for ScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScopeError::UnknownScope { input } => {
                write!(f, "unknown scope: {input}")
            }
            ScopeError::MissingColumnName { input } => {
                write!(f, "column scope requires a column name: {input}")
            }
            ScopeError::UnexpectedScope { expected, found } => {
                write!(f, "unexpected scope: expected {expected}, found {found}")
            }
        }
    }
}

impl Error for ScopeError {}

// pub type ScopeResult<T> = Result<T, ScopeError>;
