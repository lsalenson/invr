use std::{error::Error, fmt};

use crate::severity::Severity;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeverityError {
    UnknownSeverity { input: String },

    UnexpectedSeverity { expected: Severity, found: Severity },
}

impl SeverityError {
    pub fn unknown(input: impl Into<String>) -> Self {
        Self::UnknownSeverity {
            input: input.into(),
        }
    }

    pub fn unexpected(expected: Severity, found: Severity) -> Self {
        Self::UnexpectedSeverity { expected, found }
    }
}

impl fmt::Display for SeverityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SeverityError::UnknownSeverity { input } => {
                write!(f, "unknown severity: {input}")
            }
            SeverityError::UnexpectedSeverity { expected, found } => {
                write!(f, "unexpected severity: expected {expected}, found {found}")
            }
        }
    }
}

impl Error for SeverityError {}

pub type SeverityResult<T> = Result<T, SeverityError>;
