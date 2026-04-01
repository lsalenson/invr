use crate::violation::error::ViolationError;
use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq)]
pub enum ReportError {
    InvalidViolation { index: usize, error: ViolationError },
}

impl ReportError {
    pub fn invalid_violation(index: usize, error: ViolationError) -> Self {
        Self::InvalidViolation { index, error }
    }
}

impl fmt::Display for ReportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReportError::InvalidViolation { index, error } => {
                write!(f, "invalid violation at index {index}: {error}")
            }
        }
    }
}

impl Error for ReportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ReportError::InvalidViolation { error, .. } => Some(error),
        }
    }
}

pub type ReportResult<T> = Result<T, ReportError>;
