use std::{error::Error, fmt};

use crate::invariant::error::InvariantError;
use crate::invariant::value_object::id::InvariantId;

#[derive(Debug, Clone, PartialEq)]
pub enum SpecError {
    DuplicateInvariantId { id: InvariantId },
    InvalidInvariant { index: usize, error: InvariantError },
}

impl SpecError {
    pub fn duplicate_invariant_id(id: InvariantId) -> Self {
        Self::DuplicateInvariantId { id }
    }

    pub fn invalid_invariant(index: usize, error: InvariantError) -> Self {
        Self::InvalidInvariant { index, error }
    }
}

impl fmt::Display for SpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpecError::DuplicateInvariantId { id } => {
                write!(f, "duplicate invariant id: '{}'", id.as_str())
            }
            SpecError::InvalidInvariant { index, error } => {
                write!(f, "invalid invariant at index {index}: {error}")
            }
        }
    }
}

impl Error for SpecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SpecError::InvalidInvariant { error, .. } => Some(error),
            SpecError::DuplicateInvariantId { .. } => None,
        }
    }
}

pub type SpecResult<T> = Result<T, SpecError>;
