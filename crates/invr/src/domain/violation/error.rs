use std::{error::Error, fmt};

use crate::invariant::value_object::id::InvariantId;

#[derive(Debug, Clone, PartialEq)]
pub enum ViolationError {
    EmptyReason {
        invariant_id: InvariantId,
    },
    MissingMetric {
        invariant_id: InvariantId,
        name: String,
    },
    DuplicateMetric {
        invariant_id: InvariantId,
        name: String,
    },
}

impl ViolationError {
    pub fn empty_reason(invariant_id: InvariantId) -> Self {
        Self::EmptyReason { invariant_id }
    }

    pub fn missing_metric(invariant_id: InvariantId, name: impl Into<String>) -> Self {
        Self::MissingMetric {
            invariant_id,
            name: name.into(),
        }
    }

    pub fn duplicate_metric(invariant_id: InvariantId, name: impl Into<String>) -> Self {
        Self::DuplicateMetric {
            invariant_id,
            name: name.into(),
        }
    }
}

impl fmt::Display for ViolationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViolationError::EmptyReason { invariant_id } => {
                write!(
                    f,
                    "violation for invariant '{}' has an empty reason",
                    invariant_id.as_str()
                )
            }
            ViolationError::MissingMetric { invariant_id, name } => {
                write!(
                    f,
                    "violation for invariant '{}' is missing required metric '{}'",
                    invariant_id.as_str(),
                    name
                )
            }
            ViolationError::DuplicateMetric { invariant_id, name } => {
                write!(
                    f,
                    "violation for invariant '{}' already contains metric '{}'",
                    invariant_id.as_str(),
                    name
                )
            }
        }
    }
}

impl Error for ViolationError {}

pub type ViolationResult<T> = Result<T, ViolationError>;
