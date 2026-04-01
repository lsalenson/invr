use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvariantIdError {
    Empty,
    InvalidFormat { value: String },
}

impl fmt::Display for InvariantIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvariantIdError::Empty => write!(f, "invariant id cannot be empty"),
            InvariantIdError::InvalidFormat { value } => {
                write!(f, "invalid invariant id format: {value}")
            }
        }
    }
}

impl Error for InvariantIdError {}
