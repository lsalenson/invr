use crate::report::ReportError;
use crate::spec::error::SpecError;
use std::{error::Error, fmt};

#[derive(Debug)]
pub enum ApplicationError {
    InvalidSpec(SpecError),
    InvalidReport(ReportError),
    EngineFailure(String),
}

impl ApplicationError {
    pub fn engine_failure(msg: impl Into<String>) -> Self {
        Self::EngineFailure(msg.into())
    }
}

impl fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplicationError::InvalidSpec(e) => write!(f, "invalid spec: {e}"),
            ApplicationError::InvalidReport(e) => write!(f, "invalid report: {e}"),
            ApplicationError::EngineFailure(msg) => write!(f, "engine failure: {msg}"),
        }
    }
}

impl Error for ApplicationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ApplicationError::InvalidSpec(e) => Some(e),
            ApplicationError::InvalidReport(e) => Some(e),
            ApplicationError::EngineFailure(_) => None,
        }
    }
}

pub type ApplicationResult<T> = Result<T, ApplicationError>;
