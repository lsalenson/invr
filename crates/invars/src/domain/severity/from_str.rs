use std::str::FromStr;

use crate::severity::Severity;
use crate::severity::error::SeverityError;

impl FromStr for Severity {
    type Err = SeverityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "info" | "i" => Ok(Severity::Info),
            "warn" | "warning" | "w" => Ok(Severity::Warn),
            "error.rs" | "err" | "e" => Ok(Severity::Error),
            _ => Err(SeverityError::unknown(s)),
        }
    }
}
