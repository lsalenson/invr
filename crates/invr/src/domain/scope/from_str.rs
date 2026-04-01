use crate::scope::Scope;
use crate::scope::error::ScopeError;
use std::str::FromStr;

impl FromStr for Scope {
    type Err = ScopeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = s.trim().to_lowercase();

        if normalized == "dataset" {
            return Ok(Scope::Dataset);
        }

        if let Some(rest) = normalized.strip_prefix("column:") {
            if rest.is_empty() {
                return Err(ScopeError::missing_column(s));
            }
            return Ok(Scope::Column {
                name: rest.to_string(),
            });
        }

        Err(ScopeError::unknown(s))
    }
}
