use crate::spec::error::SpecError;
use std::{error::Error, fmt};

/// Error returned when loading a spec from YAML fails.
#[derive(Debug)]
pub enum YamlLoadError {
    /// The YAML string could not be parsed.
    Parse(serde_yml::Error),
    /// The parsed YAML could not be converted to a valid [`Spec`](crate::spec::Spec).
    Spec(SpecError),
}

impl fmt::Display for YamlLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            YamlLoadError::Parse(e) => write!(f, "yaml parse error: {e}"),
            YamlLoadError::Spec(e) => write!(f, "invalid spec: {e}"),
        }
    }
}

impl Error for YamlLoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            YamlLoadError::Parse(e) => Some(e),
            YamlLoadError::Spec(e) => Some(e),
        }
    }
}
