use crate::spec::error::SpecError;

#[derive(Debug)]
pub enum YamlError {
    Io(std::io::Error),
    Parse(serde_yaml::Error),
    Spec(SpecError),
}

impl std::fmt::Display for YamlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            YamlError::Io(e) => write!(f, "IO error: {e}"),
            YamlError::Parse(e) => write!(f, "YAML parse error: {e}"),
            YamlError::Spec(e) => write!(f, "Spec error: {e}"),
        }
    }
}

impl std::error::Error for YamlError {}
