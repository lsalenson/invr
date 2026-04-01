use serde::Deserialize;

/// YAML representation of severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SeverityYaml {
    Info,
    Warn,
    #[default]
    Error,
}
