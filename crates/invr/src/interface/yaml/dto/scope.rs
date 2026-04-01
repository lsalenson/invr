use serde::Deserialize;

/// YAML representation of invariant scope.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScopeYaml {
    Column { name: String },
    Dataset,
}
