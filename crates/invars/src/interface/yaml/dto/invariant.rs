use serde::Deserialize;
use std::collections::BTreeMap;

use super::scope::ScopeYaml;
use super::severity::SeverityYaml;

/// YAML representation of an invariant definition.
#[derive(Debug, Deserialize)]
pub struct InvariantYaml<K> {
    pub id: String,
    pub kind: K,
    pub scope: ScopeYaml,

    #[serde(default)]
    pub severity: SeverityYaml,

    #[serde(default)]
    pub params: BTreeMap<String, String>,
}
