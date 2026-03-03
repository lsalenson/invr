use super::invariant::InvariantYaml;
use serde::Deserialize;

/// Root YAML specification.
#[derive(Debug, Deserialize)]
pub struct SpecYaml<K> {
    pub invariants: Vec<InvariantYaml<K>>,
}
