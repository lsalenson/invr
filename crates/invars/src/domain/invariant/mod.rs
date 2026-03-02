mod display;
pub mod error;
pub mod value_object;

use crate::invariant::error::{InvariantError, InvariantResult};
use crate::invariant::value_object::id::InvariantId;
use crate::scope::Scope;
use crate::severity::Severity;
use std::collections::BTreeMap;

/// A declared rule applied to a dataset.
///
/// An `Invariant<K>` describes a validation rule but does not contain
/// execution logic or results.
///
/// The type parameter `K` represents the invariant kind,
/// defined by the infrastructure (e.g. `PolarsKind`, `SqlKind`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invariant<K> {
    id: InvariantId,
    kind: K,
    scope: Scope,
    severity: Severity,
    params: BTreeMap<String, String>,
}

impl<K> Invariant<K> {
    pub fn new(id: InvariantId, kind: K, scope: Scope) -> Self {
        Self {
            id,
            kind,
            scope,
            severity: Severity::Error,
            params: BTreeMap::new(),
        }
    }

    pub fn id(&self) -> &InvariantId {
        &self.id
    }

    pub fn kind(&self) -> &K {
        &self.kind
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn severity(&self) -> Severity {
        self.severity
    }

    pub fn params(&self) -> &BTreeMap<String, String> {
        &self.params
    }

    pub fn with_severity(mut self, severity: Severity) -> Self {
        self.severity = severity;
        self
    }

    pub fn with_params(mut self, params: BTreeMap<String, String>) -> Self {
        self.params = params;
        self
    }

    pub fn with_param_value(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    pub fn has_param(&self, key: &str) -> bool {
        self.params.contains_key(key)
    }

    pub fn param(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(|s| s.as_str())
    }

    pub fn require_param(&self, key: &str) -> InvariantResult<&str> {
        self.param(key)
            .ok_or_else(|| InvariantError::missing_param(self.id.as_str(), key))
    }
}
