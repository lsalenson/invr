/// Represents a failure of an invariant.
///
/// Violations are produced during execution.
/// They contain contextual information, metrics,
/// and optional examples.
pub mod display;
pub mod error;
pub mod value_object;

use crate::invariant::value_object::id::InvariantId;
use crate::scope::Scope;
use crate::severity::Severity;
use crate::violation::error::{ViolationError, ViolationResult};
use crate::violation::value_object::metric_value::MetricValue;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Violation {
    invariant_id: InvariantId,
    severity: Severity,
    scope: Scope,
    reason: String,
    metrics: BTreeMap<String, MetricValue>,
    examples: Vec<String>,
}

impl Violation {
    pub fn new(
        invariant_id: InvariantId,
        severity: Severity,
        scope: Scope,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            invariant_id,
            severity,
            scope,
            reason: reason.into(),
            metrics: BTreeMap::new(),
            examples: Vec::new(),
        }
    }
    pub fn validate(&self) -> ViolationResult<()> {
        if self.reason.trim().is_empty() {
            return Err(ViolationError::empty_reason(self.invariant_id.clone()));
        }
        Ok(())
    }
    pub fn invariant_id(&self) -> &InvariantId {
        &self.invariant_id
    }

    pub fn severity(&self) -> Severity {
        self.severity
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn metrics(&self) -> &BTreeMap<String, MetricValue> {
        &self.metrics
    }

    pub fn examples(&self) -> &[String] {
        &self.examples
    }

    pub fn add_metric(&mut self, name: impl Into<String>, value: MetricValue) {
        self.metrics.insert(name.into(), value);
    }

    pub fn add_example(&mut self, example: impl Into<String>) {
        self.examples.push(example.into());
    }

    pub fn has_metrics(&self) -> bool {
        !self.metrics.is_empty()
    }

    pub fn has_examples(&self) -> bool {
        !self.examples.is_empty()
    }

    pub fn metric(&self, name: &str) -> Option<&MetricValue> {
        self.metrics.get(name)
    }

    #[must_use]
    pub fn with_metric(mut self, name: impl Into<String>, value: MetricValue) -> Self {
        self.metrics.insert(name.into(), value);
        self
    }

    #[must_use]
    pub fn with_example(mut self, example: impl Into<String>) -> Self {
        self.examples.push(example.into());
        self
    }
}
