/// Execution result of a `Spec`.
///
/// A `Report` contains zero or more `Violation`s
/// and exposes a `failed()` method to determine
/// whether the dataset satisfies the contract.
mod display;

pub mod error;
mod value_object;

pub use error::{ReportError, ReportResult};

use crate::report::value_object::metric::ReportMetric;
use crate::severity::Severity;
use crate::violation::Violation;
use std::cmp::PartialEq;

#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    violations: Vec<Violation>,
    metrics: ReportMetric,
}

impl Report {
    pub fn new() -> Self {
        Self {
            violations: Vec::new(),
            metrics: Default::default(),
        }
    }
    pub fn validate(&self) -> ReportResult<()> {
        for (i, v) in self.violations.iter().enumerate() {
            v.validate()
                .map_err(|e| ReportError::invalid_violation(i, e))?;
        }
        Ok(())
    }
    pub fn from_violations(violations: Vec<Violation>) -> Self {
        Self {
            violations,
            metrics: Default::default(),
        }
    }
    pub fn metrics(&self) -> &ReportMetric {
        &self.metrics
    }
    pub fn set_violations(&mut self, violations: Vec<Violation>) {
        self.violations = violations;
    }
    pub fn metrics_mut(&mut self) -> &mut ReportMetric {
        &mut self.metrics
    }
    pub fn set_metrics(&mut self, metrics: ReportMetric) {
        self.metrics = metrics;
    }
    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }

    pub fn into_violations(self) -> Vec<Violation> {
        self.violations
    }

    pub fn push(&mut self, violation: Violation) {
        self.violations.push(violation);
    }

    pub fn extend<I>(&mut self, violations: I)
    where
        I: IntoIterator<Item = Violation>,
    {
        self.violations.extend(violations);
    }

    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.violations.len()
    }

    pub fn failed(&self) -> bool {
        self.violations.iter().any(|v| v.severity().is_error())
    }

    pub fn has_errors(&self) -> bool {
        self.violations
            .iter()
            .any(|v| matches!(v.severity(), Severity::Error))
    }

    pub fn has_warnings(&self) -> bool {
        self.violations
            .iter()
            .any(|v| v.severity() == Severity::Warn)
    }

    pub fn count_by_severity(&self, severity: Severity) -> usize {
        self.violations
            .iter()
            .filter(|v| v.severity() == severity)
            .count()
    }

    pub fn error_count(&self) -> usize {
        self.count_by_severity(Severity::Error)
    }

    pub fn warn_count(&self) -> usize {
        self.count_by_severity(Severity::Warn)
    }

    pub fn info_count(&self) -> usize {
        self.count_by_severity(Severity::Info)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Violation> {
        self.violations.iter()
    }

    pub fn errors(&self) -> impl Iterator<Item = &Violation> {
        self.violations
            .iter()
            .filter(|v| matches!(v.severity(), Severity::Error))
    }

    pub fn warnings(&self) -> impl Iterator<Item = &Violation> {
        self.violations
            .iter()
            .filter(|v| v.severity() == Severity::Warn)
    }
}

impl Default for Report {
    fn default() -> Self {
        Self::new()
    }
}
