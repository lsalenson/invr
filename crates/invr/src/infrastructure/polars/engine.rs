use crate::engine::Engine;
use crate::error::{ApplicationError, ApplicationResult};
use crate::infrastructure::polars::{checks, kind::PolarsKind};
use crate::report::Report;
use crate::spec::Spec;
use polars::prelude::*;

/// Polars-based execution engine for evaluating invariants on a `DataFrame`.
///
/// This engine implements the generic `Engine` trait for `PolarsKind` invariants.
///
/// Execution flow:
/// 1. Receives a `Spec` containing a collection of invariants.
/// 2. Delegates execution to `checks::run_all`.
/// 3. Collects all produced `Violation`s.
/// 4. Builds and returns a `Report`.
///
/// This struct is intentionally stateless and lightweight.
///
/// # Example
/// ```ignore
/// let engine = EnginePolarsDataFrame;
/// let report = engine.execute(&df, &spec)?;
/// ```
pub struct EnginePolarsDataFrame;

impl Engine for EnginePolarsDataFrame {
    type Dataset = DataFrame;
    type Kind = PolarsKind;

    /// Executes all invariants from the provided `Spec` against the given `DataFrame`.
    ///
    /// Returns a `Report` containing zero or more violations.
    ///
    /// Errors occurring during Polars expression evaluation are
    /// converted into `ApplicationError::engine_failure`.
    fn execute(
        &self,
        dataset: &Self::Dataset,
        spec: &Spec<Self::Kind>,
    ) -> ApplicationResult<Report> {
        let violations = checks::run_all(dataset, spec.invariants())
            .map_err(|e| ApplicationError::engine_failure(e.to_string()))?;

        let mut report = Report::new();
        report.extend(violations);
        Ok(report)
    }
}
