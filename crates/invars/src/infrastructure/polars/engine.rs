use crate::engine::Engine;
use crate::error::{ApplicationError, ApplicationResult};
use crate::infrastructure::polars::{checks, kind::PolarsKind};
use crate::report::Report;
use crate::spec::Spec;
use polars::prelude::*;

pub struct EnginePolarsDataFrame;

impl Engine<PolarsKind> for EnginePolarsDataFrame {
    type Dataset = DataFrame;

    fn execute(&self, df: &Self::Dataset, spec: &Spec<PolarsKind>) -> ApplicationResult<Report> {
        let violations = checks::run_all(df, spec.invariants())
            .map_err(|e| ApplicationError::engine_failure(e.to_string()))?;

        let mut report = Report::new();
        report.extend(violations);
        Ok(report)
    }
}
