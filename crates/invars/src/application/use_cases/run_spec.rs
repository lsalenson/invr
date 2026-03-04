use crate::engine::Engine;
use crate::error::{ApplicationError, ApplicationResult};
use crate::report::Report;
use crate::spec::Spec;
use std::time::Instant;

pub struct RunSpec<E>
where
    E: Engine,
{
    engine: E,
}

impl<E> RunSpec<E>
where
    E: Engine,
{
    pub fn new(engine: E) -> Self {
        Self { engine }
    }

    pub fn run(&self, dataset: &E::Dataset, spec: &Spec<E::Kind>) -> ApplicationResult<Report> {
        spec.validate().map_err(ApplicationError::InvalidSpec)?;
        let start = Instant::now();

        let mut report = self.engine.execute(dataset, spec)?;

        report.validate().map_err(ApplicationError::InvalidReport)?;

        let elapsed = start.elapsed().as_millis();

        {
            let violations_count = report.violations().len();

            let metrics = report.metrics_mut();
            metrics.execution_time_ms = elapsed;
            metrics.total_invariants = spec.invariants().len();
            metrics.violations = violations_count;
        }

        Ok(report)
    }
}
