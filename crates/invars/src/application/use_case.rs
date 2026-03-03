use crate::engine::Engine;
use crate::error::{ApplicationError, ApplicationResult};
use crate::report::Report;
use crate::spec::Spec;

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

        let report = self.engine.execute(dataset, spec)?;

        report.validate().map_err(ApplicationError::InvalidReport)?;

        Ok(report)
    }
}
