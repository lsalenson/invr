use crate::engine::Engine;
use crate::error::{ApplicationError, ApplicationResult};
use crate::report::Report;
use crate::spec::Spec;

pub struct RunSpec<E, K>
where
    E: Engine<K>,
{
    engine: E,
    _marker: std::marker::PhantomData<K>,
}

impl<E, K> RunSpec<E, K>
where
    E: Engine<K>,
{
    pub fn new(engine: E) -> Self {
        Self {
            engine,
            _marker: std::marker::PhantomData,
        }
    }
    //TODO LOGGING + METRIC (ajout dans le report ?)
    pub fn run(&self, dataset: &E::Dataset, spec: &Spec<K>) -> ApplicationResult<Report> {
        spec.validate().map_err(ApplicationError::InvalidSpec)?;

        let report = self.engine.execute(dataset, spec)?;

        report.validate().map_err(ApplicationError::InvalidReport)?;

        Ok(report)
    }
}
