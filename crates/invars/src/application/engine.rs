use crate::error::ApplicationResult;
use crate::report::Report;
use crate::spec::Spec;

pub trait Engine {
    type Dataset;
    type Kind;

    fn execute(
        &self,
        dataset: &Self::Dataset,
        spec: &Spec<Self::Kind>,
    ) -> ApplicationResult<Report>;
}
