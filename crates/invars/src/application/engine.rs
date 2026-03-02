use crate::error::ApplicationResult;
use crate::report::Report;
use crate::spec::Spec;

pub trait Engine<K> {
    type Dataset;

    fn execute(&self, dataset: &Self::Dataset, spec: &Spec<K>) -> ApplicationResult<Report>;
}
