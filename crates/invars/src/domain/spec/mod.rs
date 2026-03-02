/// A collection of invariants.
///
/// A `Spec` defines the validation contract for a dataset.
/// It can be validated before execution.
mod display;
pub mod error;

use crate::invariant::Invariant;
use crate::invariant::value_object::id::InvariantId;
use crate::spec::error::{SpecError, SpecResult};

use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
pub struct Spec<K> {
    invariants: Vec<Invariant<K>>,
}

impl<K> Spec<K> {
    pub fn new() -> Self {
        Self {
            invariants: Vec::new(),
        }
    }
    pub fn validate(&self) -> SpecResult<()> {
        let mut seen: BTreeSet<InvariantId> = BTreeSet::new();

        for invariant in &self.invariants {
            let id = invariant.id().clone();
            if !seen.insert(id.clone()) {
                return Err(SpecError::duplicate_invariant_id(id));
            }
        }

        Ok(())
    }
    pub fn from_invariants(invariants: Vec<Invariant<K>>) -> Self {
        Self { invariants }
    }

    pub fn invariants(&self) -> &[Invariant<K>] {
        &self.invariants
    }

    pub fn into_invariants(self) -> Vec<Invariant<K>> {
        self.invariants
    }

    pub fn push(&mut self, invariant: Invariant<K>) {
        self.invariants.push(invariant);
    }

    pub fn extend<I>(&mut self, invariants: I)
    where
        I: IntoIterator<Item = Invariant<K>>,
    {
        self.invariants.extend(invariants);
    }

    pub fn is_empty(&self) -> bool {
        self.invariants.is_empty()
    }

    pub fn len(&self) -> usize {
        self.invariants.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Invariant<K>> {
        self.invariants.iter()
    }

    pub fn find_by_id(&self, id: &InvariantId) -> Option<&Invariant<K>> {
        self.invariants.iter().find(|inv| inv.id() == id)
    }
}

impl<K> Default for Spec<K> {
    fn default() -> Self {
        Self::new()
    }
}
