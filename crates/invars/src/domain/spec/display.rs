use std::fmt;

use crate::spec::Spec;

impl<K: fmt::Display> fmt::Display for Spec<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Spec: {} invariant(s)", self.invariants().len())?;
        for (i, inv) in self.invariants().iter().enumerate() {
            writeln!(f, "  {}. {}", i + 1, inv)?;
        }
        Ok(())
    }
}
