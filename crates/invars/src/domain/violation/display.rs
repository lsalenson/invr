use std::fmt;

use crate::violation::Violation;

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "[{}] invariant '{:?}' failed on {}",
            self.severity, self.invariant_id, self.scope
        )?;

        writeln!(f, "  ↳ {}", self.reason)?;

        if !self.metrics.is_empty() {
            writeln!(f, "  metrics:")?;
            for (key, value) in &self.metrics {
                writeln!(f, "    - {key}: {value:?}")?;
            }
        }

        if !self.examples.is_empty() {
            writeln!(f, "  examples:")?;
            for example in &self.examples {
                writeln!(f, "    - {example}")?;
            }
        }

        Ok(())
    }
}
