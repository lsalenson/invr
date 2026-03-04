use std::fmt;

use crate::report::Report;

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Report: {} violation(s) — {} info, {} warn, {} error.rs — status: {}",
            self.len(),
            self.info_count(),
            self.warn_count(),
            self.error_count(),
            if self.failed() { "FAILED" } else { "PASSED" }
        )?;

        for (i, v) in self.violations().iter().enumerate() {
            writeln!(f, "\n{}. {}", i + 1, v)?;
        }

        Ok(())
    }
}
