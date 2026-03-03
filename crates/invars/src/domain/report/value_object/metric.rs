#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReportMetric {
    pub total_invariants: usize,
    pub evaluated_invariants: usize,
    pub violations: usize,
    pub execution_time_ms: u128,
}

impl ReportMetric {
    pub fn new(
        total_invariants: usize,
        evaluated_invariants: usize,
        violations: usize,
        execution_time_ms: u128,
    ) -> Self {
        Self {
            total_invariants,
            evaluated_invariants,
            violations,
            execution_time_ms,
        }
    }

    pub fn violation_rate(&self) -> f64 {
        if self.evaluated_invariants == 0 {
            return 0.0;
        }

        self.violations as f64 / self.evaluated_invariants as f64
    }

    pub fn success_rate(&self) -> f64 {
        1.0 - self.violation_rate()
    }

    pub fn has_violations(&self) -> bool {
        self.violations > 0
    }
}
