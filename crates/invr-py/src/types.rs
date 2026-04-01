use pyo3::prelude::*;

/// Python-facing violation.
#[pyclass(name = "Violation", get_all)]
#[derive(Clone)]
pub struct PyViolation {
    pub invariant_id: String,
    pub severity: String,
    pub reason: String,
    pub column: Option<String>,
}

#[pymethods]
impl PyViolation {
    fn __repr__(&self) -> String {
        match &self.column {
            Some(col) => format!(
                "Violation(id='{}', severity='{}', column='{}', reason='{}')",
                self.invariant_id, self.severity, col, self.reason
            ),
            None => format!(
                "Violation(id='{}', severity='{}', reason='{}')",
                self.invariant_id, self.severity, self.reason
            ),
        }
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("invariant_id", &self.invariant_id)?;
        dict.set_item("severity", &self.severity)?;
        dict.set_item("reason", &self.reason)?;
        dict.set_item("column", &self.column)?;
        Ok(dict.into())
    }
}

/// Python-facing report.
#[pyclass(name = "Report", get_all)]
pub struct PyReport {
    pub total_invariants: usize,
    pub violations_count: usize,
    pub execution_time_ms: u128,
    pub violations: Vec<PyViolation>,
}

#[pymethods]
impl PyReport {
    /// True if no violations were found.
    fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    /// True if any error-severity violations were found.
    fn failed(&self) -> bool {
        self.violations.iter().any(|v| v.severity == "ERROR")
    }

    fn __repr__(&self) -> String {
        format!(
            "Report(invariants={}, violations={}, elapsed={}ms)",
            self.total_invariants, self.violations_count, self.execution_time_ms
        )
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("total_invariants", self.total_invariants)?;
        dict.set_item("violations_count", self.violations_count)?;
        dict.set_item("execution_time_ms", self.execution_time_ms)?;
        let violations: Vec<PyObject> = self
            .violations
            .iter()
            .map(|v| v.to_dict(py))
            .collect::<PyResult<_>>()?;
        dict.set_item("violations", violations)?;
        Ok(dict.into())
    }
}
