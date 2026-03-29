mod convert;
mod types;

use std::path::Path;

use invars::infrastructure::polars::EnginePolarsDataFrame;
use invars::interface::yaml::import::load_spec_from_path;
use invars::use_cases::run_spec::RunSpec;
use polars::prelude::*;
use pyo3::prelude::*;

use crate::convert::pandas_to_polars;
use crate::types::{PyReport, PyViolation};

/// Run a YAML spec against a pandas DataFrame.
///
/// Args:
///     spec: path to the YAML spec file.
///     df: a pandas DataFrame.
///
/// Returns:
///     A Report object with violations and metrics.
///
/// Raises:
///     RuntimeError: if the spec or data cannot be loaded, or execution fails.
#[pyfunction]
fn run(py: Python, spec: &str, df: &Bound<'_, PyAny>) -> PyResult<PyReport> {
    let spec = load_spec_from_path(Path::new(spec))
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let polars_df = pandas_to_polars(py, df)?;

    let report = RunSpec::new(EnginePolarsDataFrame)
        .run(&polars_df, &spec)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    let metrics = report.metrics();
    let violations = report
        .violations()
        .iter()
        .map(|v| PyViolation {
            invariant_id: v.invariant_id().as_str().to_owned(),
            severity: format!("{}", v.severity()),
            reason: v.reason().to_owned(),
            column: v.scope().column_name().map(str::to_owned),
        })
        .collect();

    Ok(PyReport {
        total_invariants: metrics.total_invariants,
        violations_count: metrics.violations,
        execution_time_ms: metrics.execution_time_ms,
        violations,
    })
}

/// Run a YAML spec against a data file (.csv, .parquet, .ipc).
///
/// Args:
///     spec: path to the YAML spec file.
///     data: path to the data file.
///
/// Returns:
///     A Report object with violations and metrics.
#[pyfunction]
fn run_file(spec: &str, data: &str) -> PyResult<PyReport> {
    let spec = load_spec_from_path(Path::new(spec))
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let data_path = Path::new(data);
    let ext = data_path.extension().and_then(|e| e.to_str()).unwrap_or("");

    let df = match ext {
        "csv" => CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(data_path.to_path_buf()))
            .and_then(|r| r.finish())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?,
        "parquet" => {
            let file = std::fs::File::open(data_path)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            ParquetReader::new(file)
                .finish()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
        }
        "ipc" | "arrow" | "feather" => {
            let file = std::fs::File::open(data_path)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            IpcReader::new(file)
                .finish()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
        }
        other => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unsupported file format: '{other}'. Use .csv, .parquet, or .ipc"
            )));
        }
    };

    let report = RunSpec::new(EnginePolarsDataFrame)
        .run(&df, &spec)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    let metrics = report.metrics();
    let violations = report
        .violations()
        .iter()
        .map(|v| PyViolation {
            invariant_id: v.invariant_id().as_str().to_owned(),
            severity: format!("{}", v.severity()),
            reason: v.reason().to_owned(),
            column: v.scope().column_name().map(str::to_owned),
        })
        .collect();

    Ok(PyReport {
        total_invariants: metrics.total_invariants,
        violations_count: metrics.violations,
        execution_time_ms: metrics.execution_time_ms,
        violations,
    })
}

#[pymodule]
fn invars_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyReport>()?;
    m.add_class::<PyViolation>()?;
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(run_file, m)?)?;
    Ok(())
}
