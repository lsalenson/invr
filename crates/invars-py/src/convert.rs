use polars::prelude::*;
use pyo3::prelude::*;

/// Convert a pandas DataFrame to a Polars DataFrame.
///
/// Strategy: pandas → Parquet bytes (via BytesIO) → Polars.
/// This uses pyarrow under the hood (pandas dep) and avoids
/// any direct C-level Arrow interop complexity.
pub fn pandas_to_polars(py: Python, df: &Bound<'_, PyAny>) -> PyResult<DataFrame> {
    let io = py.import("io")?;
    let buffer = io.getattr("BytesIO")?.call0()?;

    df.call_method1("to_parquet", (&buffer,))?;
    buffer.call_method1("seek", (0i64,))?;

    let bytes: Vec<u8> = buffer.call_method0("read")?.extract()?;
    let cursor = std::io::Cursor::new(bytes);

    ParquetReader::new(cursor)
        .finish()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}
