use std::path::Path;

use polars::prelude::*;

pub fn load_dataframe(path: &Path) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    match ext {
        "csv" => Ok(CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(path.to_path_buf()))?
            .finish()?),
        "parquet" => {
            let file = std::fs::File::open(path)?;
            Ok(ParquetReader::new(file).finish()?)
        }
        "ipc" | "arrow" | "feather" => {
            let file = std::fs::File::open(path)?;
            Ok(IpcReader::new(file).finish()?)
        }
        other => {
            Err(format!("unsupported file format: '{other}'. Use .csv, .parquet, or .ipc").into())
        }
    }
}
