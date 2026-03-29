use std::path::{Path, PathBuf};
use std::process;

use clap::{Parser, ValueEnum};
use invars::interface::yaml::import::load_spec_from_path;
use invars::prelude::*;
use invars::report::Report;
use invars::use_cases::run_spec::RunSpec;
use polars::prelude::*;
use serde::Serialize;

/// Invars — declarative data validation CLI
#[derive(Parser)]
#[command(name = "invars", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Run a YAML spec against a data file
    Run {
        /// Path to the YAML spec file
        #[arg(short, long)]
        spec: PathBuf,

        /// Path to the data file (.csv, .parquet, or .ipc/.arrow)
        #[arg(short, long)]
        data: PathBuf,

        /// Output format
        #[arg(short, long, default_value = "text")]
        format: Format,

        /// Exit with code 1 if any violations are found (default: true)
        #[arg(long, default_value_t = true)]
        fail_on_violations: bool,
    },
}

#[derive(Clone, ValueEnum)]
enum Format {
    Text,
    Json,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Run {
            spec,
            data,
            format,
            fail_on_violations,
        } => {
            let exit_code = run(&spec, &data, &format, fail_on_violations);
            process::exit(exit_code);
        }
    }
}

fn run(spec_path: &Path, data_path: &Path, format: &Format, fail_on_violations: bool) -> i32 {
    let spec = match load_spec_from_path(spec_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to load spec '{}': {e}", spec_path.display());
            return 2;
        }
    };

    let df = match load_dataframe(data_path) {
        Ok(df) => df,
        Err(e) => {
            eprintln!("error: failed to load data '{}': {e}", data_path.display());
            return 2;
        }
    };

    let engine = EnginePolarsDataFrame;
    let use_case = RunSpec::new(engine);

    let report = match use_case.run(&df, &spec) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: execution failed: {e}");
            return 2;
        }
    };

    match format {
        Format::Text => print_text(&report),
        Format::Json => print_json(&report),
    }

    if fail_on_violations && !report.violations().is_empty() {
        1
    } else {
        0
    }
}

fn load_dataframe(path: &Path) -> Result<DataFrame, Box<dyn std::error::Error>> {
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
        other => Err(format!(
            "unsupported file format: '{other}'. Use .csv, .parquet, or .ipc"
        )
        .into()),
    }
}

fn print_text(report: &Report) {
    let metrics = report.metrics();
    let violations = report.violations();

    println!(
        "invars report — {} invariants | {} violations | {}ms",
        metrics.total_invariants, metrics.violations, metrics.execution_time_ms
    );

    if violations.is_empty() {
        println!("all invariants passed");
    } else {
        println!();
        for v in violations {
            println!("{v}");
        }
        println!();
        println!("{} violation(s) found", violations.len());
    }
}

// Serializable DTO for JSON output
#[derive(Serialize)]
struct ReportJson {
    total_invariants: usize,
    violations_count: usize,
    execution_time_ms: u128,
    violations: Vec<ViolationJson>,
}

#[derive(Serialize)]
struct ViolationJson {
    invariant_id: String,
    severity: String,
    reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    column: Option<String>,
}

fn print_json(report: &Report) {
    let metrics = report.metrics();
    let dto: ReportJson = ReportJson {
        total_invariants: metrics.total_invariants,
        violations_count: metrics.violations,
        execution_time_ms: metrics.execution_time_ms,
        violations: report
            .violations()
            .iter()
            .map(|v| ViolationJson {
                invariant_id: v.invariant_id().as_str().to_owned(),
                severity: format!("{}", v.severity()),
                reason: v.reason().to_owned(),
                column: v.scope().column_name().map(str::to_owned),
            })
            .collect(),
    };

    match serde_json::to_string_pretty(&dto) {
        Ok(json) => println!("{json}"),
        Err(e) => eprintln!("error: failed to serialize report: {e}"),
    }
}
