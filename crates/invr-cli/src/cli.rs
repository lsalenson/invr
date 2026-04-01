use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(name = "invr", version, about = "Declarative data validation CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(clap::Subcommand)]
pub enum Command {
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

        /// Exit with code 1 if any violations are found
        #[arg(long, default_value_t = true)]
        fail_on_violations: bool,
    },
}

#[derive(Clone, ValueEnum)]
pub enum Format {
    Text,
    Json,
}
