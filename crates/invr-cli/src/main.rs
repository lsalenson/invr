use std::process;

use clap::Parser;

mod cli;
mod commands;
mod loader;
mod output;

fn main() {
    let cli = cli::Cli::parse();

    let exit_code = match cli.command {
        cli::Command::Run {
            spec,
            data,
            format,
            fail_on_violations,
        } => commands::run::execute(&spec, &data, &format, fail_on_violations),
    };

    process::exit(exit_code);
}
