use std::path::Path;

use invars::interface::yaml::import::load_spec_from_path;
use invars::prelude::*;
use invars::use_cases::run_spec::RunSpec;

use crate::cli::Format;
use crate::loader::load_dataframe;
use crate::output;

pub fn execute(
    spec_path: &Path,
    data_path: &Path,
    format: &Format,
    fail_on_violations: bool,
) -> i32 {
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

    let report = match RunSpec::new(EnginePolarsDataFrame).run(&df, &spec) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: execution failed: {e}");
            return 2;
        }
    };

    output::print(&report, format);

    if fail_on_violations && !report.violations().is_empty() {
        1
    } else {
        0
    }
}
