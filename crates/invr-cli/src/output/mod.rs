pub mod json;
pub mod text;

use crate::cli::Format;
use invr::report::Report;

pub fn print(report: &Report, format: &Format) {
    match format {
        Format::Text => text::print(report),
        Format::Json => json::print(report),
    }
}
