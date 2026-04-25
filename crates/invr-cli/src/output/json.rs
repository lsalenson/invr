use invr::report::Report;
use serde::Serialize;

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

pub fn print(report: &Report) {
    let metrics = report.metrics();
    let dto = ReportJson {
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
