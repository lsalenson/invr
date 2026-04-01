use invars::report::Report;

pub fn print(report: &Report) {
    let metrics = report.metrics();
    let violations = report.violations();

    println!(
        "invr report — {} invariants | {} violations | {}ms",
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
