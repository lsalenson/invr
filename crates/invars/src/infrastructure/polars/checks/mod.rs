use polars::prelude::*;

use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::violation::Violation;

mod column;
mod count;
mod custom;
mod date;
mod domain;
mod null;
mod numeric;
mod relational;
mod stat;
mod string;
mod unique;

pub type CheckResult = Result<Vec<Violation>, Box<dyn std::error::Error>>;

pub fn run_all(df: &DataFrame, invariants: &[Invariant<PolarsKind>]) -> CheckResult {
    let mut violations = Vec::with_capacity(invariants.len());

    // Phase 1: direct metadata checks (no lazy scan)
    for inv in invariants {
        match inv.kind() {
            PolarsKind::ColumnExists => {
                if let Some(v) = column::column_exists::run_direct(df, inv) {
                    violations.push(v);
                }
            }
            PolarsKind::ColumnMissing => {
                if let Some(v) = column::column_missing::run_direct(df, inv) {
                    violations.push(v);
                }
            }
            PolarsKind::DTypeIs => {
                if let Some(v) = column::dtype_is::run_direct(df, inv) {
                    violations.push(v);
                }
            }
            PolarsKind::SchemaEquals => {
                if let Some(v) = column::schema_equals::run_direct(df, inv) {
                    violations.push(v);
                }
            }
            _ => {}
        }
    }

    // Row count is injected into uniqueness checks that need it as context.
    let row_count = df.height().to_string();

    let mut projections = Vec::with_capacity(invariants.len());
    // Store owned invariants so we can inject row_count_cache where needed.
    let mut planned_invariants: Vec<Invariant<PolarsKind>> = Vec::with_capacity(invariants.len());

    for inv in invariants {
        match inv.kind() {
            PolarsKind::ColumnExists
            | PolarsKind::ColumnMissing
            | PolarsKind::DTypeIs
            | PolarsKind::SchemaEquals => {
                // already handled in direct phase
            }
            kind => {
                // Uniqueness checks require row_count_cache; inject it automatically
                // when the caller has not provided it explicitly.
                let enriched;
                let effective = match kind {
                    PolarsKind::Unique
                    | PolarsKind::CompositeUnique
                    | PolarsKind::DuplicateRatioMax
                        if !inv.has_param("row_count_cache") =>
                    {
                        enriched = inv.clone().with_param_value("row_count_cache", &row_count);
                        &enriched
                    }
                    _ => inv,
                };

                if let Some(expr) = plan_expr(effective) {
                    projections.push(expr.alias(effective.id().as_str()));
                    planned_invariants.push(effective.clone());
                }
            }
        }
    }

    if projections.is_empty() {
        return Ok(violations);
    }

    let result = df.clone().lazy().select(projections).collect()?;

    for inv in &planned_invariants {
        let col = result.column(inv.id().as_str())?;
        let value = col.get(0)?;
        if let Some(v) = map_violation(inv, value) {
            violations.push(v);
        }
    }

    Ok(violations)
}

///
/// Builds the Polars `Expr` associated with a given invariant.
///
/// This function translates a domain-level `Invariant<PolarsKind>`
/// into a concrete Polars expression that computes a metric.
///
/// Responsibilities:
/// - Ignore metadata-only checks (handled in the direct phase).
/// - Delegate expression construction to the appropriate module.
/// - Return `None` when the invariant does not require lazy execution.
///
/// The resulting expression must:
/// - Produce a single scalar metric (count, ratio, percentile, etc.).
/// - Be aliased with the invariant ID by the caller.
///
/// This separation keeps execution orchestration (`run_all`) decoupled
/// from invariant-specific logic.
fn plan_expr(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    match inv.kind() {
        // Direct-only metadata checks (handled in phase 1)
        PolarsKind::ColumnExists
        | PolarsKind::ColumnMissing
        | PolarsKind::DTypeIs
        | PolarsKind::SchemaEquals => None,

        PolarsKind::NotNull => null::not_null::plan(inv),
        PolarsKind::NullRatioMax => null::null_ratio_max::plan(inv),

        PolarsKind::Unique => unique::n_unique::plan(inv),
        PolarsKind::CompositeUnique => unique::composite_unique::plan(inv),
        PolarsKind::DuplicateRatioMax => unique::duplicate_ratio_max::plan(inv),

        PolarsKind::RowCountMin => Some(count::plan_row_count()),
        PolarsKind::RowCountMax => Some(count::plan_row_count()),
        PolarsKind::RowCountBetween => Some(count::plan_row_count()),

        PolarsKind::ValueMin => numeric::value_min::plan(inv),
        PolarsKind::ValueMax => numeric::value_max::plan(inv),
        PolarsKind::ValueBetween => numeric::value_between::plan(inv),
        PolarsKind::MeanBetween => numeric::mean_between::plan(inv),
        PolarsKind::StdDevMax => numeric::stddev_max::plan(inv),
        PolarsKind::SumBetween => numeric::sum_between::plan(inv),

        PolarsKind::DateBetween => date::date_between::plan(inv),
        PolarsKind::NoFutureDates => date::no_future_dates::plan(inv),
        PolarsKind::MonotonicIncreasing => date::monotonic_increasing::plan(inv),
        PolarsKind::NoGapsInSequence => date::no_gaps_in_sequence::plan(inv),

        PolarsKind::RegexMatch => string::regex_match::plan(inv),
        PolarsKind::StringLengthMin => string::string_length_min::plan(inv),
        PolarsKind::StringLengthMax => string::string_length_max::plan(inv),
        PolarsKind::StringLengthBetween => string::string_length_between::plan(inv),

        PolarsKind::AllowedValues => domain::allowed_values::plan(inv),
        PolarsKind::ForbiddenValues => domain::forbidden_values::plan(inv),

        PolarsKind::OutlierRatioMax => stat::outlier_ratio_max::plan(inv),
        PolarsKind::PercentileBetween => stat::percentile_between::plan(inv),

        PolarsKind::ForeignKey => relational::foreign_key::plan(inv),
        PolarsKind::ColumnEquals => relational::column_equals::plan(inv),
        PolarsKind::ConditionalNotNull => relational::conditional_not_null::plan(inv),

        PolarsKind::CustomExpr => custom::plan(inv),
    }
}

fn map_violation(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    match inv.kind() {
        // Direct-only metadata checks (handled in phase 1)
        PolarsKind::ColumnExists
        | PolarsKind::ColumnMissing
        | PolarsKind::DTypeIs
        | PolarsKind::SchemaEquals => None,

        PolarsKind::NotNull => null::not_null::map(inv, value),
        PolarsKind::NullRatioMax => null::null_ratio_max::map(inv, value),

        PolarsKind::Unique => unique::n_unique::map(inv, value),
        PolarsKind::CompositeUnique => unique::composite_unique::map(inv, value),
        PolarsKind::DuplicateRatioMax => unique::duplicate_ratio_max::map(inv, value),

        PolarsKind::RowCountMin => count::map_row_count(inv, value),
        PolarsKind::RowCountMax => count::map_row_count(inv, value),
        PolarsKind::RowCountBetween => count::map_row_count(inv, value),

        PolarsKind::ValueMin => numeric::map_count_violation(inv, value, "value_min"),
        PolarsKind::ValueMax => numeric::map_count_violation(inv, value, "value_max"),
        PolarsKind::ValueBetween => numeric::map_count_violation(inv, value, "value_between"),
        PolarsKind::MeanBetween => numeric::mean_between::map(inv, value),
        PolarsKind::StdDevMax => numeric::stddev_max::map(inv, value),
        PolarsKind::SumBetween => numeric::sum_between::map(inv, value),

        PolarsKind::DateBetween => date::map(inv, value),
        PolarsKind::NoFutureDates => date::map(inv, value),
        PolarsKind::MonotonicIncreasing => date::map(inv, value),
        PolarsKind::NoGapsInSequence => date::map(inv, value),

        PolarsKind::RegexMatch => string::regex_match::map(inv, value),
        PolarsKind::StringLengthMin => string::map(inv, value),
        PolarsKind::StringLengthMax => string::map(inv, value),
        PolarsKind::StringLengthBetween => string::map(inv, value),

        PolarsKind::AllowedValues => domain::allowed_values::map(inv, value),
        PolarsKind::ForbiddenValues => domain::forbidden_values::map(inv, value),

        PolarsKind::OutlierRatioMax => stat::outlier_ratio_max::map(inv, value),
        PolarsKind::PercentileBetween => stat::percentile_between::map(inv, value),

        PolarsKind::ForeignKey => relational::foreign_key::map(inv, value),
        PolarsKind::ColumnEquals => relational::column_equals::map(inv, value),
        PolarsKind::ConditionalNotNull => relational::conditional_not_null::map(inv, value),

        PolarsKind::CustomExpr => custom::map(inv, value),
    }
}
