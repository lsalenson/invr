use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::prelude::AnyValue;
use polars::prelude::*;

/// Simple custom expression that must evaluate to boolean per-row.
/// We count rows where expression == true.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let column = inv.require_param("column").ok()?;

    Some(col(column).cast(DataType::Boolean).eq(lit(false)).sum())
}

pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let count = value.try_extract::<i64>().ok()?;

    if count > 0 {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("custom expression failed on {count} rows"),
            )
            .with_metric("failure_count", MetricValue::Int(count)),
        )
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use crate::domain::scope::Scope;
    use crate::domain::severity::Severity;
    use polars::df;

    fn make_invariant(column: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("custom_test").unwrap();
        Invariant::new(
            id,
            PolarsKind::CustomExpr,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_severity(Severity::Error)
        .with_param_value("column".to_string(), column.to_string())
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a");
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_map_returns_violation_when_count_positive() {
        let inv = make_invariant("a");
        let value = AnyValue::Int64(3);

        let result = map(&inv, value);

        assert!(result.is_some());
    }

    #[test]
    fn test_map_returns_none_when_count_zero() {
        let inv = make_invariant("a");
        let value = AnyValue::Int64(0);

        let result = map(&inv, value);

        assert!(result.is_none());
    }

    #[test]
    fn test_integration_custom_check_counts_false_rows() {
        let df = df! {
            "a" => &[true, false, true, false]
        }
        .unwrap();

        let inv = make_invariant("a");

        let result = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let violation = map(&inv, value);

        assert!(violation.is_some());
    }
}
