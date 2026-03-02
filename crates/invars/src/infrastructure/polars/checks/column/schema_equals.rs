use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::violation::Violation;
use polars::frame::DataFrame;
pub fn run_direct(df: &DataFrame, inv: &Invariant<PolarsKind>) -> Option<Violation> {
    if !matches!(inv.kind(), PolarsKind::SchemaEquals) {
        return None;
    }

    let expected = inv.require_param("schema").ok()?;

    let actual = df
        .columns()
        .iter()
        .map(|c| format!("{}:{}", c.name(), c.dtype()))
        .collect::<Vec<_>>()
        .join(",");

    if actual != expected {
        Some(Violation::new(
            inv.id().clone(),
            inv.severity(),
            inv.scope().clone(),
            "schema mismatch".to_string(),
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::severity::Severity;
    use crate::scope::Scope;
    use polars::df;

    fn make_invariant_schema(expected: &str) -> Invariant<PolarsKind> {
        Invariant::new(
            "schema_test".to_string().parse().unwrap(),
            PolarsKind::SchemaEquals,
            Scope::Dataset,
        )
        .with_severity(Severity::Error)
        .with_param_value("schema".to_string(), expected.to_string())
    }

    #[test]
    fn test_schema_equals_ok() {
        let df = df! {
            "a" => &[1, 2],
            "b" => &[3, 4]
        }
        .unwrap();

        let expected = df
            .columns()
            .iter()
            .map(|c| format!("{}:{}", c.name(), c.dtype()))
            .collect::<Vec<_>>()
            .join(",");

        let inv = make_invariant_schema(&expected);

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }

    #[test]
    fn test_schema_equals_violation() {
        let df = df! {
            "a" => &[1, 2],
            "b" => &[3, 4]
        }
        .unwrap();

        let wrong_schema = "a:Int64";

        let inv = make_invariant_schema(wrong_schema);

        let result = run_direct(&df, &inv);

        assert!(result.is_some());

        let violation = result.unwrap();
        assert_eq!(violation.invariant_id().as_str(), "schema_test");
    }

    #[test]
    fn test_schema_equals_wrong_kind_returns_none() {
        let df = df! {
            "a" => &[1, 2]
        }
        .unwrap();

        let inv = Invariant::new(
            "other_kind_test".to_string().parse().unwrap(),
            PolarsKind::RowCountMin,
            Scope::Dataset,
        )
        .with_severity(Severity::Error)
        .with_param_value("schema".to_string(), "a:Int64".to_string());

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }
}
