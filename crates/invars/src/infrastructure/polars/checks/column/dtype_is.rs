use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use polars::frame::DataFrame;

pub fn run_direct(df: &DataFrame, inv: &Invariant<PolarsKind>) -> Option<Violation> {
    if !matches!(inv.kind(), PolarsKind::DTypeIs) {
        return None;
    }

    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    let expected = inv.require_param("dtype").ok()?;
    let actual = df.column(name).ok()?.dtype().to_string();

    if actual != expected {
        Some(Violation::new(
            inv.id().clone(),
            inv.severity(),
            inv.scope().clone(),
            format!("dtype '{actual}' != expected '{expected}'"),
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

    fn make_invariant_dtype(column: &str, dtype: &str) -> Invariant<PolarsKind> {
        Invariant::new(
            "dtype_test".to_string().parse().unwrap(),
            PolarsKind::DTypeIs,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_severity(Severity::Error)
        .with_param_value("dtype".to_string(), dtype.to_string())
    }

    #[test]
    fn test_dtype_is_ok() {
        let df = df! {
            "a" => &[1i64, 2i64, 3i64]
        }
        .unwrap();

        let expected = df.column("a").unwrap().dtype().to_string();
        let inv = make_invariant_dtype("a", &expected);

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }

    #[test]
    fn test_dtype_is_violation() {
        let df = df! {
        "a" => &[1i64, 2i64, 3i64]
    }
            .unwrap();

        let inv = make_invariant_dtype("a", "Int32");

        let result = run_direct(&df, &inv);

        assert!(result.is_some());

        let violation = result.unwrap();
        assert_eq!(violation.invariant_id().as_str(), "dtype_test");
    }

    #[test]
    fn test_dtype_is_wrong_scope_returns_none() {

        let df = df! {
            "a" => &[1i64, 2i64, 3i64]
        }
            .unwrap();
        let inv = Invariant::new(
            "wrong_scope_test".to_string().parse().unwrap(),
            PolarsKind::DTypeIs,
            Scope::Dataset,
        )
        .with_severity(Severity::Error)
        .with_param_value("dtype".to_string(), "Int64".to_string());

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }

    #[test]
    fn test_dtype_is_wrong_kind_returns_none() {
        let df = df! {
            "a" => &[1i64, 2i64, 3i64]
        }
        .unwrap();

        let inv = Invariant::new(
            "other_kind_test".to_string().parse().unwrap(),
            PolarsKind::RowCountMin,
            Scope::Column {
                name: "a".to_string(),
            },
        )
        .with_severity(Severity::Error)
        .with_param_value("dtype".to_string(), "Int64".to_string());

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }
}
