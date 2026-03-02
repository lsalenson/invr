use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use polars::frame::DataFrame;


/// Executes a direct (non-lazy) check ensuring that a specified column
/// is absent from the provided `DataFrame`.
///
/// This check is evaluated immediately and does NOT participate in the
/// Polars lazy execution pipeline.
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Verifies that the invariant kind is `PolarsKind::ColumnMissing`
/// - Extracts the column name from the invariant scope
/// - Calls `df.column(name)` to test existence
/// - Returns a `Violation` if the column IS present
///
/// A return value of `None` indicates that:
/// - The column is absent (constraint satisfied), OR
/// - The invariant kind / scope does not match this check.
pub fn run_direct(df: &DataFrame, inv: &Invariant<PolarsKind>) -> Option<Violation> {
    if !matches!(inv.kind(), PolarsKind::ColumnMissing) {
        return None;
    }

    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    if df.column(name).is_ok() {
        Some(Violation::new(
            inv.id().clone(),
            inv.severity(),
            inv.scope().clone(),
            format!("column '{name}' should be missing"),
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::severity::Severity;
    use polars::df;

    fn make_invariant_column_missing(column: &str) -> Invariant<PolarsKind> {
        Invariant::new(
            "col_missing_test".to_string().parse().unwrap(),
            PolarsKind::ColumnMissing,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_severity(Severity::Error)
    }

    #[test]
    fn test_column_missing_ok_when_absent() {
        let df = df! {
            "a" => &[1, 2, 3]
        }
        .unwrap();

        let inv = make_invariant_column_missing("missing");

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }

    #[test]
    fn test_column_missing_violation_when_present() {
        let df = df! {
            "a" => &[1, 2, 3]
        }
        .unwrap();

        let inv = make_invariant_column_missing("a");

        let result = run_direct(&df, &inv);

        assert!(result.is_some());

        let violation = result.unwrap();
        assert_eq!(violation.invariant_id().as_str(), "col_missing_test");
    }

    #[test]
    fn test_column_missing_wrong_scope_returns_none() {
        let df = df! {
            "a" => &[1, 2, 3]
        }
        .unwrap();

        let inv = Invariant::new(
            "wrong_scope_test".to_string().parse().unwrap(),
            PolarsKind::ColumnMissing,
            Scope::Dataset,
        )
        .with_severity(Severity::Error);

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }

    #[test]
    fn test_column_missing_wrong_kind_returns_none() {
        let df = df! {
            "a" => &[1, 2, 3]
        }
        .unwrap();

        let inv = Invariant::new(
            "other_kind_test".to_string().parse().unwrap(),
            PolarsKind::RowCountMin,
            Scope::Column {
                name: "a".to_string(),
            },
        )
        .with_severity(Severity::Error);

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }
}
