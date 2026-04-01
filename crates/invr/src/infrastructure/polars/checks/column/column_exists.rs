use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use polars::frame::DataFrame;

/// Executes a direct (non-lazy) check ensuring that a specified column
/// exists in the provided `DataFrame`.
///
/// This check is evaluated immediately and does NOT participate in the
/// Polars lazy execution pipeline.
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Verifies that the invariant kind is `PolarsKind::ColumnExists`
/// - Extracts the column name from the invariant scope
/// - Calls `df.column(name)` to check existence
/// - Returns a `Violation` if the column is missing
///
/// A return value of `None` indicates that:
/// - The column exists, OR
/// - The invariant kind / scope does not match this check.
pub fn run_direct(df: &DataFrame, inv: &Invariant<PolarsKind>) -> Option<Violation> {
    if !matches!(inv.kind(), PolarsKind::ColumnExists) {
        return None;
    }

    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    if df.column(name).is_err() {
        Some(Violation::new(
            inv.id().clone(),
            inv.severity(),
            inv.scope().clone(),
            format!("column '{name}' does not exist"),
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

    fn make_invariant_column_exists(column: &str) -> Invariant<PolarsKind> {
        Invariant::new(
            "col_exists_test".to_string().parse().unwrap(),
            PolarsKind::ColumnExists,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_severity(Severity::Error)
    }

    #[test]
    fn test_column_exists_ok() {
        let df = df! {
            "a" => &[1, 2, 3],
            "b" => &[4, 5, 6]
        }
        .unwrap();

        let inv = make_invariant_column_exists("a");

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }

    #[test]
    fn test_column_exists_missing() {
        let df = df! {
            "a" => &[1, 2, 3]
        }
        .unwrap();

        let inv = make_invariant_column_exists("missing");

        let result = run_direct(&df, &inv);

        assert!(result.is_some());

        let violation = result.unwrap();
        assert_eq!(violation.invariant_id().as_str(), "col_exists_test");
    }

    #[test]
    fn test_column_exists_wrong_kind_returns_none() {
        let df = df! {
            "a" => &[1, 2, 3]
        }
        .unwrap();

        let inv = Invariant::new(
            "other_kind_test".to_string().parse().unwrap(),
            PolarsKind::RowCountMin,
            Scope::Dataset,
        )
        .with_severity(Severity::Error);

        let result = run_direct(&df, &inv);

        assert!(result.is_none());
    }
}
