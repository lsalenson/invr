use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use polars::prelude::*;


/// Builds the Polars expression counting rows where the string length
/// exceeds the configured maximum.
///
/// Required parameters:
/// - `max`: maximum allowed string length (inclusive)
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Computes `len_chars()` on the target column
/// - Marks rows where length > max
/// - Returns the total count of rows exceeding the maximum
///
/// The resulting metric represents the number of too-long values.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };
    let max: i64 = inv.require_param("max").ok()?.parse().ok()?;

    Some(col(name).str().len_chars().gt(lit(max)).sum())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use std::collections::BTreeMap;

    fn make_invariant(max: i64) -> Invariant<PolarsKind> {
        let id = InvariantId::new("string_length_max_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("max".to_string(), max.to_string());

        Invariant::new(
            id,
            PolarsKind::StringLengthMax,
            Scope::Column {
                name: "a".to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<&str>) -> DataFrame {
        let height = values.len();
        let series = Series::new("a".into(), values);
        DataFrame::new(height, vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant(3);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_no_violation_when_all_lengths_below_or_equal_max() {
        let df = df(vec!["a", "ab", "abc"]);
        let inv = make_invariant(3);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let count = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        assert_eq!(count, 0);
    }

    #[test]
    fn test_violation_when_length_exceeds_max() {
        let df = df(vec!["a", "abcd", "abcdef"]); // 2 violations
        let inv = make_invariant(3);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let count = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        assert_eq!(count, 2);
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(id, PolarsKind::StringLengthMax, Scope::Dataset);

        assert!(plan(&inv).is_none());
    }
}
