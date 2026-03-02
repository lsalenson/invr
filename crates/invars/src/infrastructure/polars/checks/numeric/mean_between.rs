use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use polars::prelude::*;

///
/// Builds the Polars expression computing the mean value of the
/// target numeric column.
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Casts the column to `Float64`
/// - Computes `mean()` over all rows
/// - Returns a single scalar representing the column mean
///
/// The resulting metric represents the raw mean value and is not
/// validated against bounds at this stage.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    Some(col(name).cast(DataType::Float64).mean())
}

///
/// Converts the computed mean value into a bounded-range violation.
///
/// Required parameters:
/// - `min`: minimum allowed mean (inclusive)
/// - `max`: maximum allowed mean (inclusive)
///
/// Logic:
/// - Reads the computed mean value
/// - Returns a violation if `mean < min` OR `mean > max`
///
/// Produced metric (implicit in message):
/// - mean value (float)
pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let mean = value.try_extract::<f64>().ok()?;

    let min: f64 = inv.require_param("min").ok()?.parse().ok()?;
    let max: f64 = inv.require_param("max").ok()?.parse().ok()?;

    if mean < min || mean > max {
        Some(Violation::new(
            inv.id().clone(),
            inv.severity(),
            inv.scope().clone(),
            format!("mean {mean} not in [{min}, {max}]"),
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use crate::infrastructure::polars::kind::PolarsKind;
    use std::collections::BTreeMap;

    fn make_invariant(column: &str, min: f64, max: f64) -> Invariant<PolarsKind> {
        let id = InvariantId::new("mean_between_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("min".to_string(), min.to_string());
        params.insert("max".to_string(), max.to_string());

        Invariant::new(
            id,
            PolarsKind::MeanBetween,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<i32>) -> DataFrame {
        let series = Series::new(PlSmallStr::from("a"), values);
        DataFrame::new_infer_height(vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a", 0.0, 10.0);
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_mean_between_no_violation() {
        let df = df(vec![1, 2, 3]); // mean = 2
        let inv = make_invariant("a", 0.0, 5.0);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let violation = map(&inv, value);

        assert!(violation.is_none());
    }

    #[test]
    fn test_mean_between_violation_low() {
        let df = df(vec![1, 1, 1]); // mean = 1
        let inv = make_invariant("a", 2.0, 5.0);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let violation = map(&inv, value);

        assert!(violation.is_some());
    }

    #[test]
    fn test_mean_between_violation_high() {
        let df = df(vec![10, 10, 10]); // mean = 10
        let inv = make_invariant("a", 0.0, 5.0);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let violation = map(&inv, value);

        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(id, PolarsKind::MeanBetween, Scope::Dataset);

        let expr = plan(&inv);
        assert!(expr.is_none());
    }
}
