use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::prelude::AnyValue;
use polars::prelude::*;

/// Builds the Polars expression computing the requested percentile
/// for the target numeric column.
///
/// Required parameters:
/// - `p`: percentile as a float between 0.0 and 1.0
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Uses Polars `quantile()` with `QuantileMethod::Nearest`
/// - Returns a single scalar representing the computed percentile value
///
/// The resulting metric represents the raw percentile value
/// (it is NOT validated against bounds at this stage).
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    let p: f64 = inv.require_param("p").ok()?.parse().ok()?;

    Some(col(name).quantile(lit(p), QuantileMethod::Nearest))
}

/// Converts the computed percentile value into a bounded-range violation.
///
/// Required parameters:
/// - `min`: minimum allowed percentile value (inclusive)
/// - `max`: maximum allowed percentile value (inclusive)
///
/// Logic:
/// - Reads the computed percentile value
/// - Returns a violation if `percentile < min` OR `percentile > max`
///
/// Produced metric:
/// - `percentile_value` (float)
pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let percentile = value.try_extract::<f64>().ok()?;

    let min: f64 = inv.require_param("min").ok()?.parse().ok()?;
    let max: f64 = inv.require_param("max").ok()?.parse().ok()?;

    if percentile < min || percentile > max {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("percentile out of range: {}", percentile),
            )
            .with_metric("percentile_value", MetricValue::Float(percentile)),
        )
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use std::collections::BTreeMap;

    fn make_invariant(p: f64, min: f64, max: f64) -> Invariant<PolarsKind> {
        let id = InvariantId::new("percentile_between_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("p".to_string(), p.to_string());
        params.insert("min".to_string(), min.to_string());
        params.insert("max".to_string(), max.to_string());

        Invariant::new(
            id,
            PolarsKind::PercentileBetween,
            Scope::Column {
                name: "a".to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<f64>) -> DataFrame {
        let height = values.len();
        let series = Series::new("a".into(), values);
        DataFrame::new(height, vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant(0.5, 0.0, 100.0);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_percentile_no_violation() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let df = df(values);
        let inv = make_invariant(0.5, 2.0, 4.0); // median should be 3

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let val = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<f64>()
            .unwrap();

        assert!(map(&inv, AnyValue::Float64(val)).is_none());
    }

    #[test]
    fn test_percentile_violation_out_of_range() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let df = df(values);
        let inv = make_invariant(0.5, 4.5, 10.0); // median = 3, below min

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let val = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<f64>()
            .unwrap();

        let violation = map(&inv, AnyValue::Float64(val));
        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(id, PolarsKind::PercentileBetween, Scope::Dataset);

        assert!(plan(&inv).is_none());
    }
}
