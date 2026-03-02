use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::prelude::AnyValue;
use polars::prelude::*;

///
/// Builds the Polars expression counting statistical outliers
/// in the target numeric column using a Z-score threshold.
///
/// Required parameters:
/// - `z`: Z-score threshold (float)
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Computes mean and standard deviation of the column
/// - Calculates Z-score for each row
/// - Marks rows where `|z_score| > z`
/// - Returns the total count of detected outliers
///
/// The resulting metric represents the number of outlier rows.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    let z: f64 = inv.require_param("z").ok()?.parse().ok()?;

    Some(
        ((col(name) - col(name).mean()) / col(name).std(1))
            .abs()
            .gt(lit(z))
            .sum(),
    )
}

///
/// Converts the computed outlier count into a ratio-based violation.
///
/// Required parameters:
/// - `row_count_cache`: total number of rows in the dataset
/// - `max_ratio`: maximum allowed outlier ratio (float)
///
/// Logic:
/// - Computes `ratio = outlier_count / total_rows`
/// - Returns a violation if `ratio > max_ratio`
///
/// Produced metrics:
/// - `outlier_ratio` (float)
/// - `outlier_count` (integer)
///
/// If `total_rows == 0`, no violation is produced.
pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let outlier_count = value.try_extract::<i64>().ok()?;

    let total: i64 = inv.require_param("row_count_cache").ok()?.parse().ok()?;
    let max_ratio: f64 = inv.require_param("max_ratio").ok()?.parse().ok()?;

    if total == 0 {
        return None;
    }

    let ratio = outlier_count as f64 / total as f64;

    if ratio > max_ratio {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("outlier ratio {:.4} > {:.4}", ratio, max_ratio),
            )
            .with_metric("outlier_ratio", MetricValue::Float(ratio))
            .with_metric("outlier_count", MetricValue::Int(outlier_count)),
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

    fn make_invariant(z: f64, max_ratio: f64, row_count: i64) -> Invariant<PolarsKind> {
        let id = InvariantId::new("outlier_ratio_max_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("z".to_string(), z.to_string());
        params.insert("max_ratio".to_string(), max_ratio.to_string());
        params.insert("row_count_cache".to_string(), row_count.to_string());

        Invariant::new(
            id,
            PolarsKind::OutlierRatioMax,
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
        let inv = make_invariant(2.0, 0.5, 3);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_no_violation_when_ratio_below_threshold() {
        // Values close together → no strong outlier
        let values = vec![10.0, 11.0, 12.0];
        let df = df(values.clone());
        let inv = make_invariant(3.0, 0.5, values.len() as i64);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let count = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        // Even if Polars computes something slightly different,
        // ratio should not exceed threshold.
        assert!(map(&inv, AnyValue::Int64(count)).is_none());
    }

    #[test]
    fn test_violation_when_ratio_above_threshold() {
        // Strong outlier
        let values = vec![10.0, 11.0, 1000.0];
        let df = df(values.clone());
        let inv = make_invariant(1.0, 0.1, values.len() as i64);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let count = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        let violation = map(&inv, AnyValue::Int64(count));
        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(id, PolarsKind::OutlierRatioMax, Scope::Dataset);

        assert!(plan(&inv).is_none());
    }
}
