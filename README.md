# invars

Declarative data validation engine for Rust.

Define **invariants** (validation rules) and evaluate them against a dataset using a typed execution engine.

## Features

- 33 built-in invariant types (nullability, uniqueness, numeric, string, date, relational, statistical, …)
- Lazy Polars execution backend
- Load specs from YAML
- Engine-agnostic core — bring your own backend
- Fully typed: no stringly-typed rule names

## Installation

```toml
[dependencies]
invars = { version = "0.2.0", features = ["polars"] }
```

To also load specs from YAML:

```toml
invars = { version = "0.2.0", features = ["polars", "yaml"] }
```

## Quick start

### Programmatic spec

```rust
use invars::prelude::*;
use polars::prelude::*;

let df = df![
    "age" => [25, 30, 45],
    "email" => ["a@b.com", "c@d.com", "e@f.com"],
]?;

let spec = Spec::from_invariants(vec![
    Invariant::new(
        InvariantId::new("age_not_null")?,
        PolarsKind::NotNull,
        Scope::Column { name: "age".into() },
    ),
    Invariant::new(
        InvariantId::new("row_count_min")?,
        PolarsKind::RowCountMin,
        Scope::Dataset,
    )
    .with_param_value("min", "1"),
]);

let runner = RunSpec::new(EnginePolarsDataFrame);
let report = runner.run(&df, &spec)?;

if report.failed() {
    for v in report.errors() {
        eprintln!("violation: {}", v.reason());
    }
}
```

### YAML spec

```yaml
# spec.yaml
invariants:
  - id: age_not_null
    kind: not_null
    scope:
      type: column
      name: age

  - id: email_unique
    kind: unique
    scope:
      type: column
      name: email
    severity: error

  - id: row_count_check
    kind: row_count_min
    scope:
      type: dataset
    params:
      min: "10"
```

```rust
use invars::prelude::*;

let yaml = std::fs::read_to_string("spec.yaml")?;
let spec = spec_from_str(&yaml)?;

let runner = RunSpec::new(EnginePolarsDataFrame);
let report = runner.run(&df, &spec)?;
```

## Invariant types

| Category     | Kinds |
|--------------|-------|
| Nullability  | `not_null`, `null_ratio_max` |
| Uniqueness   | `unique`, `composite_unique`, `duplicate_ratio_max` |
| Row count    | `row_count_min`, `row_count_max`, `row_count_between` |
| Structure    | `column_exists`, `column_missing`, `dtype_is`, `schema_equals` |
| Numeric      | `value_min`, `value_max`, `value_between`, `mean_between`, `stddev_max`, `sum_between` |
| Date / Time  | `date_between`, `no_future_dates`, `monotonic_increasing`, `no_gaps_in_sequence` |
| String       | `regex_match`, `string_length_min`, `string_length_max`, `string_length_between` |
| Domain       | `allowed_values`, `forbidden_values` |
| Statistical  | `outlier_ratio_max`, `percentile_between` |
| Relational   | `foreign_key`, `column_equals`, `conditional_not_null` |
| Custom       | `custom_expr` |

### Parameters reference

All param values are strings. Numeric and float values are passed as string literals.

| Kind | Scope | Params |
|------|-------|--------|
| `not_null` | Column | — |
| `null_ratio_max` | Column | `max_ratio` — float 0.0–1.0 |
| `unique` | Column | — |
| `composite_unique` | Dataset | `columns` — comma-sep column list, e.g. `"a,b"` |
| `duplicate_ratio_max` | Column | `max_ratio` — float 0.0–1.0 |
| `row_count_min` | Dataset | `min` — integer |
| `row_count_max` | Dataset | `max` — integer |
| `row_count_between` | Dataset | `min`, `max` — integers |
| `column_exists` | Column | — |
| `column_missing` | Column | — |
| `dtype_is` | Column | `dtype` — Polars dtype, e.g. `"Int64"`, `"Utf8"`, `"Float64"` |
| `schema_equals` | Dataset | `schema` — comma-sep `col:dtype` pairs, e.g. `"a:Int64,b:Utf8"` |
| `value_min` | Column | `min` |
| `value_max` | Column | `max` |
| `value_between` | Column | `min`, `max` |
| `mean_between` | Column | `min`, `max` |
| `stddev_max` | Column | `max` |
| `sum_between` | Column | `min`, `max` |
| `date_between` | Column | `start`, `end` — ISO 8601 date strings |
| `no_future_dates` | Column | — |
| `monotonic_increasing` | Column | — |
| `no_gaps_in_sequence` | Column | — |
| `regex_match` | Column | `pattern` — regex string |
| `string_length_min` | Column | `min` — integer |
| `string_length_max` | Column | `max` — integer |
| `string_length_between` | Column | `min`, `max` — integers |
| `allowed_values` | Column | `values` — comma-sep list, e.g. `"A,B,C"` |
| `forbidden_values` | Column | `values` — comma-sep list |
| `outlier_ratio_max` | Column | `z` — Z-score threshold, `max_ratio` — float 0.0–1.0 |
| `percentile_between` | Column | `p` — percentile 0.0–1.0, `min`, `max` — bounds |
| `foreign_key` | Column | `allowed_values` — comma-sep valid FK values |
| `column_equals` | Column | `other_column` — column name to compare against |
| `conditional_not_null` | Column | `condition_column`, `condition_value` |
| `custom_expr` | Column | `column` — column name |

## Report API

```rust
report.failed()          // true if any Error-severity violation exists
report.violations()      // all violations
report.errors()          // iterator over Error violations
report.warnings()        // iterator over Warn violations
report.error_count()     // number of Error violations
report.metrics()         // execution_time_ms, total_invariants, violations
```

## Severity

Each invariant defaults to `Error`. Override with:

```rust
invariant.with_severity(Severity::Warn)
```

Or in YAML:
```yaml
severity: warn   # info | warn | error
```

## Feature flags

| Feature  | Description |
|----------|-------------|
| `polars` | Enables the Polars execution engine |
| `yaml`   | Enables loading specs from YAML strings |

## License

Apache-2.0
