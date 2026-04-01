# invr — Claude Code Guide

## What this project is

`invr` is a declarative data validation engine for Rust with Python bindings.
- Rust crate: `invr` (crates.io) — features: `polars`, `yaml`
- Python package: `invr` (PyPI) — wraps the Rust core via PyO3

## Crate structure

```
crates/
├── invr/        # Core library — always import as `invr::`
├── invr-cli/    # CLI binary
└── invr-py/     # Python bindings (PyO3 + maturin)
python/invr/     # Python package wrapper
```

## Critical: import paths

The crate is named `invr`, NOT `invars`. Always use:
```rust
use invr::prelude::*;          // ✅
use invars::prelude::*;        // ❌ wrong — old name, will not compile
```

## Prelude exports

```rust
use invr::prelude::*;
// Gives you: Invariant, InvariantId, Scope, Severity, Spec
//            RunSpec, EnginePolarsDataFrame, PolarsKind  (feature = "polars")
//            spec_from_str, YamlError                   (feature = "yaml")
```

## Versions

- Workspace: `0.1.1` (Cargo.toml)
- Python package: `0.2.2` (pyproject.toml)
- Rust edition: 2024, MSRV 1.92.0

## Build commands

```bash
cargo build                        # build all crates
cargo test                         # run all tests
maturin build --release            # build Python wheel
pip3 install twine && twine upload target/wheels/*.whl  # publish to PyPI
```

## Key design rules

- Invariant kinds are enums (`PolarsKind`), never strings
- All `params` values are `&str` — numeric values passed as string literals e.g. `"42"`
- `Scope::column("name")` or `Scope::Dataset` — use the constructor methods
- `report.failed()` → true only for `Severity::Error` violations
- Engine is generic: `RunSpec::new(EnginePolarsDataFrame).run(&df, &spec)`

## YAML format

```yaml
invariants:
  - id: my_check          # [a-zA-Z0-9_-]+
    kind: not_null         # snake_case PolarsKind
    scope:
      type: column         # or: dataset
      name: my_column
    severity: error        # error (default) | warn | info
    params:
      min: "10"            # all values are strings
```

## All 33 invariant kinds with their params

| kind | scope | params |
|---|---|---|
| `not_null` | column | — |
| `null_ratio_max` | column | `max_ratio` |
| `unique` | column | — |
| `composite_unique` | dataset | `columns` (comma-sep) |
| `duplicate_ratio_max` | column | `max_ratio` |
| `row_count_min` | dataset | `min` |
| `row_count_max` | dataset | `max` |
| `row_count_between` | dataset | `min`, `max` |
| `column_exists` | column | — |
| `column_missing` | column | — |
| `dtype_is` | column | `dtype` e.g. `"Int64"` |
| `schema_equals` | dataset | `schema` e.g. `"a:Int64,b:Utf8"` |
| `value_min` | column | `min` |
| `value_max` | column | `max` |
| `value_between` | column | `min`, `max` |
| `mean_between` | column | `min`, `max` |
| `stddev_max` | column | `max` |
| `sum_between` | column | `min`, `max` |
| `date_between` | column | `start`, `end` (ISO 8601) |
| `no_future_dates` | column | — |
| `monotonic_increasing` | column | — |
| `no_gaps_in_sequence` | column | — |
| `regex_match` | column | `pattern` |
| `string_length_min` | column | `min` |
| `string_length_max` | column | `max` |
| `string_length_between` | column | `min`, `max` |
| `allowed_values` | column | `values` (comma-sep) |
| `forbidden_values` | column | `values` (comma-sep) |
| `outlier_ratio_max` | column | `z`, `max_ratio` |
| `percentile_between` | column | `p`, `min`, `max` |
| `foreign_key` | column | `allowed_values` (comma-sep) |
| `column_equals` | column | `other_column` |
| `conditional_not_null` | column | `condition_column`, `condition_value` |
| `custom_expr` | column | `column` |
