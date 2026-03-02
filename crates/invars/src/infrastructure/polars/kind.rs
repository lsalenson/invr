///
/// Represents all invariant types supported by the Polars execution engine.
///
/// Each `PolarsKind` corresponds to a specific validation rule.
///
/// Execution model:
/// - `plan(kind)` builds a Polars `Expr`
/// - The expression is executed lazily
/// - The result metric is converted into a `Violation` via `map()`
///
/// Categories are grouped by semantic domain:
/// - Nullability
/// - Uniqueness
/// - Row count
/// - Structure
/// - Numeric
/// - Date / Time
/// - String
/// - Domain
/// - Statistical
/// - Relational
/// - Custom
///
/// This enum is intentionally explicit (instead of dynamic strings)
/// to improve type safety, discoverability, and AI-readability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolarsKind {
    /// Nullability constraints
    NotNull,
    NullRatioMax,

    /// Uniqueness constraints
    Unique,
    CompositeUnique,
    DuplicateRatioMax,

    /// Dataset row count constraints
    RowCountMin,
    RowCountMax,
    RowCountBetween,

    /// Column structure and schema constraints
    ColumnExists,
    ColumnMissing,
    DTypeIs,
    SchemaEquals,

    /// Numeric value constraints
    ValueMin,
    ValueMax,
    ValueBetween,
    MeanBetween,
    StdDevMax,
    SumBetween,

    /// Date and time constraints
    DateBetween,
    NoFutureDates,
    MonotonicIncreasing,
    NoGapsInSequence,

    /// String-based constraints
    RegexMatch,
    StringLengthMin,
    StringLengthMax,
    StringLengthBetween,

    /// Domain / allowed value constraints
    AllowedValues,
    ForbiddenValues,

    /// Statistical and distribution-based constraints
    OutlierRatioMax,
    PercentileBetween,

    /// Relational / cross-column constraints
    ForeignKey,
    ColumnEquals,
    ConditionalNotNull,

    /// Custom Polars expression constraint
    CustomExpr,
}
