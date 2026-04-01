use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolarsKindYaml {
    NotNull,
    NullRatioMax,

    Unique,
    CompositeUnique,
    DuplicateRatioMax,

    RowCountMin,
    RowCountMax,
    RowCountBetween,

    ColumnExists,
    ColumnMissing,
    DTypeIs,
    SchemaEquals,

    ValueMin,
    ValueMax,
    ValueBetween,
    MeanBetween,
    StdDevMax,
    SumBetween,

    DateBetween,
    NoFutureDates,
    MonotonicIncreasing,
    NoGapsInSequence,

    RegexMatch,
    StringLengthMin,
    StringLengthMax,
    StringLengthBetween,

    AllowedValues,
    ForbiddenValues,

    OutlierRatioMax,
    PercentileBetween,

    ForeignKey,
    ColumnEquals,
    ConditionalNotNull,

    CustomExpr,
}
