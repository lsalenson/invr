use crate::domain::scope::Scope;
use crate::infrastructure::polars::kind::PolarsKind;
use crate::interface::yaml::dto::invariant::InvariantYaml;
use crate::interface::yaml::dto::kind::polars::PolarsKindYaml;
use crate::interface::yaml::dto::spec::SpecYaml;
use crate::invariant::error::InvariantError;
use crate::prelude::{Invariant, InvariantId};
use crate::spec::Spec;
use crate::spec::error::SpecError;

impl TryFrom<PolarsKindYaml> for PolarsKind {
    type Error = InvariantError;

    fn try_from(yaml: PolarsKindYaml) -> Result<Self, Self::Error> {
        Ok(match yaml {
            PolarsKindYaml::NotNull => PolarsKind::NotNull,
            PolarsKindYaml::NullRatioMax => PolarsKind::NullRatioMax,

            PolarsKindYaml::Unique => PolarsKind::Unique,
            PolarsKindYaml::CompositeUnique => PolarsKind::CompositeUnique,
            PolarsKindYaml::DuplicateRatioMax => PolarsKind::DuplicateRatioMax,

            PolarsKindYaml::RowCountMin => PolarsKind::RowCountMin,
            PolarsKindYaml::RowCountMax => PolarsKind::RowCountMax,
            PolarsKindYaml::RowCountBetween => PolarsKind::RowCountBetween,

            PolarsKindYaml::ColumnExists => PolarsKind::ColumnExists,
            PolarsKindYaml::ColumnMissing => PolarsKind::ColumnMissing,
            PolarsKindYaml::DTypeIs => PolarsKind::DTypeIs,
            PolarsKindYaml::SchemaEquals => PolarsKind::SchemaEquals,

            PolarsKindYaml::ValueMin => PolarsKind::ValueMin,
            PolarsKindYaml::ValueMax => PolarsKind::ValueMax,
            PolarsKindYaml::ValueBetween => PolarsKind::ValueBetween,
            PolarsKindYaml::MeanBetween => PolarsKind::MeanBetween,
            PolarsKindYaml::StdDevMax => PolarsKind::StdDevMax,
            PolarsKindYaml::SumBetween => PolarsKind::SumBetween,

            PolarsKindYaml::DateBetween => PolarsKind::DateBetween,
            PolarsKindYaml::NoFutureDates => PolarsKind::NoFutureDates,
            PolarsKindYaml::MonotonicIncreasing => PolarsKind::MonotonicIncreasing,
            PolarsKindYaml::NoGapsInSequence => PolarsKind::NoGapsInSequence,

            PolarsKindYaml::RegexMatch => PolarsKind::RegexMatch,
            PolarsKindYaml::StringLengthMin => PolarsKind::StringLengthMin,
            PolarsKindYaml::StringLengthMax => PolarsKind::StringLengthMax,
            PolarsKindYaml::StringLengthBetween => PolarsKind::StringLengthBetween,

            PolarsKindYaml::AllowedValues => PolarsKind::AllowedValues,
            PolarsKindYaml::ForbiddenValues => PolarsKind::ForbiddenValues,

            PolarsKindYaml::OutlierRatioMax => PolarsKind::OutlierRatioMax,
            PolarsKindYaml::PercentileBetween => PolarsKind::PercentileBetween,

            PolarsKindYaml::ForeignKey => PolarsKind::ForeignKey,
            PolarsKindYaml::ColumnEquals => PolarsKind::ColumnEquals,
            PolarsKindYaml::ConditionalNotNull => PolarsKind::ConditionalNotNull,

            PolarsKindYaml::CustomExpr => PolarsKind::CustomExpr,
        })
    }
}
impl TryFrom<InvariantYaml<PolarsKindYaml>> for Invariant<PolarsKind> {
    type Error = InvariantError;

    fn try_from(value: InvariantYaml<PolarsKindYaml>) -> Result<Self, Self::Error> {
        Ok(Invariant::new(
            InvariantId::new(value.id)?,
            PolarsKind::try_from(value.kind)?,
            Scope::try_from(value.scope)?,
        )
        .with_severity(value.severity.into())
        .with_params(value.params))
    }
}

impl TryFrom<SpecYaml<PolarsKindYaml>> for Spec<PolarsKind> {
    type Error = SpecError;
    fn try_from(value: SpecYaml<PolarsKindYaml>) -> Result<Self, Self::Error> {
        let invariants = value
            .invariants
            .into_iter()
            .enumerate()
            .map(|(index, yaml)| {
                Invariant::try_from(yaml)
                    .map_err(|error| SpecError::invalid_invariant(index, error))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let spec = Spec::from_invariants(invariants);
        spec.validate()?;
        Ok(spec)
    }
}
