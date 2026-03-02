//! # Invars
//!
//! Invars is a data validation framework for Rust.
//!
//! It allows you to declare invariants on datasets and validate them
//! against a Polars DataFrame or LazyFrame.

mod application;
mod domain;

#[cfg(feature = "polars")]
pub mod infrastructure;

pub use crate::application::*;
pub use crate::domain::*;

pub mod prelude {
    pub use crate::domain::{
        invariant::Invariant, invariant::value_object::id::InvariantId, scope::Scope,
        severity::Severity, spec::Spec,
    };

    pub use crate::use_case::RunSpec;

    #[cfg(feature = "polars")]
    pub use crate::infrastructure::polars::EnginePolarsDataFrame;
    #[cfg(feature = "polars")]
    pub use crate::infrastructure::polars::kind::PolarsKind;
}
