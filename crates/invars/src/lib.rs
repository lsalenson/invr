//! # Invars
//!
//! Invars is a declarative data validation engine for Rust.
//!
//! It allows you to define **invariants** (validation rules) and evaluate them
//! against a dataset using an execution engine (currently Polars).
//!
//! ---
//!
//! ## Core Concepts
//!
//! - **Invariant**: A validation rule (e.g. "column must be unique").
//! - **Scope**: Where the rule applies (`Dataset` or `Column`).
//! - **Engine**: Executes invariants against data.
//! - **Violation**: Returned when an invariant fails.
//!
//! Execution model:
//!
//! ```text
//! Invariant
//!    ↓
//! plan() -> Metric Expression
//!    ↓
//! Engine execution (Polars LazyFrame)
//!    ↓
//! map() -> Option<Violation>
//! ```
//!
//!
//! ---
//!
//! ## Feature Flags
//!
//! - `polars` — Enables the Polars execution engine.
//!
//! ## Design Goals
//!
//! - Declarative validation rules
//! - Engine-agnostic core domain
//! - Lazy execution support
//! - Explicit metric-to-violation mapping
//! - Fully testable invariant units
//!
//! Invars is designed to be predictable, extensible, and AI-friendly.

mod application;
mod domain;

#[cfg(feature = "polars")]
pub mod infrastructure;
pub mod interface;

pub use crate::application::*;
pub use crate::domain::*;

pub mod prelude {
    pub use crate::domain::{
        invariant::Invariant, invariant::value_object::id::InvariantId, scope::Scope,
        severity::Severity, spec::Spec,
    };

    pub use crate::use_cases::run_spec::RunSpec;

    #[cfg(feature = "polars")]
    pub use crate::infrastructure::polars::EnginePolarsDataFrame;
    #[cfg(feature = "polars")]
    pub use crate::infrastructure::polars::kind::PolarsKind;
}
