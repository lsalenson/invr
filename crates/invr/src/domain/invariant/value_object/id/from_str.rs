use crate::invariant::value_object::id::InvariantId;
use crate::invariant::value_object::id::error::InvariantIdError;
use std::str::FromStr;

impl FromStr for InvariantId {
    type Err = InvariantIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}
