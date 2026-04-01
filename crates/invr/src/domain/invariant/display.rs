use crate::invariant::Invariant;

impl<K: std::fmt::Display> std::fmt::Display for Invariant<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} [{}] on {}",
            self.id.as_str(),
            self.kind(),
            self.scope
        )
    }
}
