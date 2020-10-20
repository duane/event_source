use super::aggregate::Aggregate;
use std::error::Error;
use std::fmt::Debug;

/// A single mutation to the event store.
pub trait Command: Send + Sync + Clone + Debug {
  /// The aggregate type to mutate.
  type Aggregate: Aggregate;

  /// The event type. THe command is a series of events.
  type Error: Error;

  /// Produce an new aggregate with the command applied.
  fn apply(
    &self,
    aggregate: &Self::Aggregate,
  ) -> Result<Vec<<<Self as Command>::Aggregate as Aggregate>::Event>, Self::Error>;
}
