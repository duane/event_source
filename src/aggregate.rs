use super::events::Event;
use std::default::Default;
use uuid::Uuid;
/// A data type for working with the result of events over time.
///
/// This is generally used for working with the internal logic of Commands.
pub trait Aggregate: Default + Clone + Sized {
  /// The event type producing this aggregate type.
  type Event: Event;

  /// Initialize the aggregate with a unique identifier.
  fn with_id(id: Uuid) -> Self;

  /// Apply an event to this aggregate to produce the new, resulting aggregate.
  fn apply(&self, event: &Self::Event) -> Self;

  /// Get the current version of the aggregate.
  ///
  /// This is an increasing number. Higher numbers are more recent aggregates.
  fn version(&self) -> i64;

  /// Get the unique identifier of the aggregate.
  fn id(&self) -> Uuid;
}
