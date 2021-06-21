use crate::events::Event;

/// A datatype that may be mutated with an event.
pub trait EventApplicable {
  /// The event type used to mutate this data.
  type Event: Event;

  /// The function to produce a new version of this data type with the event applied.
  fn apply(self: &Self, event: &Self::Event) -> Self;
}
