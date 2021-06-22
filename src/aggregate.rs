use crate::client::applicable::EventApplicable;
use std::default::Default;
use uuid::Uuid;
/// A data type for working with the result of events over time.
///
/// This is generally used for working with the internal logic of Commands.
pub trait Aggregate: EventApplicable + Default + Clone + Sized + core::fmt::Debug {
  /// Initialize the aggregate with a unique identifier.
  fn with_id(id: Uuid) -> Self;

  /// Get the current version of the aggregate.
  ///
  /// This is an increasing number. Higher numbers are more recent aggregates.
  fn version(&self) -> i64;

  /// Get the unique identifier of the aggregate.
  fn id(&self) -> Uuid;
}
