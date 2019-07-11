use super::events::Event;
use std::default::Default;
use uuid::Uuid;

pub trait Aggregate: Default + Clone + Sized {
  type Event: Event;
  fn with_id(id: Uuid) -> Self;
  fn apply(&self, event: &Self::Event) -> Self;
  fn version(&self) -> i64;
  fn id(&self) -> Uuid;
}
