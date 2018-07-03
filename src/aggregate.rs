use super::events::Event;
use std::default::Default;

pub trait Aggregate: Default + Clone {
  type Event: Event;
  fn apply(&self, event: &Self::Event) -> Box<Self>;
  fn version(&self) -> i64;
  fn id(&self) -> i64;
}
