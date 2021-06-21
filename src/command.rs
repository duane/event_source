use super::aggregate::Aggregate;
use std::error::Error;
use std::fmt::Debug;
use async_trait::async_trait;
use uuid::Uuid;
use crate::client::applicable::EventApplicable;
use serde::Serialize;
use serde::de::DeserializeOwned;

#[async_trait]
/// A single mutation to the event store.
pub trait Command: Serialize + DeserializeOwned + Send + Sync + Clone + Debug {
  /// The aggregate type to mutate.
  type Aggregate: Aggregate;

  /// The event type. THe command is a series of events.
  type Error: Error + Serialize + DeserializeOwned;

  /// Return the id of the aggregate that this command is mutating.
  fn id(&self) -> &Uuid;

  /// Produce a new aggregate with the command applied.
  fn apply(
    &self,
    aggregate: &Self::Aggregate,
  ) -> Result<Vec<<<Self as Command>::Aggregate as EventApplicable>::Event>, Self::Error>;

  /// Produce a new aggregate with the command applied.
  async fn async_apply(
    &self,
    aggregate: &Self::Aggregate,
  ) -> Result<Vec<<<Self as Command>::Aggregate as EventApplicable>::Event>, Self::Error>;
}
