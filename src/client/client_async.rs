use crate::aggregate::Aggregate;
use crate::client::applicable::EventApplicable;
use crate::command::Command;
use crate::commit::*;
use crate::dispatch_async::{Dispatcher, AsyncDispatcher};
use crate::store::*;
use chrono::Utc;
use either::Either;
use serde::Serialize;
use serde_json::Error as JsonError;
use uuid::Uuid;

/// Error that may be returned from client.
#[derive(Debug)]
pub enum ClientError {
  /// An error occured with serialization or deserialization.
  SerializationError(JsonError),

  /// A store error occurred.
  StoreError(Box<dyn StoreError>),
}

#[derive(Debug)]
/// A type wrapping an internal JsonError.
pub struct ClientJsonError {
  /// The internal error.
  pub cause: JsonError,
}

impl From<JsonError> for ClientError {
  fn from(error: JsonError) -> ClientError {
    ClientError::SerializationError(error)
  }
}

impl From<Box<dyn StoreError>> for ClientError {
  fn from(error: Box<dyn StoreError>) -> ClientError {
    ClientError::StoreError(error)
  }
}

/// A read/write client for interfacing with a store and event schema.
pub struct AsyncClient {
  /// State for processing events after commit.
  pub dispatcher: AsyncDispatcher,

  /// The store holding events.
  pub store: Box<dyn Store>,

  /// The latest seen commit id.
  pub commit_sequence: i64,
}

impl AsyncClient {
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, Box<dyn StoreError>> {
    let commit_number = self.store.as_mut().commit(commit_attempt)?;
    self.dispatcher.dispatch().expect("dispatch failed");
    Ok(commit_number)
  }

  /// Replay commits for the given aggregate id into a domain type.
  pub fn fetch_latest<A: Aggregate>(&mut self, aggregate_id: Uuid) -> Result<A, ClientError> {
    let commits: Vec<Commit> = {
      self.store.as_mut()
        .get_aggregate_range(aggregate_id, 0, i64::max_value())
        .map_err(ClientError::StoreError)?
    };
    let mut aggregate: A = Default::default();
    for commit in commits {
      let events: Vec<<A as EventApplicable>::Event> =
        serde_json::from_value(commit.events).unwrap();
      for event in events {
        aggregate = aggregate.apply(&event);
      }
      self.commit_sequence = commit.commit_sequence;
    }
    Ok(aggregate)
  }

  /// Issue a command to the store.
  ///
  /// The command may fail for semantic or store reasons.
  /// You may store the command itself in the metadata.
  pub fn issue_command<C: Command, M: Serialize>(
    &mut self,
    aggregate: &C::Aggregate,
    command: &C,
    metadata: &M,
  ) -> Result<Commit, Either<ClientError, C::Error>> {
    let aggregate_update_events: Vec<<<C as Command>::Aggregate as EventApplicable>::Event> =
      command.apply(aggregate).map_err(Either::Right)?;
    let events_count = aggregate_update_events.len() as i64;

    let commit_attempt = CommitAttempt {
      aggregate_id: aggregate.id(),
      aggregate_version: aggregate.version(),
      commit_id: Uuid::new_v4(),
      commit_timestamp: Utc::now(),
      commit_sequence: self.commit_sequence + 1,
      metadata: serde_json::to_value(metadata).expect("could not serialize metadata"),
      events: serde_json::to_value(aggregate_update_events).expect("could not serialize events"),
      events_count,
    };
    self
      .commit(&commit_attempt)
      .and_then(|_| self.store.as_mut().get_commit(&commit_attempt.commit_id))
      .map_err(ClientError::StoreError)
      .map_err(Either::Left)
  }

  /// like above but async
  pub async fn issue_command_async<C: Command, M: Serialize>(
    &mut self,
    aggregate: &C::Aggregate,
    command: &C,
    metadata: &M,
  ) -> Result<Commit, Either<ClientError, C::Error>> {
    let aggregate_update_events: Vec<<<C as Command>::Aggregate as EventApplicable>::Event> =
      command
        .async_apply(aggregate)
        .await
        .map_err(Either::Right)?;
    let events_count = aggregate_update_events.len() as i64;

    let commit_attempt = CommitAttempt {
      aggregate_id: aggregate.id(),
      aggregate_version: aggregate.version(),
      commit_id: Uuid::new_v4(),
      commit_timestamp: Utc::now(),
      commit_sequence: self.commit_sequence + 1,
      metadata: serde_json::to_value(metadata).expect("could not serialize metadata"),
      events: serde_json::to_value(aggregate_update_events).expect("could not serialize events"),
      events_count,
    };
    self
      .commit(&commit_attempt)
      .and_then(|_| self.store.as_mut().get_commit(&commit_attempt.commit_id))
      .map_err(ClientError::StoreError)
      .map_err(Either::Left)
  }
}