use crate::aggregate::Aggregate;
use crate::command::Command;
use crate::store::{Store, StoreError};
use crate::client::applicable::EventApplicable;
use crate::client::ClientError;
use crate::client::write_client::WriteClient;
use crate::commit::{CommitAttempt, Commit};
use crate::dispatch::{Dispatcher, DispatchDelegate};

use either::Either;
use serde::Serialize;
use serde_json::Error as JsonError;
use serde_json::Serializer as JsonSerializer;
use uuid::Uuid;
use chrono::Utc;

/// A write client for the store.
pub struct AggregateStream<C: Command, S: Store, D: DispatchDelegate> {
  aggregate_id: Uuid,
  store: S,
  dispatcher: Dispatcher<D>,
  aggregate: C::Aggregate,
  commit_sequence: i64
}

impl<C: Command, S: Store, D: DispatchDelegate> AggregateStream<C, S, D> {
  /// Produce a new, empty aggregate from a store, dispatcher, and aggregate.
  pub fn new(aggregate_id: Uuid, store: S, delegate: D) -> Self {
    AggregateStream{
      aggregate_id,
      store,
      dispatcher: Dispatcher::new(delegate),
      aggregate: Default::default(),
      commit_sequence: 0
    }
  }

  /// Fetch and apply the events producing the aggregate from the stream.
  pub fn sync(&mut self) -> Result<(), ClientError>  {
    let commits: Vec<Commit> = {
      self
        .store
        .get_range(self.aggregate_id, self.commit_sequence, i64::max_value())
        .map_err(ClientError::StoreError)?
    };
    for commit in commits {
      let events: Vec<<C::Aggregate as EventApplicable>::Event> = serde_json::from_value(commit.deserialize().events).unwrap();
      for event in events {
        self.aggregate = self.aggregate.apply(&event);
      }
      self.commit_sequence = commit.commit_sequence;
    }
    Ok(())
  }

  /// Write a commit to the store, unconditionally.
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, Box<dyn StoreError>> {
    let commit_number = self.store.commit(commit_attempt)?;
    let _unhandled_result = self.dispatcher.dispatch(&mut self.store);
    Ok(commit_number)
  }

  /// Issue a command to the store.
  ///
  /// The command may fail for semantic or store reasons.
  /// You may store the command itself in the metadata.
  pub fn issue_command<M: Serialize + core::fmt::Debug>(
    &mut self,
    command: &C,
    metadata: &M,
  ) -> Result<Commit, Either<ClientError, C::Error>> {
    // println!("issuing command: {:#?}\nwith metadata: {:#?}\non aggregate: {:#?}", command, metadata, self.aggregate);
    let aggregate_update_events: Vec<<<C as Command>::Aggregate as EventApplicable>::Event> =
      command.apply(&self.aggregate).map_err(Either::Right)?;
    let mut events_buffer = Vec::<u8>::new();
    let mut metadata_buffer = Vec::<u8>::new();
    let events_count = aggregate_update_events.len() as i64;

    {
      let mut events_serializer = JsonSerializer::new(&mut events_buffer);
      aggregate_update_events
        .serialize(&mut events_serializer)
        .map_err(ClientError::SerializationError)
        .map_err(Either::Left)?;
    }

    {
      let mut metadata_serializer = JsonSerializer::new(&mut metadata_buffer);
      metadata
        .serialize(&mut metadata_serializer)
        .map_err(ClientError::SerializationError)
        .map_err(Either::Left)?;
    }

    let commit_attempt = CommitAttempt {
      aggregate_id: self.aggregate_id,
      aggregate_version: self.aggregate.version(),
      commit_id: Uuid::new_v4(),
      commit_timestamp: Utc::now(),
      commit_sequence: self.commit_sequence + 1,
      serialized_metadata: metadata_buffer,
      serialized_events: events_buffer,
      events_count,
    };
    self
      .commit(&commit_attempt)
      .and_then(|_| self.store.get_commit(&commit_attempt.commit_id))
      .and_then(|commit| {
        self.commit_sequence = commit.commit_sequence;
        for ref event in aggregate_update_events {
          self.aggregate = self.aggregate.apply(event);
        }
        Ok(commit)
      })
      .map_err(ClientError::StoreError)
      .map_err(Either::Left)
  }
}

