use crate::aggregate::Aggregate;
use chrono::Utc;
use crate::command::Command;
use crate::commit::*;
use crate::dispatch::*;
use either::Either;
use serde::Serialize;
use serde_json::Error as JsonError;
use serde_json::Serializer as JsonSerializer;
use crate::store::*;
use uuid::Uuid;
use crate::client::applicable::EventApplicable;

/// The applicable data type.
pub mod applicable;

/// safe write client
pub mod aggregate_stream;

/// generic sync projection
pub mod sync_streamable;

/// event source
pub mod event_source;

/// command processor
pub mod command_processor;

/// The HTTP-backed store
#[cfg(feature = "httpc")]
pub mod http;

/// State used in construction of client.
pub struct ClientBuilder<D: DispatchDelegate, S: Store> {
  store: Option<S>,
  dispatcher: Option<Dispatcher<D>>,
}

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
pub struct Client<D: DispatchDelegate, S: Store> {
  /// State for processing events after commit.
  pub dispatcher: Dispatcher<D>,

  /// The store holding events.
  pub store: S,

  /// The latest seen commit id.
  pub commit_sequence: i64,
}

impl<D: DispatchDelegate, S: Store> Default for ClientBuilder<D, S> {
  fn default() -> ClientBuilder<D, S> {
    ClientBuilder {
      dispatcher: None,
      store: None,
    }
  }
}

impl<D: DispatchDelegate, S: Store> ClientBuilder<D, S> {
  /// Configure the client to talk to a given store.
  pub fn with_store(mut self, s: S) -> ClientBuilder<D, S> {
    self.store = Some(s);
    self
  }

  /// Configure the client to use a delegate to process new commits.
  pub fn with_dispatch_delegate(mut self, delegate: D) -> ClientBuilder<D, S> {
    self.dispatcher = Some(Dispatcher::new(delegate));
    self
  }

  /// Produce a new client with the given store and delegate.
  pub fn finish(self) -> Result<Client<D, S>, &'static str> {
    if self.store.is_none() {
      return Err("Cannot build a client; missing a store.");
    }
    if self.dispatcher.is_none() {
      return Err("Cannot build a client; missing a dispatcher.");
    }
    Ok(Client {
      store: self.store.unwrap(),
      dispatcher: self.dispatcher.unwrap(),
      commit_sequence: 0,
    })
  }
}

impl<D: DispatchDelegate, S: Store> Client<D, S> {
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, Box<dyn StoreError>> {
    let commit_number = self.store.commit(commit_attempt)?;
    let _unhandled_result = self.dispatcher.dispatch(&mut self.store);
    Ok(commit_number)
  }

  /// Replay commits for the given aggregate id into a domain type.
  pub fn fetch_latest<A: Aggregate>(
    &mut self,
    aggregate_id: Uuid,
  ) -> Result<A, ClientError> {
    let commits: Vec<DeserializedCommit> = {
      self
        .store
        .get_range(aggregate_id, 0, i64::max_value())
        .map_err(ClientError::StoreError)?
    };
    let mut aggregate: A = Default::default();
    for commit in commits {
      let events: Vec<<A as EventApplicable>::Event> = serde_json::from_value(commit.events).unwrap();
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
  ) -> Result<DeserializedCommit, Either<ClientError, C::Error>> {
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
      .and_then(|_| self.store.get_commit(&commit_attempt.commit_id))
      .map_err(ClientError::StoreError)
      .map_err(Either::Left)
  }

  /// like above but async
  pub async fn issue_command_async<C: Command, M: Serialize>(
    &mut self,
    aggregate: &C::Aggregate,
    command: &C,
    metadata: &M,
  ) -> Result<DeserializedCommit, Either<ClientError, C::Error>> {
    let aggregate_update_events: Vec<<<C as Command>::Aggregate as EventApplicable>::Event> =
      command.async_apply(aggregate).await.map_err(Either::Right)?;
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
        .and_then(|_| self.store.get_commit(&commit_attempt.commit_id))
        .map_err(ClientError::StoreError)
        .map_err(Either::Left)
  }
}

#[cfg(test)]
mod tests {
  use super::super::events::Event;
  use super::super::store::sqlite::SqliteStore;
  use super::*;
  use crate::client::applicable::EventApplicable;
  use chrono::Utc;
  use std::default::Default;
  use uuid::Uuid;

  struct MockDispatcher {
    dispatched_id: Option<Uuid>,
  }

  impl DispatchDelegate for MockDispatcher {
    fn dispatch(&mut self, commit: &Commit) -> Result<(), String> {
      self.dispatched_id = Some(commit.commit_id);
      Ok(())
    }
  }

  #[derive(Serialize, Deserialize, Debug)]
  enum MockEvent {
    IncrementVersion,
  }

  impl Event for MockEvent {}

  #[derive(Default, Clone, Debug)]
  struct MockAggregate {
    id: Uuid,
    version: i64,
  }

  impl EventApplicable for MockAggregate {
    type Event = MockEvent;

    fn apply(&self, _event: &Self::Event) -> MockAggregate {
      MockAggregate {
        id: self.id,
        version: self.version + 1,
      }
    }
  }

  impl Aggregate for MockAggregate {
    fn with_id(id: Uuid) -> Self {
      MockAggregate { id, version: 0 }
    }

    fn version(&self) -> i64 {
      self.version
    }

    fn id(&self) -> Uuid {
      self.id
    }
  }

  #[test]
  fn it_requires_store_and_dispatcher() {
    assert!(ClientBuilder::<MockDispatcher, SqliteStore>::default()
      .finish()
      .is_err());
    let dispatch_delegate = MockDispatcher {
      dispatched_id: None,
    };
    assert_eq!(
      "Cannot build a client; missing a store.",
      ClientBuilder::<MockDispatcher, SqliteStore>::default()
        .with_dispatch_delegate(dispatch_delegate)
        .finish()
        .err()
        .unwrap()
    );
    assert_eq!(
      "Cannot build a client; missing a dispatcher.",
      ClientBuilder::<MockDispatcher, SqliteStore>::default()
        .with_store(SqliteStore::with_new_in_memory_connection())
        .finish()
        .err()
        .unwrap()
    );
  }

  #[test]
  fn it_dispatches() {
    let dispatch_delegate = MockDispatcher {
      dispatched_id: None,
    };
    let store = SqliteStore::with_new_in_memory_connection();
    store.initialize();
    let mut client = ClientBuilder::<MockDispatcher, SqliteStore>::default()
      .with_store(store)
      .with_dispatch_delegate(dispatch_delegate)
      .finish()
      .unwrap();
    let commit_id = Uuid::new_v4();
    let commit_attempt = CommitAttempt {
      aggregate_id: Uuid::new_v4(),
      aggregate_version: 0,
      commit_id,
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_metadata: String::from("\"metadata\"").into_bytes(),
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert!(client.commit(&commit_attempt).is_ok());
    assert_eq!(
      Some(commit_id),
      client.dispatcher.dispatch_delegate.dispatched_id
    );
  }
}
