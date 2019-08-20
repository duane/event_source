use aggregate::Aggregate;
use chrono::Utc;
use command::Command;
use commit::*;
use dispatch::*;
use either::Either;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Deserializer as JsonDeserializer;
use serde_json::Error as JsonError;
use serde_json::Serializer as JsonSerializer;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use store::*;
use uuid::Uuid;

pub struct ClientBuilder<D: DispatchDelegate, S: Store> {
  store: Option<S>,
  dispatcher: Option<Dispatcher<D>>,
}

#[derive(Debug)]
pub enum ClientError<Se: Error> {
  SerializationError(JsonError),
  StoreError(Se),
}

impl<Se: Error> Display for ClientError<Se> {
  fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
    write!(fmt, "{:?}", self)
  }
}

impl<'a, T: Error> Error for ClientError<T> {
  fn description(&self) -> &'static str {
    "This error type represents any error that can come from a client function."
  }

  fn cause(&self) -> Option<&dyn Error> {
    match *self {
      ClientError::SerializationError(ref err) => Some(err),
      ClientError::StoreError(ref err) => Some(err as &dyn Error),
    }
  }
}

impl<T: Error> From<JsonError> for ClientError<T> {
  fn from(error: JsonError) -> Self {
    ClientError::SerializationError(error)
  }
}

pub struct Client<D: DispatchDelegate, S: Store> {
  pub dispatcher: Dispatcher<D>,
  pub store: S,
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
  pub fn with_store(mut self, s: S) -> ClientBuilder<D, S> {
    self.store = Some(s);
    self
  }

  pub fn with_dispatch_delegate(mut self, delegate: D) -> ClientBuilder<D, S> {
    self.dispatcher = Some(Dispatcher::new(delegate));
    self
  }

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
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, S::Error> {
    let commit_number = self.store.commit(commit_attempt)?;
    let _unhandled_result = self.dispatcher.dispatch(&mut self.store);
    Ok(commit_number)
  }

  pub fn fetch_latest<A: Aggregate>(
    &mut self,
    aggregate_id: Uuid,
  ) -> Result<A, ClientError<S::Error>> {
    let commits: Vec<Commit> = {
      self
        .store
        .get_range(aggregate_id, self.commit_sequence, i64::max_value())
        .map_err(ClientError::StoreError)?
    };
    let mut aggregate: A = Default::default();
    for commit in commits {
      let mut deserializer = JsonDeserializer::from_slice(commit.serialized_events.as_slice());
      let events = Vec::<A::Event>::deserialize(&mut deserializer)?;
      for event in events {
        aggregate = aggregate.apply(&event);
      }
      self.commit_sequence = commit.commit_sequence;
    }
    Ok(aggregate)
  }

  pub fn issue_command<C: Command, M: Serialize>(
    &mut self,
    aggregate: &C::Aggregate,
    command: &C,
    metadata: &M,
  ) -> Result<Commit, Either<ClientError<S::Error>, C::Error>> {
    let aggregate_update_events: Vec<<<C as Command>::Aggregate as Aggregate>::Event> =
      command.apply(aggregate).map_err(Either::Right)?;
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
      aggregate_id: aggregate.id(),
      aggregate_version: aggregate.version(),
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
      .map_err(ClientError::StoreError)
      .map_err(Either::Left)
  }
}

#[cfg(test)]
mod tests {
  use super::super::events::Event;
  use super::super::store::sqlite::SqliteStore;
  use super::*;
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

  #[derive(Default, Clone)]
  struct MockAggregate {
    id: Uuid,
    version: i64,
  }

  impl Aggregate for MockAggregate {
    type Event = MockEvent;

    fn with_id(id: Uuid) -> Self {
      MockAggregate { id, version: 0 }
    }

    fn apply(&self, _event: &Self::Event) -> MockAggregate {
      MockAggregate {
        id: self.id,
        version: self.version + 1,
      }
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
