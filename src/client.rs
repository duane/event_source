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

pub struct ClientBuilder<A: Aggregate, D: DispatchDelegate, S: Store> {
  store: Option<Box<S>>,
  dispatcher: Option<Dispatcher<D>>,
  aggregate: Option<Box<A>>,
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

  fn cause(&self) -> Option<&Error> {
    match self {
      &ClientError::SerializationError(ref err) => Some(err),
      &ClientError::StoreError(ref err) => Some(err as &Error),
    }
  }
}

impl<T: Error> From<JsonError> for ClientError<T> {
  fn from(error: JsonError) -> Self {
    ClientError::SerializationError(error)
  }
}

pub struct Client<A: Aggregate, D: DispatchDelegate, S: Store> {
  pub dispatcher: Dispatcher<D>,
  pub store: Box<S>,
  pub aggregate: Box<A>,
  pub commit_sequence: i64,
}

impl<A: Aggregate, D: DispatchDelegate, S: Store> ClientBuilder<A, D, S> {
  pub fn new() -> ClientBuilder<A, D, S> {
    ClientBuilder {
      store: None,
      dispatcher: None,
      aggregate: None,
    }
  }

  pub fn with_aggregate(mut self, aggregate: Box<A>) -> ClientBuilder<A, D, S> {
    self.aggregate = Some(aggregate);
    self
  }

  pub fn with_store(mut self, s: Box<S>) -> ClientBuilder<A, D, S> {
    self.store = Some(s);
    self
  }

  pub fn with_dispatch_delegate(mut self, delegate: Box<D>) -> ClientBuilder<A, D, S> {
    self.dispatcher = Some(Dispatcher::new(delegate));
    self
  }

  pub fn finish(self) -> Result<Client<A, D, S>, &'static str> {
    if self.store.is_none() {
      return Err("Cannot build a client; missing a store.");
    }
    if self.dispatcher.is_none() {
      return Err("Cannot build a client; missing a dispatcher.");
    }
    if self.aggregate.is_none() {
      return Err("Cannot build a client; missing an aggregate id.");
    }
    Ok(Client {
      store: self.store.unwrap(),
      dispatcher: self.dispatcher.unwrap(),
      aggregate: self.aggregate.unwrap(),
      commit_sequence: 0,
    })
  }
}

impl<A: Aggregate, D: DispatchDelegate, S: Store> Client<A, D, S> {
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, S::Error> {
    let commit_number = self.store.commit(commit_attempt)?;
    let _unhandled_result = self.dispatcher.dispatch(&mut *self.store);
    Ok(commit_number)
  }

  pub fn fetch_latest(&mut self) -> Result<A, ClientError<S::Error>> {
    let commits: Vec<Commit> = {
      self
        .store
        .get_range(self.aggregate.id(), self.commit_sequence, i64::max_value())
        .map_err(ClientError::StoreError)?
    };
    for commit in commits.into_iter() {
      let mut deserializer = JsonDeserializer::from_slice(commit.serialized_events.as_slice());
      let events = Vec::<A::Event>::deserialize(&mut deserializer)?;
      for event in events.iter() {
        self.aggregate = self.aggregate.apply(event);
      }
      self.commit_sequence = commit.commit_sequence;
    }
    Ok((*self.aggregate).clone())
  }

  pub fn issue_command<C: Command<Aggregate = A>, M: Serialize>(
    &mut self,
    aggregate: &mut A,
    command: &C,
    metadata: &M,
  ) -> Result<Commit, Either<ClientError<S::Error>, C::Error>> {
    let aggregate_update_events: Vec<A::Event> = command.apply(aggregate).map_err(Either::Right)?;
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
      aggregate_id: self.aggregate.id(),
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
    fn apply(&self, _event: &Self::Event) -> Box<MockAggregate> {
      Box::new(MockAggregate {
        id: self.id,
        version: self.version + 1,
      })
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
    assert!(
      ClientBuilder::<MockAggregate, MockDispatcher, SqliteStore>::new()
        .finish()
        .is_err()
    );
    let dispatch_delegate = MockDispatcher {
      dispatched_id: None,
    };
    assert_eq!(
      "Cannot build a client; missing a store.",
      ClientBuilder::<MockAggregate, MockDispatcher, SqliteStore>::new()
        .with_dispatch_delegate(Box::new(dispatch_delegate))
        .finish()
        .err()
        .unwrap()
    );
    assert_eq!(
      "Cannot build a client; missing a dispatcher.",
      ClientBuilder::<MockAggregate, MockDispatcher, SqliteStore>::new()
        .with_store(Box::new(SqliteStore::with_new_in_memory_connection()))
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
    let mut client = ClientBuilder::<MockAggregate, MockDispatcher, SqliteStore>::new()
      .with_store(Box::new(SqliteStore::with_new_in_memory_connection()))
      .with_dispatch_delegate(Box::new(dispatch_delegate))
      .with_aggregate(Default::default())
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
