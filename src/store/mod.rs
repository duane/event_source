pub mod sqlite;
use super::commit::{Commit, CommitAttempt};
use std::error::Error;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub enum StorageCommitConflict {
  CommitIdConflict,
  CommitSequenceConflict,
  AggregateVersionConflict,
}

#[derive(Debug, PartialEq)]
pub enum CommitErrorType {
  DuplicateCommitError(StorageCommitConflict),
  SerializationError,
  UnknownError,
}

#[derive(Debug)]
pub struct CommitError {
  pub error_type: CommitErrorType,
  pub cause: Box<Error>,
}

#[derive(Debug, PartialEq)]
pub enum QueryErrorType {
  UnknownError,
}

#[derive(Debug)]
pub struct QueryError {
  pub error_type: QueryErrorType,
  pub cause: Box<Error>,
}

pub trait Store {
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, CommitError>;
  fn get_range(&mut self, aggregate_id: i64, min_version: i64, max_version: i64) -> Result<Vec<Commit>, Box<Error>>;
  fn get_undispatched_commits(&mut self) -> Result<Vec<Commit>, Box<Error>>;
  fn mark_commit_as_dispatched(&mut self, commit_id: Uuid) -> Result<(), Box<Error>>;
  fn list_aggregate_ids(&mut self) -> Result<Vec<i64>, Box<Error>>;
  fn get_commit(&mut self, commit_it: &Uuid) -> Result<Commit, Box<Error>>;
}

impl fmt::Display for StorageCommitConflict {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      &StorageCommitConflict::CommitIdConflict => write!(f, "CommitIdConflict"),
      &StorageCommitConflict::CommitSequenceConflict => write!(f, "CommitSequenceConflict"),
      &StorageCommitConflict::AggregateVersionConflict => write!(f, "AggregateVersionConflict"),
    }
  }
}

impl fmt::Display for CommitErrorType {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      &CommitErrorType::DuplicateCommitError(ref conflict) => {
        write!(f, "DuplicateCommitError({})", conflict)
      }
      &CommitErrorType::SerializationError => write!(f, "SerializationError"),
      &CommitErrorType::UnknownError => write!(f, "UnknownError"),
    }
  }
}

impl fmt::Display for CommitError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "CommitError(error_type: {}, cause: {:?})",
      self.error_type, self.cause
    )
  }
}

impl Error for CommitError {
  fn description(&self) -> &str {
    "An error that occurs when commiting to the event store"
  }

  fn cause(&self) -> Option<&Error> {
    // TODO: figure out how to return a boxed error here.
    None
  }
}

impl CommitError {
  pub fn new(error_type: CommitErrorType, cause: Box<Error>) -> Self {
    CommitError { error_type, cause, }
  }
}

impl fmt::Display for QueryErrorType {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "UnknownError")
  }
}

impl Error for QueryError {
  fn description(&self) -> &str {
    "An error that occurs when querying the event store"
  }

  fn cause(&self) -> Option<&Error> {
    None
  }
}

impl fmt::Display for QueryError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "QueryError(error_type: {}, cause: {:?})",
      self.error_type, self.cause
    )
  }
}

impl QueryError {
  pub fn new(error_type: QueryErrorType, cause: Box<Error>) -> Self {
    QueryError { error_type, cause, }
  }
}
