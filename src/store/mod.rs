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
pub enum StoreErrorType {
  DuplicateWriteError(StorageCommitConflict),
  UnknownError,
}

#[derive(Debug)]
pub struct StoreError<E: Error> {
  pub error_type: StoreErrorType,
  pub cause: E,
}

pub trait Store {
  type UnderlyingStoreError: Error;
  type Error: Error = StoreError<Self::UnderlyingStoreError>;

  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, Self::Error>;
  fn get_range(
    &mut self,
    aggregate_id: Uuid,
    min_version: i64,
    max_version: i64,
  ) -> Result<Vec<Commit>, Self::Error>;
  fn get_undispatched_commits(&mut self) -> Result<Vec<Commit>, Self::Error>;
  fn mark_commit_as_dispatched(&mut self, commit_id: Uuid) -> Result<(), Self::Error>;
  fn get_commit(&mut self, commit_it: &Uuid) -> Result<Commit, Self::Error>;
}

impl fmt::Display for StorageCommitConflict {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      StorageCommitConflict::CommitIdConflict => write!(f, "CommitIdConflict"),
      StorageCommitConflict::CommitSequenceConflict => write!(f, "CommitSequenceConflict"),
      StorageCommitConflict::AggregateVersionConflict => write!(f, "AggregateVersionConflict"),
    }
  }
}

impl fmt::Display for StoreErrorType {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      StoreErrorType::DuplicateWriteError(ref conflict) => {
        write!(f, "DuplicateWriteError({})", conflict)
      }
      StoreErrorType::UnknownError => write!(f, "UnknownError"),
    }
  }
}

impl<E: Error> fmt::Display for StoreError<E> {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "StoreError(error_type: {}, cause: {:?})",
      self.error_type, self.cause
    )
  }
}

impl<E: Error> Error for StoreError<E> {
  fn description(&self) -> &str {
    "An error that occurs when interacting with the persistence layer of the event store."
  }

  fn cause(&self) -> Option<&Error> {
    Some(&self.cause)
  }
}

impl<E: Error> StoreError<E> {
  pub fn new(error_type: StoreErrorType, cause: E) -> Self {
    StoreError { error_type, cause }
  }
}
