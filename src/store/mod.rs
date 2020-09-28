//pub mod dynamodb;
pub mod sqlite;
use super::commit::{Commit, CommitAttempt};
use std::error;
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

pub trait StoreError: error::Error {
  fn error_type(&self) -> StoreErrorType;
}

pub trait Store: Sized {
  type Connection;

  fn with_connection(connection: Self::Connection) -> Self;
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, Box<dyn StoreError>>;
  fn get_range(
    &self,
    aggregate_id: Uuid,
    min_version: i64,
    max_version: i64,
  ) -> Result<Vec<Commit>, Box<dyn StoreError>>;
  fn get_undispatched_commits(&mut self) -> Result<Vec<Commit>, Box<dyn StoreError>>;
  fn mark_commit_as_dispatched(&mut self, commit_id: Uuid) -> Result<(), Box<dyn StoreError>>;
  fn get_commit(&mut self, commit_it: &Uuid) -> Result<Commit, Box<dyn StoreError>>;
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
