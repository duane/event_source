#[cfg(feature = "dynamo")]
pub mod dynamodb;

/// The sqlite store
#[cfg(feature = "sqlite")]
pub mod sqlite;

/// The postgres store
#[cfg(feature = "psql")]
pub mod postgres;

use super::commit::{Commit, CommitAttempt};
use std::error;
use std::fmt;
use uuid::Uuid;

/// A conflict between a proposed commit and an existing commit.
#[derive(Debug, PartialEq)]
pub enum StorageCommitConflict {
  /// The commit id proposed is already in the store.
  CommitIdConflict,

  /// The commit sequence is in the past.
  CommitSequenceConflict,

  /// This aggregate version has already been committed.
  AggregateVersionConflict,
}

#[derive(Debug, PartialEq)]
/// A store error.
pub enum StoreErrorType {
  /// This write is invalid and should be reconstructed against a more recent version of the store.
  DuplicateWriteError(StorageCommitConflict),

  /// Unknown error.
  UnknownError,
}

/// A trait allowing querying store-specific error type.
pub trait StoreError: error::Error {
  /// Query the error for which type of store error this represents.
  fn error_type(&self) -> StoreErrorType;
}

/// A trait allowing committing and retrieving, but not dispatching.
pub trait ClientStore: Sized {
  /// Write a commit to a store.
  ///
  /// If the commit succeeds, this will return an immutable commit representing a set of events that have occurred.
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, Box<dyn StoreError>>;

  /// Get a sequence of events specific to the aggregate id.
  ///
  /// Because the minimum and maximum bounds are passed, you may get zero commits.
  /// Getting all commits between 0 and the latest commit id should retrieve all events associated with the aggregate.
  fn get_range(
    &mut self,
    aggregate_id: Uuid,
    min_version: i64,
    max_version: i64,
  ) -> Result<Vec<Commit>, Box<dyn StoreError>>;

  /// Get a single commit.
  fn get_commit(&mut self, commit_it: &Uuid) -> Result<Commit, Box<dyn StoreError>>;
}

/// A store for reading and writing events.
pub trait Store: ClientStore {
  /// Get all commits which have not been marked as dispatched by some dispatcher.
  fn get_undispatched_commits(&mut self) -> Result<Vec<Commit>, Box<dyn StoreError>>;

  /// Mark a single commit as having been dispatched.
  fn mark_commit_as_dispatched(&mut self, commit_id: Uuid) -> Result<(), Box<dyn StoreError>>;
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
