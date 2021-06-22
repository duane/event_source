use super::super::commit::{DeserializedCommit, Commit, CommitAttempt};
use super::{ClientStore, Store, StoreError, StoreErrorType};
use postgres::Client;
use uuid::Uuid;
use std::error::Error;
use std::fmt;

/// The store representing a sqlite connection.
pub struct PostgresStore {
  client: Client,
}

/// The errors returned by this store.
#[derive(Debug)]
pub struct PostgresStoreError {
  cause: postgres::Error
}

impl fmt::Display for PostgresStoreError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "SqliteStoreError({:}, {:})", self.error_type(), self.cause)
  }
}

impl Error for PostgresStoreError {
  fn source(&self) -> Option<&(dyn Error + 'static)> {
    Some(&self.cause)
  }
}

impl From<postgres::Error> for PostgresStoreError {
  fn from(cause: postgres::Error) -> Self {
    PostgresStoreError { cause }
  }
}

impl Into<Box<dyn StoreError>> for PostgresStoreError {
  fn into(self) -> Box<dyn StoreError> {
    Box::new(self)
  }
}

impl PostgresStore {
  /// Provision a new database.
  pub fn initialize(&mut self) {
    self.client.batch_execute(
      "CREATE TABLE IF NOT EXISTS commits (
        aggregate_id      UUID NOT NULL,
        aggregate_version INTEGER NOT NULL,
        commit_id         UUID NOT NULL,
        commit_sequence   INTEGER NOT NULL,
        commit_number     SERIAL PRIMARY KEY,
        commit_timestamp  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        events_count      INTEGER NOT NULL,
        metadata          JSON NOT NULL,
        events            JSON NOT NULL,
        dispatched        INTEGER NOT NULL DEFAULT 0
      );
      CREATE UNIQUE INDEX IF NOT EXISTS commits_commit_id_unique_idx ON commits (commit_id);
      CREATE UNIQUE INDEX IF NOT EXISTS commits_commit_aggregate_idx ON commits (aggregate_id, aggregate_version);
      CREATE UNIQUE INDEX IF NOT EXISTS commits_commit_sequence_idx ON commits (aggregate_id, commit_sequence);
      CREATE INDEX IF NOT EXISTS commits_dispatched_idx ON commits (dispatched);"
    ).expect("could not intialize postgres commits table");
  }
}

impl StoreError for PostgresStoreError {
  fn error_type(&self) -> StoreErrorType {
    match self.cause {
      // RusqliteError::SqliteFailure(_, Some(ref msg))
      //   if msg == "UNIQUE constraint failed: commits.aggregate_id, commits.commit_sequence" =>
      // {
      //   StoreErrorType::DuplicateWriteError(StorageCommitConflict::CommitSequenceConflict)
      // }
      // RusqliteError::SqliteFailure(_, Some(ref msg))
      //   if msg == "UNIQUE constraint failed: commits.aggregate_id, commits.aggregate_version" =>
      // {
      //   StoreErrorType::DuplicateWriteError(StorageCommitConflict::AggregateVersionConflict)
      // }
      // RusqliteError::SqliteFailure(_, Some(ref msg))
      //   if msg == "UNIQUE constraint failed: commits.commit_id" =>
      // {
      //   StoreErrorType::DuplicateWriteError(StorageCommitConflict::CommitIdConflict)
      // }
      // RusqliteError::SqliteFailure(_, Some(ref msg)) => {
      //   panic!("{}", msg.clone());
      // }
      _ => StoreErrorType::UnknownError,
    }
  }
}

impl ClientStore for PostgresStore {
  type Connection = Client;

  fn with_connection(connection: Self::Connection) -> Self {
    PostgresStore { client: connection }
  }

  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, Box<dyn StoreError>> {
    let mut transaction = match self.client.transaction() {
      Ok(result) => result,
      Err(err) => return Err(PostgresStoreError::from(err).into()),
    };
    match transaction.query(
      "INSERT INTO commits (
        aggregate_id,
        aggregate_version,
        commit_id,
        commit_timestamp,
        commit_sequence,
        events_count,
        metadata,
        events
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING commit_number",
      &[
        &commit_attempt.aggregate_id.to_string(),
        &commit_attempt.aggregate_version,
        &commit_attempt.commit_id.to_string(),
        &commit_attempt.commit_timestamp,
        &commit_attempt.commit_sequence,
        &commit_attempt.events_count,
        &commit_attempt.serialized_metadata,
        &commit_attempt.serialized_events,
      ]
    ) {
      Ok(result) => Ok(result.get(0).expect("no commit row result").get(0)),
      Err(err) => Err(PostgresStoreError::from(err).into()),
    }
  }

  fn get_range(
    &mut self,
    aggregate_id: Uuid,
    min_version: i64,
    max_version: i64,
  ) -> Result<Vec<DeserializedCommit>, Box<dyn StoreError>> {
    let mut transaction = match self.client.transaction() {
      Ok(result) => result,
      Err(err) => return Err(PostgresStoreError::from(err).into()),
    };
    let rows = match transaction.query(
      "SELECT
          aggregate_id,
          aggregate_version,
          commit_id,
          commit_timestamp,
          commit_sequence,
          commit_number,
          events_count,
          metadata,
          events,
          dispatched
        FROM commits
        WHERE aggregate_version >= $1
        AND aggregate_version <= $2
        AND aggregate_id = $3;",
        &[
          &min_version,
          &max_version,
          &aggregate_id.to_string()
        ]
    ) {
      Ok(rows) => rows,
      Err(err) => return Err(PostgresStoreError::from(err).into()),
    };
    Ok(
      rows.into_iter().map(|row| {
        Commit {
          aggregate_id: row.get(0),
          aggregate_version: row.get(1),
          commit_id: row.get(2),
          commit_timestamp: row.get(3),
          commit_sequence: row.get(4),
          commit_number: row.get(5),
          events_count: row.get(6),
          serialized_metadata: row.get(7),
          serialized_events: row.get(8),
          dispatched: row.get(9),
        }.deserialize()
      }).collect()
    )
  }

  fn get_commit(&mut self, commit_id: &Uuid) -> Result<DeserializedCommit, Box<dyn StoreError>> {
    let mut transaction = match self.client.transaction() {
      Ok(result) => result,
      Err(err) => return Err(PostgresStoreError::from(err).into()),
    };
    let rows = match transaction.query(
      "SELECT
          aggregate_id,
          aggregate_version,
          commit_id,
          commit_timestamp,
          commit_sequence,
          commit_number,
          events_count,
          metadata,
          events,
          dispatched
        FROM commits
        WHERE commit_id = $1
        ORDER BY commit_number ASC;",
        &[&commit_id.to_string()]
    ) {
      Ok(rows) => rows,
      Err(err) => return Err(PostgresStoreError::from(err).into()),
    };
    assert_eq!(1, rows.len());
    let row = rows.get(0).expect("query did not find any rows");
    Ok(
      Commit {
        aggregate_id: row.get(0),
        aggregate_version: row.get(1),
        commit_id: row.get(2),
        commit_timestamp: row.get(3),
        commit_sequence: row.get(4),
        commit_number: row.get(5),
        events_count: row.get(6),
        serialized_metadata: row.get(7),
        serialized_events: row.get(8),
        dispatched: row.get(9),
      }.deserialize()
    )
  }
}

impl Store for PostgresStore {
  fn get_undispatched_commits(&mut self) -> Result<Vec<DeserializedCommit>, Box<dyn StoreError>> {
    let mut transaction = match self.client.transaction() {
      Ok(result) => result,
      Err(err) => return Err(PostgresStoreError::from(err).into()),
    };
    let rows = match transaction.query(
      "SELECT
          aggregate_id,
          aggregate_version,
          commit_id,
          commit_timestamp,
          commit_sequence,
          commit_number,
          events_count,
          metadata,
          events,
          dispatched
        FROM commits
        WHERE dispatched = 0
        ORDER BY commit_number ASC;", &[]
    ) {
      Ok(rows) => rows,
      Err(err) => return Err(PostgresStoreError::from(err).into()),
    };
    Ok(
      rows.into_iter().map(|row| {
        Commit {
          aggregate_id: row.get(0),
          aggregate_version: row.get(1),
          commit_id: row.get(2),
          commit_timestamp: row.get(3),
          commit_sequence: row.get(4),
          commit_number: row.get(5),
          events_count: row.get(6),
          serialized_metadata: row.get(7),
          serialized_events: row.get(8),
          dispatched: row.get(9),
        }.deserialize()
      }).collect()
    )
  }

  fn mark_commit_as_dispatched(&mut self, commit_id: Uuid) -> Result<(), Box<dyn StoreError>> {
    let mut transaction = match self.client.transaction() {
      Ok(result) => result,
      Err(err) => return Err(PostgresStoreError::from(err).into()),
    };
    match transaction.execute("UPDATE commits SET dispatched = 1 WHERE commit_id = $1", &[&commit_id.to_string()]) {
      Ok(_) => Ok(()),
      Err(err) => Err(PostgresStoreError::from(err).into()),
    }
  }
}
