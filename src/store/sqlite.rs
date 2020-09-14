use super::super::commit::{Commit, CommitAttempt};
use super::{StorageCommitConflict, Store, StoreError, StoreErrorType};
use rusqlite::{Connection as RusqliteConnection, Error as RusqliteError, ToSql};
use std::path::Path;
use uuid::Uuid;

pub struct SqliteStore {
  conn: RusqliteConnection,
}

impl SqliteStore {
  pub fn with_new_in_memory_connection() -> Self {
    Self::with_connection(RusqliteConnection::open_in_memory().unwrap())
  }

  pub fn with_new_connection_at_path(path: &Path) -> Self {
    Self::with_connection(RusqliteConnection::open(path).unwrap())
  }

  pub fn initialize(&self) {
    self.conn.execute_batch(
      "CREATE TABLE IF NOT EXISTS commits (
        aggregate_id      VARCHAR(36) NOT NULL,
        aggregate_version INTEGER NOT NULL,
        commit_id         VARCHAR(36) NOT NULL,
        commit_sequence   INTEGER NOT NULL,
        commit_number     INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        commit_timestamp  DATETIME NOT NULL,
        events_count      INTEGER NOT NULL,
        metadata          BLOB NOT NULL,
        events            BLOB NOT NULL,
        dispatched        INTEGER NOT NULL DEFAULT 0
      );
      CREATE UNIQUE INDEX IF NOT EXISTS commits_commit_id_unique_idx ON commits (commit_id);
      CREATE UNIQUE INDEX IF NOT EXISTS commits_commit_aggregate_idx ON commits (aggregate_id, aggregate_version);
      CREATE UNIQUE INDEX IF NOT EXISTS commits_commit_sequence_idx ON commits (aggregate_id, commit_sequence);
      CREATE INDEX IF NOT EXISTS commits_dispatched_idx ON commits (dispatched);"
    ).expect("could not intiailize sqlite commits table");
  }
}

impl From<RusqliteError> for StoreError<RusqliteError> {
  fn from(error: RusqliteError) -> Self {
    let error_type = match error {
      RusqliteError::SqliteFailure(_, Some(ref msg))
        if msg == "UNIQUE constraint failed: commits.aggregate_id, commits.commit_sequence" =>
      {
        StoreErrorType::DuplicateWriteError(StorageCommitConflict::CommitSequenceConflict)
      }
      RusqliteError::SqliteFailure(_, Some(ref msg))
        if msg == "UNIQUE constraint failed: commits.aggregate_id, commits.aggregate_version" =>
      {
        StoreErrorType::DuplicateWriteError(StorageCommitConflict::AggregateVersionConflict)
      }
      RusqliteError::SqliteFailure(_, Some(ref msg))
        if msg == "UNIQUE constraint failed: commits.commit_id" =>
      {
        StoreErrorType::DuplicateWriteError(StorageCommitConflict::CommitIdConflict)
      }
      RusqliteError::SqliteFailure(_, Some(ref msg)) => {
        panic!(msg.clone());
      }
      _ => StoreErrorType::UnknownError,
    };
    StoreError::new(error_type, error)
  }
}

impl Store for SqliteStore {
  type UnderlyingStoreError = RusqliteError;
  type Error = StoreError<RusqliteError>;
  type Connection = RusqliteConnection;

  fn with_connection(connection: Self::Connection) -> Self {
    SqliteStore { conn: connection }
  }

  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, Self::Error> {
    {
      {
        let mut statement = self.conn.prepare(
          "INSERT INTO commits (
            aggregate_id,
            aggregate_version,
            commit_id,
            commit_timestamp,
            commit_sequence,
            events_count,
            metadata,
            events
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )?;
        statement.execute(&[
          &commit_attempt.aggregate_id.to_string(),
          &commit_attempt.aggregate_version as &dyn ToSql,
          &commit_attempt.commit_id.to_string(),
          &commit_attempt.commit_timestamp,
          &commit_attempt.commit_sequence,
          &commit_attempt.events_count,
          &commit_attempt.serialized_metadata,
          &commit_attempt.serialized_events,
        ])?;
        statement.finalize()?;
      }
    }

    Ok(self.conn.last_insert_rowid())
  }

  fn get_range(
    &self,
    aggregate_id: Uuid,
    min_version: i64,
    max_version: i64,
  ) -> Result<Vec<Commit>, Self::Error> {
    let mut stmt = self.conn.prepare(
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
        WHERE aggregate_version >= ?
        AND aggregate_version <= ?
        AND aggregate_id = ?;",
    )?;
    let rows = stmt
      .query_map(
        &[
          &min_version,
          &max_version,
          &aggregate_id.to_string() as &dyn ToSql,
        ],
        |row| {
          let aggregate_id_str: String = row.get(0).expect("no aggregate_id result column");
          let commit_id_str: String = row.get(2).expect("no commit_id result column");
          Ok(Commit {
            aggregate_id: Uuid::parse_str(aggregate_id_str.as_ref()).unwrap(),
            aggregate_version: row.get(1).expect("no aggregate_version result column"),
            commit_id: Uuid::parse_str(commit_id_str.as_ref()).unwrap(),
            commit_timestamp: row.get(3).expect("no commit_timestamp result column"),
            commit_sequence: row.get(4).expect("no commit_sequence result column"),
            commit_number: row.get(5).expect("no commit_number result column"),
            events_count: row.get(6).expect("no events_count result column"),
            serialized_metadata: row.get(7).expect("no serialized_metadat result column"),
            serialized_events: row.get(8).expect("no serialized_events result column"),
            dispatched: row.get(9).expect("no dispatched result column"),
          })
        },
      )?
      .map(|row| row.unwrap())
      .collect();
    Ok(rows)
  }

  fn get_undispatched_commits(&mut self) -> Result<Vec<Commit>, Self::Error> {
    let mut stmt = self.conn.prepare(
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
        ORDER BY commit_number ASC;",
    )?;
    let rows = stmt
      .query_map(&vec![] as &Vec<&dyn ToSql>, |row| {
        let aggregate_id_str: String = row.get(0).expect("no aggregate_id column in result");
        let commit_id_str: String = row.get(2).expect("no commit_id column in result");
        Ok(Commit {
          aggregate_id: Uuid::parse_str(aggregate_id_str.as_ref())
            .expect("commit_id is not in Uuid format; database may be corrupted."),
          aggregate_version: row.get(1).expect("no aggregate_version column in result"),
          commit_id: Uuid::parse_str(commit_id_str.as_ref())
            .expect("commit_id is not in Uuid format; database may be corrupted."),
          commit_timestamp: row.get(3).expect("no commit_timestamp column in result"),
          commit_sequence: row.get(4).expect("no commit_sequence column in result"),
          commit_number: row.get(5).expect("no commit_number column in result"),
          events_count: row.get(6).expect("no events_count column in result"),
          serialized_metadata: row.get(7).expect("no serialized_metadata column in result"),
          serialized_events: row.get(8).expect("no serialized_events column in result"),
          dispatched: row.get(9).expect("no dispatched column in result"),
        })
      })?
      .map(|rows| {
        rows.expect("Could not read from commits row. If the schema has changed, update the store to read from the appropriate format.")
      })
      .collect();
    Ok(rows)
  }

  fn mark_commit_as_dispatched(&mut self, commit_id: Uuid) -> Result<(), Self::Error> {
    let mut statement = self
      .conn
      .prepare("UPDATE commits SET dispatched = 1 WHERE commit_id = ?")?;
    statement.execute(&[&commit_id.to_string()])?;
    statement.finalize()?;
    Ok(())
  }

  fn get_commit(&mut self, commit_id: &Uuid) -> Result<Commit, Self::Error> {
    let mut statement = self.conn.prepare(
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
        WHERE commit_id = ?
        ORDER BY commit_number ASC;",
    )?;
    let commit: Commit = statement.query_row(&[&commit_id.to_string()], |row| {
      let aggregate_id: String = row.get(0).expect("no aggregate_id column in result row");
      let commit_id: String = row.get(2).expect("no commit_id column in result row");
      Ok(Commit {
        aggregate_id: Uuid::parse_str(aggregate_id.as_ref()).unwrap(),
        aggregate_version: row
          .get(1)
          .expect("no aggregate_version column in result row"),
        commit_id: Uuid::parse_str(commit_id.as_ref()).unwrap(),
        commit_timestamp: row
          .get(3)
          .expect("no commit_timestamp column in result row"),
        commit_sequence: row.get(4).expect("no commit_sequence column in result row"),
        commit_number: row.get(5).expect("no commit_number column in result row"),
        events_count: row.get(6).expect("no events_count column in result row"),
        serialized_metadata: row
          .get(7)
          .expect("no serialized_metadata column in result row"),
        serialized_events: row
          .get(8)
          .expect("no serialized_events column in result row"),
        dispatched: row.get(9).expect("no dispatched column in result row"),
      })
    })?;
    Ok(commit)
  }
}

#[cfg(test)]
mod tests {
  use super::super::super::commit::*;
  use super::super::super::store::*;
  use chrono::Utc;
  use uuid::Uuid;
  #[test]
  fn it_allows_storing_and_retrieving_commits() {
    let mut s = sqlite::SqliteStore::with_new_in_memory_connection();
    s.initialize();
    let commit_attempt = CommitAttempt {
      aggregate_id: Uuid::new_v4(),
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_metadata: String::from("\"metadata\"").into_bytes(),
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt).unwrap(), 1);
    let commits = s.get_range(commit_attempt.aggregate_id, 0, 2).unwrap();
    assert_eq!(
      commits
        .iter()
        .map(|c| c.serialized_events.clone())
        .collect::<Vec<Vec<u8>>>(),
      vec![String::from("[\"hi\"]").into_bytes()]
    );

    let commit_attempt2 = CommitAttempt {
      aggregate_id: commit_attempt.aggregate_id,
      aggregate_version: commit_attempt.aggregate_version + 1,
      commit_id: Uuid::new_v4(),
      commit_sequence: commit_attempt.commit_sequence + 1,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_metadata: String::from("\"metadata\"").into_bytes(),
      serialized_events: String::from("[\"there\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt2).unwrap(), 2);

    let commits = s.get_range(commit_attempt.aggregate_id, 0, 2).unwrap();
    assert_eq!(
      commits
        .iter()
        .map(|c| c.serialized_events.clone())
        .collect::<Vec<Vec<u8>>>(),
      vec![
        String::from("[\"hi\"]").into_bytes(),
        String::from("[\"there\"]").into_bytes(),
      ]
    )
  }

  #[test]
  fn it_does_not_allow_double_commits_by_sequence() {
    let mut s = sqlite::SqliteStore::with_new_in_memory_connection();
    s.initialize();
    let commit_attempt = CommitAttempt {
      aggregate_id: Uuid::new_v4(),
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_metadata: String::from("\"metadata\"").into_bytes(),
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt).unwrap(), 1);
    let commits = s.get_range(commit_attempt.aggregate_id, 0, 2).unwrap();
    assert_eq!(
      commits
        .iter()
        .map(|c| c.serialized_events.clone())
        .collect::<Vec<Vec<u8>>>(),
      vec![String::from("[\"hi\"]").into_bytes()]
    );

    let commit_attempt2 = CommitAttempt {
      aggregate_id: commit_attempt.aggregate_id,
      aggregate_version: commit_attempt.aggregate_version + 1,
      commit_id: Uuid::new_v4(),
      commit_sequence: commit_attempt.commit_sequence,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_metadata: String::from("\"metadata\"").into_bytes(),
      serialized_events: String::from("[\"there\"]").into_bytes(),
    };

    assert_eq!(
      StoreErrorType::DuplicateWriteError(StorageCommitConflict::CommitSequenceConflict),
      s.commit(&commit_attempt2).err().unwrap().error_type
    );
  }

  #[test]
  fn it_does_not_allow_double_commits_by_aggregate_version() {
    let mut s = sqlite::SqliteStore::with_new_in_memory_connection();
    s.initialize();
    let commit_attempt = CommitAttempt {
      aggregate_id: Uuid::new_v4(),
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_metadata: String::from("\"metadata\"").into_bytes(),
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt).unwrap(), 1);
    let commits = s.get_range(commit_attempt.aggregate_id, 0, 2).unwrap();
    assert_eq!(
      commits
        .iter()
        .map(|c| c.serialized_events.clone())
        .collect::<Vec<Vec<u8>>>(),
      vec![String::from("[\"hi\"]").into_bytes()]
    );

    let commit_attempt2 = CommitAttempt {
      aggregate_id: commit_attempt.aggregate_id,
      aggregate_version: commit_attempt.aggregate_version,
      commit_id: Uuid::new_v4(),
      commit_sequence: commit_attempt.commit_sequence + 1,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_metadata: String::from("\"metadata\"").into_bytes(),
      serialized_events: String::from("[\"there\"]").into_bytes(),
    };
    assert_eq!(
      StoreErrorType::DuplicateWriteError(StorageCommitConflict::AggregateVersionConflict),
      s.commit(&commit_attempt2).err().unwrap().error_type
    );
  }

  #[test]
  fn it_does_not_allow_double_commits_by_commit_id() {
    let mut s = sqlite::SqliteStore::with_new_in_memory_connection();
    s.initialize();
    let commit_attempt = CommitAttempt {
      aggregate_id: Uuid::new_v4(),
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_metadata: String::from("\"metadata\"").into_bytes(),
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt).unwrap(), 1);
    let commits = s.get_range(commit_attempt.aggregate_id, 0, 2).unwrap();
    assert_eq!(
      commits
        .iter()
        .map(|c| c.serialized_events.clone())
        .collect::<Vec<Vec<u8>>>(),
      vec![String::from("[\"hi\"]").into_bytes()]
    );

    let commit_attempt2 = CommitAttempt {
      aggregate_id: commit_attempt.aggregate_id,
      aggregate_version: commit_attempt.aggregate_version + 1,
      commit_id: commit_attempt.commit_id,
      commit_sequence: commit_attempt.commit_sequence + 1,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_metadata: String::from("\"metadata\"").into_bytes(),
      serialized_events: String::from("[\"there\"]").into_bytes(),
    };

    assert_eq!(
      StoreErrorType::DuplicateWriteError(StorageCommitConflict::CommitIdConflict),
      s.commit(&commit_attempt2).err().unwrap().error_type
    );
  }
}
