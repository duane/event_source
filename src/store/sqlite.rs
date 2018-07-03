use super::super::commit::{Commit, CommitAttempt};
use super::{CommitError, CommitErrorType, StorageCommitConflict, Store};
use rusqlite::{Connection, Error as RusqliteError};
use std::error::Error;
use std::path::Path;
use uuid::Uuid;

pub struct SqliteStore {
  conn: Connection,
}

impl SqliteStore {
  fn with_connection(conn: Connection) -> Self {
    let store = SqliteStore { conn };
    store
      .conn
      .execute_batch(
        "CREATE TABLE IF NOT EXISTS commits (
          aggregate_id      INTEGER NOT NULL,
          aggregate_version INTEGER NOT NULL,
          commit_id         VARCHAR(36) NOT NULL,
          commit_sequence   INTEGER NOT NULL,
          commit_number     INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
          commit_timestamp  DATETIME NOT NULL,
          events_count      INTEGER NOT NULL,
          events            BLOB NOT NULL,
          dispatched        INTEGER NOT NULL DEFAULT 0
        );
        CREATE UNIQUE INDEX IF NOT EXISTS commits_commit_id_unique_idx ON commits (commit_id);
        CREATE UNIQUE INDEX IF NOT EXISTS commits_commit_aggregate_idx ON commits (aggregate_id, aggregate_version);
        CREATE UNIQUE INDEX IF NOT EXISTS commits_commit_sequence_idx ON commits (aggregate_id, commit_sequence);
        CREATE INDEX IF NOT EXISTS commits_dispatched_idx ON commits (dispatched);"
      )
      .unwrap();
    store
  }

  pub fn with_new_in_memory_connection() -> Self {
    Self::with_connection(Connection::open_in_memory().unwrap())
  }

  pub fn with_new_connection_at_path(path: &Path) -> Self {
    Self::with_connection(Connection::open(path).unwrap())
  }
}

fn convert_sqlite_result_to_commit_result<T>(
  sqlite_result: Result<T, RusqliteError>,
) -> Result<T, CommitError> {
  sqlite_result.map_err(|rusqlite_err| {
    let error_type = match rusqlite_err {
      RusqliteError::SqliteFailure(_, Some(ref msg))
        if msg == "UNIQUE constraint failed: commits.aggregate_id, commits.commit_sequence" =>
      {
        CommitErrorType::DuplicateCommitError(StorageCommitConflict::CommitSequenceConflict)
      }
      RusqliteError::SqliteFailure(_, Some(ref msg))
        if msg == "UNIQUE constraint failed: commits.aggregate_id, commits.aggregate_version" =>
      {
        CommitErrorType::DuplicateCommitError(StorageCommitConflict::AggregateVersionConflict)
      }
      RusqliteError::SqliteFailure(_, Some(ref msg))
        if msg == "UNIQUE constraint failed: commits.commit_id" =>
      {
        CommitErrorType::DuplicateCommitError(StorageCommitConflict::CommitIdConflict)
      }
      RusqliteError::SqliteFailure(_, Some(ref msg)) => {
        panic!(msg.clone());
      }
      _ => CommitErrorType::UnknownError,
    };
    CommitError::new(error_type, Box::new(rusqlite_err))
  })
}

impl Store for SqliteStore {
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, CommitError> {
    {
      {
        let mut statement = convert_sqlite_result_to_commit_result(self.conn.prepare(
          "INSERT INTO commits (
            aggregate_id,
            aggregate_version,
            commit_id,
            commit_timestamp,
            commit_sequence,
            events_count,
            events
          ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ))?;
        convert_sqlite_result_to_commit_result(statement.execute(&[
          &commit_attempt.aggregate_id,
          &commit_attempt.aggregate_version,
          &commit_attempt.commit_id.to_string(),
          &commit_attempt.commit_timestamp,
          &commit_attempt.commit_sequence,
          &commit_attempt.events_count,
          &commit_attempt.serialized_events,
        ]))?;
        convert_sqlite_result_to_commit_result(statement.finalize())?;
      }
    }

    Ok(self.conn.last_insert_rowid())
  }

  fn get_range(
    &mut self,
    aggregate_id: i64,
    min_version: i64,
    max_version: i64,
  ) -> Result<Vec<Commit>, Box<Error>> {
    let mut stmt = self
      .conn
      .prepare(
        "SELECT
          aggregate_id,
          aggregate_version,
          commit_id,
          commit_timestamp,
          commit_sequence,
          commit_number,
          events_count,
          events,
          dispatched
        FROM commits
        WHERE aggregate_version >= ?
        AND aggregate_version <= ?
        AND aggregate_id = ?;",
      )
      .map_err(Box::new)?;
    let rows = stmt
      .query_map(&[&min_version, &max_version, &aggregate_id], |row| {
        let uuid_str: String = row.get(2);
        Commit {
          aggregate_id: row.get(0),
          aggregate_version: row.get(1),
          commit_id: Uuid::parse_str(uuid_str.as_ref()).unwrap(),
          commit_timestamp: row.get(3),
          commit_sequence: row.get(4),
          commit_number: row.get(5),
          events_count: row.get(6),
          serialized_events: row.get(7),
          dispatched: row.get(8),
        }
      })
      .map_err(Box::new)?
      .map(|row| row.unwrap())
      .collect();
    Ok(rows)
  }

  fn get_undispatched_commits(&mut self) -> Result<Vec<Commit>, Box<Error>> {
    let mut stmt = self
      .conn
      .prepare(
        "SELECT
          aggregate_id,
          aggregate_version,
          commit_id,
          commit_timestamp,
          commit_sequence,
          commit_number,
          events_count,
          events,
          dispatched
        FROM commits
        WHERE dispatched = 0
        ORDER BY commit_number ASC;",
      )
      .map_err(Box::new)?;
    let rows = stmt
      .query_map(&[], |row| {
        let uuid_str: String = row.get(2);
        Commit {
          aggregate_id: row.get(0),
          aggregate_version: row.get(1),
          commit_id: Uuid::parse_str(uuid_str.as_ref()).unwrap(),
          commit_timestamp: row.get(3),
          commit_sequence: row.get(4),
          commit_number: row.get(5),
          events_count: row.get(6),
          serialized_events: row.get(7),
          dispatched: row.get(8),
        }
      })
      .map_err(Box::new)?
      .map(|rows| rows.unwrap())
      .collect();
    Ok(rows)
  }

  fn mark_commit_as_dispatched(&mut self, commit_id: Uuid) -> Result<(), Box<Error>> {
    let mut statement = self
      .conn
      .prepare("UPDATE commits SET dispatched = 1 WHERE commit_id = ?")
      .map_err(|err| Box::new(err))?;
    statement
      .execute(&[&commit_id.to_string()])
      .map_err(|err| Box::new(err))?;
    statement.finalize().map_err(|err| Box::new(err))?;

    Ok(())
  }

  fn list_aggregate_ids(&mut self) -> Result<Vec<i64>, Box<Error>> {
    let mut statement = self
      .conn
      .prepare("SELECT DISTINCT aggregate_id FROM commits;")
      .map_err(|err| Box::new(err))?;
    let ids: Vec<i64> = statement
      .query_map(&[], |row| row.get(0))
      .map_err(|err| Box::new(err))?
      .map(|row_attempt| row_attempt.unwrap())
      .collect();
    Ok(ids)
  }

  fn get_commit(&mut self, commit_id: &Uuid) -> Result<Commit, Box<Error>> {
    let mut statement = self
      .conn
      .prepare(
        "SELECT
          aggregate_id,
          aggregate_version,
          commit_id,
          commit_timestamp,
          commit_sequence,
          commit_number,
          events_count,
          events,
          dispatched
        FROM commits
        WHERE commit_id = ?
        ORDER BY commit_number ASC;",
      )
      .map_err(|err| Box::new(err))?;
    let commit: Commit = statement
      .query_row(&[&commit_id.to_string()], |row| {
        let commit_id: String = row.get(2);
        Commit {
          aggregate_id: row.get(0),
          aggregate_version: row.get(1),
          commit_id: Uuid::parse_str(commit_id.as_ref()).unwrap(),
          commit_timestamp: row.get(3),
          commit_sequence: row.get(4),
          commit_number: row.get(5),
          events_count: row.get(6),
          serialized_events: row.get(7),
          dispatched: row.get(8),
        }
      })
      .map_err(|err| Box::new(err))?;
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
    let commit_attempt = CommitAttempt {
      aggregate_id: 1,
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt).unwrap(), 1);
    let commits = s.get_range(1, 0, 2).unwrap();
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
      serialized_events: String::from("[\"there\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt2).unwrap(), 2);

    let commits = s.get_range(1, 0, 2).unwrap();
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
    let commit_attempt = CommitAttempt {
      aggregate_id: 1,
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt).unwrap(), 1);
    let commits = s.get_range(1, 0, 2).unwrap();
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
      serialized_events: String::from("[\"there\"]").into_bytes(),
    };

    assert_eq!(
      CommitErrorType::DuplicateCommitError(StorageCommitConflict::CommitSequenceConflict),
      s.commit(&commit_attempt2).err().unwrap().error_type
    );
  }

  #[test]
  fn it_does_not_allow_double_commits_by_aggregate_version() {
    let mut s = sqlite::SqliteStore::with_new_in_memory_connection();
    let commit_attempt = CommitAttempt {
      aggregate_id: 1,
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt).unwrap(), 1);
    let commits = s.get_range(1, 0, 2).unwrap();
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
      serialized_events: String::from("[\"there\"]").into_bytes(),
    };
    assert_eq!(
      CommitErrorType::DuplicateCommitError(StorageCommitConflict::AggregateVersionConflict),
      s.commit(&commit_attempt2).err().unwrap().error_type
    );
  }

  #[test]
  fn it_does_not_allow_double_commits_by_commit_id() {
    let mut s = sqlite::SqliteStore::with_new_in_memory_connection();
    let commit_attempt = CommitAttempt {
      aggregate_id: 1,
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt).unwrap(), 1);
    let commits = s.get_range(1, 0, 2).unwrap();
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
      serialized_events: String::from("[\"there\"]").into_bytes(),
    };

    assert_eq!(
      CommitErrorType::DuplicateCommitError(StorageCommitConflict::CommitIdConflict),
      s.commit(&commit_attempt2).err().unwrap().error_type
    );
  }

  #[test]
  fn it_allows_listing_aggregate_ids() {
    let mut s = sqlite::SqliteStore::with_new_in_memory_connection();
    let commit_attempt = CommitAttempt {
      aggregate_id: 1,
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: 0,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_events: String::from("[\"hi\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt).unwrap(), 1);
    let commits = s.get_range(1, 0, 2).unwrap();
    assert_eq!(
      commits
        .iter()
        .map(|c| c.serialized_events.clone())
        .collect::<Vec<Vec<u8>>>(),
      vec![String::from("[\"hi\"]").into_bytes()]
    );

    let commit_attempt2 = CommitAttempt {
      aggregate_id: 2,
      aggregate_version: 0,
      commit_id: Uuid::new_v4(),
      commit_sequence: commit_attempt.commit_sequence + 1,
      commit_timestamp: Utc::now(),
      events_count: 1,
      serialized_events: String::from("[\"there\"]").into_bytes(),
    };
    assert_eq!(s.commit(&commit_attempt2).unwrap(), 2);

    assert_eq!(s.list_aggregate_ids().unwrap(), vec![1, 2])
  }
}
