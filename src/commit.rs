use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Clone)]
pub struct Commit {
  pub aggregate_id: Uuid,
  pub aggregate_version: i64,
  pub commit_id: Uuid,
  pub commit_timestamp: DateTime<Utc>,
  pub commit_sequence: i64,
  pub commit_number: i64,
  pub serialized_events: Vec<u8>,
  pub serialized_metadata: Vec<u8>,
  pub events_count: i64,
  pub dispatched: bool,
}

#[derive(Clone)]
pub struct CommitAttempt {
  pub aggregate_id: Uuid,
  pub aggregate_version: i64,
  pub commit_id: Uuid,
  pub commit_timestamp: DateTime<Utc>,
  pub commit_sequence: i64,
  pub serialized_metadata: Vec<u8>,
  pub serialized_events: Vec<u8>,
  pub events_count: i64,
}
