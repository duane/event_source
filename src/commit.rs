use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeserializedCommit {
  pub aggregate_id: Uuid,
  pub aggregate_version: i64,
  pub commit_id: Uuid,
  pub commit_timestamp: DateTime<Utc>,
  pub commit_sequence: i64,
  pub commit_number: i64,
  pub events: serde_json::Value,
  pub metadata: serde_json::Value,
  pub events_count: i64,
  pub dispatched: bool,
}

impl Commit {
  pub fn deserialize(&self) -> DeserializedCommit {
    let events = serde_json::from_slice(self.serialized_events.as_slice()).unwrap();
    let metadata = serde_json::from_slice(self.serialized_metadata.as_slice()).unwrap();
    DeserializedCommit {
      aggregate_id: self.aggregate_id,
      aggregate_version: self.aggregate_version,
      commit_id: self.commit_id,
      commit_timestamp: self.commit_timestamp,
      commit_number: self.commit_number,
      commit_sequence: self.commit_sequence,
      events,
      metadata,
      events_count: self.events_count,
      dispatched: self.dispatched,
    }
  }
}
