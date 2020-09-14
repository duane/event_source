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

#[cfg(test)]
mod tests {
  use super::Commit;
  use uuid::Uuid;
  use chrono::Utc;
  #[test]
  fn deserialize() {
    let serialized_events = b"[{\"foo\": \"bar\"}, {\"baz\": \"bat\"}]".to_vec();
    let serialized_metadata = "[{\"foo2\": \"bar2\", \"baz2\": \"bat2\"}]".as_bytes().to_vec();
    let commit = Commit{
      aggregate_id: Uuid::new_v4(),
      aggregate_version: 18,
      commit_id: Uuid::new_v4(),
      commit_sequence: 101,
      commit_number: 198,
      commit_timestamp: Utc::now(),
      serialized_events,
      serialized_metadata,
      events_count: 4,
      dispatched: true,
    };

    let deserialized = commit.deserialize();

    assert_eq!(deserialized.aggregate_id, commit.aggregate_id);
    assert_eq!(deserialized.aggregate_version, commit.aggregate_version);
    assert_eq!(deserialized.commit_id, commit.commit_id);
    assert_eq!(deserialized.commit_sequence, commit.commit_sequence);
    assert_eq!(deserialized.commit_number, commit.commit_number);
    assert_eq!(deserialized.commit_timestamp, commit.commit_timestamp);
    assert_eq!(deserialized.events_count, commit.events_count);
    assert_eq!(deserialized.dispatched, commit.dispatched);

    let events_array = deserialized.events.as_array().unwrap();
    assert_eq!(events_array.len(), 2);
    assert_eq!(events_array[0].as_object().unwrap()["foo"], "bar");
    assert_eq!(events_array[1].as_object().unwrap()["baz"], "bat");

    let metadata_obj = deserialized.metadata.as_array().unwrap()[0].as_object().unwrap();
    assert_eq!(metadata_obj.len(), 2);
    assert_eq!(metadata_obj["foo2"], "bar2");
    assert_eq!(metadata_obj["baz2"], "bat2");
    assert_eq!(events_array[0].as_object().unwrap()["foo"], "bar");
  }

}