use chrono::{DateTime, Utc};
use uuid::Uuid;

/// A single commit in an event store.
#[derive(Clone, Debug)]
pub struct Commit {
  /// The latest aggregate id. Monotonically increasing.
  pub aggregate_id: Uuid,

  /// The latest aggregate version. Monotonically increasing.
  pub aggregate_version: i64,

  /// The id of the commit. Monotonically increasing.
  pub commit_id: Uuid,

  /// The timestamp of the commit in UTC from the perspective of the store.
  pub commit_timestamp: DateTime<Utc>,
  /// The sequence of the commit. Monotonically increasing.
  pub commit_sequence: i64,

  /// The commit number. TODO: distinguish from sequence.
  pub commit_number: i64,

  /// The events, serialized to json, of the commit.
  pub serialized_events: Vec<u8>,

  /// The metadata, serialized to json, of the commit.
  pub serialized_metadata: Vec<u8>,

  /// The number of events serialized in the commit.
  pub events_count: i64,

  /// Whether or not the commit has been processed by the store's dispatcher.
  pub dispatched: bool,
}

/// An attempt at a commit. This potential commit has not been written yet to the store.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CommitAttempt {
  /// The new aggregate_id.
  pub aggregate_id: Uuid,

  /// The new aggregate version.
  pub aggregate_version: i64,

  /// The new commit id.
  pub commit_id: Uuid,

  /// The new commit timestamp.
  ///
  /// May be overwritten by a more recent timestamp before or at write time.
  pub commit_timestamp: DateTime<Utc>,

  /// The new commit sequence.
  pub commit_sequence: i64,

  /// The metadata of the commit.
  pub metadata: serde_json::Value,

  /// The events to commit.
  pub events: serde_json::Value,

  /// The number of events serialized.
  pub events_count: i64,
}

/// A commit with the events deserialized.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeserializedCommit {
  /// Aggregate id.
  pub aggregate_id: Uuid,

  /// Aggregate version.
  pub aggregate_version: i64,

  /// commit id.
  pub commit_id: Uuid,

  /// Commit timestamp
  pub commit_timestamp: DateTime<Utc>,

  /// Commit sequence number.
  pub commit_sequence: i64,

  /// Commit number.
  pub commit_number: i64,

  /// Events, deserialized.
  pub events: serde_json::Value,

  /// Metadata, deserialized.
  pub metadata: serde_json::Value,

  /// Number of deserialized events.
  pub events_count: i64,

  /// Whether or not the commit has been dispatched.
  pub dispatched: bool,
}

impl Commit {
  /// Deserialize the events and metadata fields.
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