use crate::client::applicable::MutEventApplicable;
use crate::client::event_source::EventSource;
use uuid::Uuid;
use crate::commit::Commit;

/// A datatype that may be mutated with an event.
pub trait SyncStreamable<S: EventSource>: MutEventApplicable {
  /// Return the current position in the stream.
  fn pos(&self) -> i64;

  /// Update the stream position.
  fn set_pos(&mut self, pos: i64);

  /// Return the aggregate id in question.
  fn aggregate_id(&self) -> &Uuid;

  /// Return the backing store of the stream
  fn store(&mut self) -> &mut S;

  /// Sync the streamable to the latest stream position.
  fn sync(&mut self) {
    let aggregate_id = self.aggregate_id().to_owned();
    let pos = self.pos();
    let commits: Vec<Commit> = {
      self
        .store()
        .get_range(aggregate_id, pos, i64::max_value())
        .unwrap()
    };

    for commit in commits {
      let events: Vec<Self::Event> = serde_json::from_value(commit.events).unwrap();
      for ref event in events {
        self.apply(event);
      }
      self.set_pos(commit.commit_sequence);
    }
  }
}
