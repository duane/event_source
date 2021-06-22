use crate::client::applicable::EventApplicable;
use crate::client::ClientError;
use crate::command::Command;
use crate::commit::Commit;

use crate::client::command_processor::CommandProcessor;
use either::Either;
use uuid::Uuid;

/// A write client for the store.
pub struct AggregateStream<C: Command, P: CommandProcessor<C>> {
  aggregate_id: Uuid,
  processor: P,
  aggregate: C::Aggregate,
  commit_sequence: i64,
}

impl<C: Command, P: CommandProcessor<C>> AggregateStream<C, P> {
  /// Produce a new, empty aggregate from a store, dispatcher, and aggregate.
  pub fn new(aggregate_id: Uuid, processor: P) -> Self {
    AggregateStream {
      aggregate_id,
      processor,
      aggregate: Default::default(),
      commit_sequence: 0,
    }
  }

  /// Fetch and apply the events producing the aggregate from the stream.
  pub fn sync(&mut self) -> Result<(), ClientError> {
    let commits: Vec<Commit> = {
      self
        .processor
        .get_range(self.aggregate_id, self.commit_sequence, i64::max_value())
        .map_err(ClientError::StoreError)?
    };
    for commit in commits {
      let events: Vec<<C::Aggregate as EventApplicable>::Event> =
        serde_json::from_value(commit.events).unwrap();
      for event in events {
        self.aggregate = self.aggregate.apply(&event);
      }
      self.commit_sequence = commit.commit_sequence;
    }
    Ok(())
  }

  /// Issue a command to the processor.
  ///
  /// The command may fail for semantic or processor reasons.
  pub fn issue_command(&mut self, command: &C) -> Result<Commit, Either<ClientError, C::Error>> {
    self
      .processor
      .issue_command(&self.aggregate_id, command)
      .and_then(|commit| self.sync().map_err(Either::Left).map(|_| commit))
  }
}
