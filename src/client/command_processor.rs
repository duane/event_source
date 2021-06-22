use crate::client::event_source::EventSource;
use crate::client::ClientError;
use crate::command::Command;
use crate::commit::Commit;
use either::Either;
use uuid::Uuid;

/// A trait allowing committing, retrieving, and issuing commands.
pub trait CommandProcessor<C: Command>: Sized + EventSource {
  /// Apply a command to an aggregate.
  fn issue_command(
    &mut self,
    aggregate_id: &Uuid,
    command: &C,
  ) -> Result<Commit, Either<ClientError, C::Error>>;
}
