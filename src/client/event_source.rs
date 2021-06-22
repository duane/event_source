use crate::commit::Commit;
use crate::store::StoreError;
use uuid::Uuid;

/// A trait allowing committing, retrieving, and issuing commands.
pub trait EventSource: Sized {
  /// The backing database state.
  // type Connection;

  /// Back the store with the given connection.
  // fn with_connection(connection: Self::Connection) -> Self;

  /// Get a sequence of events specific to the aggregate id.
  ///
  /// Because the minimum and maximum bounds are passed, you may get zero commits.
  /// Getting all commits between 0 and the latest commit id should retrieve all events associated with the aggregate.
  fn get_range(
    &mut self,
    aggregate_id: Uuid,
    min_version: i64,
    max_version: i64,
  ) -> Result<Vec<Commit>, Box<dyn StoreError>>;
}
