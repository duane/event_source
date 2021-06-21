use crate::dispatch::{Dispatcher, DispatchDelegate};
use crate::store::{Store, StoreError};
use crate::commit::CommitAttempt;

/// A read/write client for interfacing with a store and event schema.
pub struct WriteClient<D: DispatchDelegate, S: Store> {
  /// State for processing events after commit.
  pub dispatcher: Dispatcher<D>,

  /// The store holding events.
  pub store: S,
}

impl<D: DispatchDelegate, S: Store> WriteClient<D, S> {
}
