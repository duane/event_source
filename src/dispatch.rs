use super::commit::DeserializedCommit;
use super::store::*;

/// A type encoding logic for dispatching commits.
pub trait DispatchDelegate: Sized {
  /// Process a single commit in commit_sequence order.
  fn dispatch(&mut self, commit: &DeserializedCommit) -> Result<(), String>;
}

/// A single dispatcher.
pub struct Dispatcher<D: DispatchDelegate> {
  /// The delegate processing the commit.
  pub dispatch_delegate: D,
}

impl<D: DispatchDelegate> Dispatcher<D> {
  /// Create a new dispatcher from a delegate.
  pub fn new(delegate: D) -> Dispatcher<D> {
    Dispatcher {
      dispatch_delegate: delegate,
    }
  }

  /// Dispatch a single commit and update the store on success.
  ///
  /// If the delegate returns Ok, the store is instructed to mark the commit as dispatched.
  /// This is not atomic. The dispatcher should be designed to potentially process commits twice.
  pub fn dispatch<S: Store>(&mut self, store: &mut S) -> Result<(), String> {
    let commits = store
      .get_undispatched_commits()
      .map_err(|err| err.to_string())?;
    for commit in commits {
      self.dispatch_delegate.dispatch(&commit)?;
      store
        .mark_commit_as_dispatched(commit.commit_id)
        .map_err(|err| err.to_string())?;
    }
    Ok(())
  }
}

/// A dispatcher which does nothing.
pub struct NullDispatcher;
impl DispatchDelegate for NullDispatcher {
  fn dispatch(&mut self, _commit: &DeserializedCommit) -> Result<(), String> {
    Ok(())
  }
}
