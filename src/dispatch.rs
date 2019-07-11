use super::commit::Commit;
use super::store::*;

pub trait DispatchDelegate: Sized {
  fn dispatch(&mut self, commit: &Commit) -> Result<(), String>;
}

pub struct Dispatcher<D: DispatchDelegate> {
  pub dispatch_delegate: D,
}

impl<D: DispatchDelegate> Dispatcher<D> {
  pub fn new(delegate: D) -> Dispatcher<D> {
    Dispatcher {
      dispatch_delegate: delegate,
    }
  }

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

pub struct NullDispatcher;
impl DispatchDelegate for NullDispatcher {
  fn dispatch(&mut self, _commit: &Commit) -> Result<(), String> {
    Ok(())
  }
}
