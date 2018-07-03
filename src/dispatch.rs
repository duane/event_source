use super::commit::Commit;
use super::store::*;
use std::error::Error;

pub trait DispatchDelegate {
  fn dispatch(&mut self, commit: &Commit) -> Result<(), Box<Error>>;
}

pub struct Dispatcher<D: DispatchDelegate> {
  pub dispatch_delegate: Box<D>,
}

impl<D: DispatchDelegate> Dispatcher<D> {
  pub fn new(delegate: Box<D>) -> Dispatcher<D> {
    Dispatcher {
      dispatch_delegate: delegate,
    }
  }

  pub fn dispatch<S: Store>(&mut self, store: &mut S) -> Result<(), Box<Error>> {
    let commits = store.get_undispatched_commits()?;
    for commit in commits.into_iter() {
      (*self.dispatch_delegate).dispatch(&commit)?;
      store.mark_commit_as_dispatched(commit.commit_id)?;
    }
    Ok(())
  }
}
