use super::commit::Commit;
use super::store::*;

pub trait DispatchDelegate {
  fn dispatch(&mut self, commit: &Commit) -> Result<(), Box<String>>;
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

  pub fn dispatch<S: Store>(&mut self, store: &mut S) -> Result<(), Box<String>> {
    let commits = store
      .get_undispatched_commits()
      .map_err(|err| err.to_string())?;
    for commit in commits.into_iter() {
      (*self.dispatch_delegate).dispatch(&commit)?;
      store
        .mark_commit_as_dispatched(commit.commit_id)
        .map_err(|err| err.to_string())?;
    }
    Ok(())
  }
}
