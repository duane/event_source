use super::commit::Commit;
use super::store::*;
use tokio::sync::mpsc::{Sender, channel, Receiver};

/// A single dispatcher.
pub struct AsyncDispatcher {
  subscribers: Vec<Sender<Commit>>,
  store: Box<dyn Store>,
}

/// A type used to distribute updates to (many) subscribers.
pub trait Dispatcher {
  /// Trigger a dispatch—send all undispatched commits.
  fn dispatch(&mut self) -> Result<(), String>;

  /// Subscribe to all commits following the one indicated in the commit number.
  fn subscribe(&mut self, commit_number: i64) -> Result<Receiver<Commit>, String>;
}

impl Dispatcher for AsyncDispatcher {
  /// Dispatch a single commit and update the store on success.
  ///
  /// If the delegate returns Ok, the store is instructed to mark the commit as dispatched.
  /// This is not atomic. The dispatcher should be designed to potentially process commits twice.
 fn dispatch(&mut self) -> Result<(), String> {
    let commits = self.store.as_mut()
      .get_undispatched_commits()
      .map_err(|err| err.to_string())?;
    for commit in commits {
      self.dispatch_single(&commit)?;
    }
    Ok(())
  }

  /// Subscribe to commits submitted to this dispatcher.
  fn subscribe(&mut self, commit_number: i64) -> Result<Receiver<Commit>, String> {
    let (sender, receiver) = channel(1024);

    let commits = self.store.as_mut().get_range(commit_number, i64::max_value())
      .map_err(|err| err.to_string())?;

    for commit in commits {
      sender.blocking_send(commit)
        .map_err(|err| err.to_string())?;
    }

    self.subscribers.push(sender);

    Ok(receiver)
  }
}

impl AsyncDispatcher {
  /// Create a new dispatcher from a delegate.
  pub fn new(store: Box<dyn Store>) -> AsyncDispatcher {
    AsyncDispatcher { subscribers: vec![], store }
  }

  fn dispatch_single(&mut self, commit: &Commit) -> Result<(), String> {
    for subscriber in self.subscribers.iter() {
      subscriber.blocking_send(commit.to_owned())
      .map_err(|err| err.to_string())?;
    }
    self.store.as_mut()
        .mark_commit_as_dispatched(commit.commit_id)
        .map_err(|err| err.to_string())?;
    Ok(())
  }
}
