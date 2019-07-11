pub mod aggregate;
pub mod dispatch;
pub mod store;

use command::Command;
use serde::de::DeserializeOwned;
use serde::Serialize;
use server::aggregate::commit;
use server::aggregate::get_latest;
use server::dispatch::WebSocketSubscriptions;
use server::store::commit_list;
use std::sync::Arc;
use store::Store;
use warp::Filter;

pub struct Server {
  subscriptions_state: WebSocketSubscriptions,
}

impl Clone for Server {
  fn clone(&self) -> Self {
    Server {
      subscriptions_state: self.subscriptions_state.clone(),
    }
  }
}

impl Default for Server {
  fn default() -> Self {
    Server {
      subscriptions_state: Default::default(),
    }
  }
}

impl Server {
  pub fn serve<
    S: Store + 'static,
    C: Command + Serialize + DeserializeOwned + 'static,
    Fs: Fn() -> S + Sync,
  >(
    &'static self,
    store_factory: &'static Fs,
  ) -> Result<(), String>
  where
    Fs: Clone + Send + Sync,
    C::Aggregate: Serialize,
  {
    let get_latest_route = get_latest::<S, C::Aggregate, Fs>(&store_factory);
    let commit_list_route = commit_list(&store_factory);
    let commit_subscription_route = self.subscriptions_state.commit_subscription();
    let f = move || self.subscriptions_state.clone();
    let commit_route = commit::<_, _, C, _, _>(&store_factory, &f);
    let get_routes = warp::get2().and(commit_list_route.or(get_latest_route));
    let post_routes = warp::post2().and(commit_route);
    let routes = commit_subscription_route.or(get_routes).or(post_routes);
    info!("Starting server at 127.0.0.1:4321");
    warp::serve(routes).run(([127, 0, 0, 1], 4321));
    info!("Server shut down, exiting cleanly....");
    Ok(())
  }
}
