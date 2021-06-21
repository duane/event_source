/// Endpoints for querying the aggregate directly.
pub mod aggregate;
// pub mod dispatch;
/// Endpoints for interacting with the HTTP store.
pub mod store;

use crate::command::Command;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::server::aggregate::commit;
use crate::server::aggregate::get_latest;
// use crate::server::dispatch::WebSocketSubscriptions;
use crate::server::store::commit_list;
use crate::store::Store;
use warp::Filter;
use log::info;
use crate::dispatch::NullDispatcher;

/// The HTTP server state itself.
pub struct Server {
  // subscriptions_state: WebSocketSubscriptions,
}

impl Clone for Server {
  fn clone(&self) -> Self {
    Server {
      // subscriptions_state: self.subscriptions_state.clone(),
    }
  }
}

impl Default for Server {
  fn default() -> Self {
    Server {
      // subscriptions_state: Default::default(),
    }
  }
}

impl Server {
  /// Run the server thread.
  pub async fn serve<
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
    // let commit_subscription_route = self.subscriptions_state.commit_subscription();
    // let f = move || self.subscriptions_state.clone();
    let commit_route = commit::<_, _, C, _, _>(&store_factory, &|| NullDispatcher {});
    let get_routes = warp::get().and(commit_list_route.or(get_latest_route));
    let post_routes = warp::post().and(commit_route);
    // let routes = commit_subscription_route.or(get_routes).or(post_routes);
    let routes = get_routes.or(post_routes);
    info!("Starting server at 127.0.0.1:4321");
    warp::serve(routes).run(([127, 0, 0, 1], 4321)).await;
    info!("Server shut down, exiting cleanly....");
    Ok(())
  }
}
