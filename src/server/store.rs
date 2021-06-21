use warp::{path, Filter};

use crate::commit::*;
use crate::store::*;
use uuid::Uuid;

use serde_derive::{Deserialize, Serialize};

#[derive(Serialize,Deserialize)]
struct StoreQueryParams {
  from: Option<i64>,
  to: Option<i64>
}

/// Return a list of commits for the given aggregate id.
pub fn commit_list<S: Store, Fs: Fn() -> S>(
  store_factory: &Fs,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
  Fs: Clone + Send,
{
  let owned_store_factory = store_factory.clone();
  path!("store" / Uuid / "commits")
    .and(warp::query::<StoreQueryParams>())
    .map(move |aggregate_id: Uuid, params: StoreQueryParams| {
    let store = owned_store_factory();
    let commits = store.get_range(aggregate_id, params.from.unwrap_or(0), params.to.unwrap_or(i64::max_value())).unwrap();

    let deserialized_commits: Vec<DeserializedCommit> =
      commits.into_iter().map(|c| c.deserialize()).collect();
    warp::reply::json(&deserialized_commits)
  })
}
