use warp::{path, Filter};

use commit::*;
use store::*;
use uuid::Uuid;

pub fn commit_list<S: Store, Fs: Fn() -> S>(
  store_factory: &Fs,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
  Fs: Clone + Send,
{
  let owned_store_factory = store_factory.clone();
  path!("store" / Uuid / "commits").map(move |aggregate_id: Uuid| {
    let store = owned_store_factory();
    let commits = store.get_range(aggregate_id, 0, i64::max_value()).unwrap();

    let deserialized_commits: Vec<DeserializedCommit> =
      commits.into_iter().map(|c| c.deserialize()).collect();
    warp::reply::json(&deserialized_commits)
  })
}
