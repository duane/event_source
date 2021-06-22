use warp::{path, Filter};

use crate::store::*;
use uuid::Uuid;

use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct StoreQueryParams {
  from: Option<i64>,
  to: Option<i64>,
}

/// Return a list of commits for the given aggregate id.
pub fn commit_list<Fs: Fn() -> Box<dyn Store>>(
  store_factory: &Fs,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
  Fs: Clone + Send,
{
  let owned_store_factory = store_factory.clone();
  path!("store" / Uuid / "commits")
    .and(warp::query::<StoreQueryParams>())
    .map(move |aggregate_id: Uuid, params: StoreQueryParams| {
      let mut store = owned_store_factory();
      let commits = store
        .get_range(
          aggregate_id,
          params.from.unwrap_or(0),
          params.to.unwrap_or(i64::max_value()),
        )
        .unwrap();

      warp::reply::json(&commits)
    })
}

/// Return a list of commits for the given aggregate id.
pub fn commit_get<Fs: Fn() -> Box<dyn Store>>(
  store_factory: &Fs,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
  Fs: Clone + Send,
{
  let owned_store_factory = store_factory.clone();
  path!("store" / Uuid).map(move |commit_id: Uuid| {
    let mut store = owned_store_factory();
    let commit = store.get_commit(&commit_id).unwrap();
    warp::reply::json(&commit)
  })
}
