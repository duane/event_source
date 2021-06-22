use warp::{path, Filter};

use crate::aggregate::Aggregate;
use crate::client::ClientBuilder;
use crate::command::Command;
use crate::dispatch::{DispatchDelegate, NullDispatcher};
use crate::store::Store;
use either::{Left, Right};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
/// blah
pub struct HttpErrorResponse {
  /// hah
  pub error: serde_json::Value,
}

/// Fetch the latest aggregate for a uuid.
pub fn get_latest<S: Store, A: Aggregate + Serialize, Fs: Fn() -> S>(
  store_factory: &Fs,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
  Fs: Clone + Send,
{
  let owned_factory = store_factory.clone();
  path!("aggregate" / Uuid / "latest").map(move |aggregate_id: Uuid| {
    let store = owned_factory();
    let dispatcher = NullDispatcher {};
    let mut client = ClientBuilder::default()
      .with_store(store)
      .with_dispatch_delegate(dispatcher)
      .finish()
      .unwrap();
    let aggregate: A = client.fetch_latest(aggregate_id).unwrap();
    warp::reply::json(&aggregate)
  })
}

/// Issue a command to the aggregate.
pub fn commit<
  S: Store,
  D: DispatchDelegate,
  C: Command + Serialize + DeserializeOwned,
  Fs: Fn() -> S,
  Fd: Fn() -> D,
>(
  store_factory: &Fs,
  dispatch_factory: &Fd,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
  Fs: Clone + Send,
  Fd: Clone + Send,
  C::Aggregate: Serialize,
{
  let owned_store_factory = store_factory.clone();
  let owned_dispatch_factory = dispatch_factory.clone();
  path!("commit" / Uuid)
    .and(warp::body::json())
    .map(move |aggregate_id: Uuid, command: C| {
      let store = owned_store_factory();
      let dispatch = owned_dispatch_factory();
      let mut client = ClientBuilder::default()
        .with_store(store)
        .with_dispatch_delegate(dispatch)
        .finish()
        .unwrap();
      let aggregate = client.fetch_latest(aggregate_id).unwrap();

      match client.issue_command(&aggregate, &command, &command) {
        Ok(ref commit) => {
          warp::reply::with_status(warp::reply::json(&commit), http::status::StatusCode::OK)
        }
        Err(Left(error)) => warp::reply::with_status(
          warp::reply::json(&HttpErrorResponse {
            error: serde_json::to_value(format!("{:#?}", error)).unwrap(),
          }),
          http::status::StatusCode::UNPROCESSABLE_ENTITY,
        ),
        Err(Right(error)) => warp::reply::with_status(
          warp::reply::json(&HttpErrorResponse {
            error: serde_json::to_value(error).unwrap(),
          }),
          http::status::StatusCode::UNPROCESSABLE_ENTITY,
        ),
      }
    })
}
