use warp::{self, path, Filter};

use commit::*;
use store::*;
use uuid::Uuid;

pub fn serve<S: Store>(f: &'static (Fn() -> S::Connection + Sync)) {
  let hello = path!("list" / String).map(move |aggregate_id: String| {
    let connection = f();
    let mut store = S::with_connection(connection);
    let commits = store
      .get_range(
        Uuid::parse_str(aggregate_id.as_str()).unwrap(),
        0,
        i64::max_value(),
      )
      .unwrap();

    let deserialized_commits: Vec<DeserializedCommit> =
      commits.into_iter().map(|c| c.deserialize()).collect();
    serde_json::to_string(&deserialized_commits).unwrap()
  });
  warp::serve(hello).run(([127, 0, 0, 1], 3030));
}
