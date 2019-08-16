use warp::{self, Filter, Future, Sink};

use chashmap::CHashMap;
use commit::Commit;
use dispatch::DispatchDelegate;
use futures::future::join_all;
use futures::stream::Stream;
use futures::sync::mpsc;
use serde::Serialize;
use serde_json::Serializer as JsonSerializer;
use std::str::from_utf8;
use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};
use uuid::Uuid;
use warp::filters::ws::{Message, WebSocket};

static SUBSCRIBER_ID: AtomicUsize = AtomicUsize::new(1);

type AggregateMap = Arc<CHashMap<Uuid, CHashMap<usize, mpsc::UnboundedSender<Message>>>>;

#[derive(Clone)]
pub struct WebSocketSubscriptions {
  pub aggregate_map: AggregateMap,
}

impl DispatchDelegate for WebSocketSubscriptions {
  fn dispatch(&mut self, commit: &Commit) -> Result<(), String> {
    self.publish(commit.clone());
    Ok(())
  }
}

impl Default for WebSocketSubscriptions {
  fn default() -> Self {
    WebSocketSubscriptions {
      aggregate_map: Default::default(),
    }
  }
}

impl WebSocketSubscriptions {
  pub fn commit_subscription(
    &self,
  ) -> impl Filter<Error = warp::Rejection, Extract = (impl warp::Reply,)> {
    let state_handle = Arc::clone(&self.aggregate_map);
    warp::path("commits")
      .and(warp::path::param())
      .and(warp::ws2())
      .map(move |aggregate_id: Uuid, ws: warp::ws::Ws2| {
        let state_handle = Arc::clone(&state_handle);
        ws.on_upgrade(move |websocket| subscribe(aggregate_id, state_handle, websocket))
      })
  }

  fn publish(&self, commit: Commit) -> impl Future<Item = (), Error = ()> {
    let option = self.aggregate_map.get(&commit.aggregate_id);
    let mut serialized_buffer = Vec::<u8>::new();
    {
      let mut buffer_serializer = JsonSerializer::new(&mut serialized_buffer);
      commit
        .deserialize()
        .serialize(&mut buffer_serializer)
        .unwrap();
    }
    match option {
      Some(ref subscriber_map_guard) => {
        let subscriber_map = (*subscriber_map_guard).clone();
        let futures = subscriber_map.into_iter().map(move |(_, subscriber)| {
          subscriber
            .send(Message::text(
              from_utf8(serialized_buffer.as_slice()).unwrap(),
            ))
            .wait()
            .map_err(|err| error!("publish error: {:?}", err))
        });
        join_all(futures)
      }
      None => {
        unreachable!("No subscriber to contact!");
      }
    }
    .map_err(|_| ())
    .map(|_| ())
  }
}

fn disconnect(aggregate_id: Uuid, aggregate_map: &AggregateMap, subscriber_id: usize) {
  info!("disconnecting subscriber {}", subscriber_id);
  aggregate_map.alter(aggregate_id, |subscriber_by_id_map| {
    subscriber_by_id_map.map(|subscriber_by_id| {
      subscriber_by_id.remove(&subscriber_id);
      subscriber_by_id
    })
  });
}

fn subscribe(
  aggregate_id: Uuid,
  aggregate_map: AggregateMap,
  websocket: WebSocket,
) -> impl Future<Item = (), Error = ()> {
  let subscriber_id = SUBSCRIBER_ID.fetch_add(1, Ordering::Relaxed);
  let (subscriber_ws_tx, subscriber_ws_rx) = websocket.split();
  let (tx, rx) = mpsc::unbounded();
  let tx_clone = tx.clone();
  let after_clone = tx.clone();
  let state_handle = Arc::clone(&aggregate_map);
  warp::spawn(
    rx.map_err(|()| -> warp::Error { unreachable!("unbounded rx never errors") })
      .forward(subscriber_ws_tx)
      .map(|_tx_rx| ())
      .map_err(|ws_err| error!("websocket send error: {}", ws_err)),
  );
  {
    state_handle.upsert(
      aggregate_id,
      || {
        let new_map = CHashMap::new();
        new_map.insert_new(subscriber_id, after_clone);
        new_map
      },
      |hash_map| {
        hash_map.insert(subscriber_id, tx_clone);
      },
    );
  }

  info!("new subscriber: {}", subscriber_id);
  subscriber_ws_rx
    .for_each(|_| Ok(()))
    .map_err(move |err| {
      error!("websocket error: {:?}", err);
    })
    .then(move |_: Result<(), ()>| {
      disconnect(aggregate_id, &state_handle, subscriber_id);
      Ok(())
    })
}
