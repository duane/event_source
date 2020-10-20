use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
/// A single event type.
///
/// This effectively encapsulates serialization logic for individual events.
pub trait Event: Serialize + DeserializeOwned + Debug {}
