use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

pub trait Event: Serialize + DeserializeOwned + Debug {}
