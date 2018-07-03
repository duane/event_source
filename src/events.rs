use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait Event: Serialize + DeserializeOwned {}
