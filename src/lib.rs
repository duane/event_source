#![allow(unknown_lints)]

extern crate bytes;
extern crate chrono;
extern crate either;

extern crate serde;
extern crate serde_json;
extern crate uuid;
extern crate pretty_env_logger;
extern crate chashmap;

#[macro_use]
extern crate serde_derive;
#[cfg(feature = "httpd")]
extern crate hyper;
#[cfg(feature = "dynamo")]
extern crate rusoto_core;
#[cfg(feature = "dynamo")]
extern crate rusoto_dynamodb;
#[cfg(feature = "httpd")]
extern crate tokio_timer;
#[cfg(feature = "httpd")]
extern crate warp;

#[cfg(any(feature = "httpd", feature = "dynamo"))]
extern crate futures;
#[cfg(feature = "httpd")]
extern crate log;

#[cfg(feature = "sqlite")]
extern crate rusqlite;

pub mod aggregate;
pub mod client;
pub mod command;
pub mod commit;
pub mod dispatch;
pub mod events;

pub mod store;

#[cfg(feature = "httpd")]
pub mod server;
