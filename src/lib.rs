#![feature(associated_type_defaults)]
#![allow(unknown_lints)]

extern crate bytes;
extern crate chrono;
extern crate either;
extern crate rusqlite;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate futures;
extern crate pretty_env_logger;
extern crate log;
extern crate chashmap;
extern crate hyper;
extern crate rusoto_core;
extern crate rusoto_dynamodb;
extern crate tokio_timer;
extern crate uuid;
extern crate warp;
pub mod aggregate;
pub mod client;
pub mod command;
pub mod commit;
pub mod dispatch;
pub mod events;
// pub mod server;
pub mod store;
