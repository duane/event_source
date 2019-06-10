#![feature(associated_type_defaults)]
#![allow(unknown_lints)]
#![warn(clippy)]

extern crate chrono;
extern crate either;
extern crate rusqlite;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate uuid;
extern crate warp;

#[cfg(test)]
#[macro_use]
extern crate serde_derive;

pub mod aggregate;
pub mod client;
pub mod command;
pub mod commit;
pub mod dispatch;
pub mod events;
pub mod server;
pub mod store;
