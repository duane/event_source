#![feature(associated_type_defaults)]

extern crate chrono;
extern crate rusqlite;
extern crate serde;
extern crate serde_json;
extern crate uuid;

#[cfg(test)]
#[macro_use]
extern crate serde_derive;

pub mod aggregate;
pub mod client;
pub mod command;
pub mod commit;
pub mod dispatch;
pub mod events;
pub mod store;
