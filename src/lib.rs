#![allow(unknown_lints)]
#![deny(missing_docs)]

//! event_source is a library for working with persistent event streams.

extern crate bytes;
extern crate chrono;
extern crate either;

extern crate async_trait;
extern crate chashmap;
extern crate pretty_env_logger;
extern crate serde;
extern crate serde_json;
extern crate uuid;

#[macro_use]
extern crate serde_derive;
#[cfg(any(feature = "httpd", feature = "httpc"))]
extern crate http;
#[cfg(any(feature = "httpd", feature = "httpc"))]
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

#[cfg(feature = "psql")]
extern crate postgres;
#[cfg(feature = "psql")]
extern crate native_tls;
#[cfg(feature = "psql")]
extern crate postgres_native_tls;

extern crate toml;

/// Type representing the result of events.
pub mod aggregate;

/// Type that handles reading of aggregates and execution of commands.
pub mod client;

/// Type of a mutation that may be applied to an event store.
pub mod command;

/// Type of a commit in the event store.
pub mod commit;

/// Type representing notification of events.
pub mod dispatch;

/// Type representing single events.
pub mod events;

/// Type representing the service that stores events over time.
pub mod store;

#[cfg(feature = "httpd")]
/// The HTTP server.
pub mod server;
