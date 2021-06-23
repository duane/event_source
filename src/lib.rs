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

extern crate tokio;
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

/// A dispatch for asynchronous subscription.
pub mod dispatch_async;

/// Type representing single events.
pub mod events;

/// Type representing the service that stores events over time.
pub mod store;
