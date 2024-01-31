#![allow(dead_code)]

mod builder;
mod environment;

pub use builder::ClientBuilder;
pub use environment::{EnvironmentBuilder, RuntimeContext};

/// The core Zgs client.
///
/// Holds references to running services, cleanly shutting them down when dropped.
pub struct Client;
