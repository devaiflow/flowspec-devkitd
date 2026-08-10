//! Orchestration layer: domain decisions -> port effects.

pub mod flow_source;
pub mod ids;
pub mod ports;
pub mod recovery;
pub mod scheduler;
pub mod use_cases;
pub mod wire;

#[cfg(any(test, feature = "test-fakes"))]
pub mod testkit;
