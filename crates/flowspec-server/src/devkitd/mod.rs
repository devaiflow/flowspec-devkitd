//! Real devkitd MCP client adapter — implements `flowspec_app::ports::Devkitd`
//! against a live (or stub) devkitd Streamable HTTP MCP server.

mod client;

pub use client::{DevkitdClient, DevkitdClientConfig};
