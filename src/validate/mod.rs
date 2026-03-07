pub mod message;
pub mod router;
pub mod schema;
pub mod topic;

// Re-export the core types so `main.rs` can just `use crate::validate::{Router, HandledMessage};`
pub use message::{HandledMessage, MessageType};
pub use router::{Route, Router};