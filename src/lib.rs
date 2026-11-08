pub mod calculations;
pub mod config;
pub mod infrastructure;
pub mod model;
pub mod pipeline;

#[cfg(all(feature = "integration-test-hooks", debug_assertions))]
mod test_hooks;
