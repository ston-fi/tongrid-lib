//! Contract classification with a caller-supplied predefined-trait lookup.
//! Application configuration and live orchestration belong to the caller.
mod handler;

pub use handler::classifier::Classifier;
pub use handler::ClassifierHandler;

pub mod config;

#[cfg(test)]
#[path = "tests/test_classifier_config.rs"]
mod test_classifier_config;
