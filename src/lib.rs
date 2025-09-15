pub mod config;
pub mod core;
pub mod utils;

#[cfg(feature = "cli")]
pub use config::{cli::LocalStorage, CliConfig};

#[cfg(feature = "lambda")]
pub use config::lambda::{LambdaConfig, S3Storage};

pub use core::{etl::EtlEngine, pipeline::SimplePipeline};
pub use utils::error::{EtlError, Result};
