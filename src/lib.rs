pub mod config;
pub mod core;
pub mod utils;

// New layered skeleton modules (PR1). No behavior yet.
pub mod adapters;
pub mod app;
pub mod domain;

#[cfg(feature = "cli")]
pub use config::{cli::LocalStorage, CliConfig};

#[cfg(feature = "lambda")]
pub use config::lambda::{LambdaConfig, S3Storage};

pub use core::{etl::EtlEngine, mvp_pipeline::MvpPipeline, pipeline::SimplePipeline};
pub use utils::error::{EtlError, Result};
