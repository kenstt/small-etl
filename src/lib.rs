pub mod config;
pub mod core;
pub mod utils;

pub use config::{cli::LocalStorage, CliConfig};
pub use core::{etl::EtlEngine, pipeline::SimplePipeline};
pub use utils::error::{EtlError, Result};
