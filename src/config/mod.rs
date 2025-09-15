pub mod cli;

use crate::core::ConfigProvider;
use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Parser)]
#[command(name = "samll-etl")]
#[command(about = "A small ETL tool for data processing")]
pub struct CliConfig {
    #[arg(long, default_value = "https://jsonplaceholder.typicode.com/posts")]
    pub api_endpoint: String,

    #[arg(long, default_value = "./output")]
    pub output_path: String,

    #[arg(long, value_delimiter = ',')]
    pub lookup_files: Vec<String>,

    #[arg(long, default_value = "5")]
    pub concurrent_requests: usize,

    #[arg(long, help = "Enable verbose output")]
    pub verbose: bool,
}

impl ConfigProvider for CliConfig {
    fn api_endpoint(&self) -> &str {
        &self.api_endpoint
    }

    fn output_path(&self) -> &str {
        &self.output_path
    }

    fn lookup_files(&self) -> &[String] {
        &self.lookup_files
    }

    fn concurrent_requests(&self) -> usize {
        self.concurrent_requests
    }
}
