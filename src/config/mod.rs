pub mod cli;

#[cfg(feature = "lambda")]
pub mod lambda;

#[cfg(feature = "cli")]
use crate::core::ConfigProvider;
#[cfg(feature = "cli")]
use clap::Parser;
#[cfg(feature = "cli")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "cli")]
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

    #[arg(long, help = "Enable system resource monitoring (CPU/Memory)")]
    pub monitor: bool,
}

#[cfg(feature = "cli")]
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

#[cfg(feature = "cli")]
impl crate::utils::validation::Validate for CliConfig {
    fn validate(&self) -> crate::utils::error::Result<()> {
        use crate::utils::validation::*;

        // 驗證API端點
        validate_url("api_endpoint", &self.api_endpoint)?;

        // 驗證輸出路徑
        validate_path("output_path", &self.output_path)?;

        // 驗證並發請求數
        validate_positive_number("concurrent_requests", self.concurrent_requests, 1)?;
        validate_range("concurrent_requests", self.concurrent_requests, 1, 100)?;

        // 驗證lookup文件
        if !self.lookup_files.is_empty() {
            validate_file_extensions("lookup_files", &self.lookup_files, &["csv", "tsv", "json"])?;
        }

        tracing::info!("✅ Configuration validation passed");
        Ok(())
    }
}
