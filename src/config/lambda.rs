#[cfg(feature = "lambda")]
use crate::core::{ConfigProvider, Storage};
#[cfg(feature = "lambda")]
use crate::utils::error::Result;
#[cfg(feature = "lambda")]
use aws_sdk_s3::Client as S3Client;
#[cfg(feature = "lambda")]
use std::env;

#[cfg(feature = "lambda")]
#[derive(Debug, Clone)]
pub struct LambdaConfig {
    pub api_endpoint: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub concurrent_requests: usize,
}

#[cfg(feature = "lambda")]
impl LambdaConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            api_endpoint: env::var("API_ENDPOINT")
                .unwrap_or_else(|_| "https://jsonplaceholder.typicode.com/posts".to_string()),
            s3_bucket: env::var("S3_BUCKET")
                .map_err(|_| crate::utils::error::EtlError::ConfigError {
                    message: "S3_BUCKET environment variable is required".to_string(),
                })?,
            s3_prefix: env::var("S3_PREFIX").unwrap_or_else(|_| "etl-output".to_string()),
            concurrent_requests: env::var("CONCURRENT_REQUESTS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        })
    }
}

#[cfg(feature = "lambda")]
impl ConfigProvider for LambdaConfig {
    fn api_endpoint(&self) -> &str {
        &self.api_endpoint
    }

    fn output_path(&self) -> &str {
        &self.s3_prefix
    }

    fn lookup_files(&self) -> &[String] {
        &[]
    }

    fn concurrent_requests(&self) -> usize {
        self.concurrent_requests
    }
}

#[cfg(feature = "lambda")]
#[derive(Debug, Clone)]
pub struct S3Storage {
    client: S3Client,
    bucket: String,
}

#[cfg(feature = "lambda")]
impl S3Storage {
    pub fn new(client: S3Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

#[cfg(feature = "lambda")]
impl Storage for S3Storage {
    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|e| crate::utils::error::EtlError::ConfigError {
                message: format!("Failed to read from S3: {}", e),
            })?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| crate::utils::error::EtlError::ConfigError {
                message: format!("Failed to collect S3 data: {}", e),
            })?;

        Ok(data.into_bytes().to_vec())
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(data.to_vec().into())
            .send()
            .await
            .map_err(|e| crate::utils::error::EtlError::ConfigError {
                message: format!("Failed to write to S3: {}", e),
            })?;

        Ok(())
    }
}