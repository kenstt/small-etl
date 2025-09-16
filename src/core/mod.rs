pub mod etl;
pub mod mvp_pipeline;
pub mod pipeline;

use crate::utils::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub data: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct TransformResult {
    pub processed_records: Vec<Record>,
    pub csv_output: String,
    pub tsv_output: String,
    pub intermediate_data: Vec<Record>,
}

pub trait Storage: Send + Sync {
    fn read_file(&self, path: &str) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send;
    fn write_file(
        &self,
        path: &str,
        data: &[u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

pub trait ConfigProvider: Send + Sync {
    fn api_endpoint(&self) -> &str;
    fn output_path(&self) -> &str;
    fn lookup_files(&self) -> &[String];
    fn concurrent_requests(&self) -> usize;
}

#[async_trait::async_trait]
pub trait Pipeline: Send + Sync {
    async fn extract(&self) -> Result<Vec<Record>>;
    async fn transform(&self, data: Vec<Record>) -> Result<TransformResult>;
    async fn load(&self, result: TransformResult) -> Result<String>;
}
