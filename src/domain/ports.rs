use crate::domain::model::{Record, TransformResult};
use crate::utils::error::Result;
use async_trait::async_trait;

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

#[async_trait]
pub trait Pipeline: Send + Sync {
    async fn extract(&self) -> Result<Vec<Record>>;
    async fn transform(&self, data: Vec<Record>) -> Result<TransformResult>;
    async fn load(&self, result: TransformResult) -> Result<String>;
}
