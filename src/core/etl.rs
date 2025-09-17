pub use crate::domain::services::etl_engine::EtlEngine;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ConfigProvider, Record, Storage, TransformResult};
    use crate::domain::ports::Pipeline;
    use crate::utils::error::{EtlError, Result};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct MockStorage {
        files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                files: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl Storage for MockStorage {
        async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
            let files = self.files.lock().await;
            files.get(path).cloned().ok_or_else(|| {
                EtlError::IoError(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File not found: {}", path),
                ))
            })
        }

        async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
            let mut files = self.files.lock().await;
            files.insert(path.to_string(), data.to_vec());
            Ok(())
        }
    }

    struct MockConfig {
        api_endpoint: String,
        output_path: String,
    }

    impl MockConfig {
        fn new() -> Self {
            Self {
                api_endpoint: "http://test.com".to_string(),
                output_path: "test_output".to_string(),
            }
        }
    }

    impl ConfigProvider for MockConfig {
        fn api_endpoint(&self) -> &str {
            &self.api_endpoint
        }

        fn output_path(&self) -> &str {
            &self.output_path
        }

        fn lookup_files(&self) -> &[String] {
            &[]
        }

        fn concurrent_requests(&self) -> usize {
            5
        }
    }

    struct MockPipeline {
        _storage: MockStorage,
        _config: MockConfig,
        should_fail_at: Option<String>, // "extract", "transform", or "load"
    }

    impl MockPipeline {
        fn new() -> Self {
            Self {
                _storage: MockStorage::new(),
                _config: MockConfig::new(),
                should_fail_at: None,
            }
        }

        fn with_failure_at(mut self, stage: &str) -> Self {
            self.should_fail_at = Some(stage.to_string());
            self
        }
    }

    #[async_trait::async_trait]
    impl Pipeline for MockPipeline {
        async fn extract(&self) -> Result<Vec<Record>> {
            if self.should_fail_at.as_deref() == Some("extract") {
                return Err(EtlError::DataValidationError {
                    message: "Mock extract failure".to_string(),
                });
            }

            let mut records = Vec::new();
            for i in 1..=3 {
                let mut data = HashMap::new();
                data.insert("id".to_string(), serde_json::Value::Number(i.into()));
                data.insert(
                    "name".to_string(),
                    serde_json::Value::String(format!("Item {}", i)),
                );
                records.push(Record { data });
            }
            Ok(records)
        }

        async fn transform(&self, data: Vec<Record>) -> Result<TransformResult> {
            if self.should_fail_at.as_deref() == Some("transform") {
                return Err(EtlError::TransformationError {
                    stage: "test".to_string(),
                    details: "Mock transformation failure".to_string(),
                });
            }

            Ok(TransformResult {
                processed_records: data,
                csv_output: "id,name\n1,Item 1\n2,Item 2\n3,Item 3".to_string(),
                tsv_output: "id\tname\n1\tItem 1\n2\tItem 2\n3\tItem 3".to_string(),
                intermediate_data: vec![],
            })
        }

        async fn load(&self, _result: TransformResult) -> Result<String> {
            if self.should_fail_at.as_deref() == Some("load") {
                return Err(EtlError::IoError(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "Permission denied",
                )));
            }

            Ok("test_output/etl_output.zip".to_string())
        }
    }

    #[tokio::test]
    async fn test_etl_engine_successful_run() {
        let pipeline = MockPipeline::new();
        let engine = EtlEngine::new(pipeline);

        let result = engine.run().await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_output/etl_output.zip");
    }

    #[tokio::test]
    async fn test_etl_engine_with_monitoring() {
        let pipeline = MockPipeline::new();
        let engine = EtlEngine::new_with_monitoring(pipeline, true);

        let result = engine.run().await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_output/etl_output.zip");
    }

    #[tokio::test]
    async fn test_etl_engine_extract_failure() {
        let pipeline = MockPipeline::new().with_failure_at("extract");
        let engine = EtlEngine::new(pipeline);

        let result = engine.run().await;

        assert!(result.is_err());
        match result.unwrap_err() {
            EtlError::DataValidationError { message } => {
                assert_eq!(message, "Mock extract failure");
            }
            other => panic!("Expected DataValidationError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_etl_engine_transform_failure() {
        let pipeline = MockPipeline::new().with_failure_at("transform");
        let engine = EtlEngine::new(pipeline);

        let result = engine.run().await;

        assert!(result.is_err());
        match result.unwrap_err() {
            EtlError::TransformationError { stage, details } => {
                assert_eq!(stage, "test");
                assert_eq!(details, "Mock transformation failure");
            }
            other => panic!("Expected TransformationError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_etl_engine_load_failure() {
        let pipeline = MockPipeline::new().with_failure_at("load");
        let engine = EtlEngine::new(pipeline);

        let result = engine.run().await;

        assert!(result.is_err());
        match result.unwrap_err() {
            EtlError::IoError(_) => {} // Expected error type
            other => panic!("Expected IoError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_etl_engine_without_monitoring() {
        let pipeline = MockPipeline::new();
        let engine = EtlEngine::new_with_monitoring(pipeline, false);

        let result = engine.run().await;

        assert!(result.is_ok());
    }
}
