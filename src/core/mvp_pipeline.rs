
pub use crate::app::pipelines::mvp_pipeline::MvpPipeline;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::toml_config::TomlConfig;
    use crate::domain::model::{Record, TransformResult};
    use crate::domain::ports::{Pipeline, Storage};
    use crate::utils::error::Result;
    use httpmock::prelude::*;
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

        #[allow(dead_code)]
        async fn get_file(&self, path: &str) -> Option<Vec<u8>> {
            let files = self.files.lock().await;
            files.get(path).cloned()
        }
    }

    impl Storage for MockStorage {
        async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
            let files = self.files.lock().await;
            files.get(path).cloned().ok_or_else(|| {
                crate::utils::error::EtlError::IoError(std::io::Error::new(
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

    #[tokio::test]
    async fn test_mvp_pipeline_extract_first_record_only() {
        let server = MockServer::start();
        let mock_data = serde_json::json!([
            {"id": 1, "title": "First Post", "body": "Content 1", "userId": 1},
            {"id": 2, "title": "Second Post", "body": "Content 2", "userId": 2},
            {"id": 3, "title": "Third Post", "body": "Content 3", "userId": 3}
        ]);

        let api_mock = server.mock(|when, then| {
            when.method(GET).path("/posts");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(mock_data);
        });

        let toml_content = format!(
            r#"
[pipeline]
name = "mvp-test"
description = "MVP test"
version = "1.0"

[source]
type = "api"
endpoint = "{}/posts"

[extract]
first_record_only = true
max_records = 1

[transform]

[load]
output_path = "./test-output"
output_formats = ["csv", "json"]
"#,
            server.base_url()
        );

        let config = TomlConfig::from_str(&toml_content).unwrap();
        let storage = MockStorage::new();
        let pipeline = MvpPipeline::new(storage, config);

        let result = pipeline.extract().await.unwrap();

        api_mock.assert();
        assert_eq!(result.len(), 1); // MVP mode should only extract first record
        assert_eq!(result[0].data.get("id").unwrap().as_i64().unwrap(), 1);
        assert_eq!(
            result[0].data.get("title").unwrap().as_str().unwrap(),
            "First Post"
        );
    }

    #[tokio::test]
    async fn test_mvp_pipeline_field_mapping() {
        let server = MockServer::start();
        let mock_data = serde_json::json!([
            {"id": 1, "title": "Test Post", "body": "Test Content", "userId": 1}
        ]);

        let api_mock = server.mock(|when, then| {
            when.method(GET).path("/data");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(mock_data);
        });

        let toml_content = format!(
            r#"
[pipeline]
name = "mapping-test"
description = "Field mapping test"
version = "1.0"

[source]
type = "api"
endpoint = "{}/data"

[extract]
first_record_only = true

[extract.field_mapping]
id = "post_id"
title = "post_title"
body = "post_content"
userId = "author_id"

[transform]

[load]
output_path = "./test-output"
output_formats = ["csv"]
"#,
            server.base_url()
        );

        let config = TomlConfig::from_str(&toml_content).unwrap();
        let storage = MockStorage::new();
        let pipeline = MvpPipeline::new(storage, config);

        let result = pipeline.extract().await.unwrap();

        api_mock.assert();
        assert_eq!(result.len(), 1);
        assert!(result[0].data.contains_key("post_id"));
        assert!(result[0].data.contains_key("post_title"));
        assert!(result[0].data.contains_key("post_content"));
        assert!(result[0].data.contains_key("author_id"));
    }
}
