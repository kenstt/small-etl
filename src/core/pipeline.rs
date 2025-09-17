pub use crate::app::pipelines::simple_pipeline::SimplePipeline;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::{Record, TransformResult};
    use crate::domain::ports::{ConfigProvider, Pipeline, Storage};
    use crate::utils::error::{EtlError, Result};
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

        async fn get_file(&self, path: &str) -> Option<Vec<u8>> {
            let files = self.files.lock().await;
            files.get(path).cloned()
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
        lookup_files: Vec<String>,
        concurrent_requests: usize,
    }

    impl MockConfig {
        fn new(api_endpoint: String) -> Self {
            Self {
                api_endpoint,
                output_path: "test_output".to_string(),
                lookup_files: vec![],
                concurrent_requests: 5,
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
            &self.lookup_files
        }

        fn concurrent_requests(&self) -> usize {
            self.concurrent_requests
        }
    }

    #[tokio::test]
    async fn test_extract_successful_api_response() {
        let server = MockServer::start();
        let mock_data = serde_json::json!([
            {"id": 1, "name": "Item 1", "value": 10},
            {"id": 2, "name": "Item 2", "value": 20}
        ]);

        let api_mock = server.mock(|when, then| {
            when.method(GET).path("/");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(mock_data);
        });

        let storage = MockStorage::new();
        let config = MockConfig::new(server.url("/"));
        let pipeline = SimplePipeline::new(storage, config);

        let result = pipeline.extract().await.unwrap();

        api_mock.assert();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data.get("id").unwrap().as_i64().unwrap(), 1);
        assert_eq!(result[1].data.get("id").unwrap().as_i64().unwrap(), 2);
    }

    #[tokio::test]
    async fn test_extract_single_object_response() {
        let server = MockServer::start();
        let mock_data = serde_json::json!({"id": 1, "name": "Single Item"});

        let api_mock = server.mock(|when, then| {
            when.method(GET).path("/");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(mock_data);
        });

        let storage = MockStorage::new();
        let config = MockConfig::new(server.url("/"));
        let pipeline = SimplePipeline::new(storage, config);

        let result = pipeline.extract().await.unwrap();

        api_mock.assert();
        assert_eq!(result.len(), 1);
        assert!(result[0].data.contains_key("response"));
    }

    #[tokio::test]
    async fn test_extract_api_failure_generates_sample_data() {
        let server = MockServer::start();

        let api_mock = server.mock(|when, then| {
            when.method(GET).path("/");
            then.status(500);
        });

        let storage = MockStorage::new();
        let config = MockConfig::new(server.url("/"));
        let pipeline = SimplePipeline::new(storage, config);

        let result = pipeline.extract().await.unwrap();

        api_mock.assert();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].data.get("id").unwrap().as_i64().unwrap(), 1);
        assert_eq!(result[4].data.get("id").unwrap().as_i64().unwrap(), 5);
    }

    #[tokio::test]
    async fn test_extract_empty_api_response_generates_sample_data() {
        let server = MockServer::start();

        let api_mock = server.mock(|when, then| {
            when.method(GET).path("/");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!([]));
        });

        let storage = MockStorage::new();
        let config = MockConfig::new(server.url("/"));
        let pipeline = SimplePipeline::new(storage, config);

        let result = pipeline.extract().await.unwrap();

        api_mock.assert();
        assert_eq!(result.len(), 5);
    }

    #[tokio::test]
    async fn test_transform_with_valid_data() {
        let mut input_data = Vec::new();

        // Create test records
        for i in 1..=3 {
            let mut data = HashMap::new();
            data.insert("id".to_string(), serde_json::Value::Number(i.into()));
            data.insert(
                "name".to_string(),
                serde_json::Value::String(format!("Item {}", i)),
            );
            data.insert(
                "value".to_string(),
                serde_json::Value::Number((i * 10).into()),
            );
            input_data.push(Record { data });
        }

        let storage = MockStorage::new();
        let config = MockConfig::new("http://test.com".to_string());
        let pipeline = SimplePipeline::new(storage, config);

        let result = pipeline.transform(input_data).await.unwrap();

        // Check processed records
        assert_eq!(result.processed_records.len(), 3);
        assert_eq!(
            result.processed_records[0]
                .data
                .get("processed")
                .unwrap()
                .as_bool()
                .unwrap(),
            true
        );

        // Check CSV output
        let csv_lines: Vec<&str> = result.csv_output.split('\n').collect();
        assert_eq!(csv_lines.len(), 4); // Header + 3 records
        assert_eq!(csv_lines[0], "id,name,value,processed");
        assert_eq!(csv_lines[1], "1,Item 1,10,true");

        // Check TSV output
        let tsv_lines: Vec<&str> = result.tsv_output.split('\n').collect();
        assert_eq!(tsv_lines.len(), 4);
        assert_eq!(tsv_lines[0], "id\tname\tvalue\tprocessed");
        assert_eq!(tsv_lines[1], "1\tItem 1\t10\ttrue");

        // Check intermediate data (only records with value > 20)
        assert_eq!(result.intermediate_data.len(), 1);
        assert_eq!(
            result.intermediate_data[0]
                .data
                .get("value")
                .unwrap()
                .as_i64()
                .unwrap(),
            30
        );
    }

    #[tokio::test]
    async fn test_transform_with_empty_data() {
        let input_data = Vec::new();

        let storage = MockStorage::new();
        let config = MockConfig::new("http://test.com".to_string());
        let pipeline = SimplePipeline::new(storage, config);

        let result = pipeline.transform(input_data).await.unwrap();

        assert_eq!(result.processed_records.len(), 0);
        assert_eq!(result.csv_output, "id,name,value,processed");
        assert_eq!(result.tsv_output, "id\tname\tvalue\tprocessed");
        assert_eq!(result.intermediate_data.len(), 0);
    }

    #[tokio::test]
    async fn test_transform_with_missing_fields() {
        let mut input_data = Vec::new();

        // Create record with missing fields
        let mut data = HashMap::new();
        data.insert("id".to_string(), serde_json::Value::Number(1.into()));
        // Missing 'name' and 'value' fields
        input_data.push(Record { data });

        let storage = MockStorage::new();
        let config = MockConfig::new("http://test.com".to_string());
        let pipeline = SimplePipeline::new(storage, config);

        let result = pipeline.transform(input_data).await.unwrap();

        assert_eq!(result.processed_records.len(), 1);

        // Check defaults are used
        let csv_lines: Vec<&str> = result.csv_output.split('\n').collect();
        assert_eq!(csv_lines[1], "1,Unknown,0,true");

        // Should not be in intermediate data (value = 0 < 20)
        assert_eq!(result.intermediate_data.len(), 0);
    }

    #[tokio::test]
    async fn test_transform_intermediate_data_filtering() {
        let mut input_data = Vec::new();

        // Create records with different values
        let values = [15, 25, 35]; // Only 25 and 35 should be in intermediate
        for (i, value) in values.iter().enumerate() {
            let mut data = HashMap::new();
            data.insert("id".to_string(), serde_json::Value::Number((i + 1).into()));
            data.insert(
                "name".to_string(),
                serde_json::Value::String(format!("Item {}", i + 1)),
            );
            data.insert(
                "value".to_string(),
                serde_json::Value::Number((*value).into()),
            );
            input_data.push(Record { data });
        }

        let storage = MockStorage::new();
        let config = MockConfig::new("http://test.com".to_string());
        let pipeline = SimplePipeline::new(storage, config);

        let result = pipeline.transform(input_data).await.unwrap();

        assert_eq!(result.processed_records.len(), 3);
        assert_eq!(result.intermediate_data.len(), 2); // Only values > 20

        // Check intermediate data contains correct records
        assert_eq!(
            result.intermediate_data[0]
                .data
                .get("value")
                .unwrap()
                .as_i64()
                .unwrap(),
            25
        );
        assert_eq!(
            result.intermediate_data[1]
                .data
                .get("value")
                .unwrap()
                .as_i64()
                .unwrap(),
            35
        );
    }

    #[tokio::test]
    async fn test_load_with_data() {
        let storage = MockStorage::new();
        let config = MockConfig::new("http://test.com".to_string());
        let pipeline = SimplePipeline::new(storage.clone(), config);

        // Create transform result
        let processed_records = vec![Record {
            data: {
                let mut data = HashMap::new();
                data.insert("id".to_string(), serde_json::Value::Number(1.into()));
                data.insert(
                    "name".to_string(),
                    serde_json::Value::String("Test".to_string()),
                );
                data
            },
        }];

        let intermediate_data = vec![Record {
            data: {
                let mut data = HashMap::new();
                data.insert("id".to_string(), serde_json::Value::Number(2.into()));
                data.insert("value".to_string(), serde_json::Value::Number(25.into()));
                data
            },
        }];

        let transform_result = TransformResult {
            processed_records,
            csv_output: "id,name,value,processed\n1,Test,10,true".to_string(),
            tsv_output: "id\tname\tvalue\tprocessed\n1\tTest\t10\ttrue".to_string(),
            intermediate_data,
        };

        let output_path = pipeline.load(transform_result).await.unwrap();

        assert_eq!(output_path, "test_output/etl_output.zip");

        // Verify ZIP file was created in storage
        let zip_data = storage.get_file("etl_output.zip").await;
        assert!(zip_data.is_some());
        assert!(!zip_data.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_load_without_intermediate_data() {
        let storage = MockStorage::new();
        let config = MockConfig::new("http://test.com".to_string());
        let pipeline = SimplePipeline::new(storage.clone(), config);

        let transform_result = TransformResult {
            processed_records: vec![],
            csv_output: "id,name,value,processed".to_string(),
            tsv_output: "id\tname\tvalue\tprocessed".to_string(),
            intermediate_data: vec![], // Empty intermediate data
        };

        let output_path = pipeline.load(transform_result).await.unwrap();

        assert_eq!(output_path, "test_output/etl_output.zip");

        // Verify ZIP file was created
        let zip_data = storage.get_file("etl_output.zip").await;
        assert!(zip_data.is_some());

        // Verify ZIP contents (should only have CSV and TSV, no JSON)
        let zip_bytes = zip_data.unwrap();
        let cursor = std::io::Cursor::new(zip_bytes);
        let mut archive = zip::ZipArchive::new(cursor).unwrap();

        assert_eq!(archive.len(), 2); // Only CSV and TSV files

        let mut file_names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().to_string())
            .collect();
        file_names.sort();

        assert_eq!(file_names, vec!["output.csv", "output.tsv"]);
    }

    #[tokio::test]
    async fn test_load_with_intermediate_data() {
        let storage = MockStorage::new();
        let config = MockConfig::new("http://test.com".to_string());
        let pipeline = SimplePipeline::new(storage.clone(), config);

        let intermediate_data = vec![Record {
            data: {
                let mut data = HashMap::new();
                data.insert("id".to_string(), serde_json::Value::Number(1.into()));
                data.insert("value".to_string(), serde_json::Value::Number(30.into()));
                data
            },
        }];

        let transform_result = TransformResult {
            processed_records: vec![],
            csv_output: "id,name,value,processed".to_string(),
            tsv_output: "id\tname\tvalue\tprocessed".to_string(),
            intermediate_data,
        };

        let output_path = pipeline.load(transform_result).await.unwrap();

        assert_eq!(output_path, "test_output/etl_output.zip");

        // Verify ZIP file was created
        let zip_data = storage.get_file("etl_output.zip").await;
        assert!(zip_data.is_some());

        // Verify ZIP contents (should have CSV, TSV, and JSON)
        let zip_bytes = zip_data.unwrap();
        let cursor = std::io::Cursor::new(zip_bytes);
        let mut archive = zip::ZipArchive::new(cursor).unwrap();

        assert_eq!(archive.len(), 3); // CSV, TSV, and JSON files

        let mut file_names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().to_string())
            .collect();
        file_names.sort();

        assert_eq!(
            file_names,
            vec!["intermediate.json", "output.csv", "output.tsv"]
        );
    }

    #[tokio::test]
    async fn test_load_zip_content_verification() {
        let storage = MockStorage::new();
        let config = MockConfig::new("http://test.com".to_string());
        let pipeline = SimplePipeline::new(storage.clone(), config);

        let csv_content = "id,name\n1,Test Item";
        let tsv_content = "id\tname\n1\tTest Item";

        let transform_result = TransformResult {
            processed_records: vec![],
            csv_output: csv_content.to_string(),
            tsv_output: tsv_content.to_string(),
            intermediate_data: vec![],
        };

        pipeline.load(transform_result).await.unwrap();

        // Verify ZIP file contents
        let zip_data = storage.get_file("etl_output.zip").await.unwrap();
        let cursor = std::io::Cursor::new(zip_data);
        let mut archive = zip::ZipArchive::new(cursor).unwrap();

        // Check CSV content
        let csv_content_read = {
            let mut csv_file = archive.by_name("output.csv").unwrap();
            let mut content = String::new();
            std::io::Read::read_to_string(&mut csv_file, &mut content).unwrap();
            content
        };
        assert_eq!(csv_content_read, csv_content);

        // Check TSV content
        let tsv_content_read = {
            let mut tsv_file = archive.by_name("output.tsv").unwrap();
            let mut content = String::new();
            std::io::Read::read_to_string(&mut tsv_file, &mut content).unwrap();
            content
        };
        assert_eq!(tsv_content_read, tsv_content);
    }
}
