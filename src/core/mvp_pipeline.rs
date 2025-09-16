use crate::config::toml_config::TomlConfig;
use crate::core::{Pipeline, Record, Storage, TransformResult};
use crate::utils::error::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::io::Write;
use zip::write::{FileOptions, ZipWriter};

/// MVP Pipeline 實現，專注於處理第一筆記錄
pub struct MvpPipeline<S: Storage> {
    storage: S,
    config: TomlConfig,
    client: Client,
}

impl<S: Storage> MvpPipeline<S> {
    pub fn new(storage: S, config: TomlConfig) -> Self {
        Self {
            storage,
            config,
            client: Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl<S: Storage> Pipeline for MvpPipeline<S> {
    async fn extract(&self) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        tracing::info!(
            "🚀 Starting MVP extraction from: {}",
            self.config.source.endpoint
        );

        // 檢查是否啟用 MVP 模式
        if self.config.is_mvp_mode() {
            tracing::info!("📋 MVP Mode enabled - will process only first record");
        }

        // 構建請求
        let mut request = self.client.get(&self.config.source.endpoint);

        // 添加自定義標頭
        if let Some(headers) = &self.config.source.headers {
            for (key, value) in headers {
                request = request.header(key, value);
            }
        }

        // 添加查詢參數
        if let Some(params) = &self.config.source.parameters {
            for (key, value) in params {
                request = request.query(&[(key, value)]);
            }
        }

        // 設定超時
        if let Some(timeout) = self.config.source.timeout_seconds {
            request = request.timeout(std::time::Duration::from_secs(timeout));
        }

        tracing::debug!("Making API request to: {}", self.config.source.endpoint);

        // 執行請求
        let response = request.send().await?;
        tracing::debug!("API response status: {}", response.status());

        if response.status().is_success() {
            let json_data: serde_json::Value = response.json().await?;

            // 處理 API 回應
            if let serde_json::Value::Array(items) = json_data {
                let max_records = if self.config.is_mvp_mode() {
                    1 // MVP: 只處理第一筆
                } else {
                    self.config.max_records().unwrap_or(items.len())
                };

                for (index, item) in items.into_iter().take(max_records).enumerate() {
                    if let serde_json::Value::Object(obj) = item {
                        let mut data = HashMap::new();

                        // 應用字段映射
                        if let Some(field_mapping) = &self.config.extract.field_mapping {
                            for (original_key, value) in obj {
                                let mapped_key =
                                    field_mapping.get(&original_key).unwrap_or(&original_key);
                                data.insert(mapped_key.clone(), value);
                            }
                        } else {
                            // 沒有映射就直接使用原始字段
                            for (key, value) in obj {
                                data.insert(key, value);
                            }
                        }

                        records.push(Record { data });

                        if self.config.is_mvp_mode() {
                            tracing::info!("✅ MVP Mode: Successfully extracted first record");
                            break; // MVP 模式只處理第一筆
                        }
                    }

                    if index + 1 >= max_records {
                        break;
                    }
                }
            } else {
                // 單一物件回應
                let mut data = HashMap::new();
                data.insert("response".to_string(), json_data);
                records.push(Record { data });
            }
        }

        // 如果沒有 API 數據或啟用錯誤處理，使用範例數據
        if records.is_empty()
            && self
                .config
                .error_handling
                .as_ref()
                .map(|eh| eh.on_api_failure.as_deref() == Some("use_sample_data"))
                .unwrap_or(true)
        {
            tracing::warn!("📝 No data from API, generating sample data for MVP");
            let sample_count = if self.config.is_mvp_mode() { 1 } else { 3 };

            for i in 1..=sample_count {
                let mut data = HashMap::new();
                data.insert("id".to_string(), serde_json::Value::Number(i.into()));
                data.insert(
                    "title".to_string(),
                    serde_json::Value::String(format!("Sample Post {}", i)),
                );
                data.insert(
                    "body".to_string(),
                    serde_json::Value::String(format!("This is sample content for post {}", i)),
                );
                data.insert("userId".to_string(), serde_json::Value::Number(1.into()));
                records.push(Record { data });

                if self.config.is_mvp_mode() {
                    break; // MVP 模式只生成一筆範例
                }
            }
        }

        tracing::info!("📊 Extracted {} records", records.len());
        Ok(records)
    }

    async fn transform(&self, data: Vec<Record>) -> Result<TransformResult> {
        let mut processed_records = Vec::new();
        let mut csv_lines = vec!["id,title,body,userId,processed".to_string()];
        let mut tsv_lines = vec!["id\ttitle\tbody\tuserId\tprocessed".to_string()];
        let mut intermediate_data = Vec::new();

        tracing::info!("🔄 Starting MVP transformation for {} records", data.len());

        for (index, record) in data.into_iter().enumerate() {
            let mut processed_record = record.clone();

            // 提取字段值
            let id = record
                .data
                .get("id")
                .or_else(|| record.data.get("post_id"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            let title = record
                .data
                .get("title")
                .or_else(|| record.data.get("post_title"))
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown Title");

            let body = record
                .data
                .get("body")
                .or_else(|| record.data.get("post_content"))
                .and_then(|v| v.as_str())
                .unwrap_or("No content");

            let user_id = record
                .data
                .get("userId")
                .or_else(|| record.data.get("author_id"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            // 應用轉換操作
            let cleaned_title = if self
                .config
                .transform
                .operations
                .as_ref()
                .map(|op| op.trim_whitespace.unwrap_or(false))
                .unwrap_or(false)
            {
                title.trim()
            } else {
                title
            };

            let cleaned_body = if self
                .config
                .transform
                .operations
                .as_ref()
                .map(|op| op.clean_text.unwrap_or(false))
                .unwrap_or(false)
            {
                body.trim().replace('\n', " ")
            } else {
                body.to_string()
            };

            // 驗證必需字段
            if let Some(validation) = self.config.transform.validation.as_ref() {
                if let Some(required_fields) = &validation.required_fields {
                    for field in required_fields {
                        if !processed_record.data.contains_key(field) {
                            tracing::warn!("⚠️ Missing required field: {}", field);
                        }
                    }
                }
            }

            // 添加處理標記
            processed_record
                .data
                .insert("processed".to_string(), serde_json::Value::Bool(true));

            // 生成 CSV 行
            csv_lines.push(format!(
                "{},{},{},{},true",
                id,
                cleaned_title,
                cleaned_body.replace(',', " "),
                user_id
            ));

            // 生成 TSV 行
            tsv_lines.push(format!(
                "{}\t{}\t{}\t{}\ttrue",
                id,
                cleaned_title,
                cleaned_body.replace('\t', " "),
                user_id
            ));

            // 檢查是否符合中繼數據條件
            let title_threshold = self
                .config
                .transform
                .intermediate
                .as_ref()
                .and_then(|i| i.title_length_threshold)
                .unwrap_or(50);

            if cleaned_title.len() > title_threshold {
                intermediate_data.push(processed_record.clone());
            }

            processed_records.push(processed_record);

            if self.config.is_mvp_mode() {
                tracing::info!("✅ MVP Mode: Processed first record successfully");
                break; // MVP 模式只處理第一筆
            }

            tracing::debug!("Processed record {}/{}", index + 1, processed_records.len());
        }

        tracing::info!(
            "📋 Transformation complete: {} processed, {} intermediate",
            processed_records.len(),
            intermediate_data.len()
        );

        Ok(TransformResult {
            processed_records,
            csv_output: csv_lines.join("\n"),
            tsv_output: tsv_lines.join("\n"),
            intermediate_data,
        })
    }

    async fn load(&self, result: TransformResult) -> Result<String> {
        let compression_config = self.config.load.compression.as_ref();
        let filename = compression_config
            .map(|c| c.filename.as_str())
            .unwrap_or("etl_output.zip");

        let output_path = format!("{}/{}", self.config.load.output_path, filename);

        tracing::info!("💾 Starting MVP load to: {}", output_path);

        let include_intermediate = compression_config
            .map(|c| c.include_intermediate.unwrap_or(true))
            .unwrap_or(true);

        let file_count = self.config.load.output_formats.len()
            + if include_intermediate && !result.intermediate_data.is_empty() {
                1
            } else {
                0
            };

        tracing::debug!("Creating ZIP file with {} files", file_count);

        // 創建 ZIP 文件
        let zip_data = {
            let mut zip = ZipWriter::new(std::io::Cursor::new(Vec::new()));

            // 根據配置的輸出格式添加文件
            for format in &self.config.load.output_formats {
                match format.as_str() {
                    "csv" => {
                        let csv_filename = self
                            .config
                            .load
                            .filenames
                            .as_ref()
                            .and_then(|f| f.csv.as_ref())
                            .map(|s| s.as_str())
                            .unwrap_or("output.csv");

                        zip.start_file::<_, ()>(csv_filename, FileOptions::default())?;
                        zip.write_all(result.csv_output.as_bytes())?;
                        tracing::debug!("Added CSV file: {}", csv_filename);
                    }
                    "tsv" => {
                        let tsv_filename = self
                            .config
                            .load
                            .filenames
                            .as_ref()
                            .and_then(|f| f.tsv.as_ref())
                            .map(|s| s.as_str())
                            .unwrap_or("output.tsv");

                        zip.start_file::<_, ()>(tsv_filename, FileOptions::default())?;
                        zip.write_all(result.tsv_output.as_bytes())?;
                        tracing::debug!("Added TSV file: {}", tsv_filename);
                    }
                    "json" => {
                        let json_filename = self
                            .config
                            .load
                            .filenames
                            .as_ref()
                            .and_then(|f| f.json.as_ref())
                            .map(|s| s.as_str())
                            .unwrap_or("processed_data.json");

                        zip.start_file::<_, ()>(json_filename, FileOptions::default())?;
                        let json_data = serde_json::to_string_pretty(&result.processed_records)?;
                        zip.write_all(json_data.as_bytes())?;
                        tracing::debug!("Added JSON file: {}", json_filename);
                    }
                    _ => {
                        tracing::warn!("Unsupported output format: {}", format);
                    }
                }
            }

            // 添加中繼結果 JSON
            if include_intermediate && !result.intermediate_data.is_empty() {
                let intermediate_filename = "intermediate.json";
                zip.start_file::<_, ()>(intermediate_filename, FileOptions::default())?;
                let json_data = serde_json::to_string_pretty(&result.intermediate_data)?;
                zip.write_all(json_data.as_bytes())?;
                tracing::debug!("Added intermediate data: {}", intermediate_filename);
            }

            // 完成並取回底層 Vec<u8>
            let cursor = zip.finish()?;
            cursor.into_inner()
        };

        // 保存 ZIP 文件
        tracing::debug!("Writing ZIP file ({} bytes) to storage", zip_data.len());
        self.storage.write_file(filename, &zip_data).await?;

        tracing::info!("✅ MVP load completed successfully");
        Ok(output_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::toml_config::TomlConfig;
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
