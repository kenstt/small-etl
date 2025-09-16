use crate::config::toml_config::TomlConfig;
use crate::core::{Pipeline, Record, Storage, TransformResult};
use crate::utils::error::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::io::Write;
use zip::write::{FileOptions, ZipWriter};

/// MVP Pipeline 實現，專注於處理第一筆記錄
pub struct MvpPipeline<S: Storage> {
    pub(crate) storage: S,
    pub(crate) config: TomlConfig,
    pub(crate) client: Client,
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
        tracing::info!("🔧 Starting MVP transform on {} records", data.len());

        let mut processed_records = Vec::new();
        let mut csv_lines = vec!["id,title,body,userId,processed".to_string()];
        let mut tsv_lines = vec!["id\ttitle\tbody\tuserId\tprocessed".to_string()];
        let mut intermediate_data = Vec::new();

        for record in data {
            let mut processed_record = record.clone();

            // 提取常見字段
            let id = record.data.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
            let title = record
                .data
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("Untitled");
            let body = record
                .data
                .get("body")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let user_id = record
                .data
                .get("userId")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            // 記錄處理狀態
            processed_record
                .data
                .insert("processed".to_string(), serde_json::Value::Bool(true));

            // 組合 CSV 與 TSV 行
            csv_lines.push(format!("{},{},{},{},true", id, title, body, user_id));
            tsv_lines.push(format!("{}\t{}\t{}\t{}\ttrue", id, title, body, user_id));

            // MVP 規則：只保留第一筆或符合條件的資料
            if self.config.is_mvp_mode() || id == 1 {
                intermediate_data.push(processed_record.clone());
            }

            processed_records.push(processed_record);

            // MVP 模式提早結束處理
            if self.config.is_mvp_mode() {
                break;
            }
        }

        tracing::info!("✅ MVP transform complete: {} records processed", processed_records.len());
        Ok(TransformResult {
            processed_records,
            csv_output: csv_lines.join("\n"),
            tsv_output: tsv_lines.join("\n"),
            intermediate_data,
        })
    }

    async fn load(&self, result: TransformResult) -> Result<String> {
        tracing::info!("💾 Loading MVP result to storage");
        let output_path = format!("{}/mvp_output.zip", self.config.output_path());

        let zip_data = {
            let mut zip = ZipWriter::new(std::io::Cursor::new(Vec::new()));

            // 添加 CSV 檔案
            zip.start_file::<_, ()>("mvp_output.csv", FileOptions::default())?;
            zip.write_all(result.csv_output.as_bytes())?;

            // 添加 TSV 檔案
            zip.start_file::<_, ()>("mvp_output.tsv", FileOptions::default())?;
            zip.write_all(result.tsv_output.as_bytes())?;

            // MVP 的中繼結果
            if !result.intermediate_data.is_empty() {
                zip.start_file::<_, ()>("mvp_intermediate.json", FileOptions::default())?;
                let json_data = serde_json::to_string_pretty(&result.intermediate_data)?;
                zip.write_all(json_data.as_bytes())?;
            }

            let cursor = zip.finish()?;
            cursor.into_inner()
        };

        self.storage
            .write_file("mvp_output.zip", &zip_data)
            .await?;

        tracing::info!("📦 MVP output saved: {}", output_path);
        Ok(output_path)
    }
}
