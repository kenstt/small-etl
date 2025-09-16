use crate::core::{ConfigProvider, Pipeline, Record, Storage, TransformResult};
use crate::utils::error::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::io::Write;
use zip::write::{FileOptions, ZipWriter};

pub struct SimplePipeline<S: Storage, C: ConfigProvider> {
    pub(crate) storage: S,
    pub(crate) config: C,
    pub(crate) client: Client,
}

impl<S: Storage, C: ConfigProvider> SimplePipeline<S, C> {
    pub fn new(storage: S, config: C) -> Self {
        Self {
            storage,
            config,
            client: Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl<S: Storage, C: ConfigProvider> Pipeline for SimplePipeline<S, C> {
    async fn extract(&self) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        // 模擬API調用
        tracing::debug!("Making API request to: {}", self.config.api_endpoint());
        let response = self.client.get(self.config.api_endpoint()).send().await?;

        tracing::debug!("API response status: {}", response.status());

        if response.status().is_success() {
            let json_data: serde_json::Value = response.json().await?;

            // 簡單處理：假設API返回一個對象數組
            if let serde_json::Value::Array(items) = json_data {
                for item in items {
                    if let serde_json::Value::Object(obj) = item {
                        let mut data = HashMap::new();
                        for (key, value) in obj {
                            data.insert(key, value);
                        }
                        records.push(Record { data });
                    }
                }
            } else {
                // 如果是單個對象，包裝成數組
                let mut data = HashMap::new();
                data.insert("response".to_string(), json_data);
                records.push(Record { data });
            }
        }

        // 如果沒有API數據，創建一些示例數據
        if records.is_empty() {
            tracing::warn!("No data from API, generating sample data");
            for i in 1..=5 {
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
                records.push(Record { data });
            }
        }

        Ok(records)
    }

    async fn transform(&self, data: Vec<Record>) -> Result<TransformResult> {
        let mut processed_records = Vec::new();
        let mut csv_lines = vec!["id,name,value,processed".to_string()];
        let mut tsv_lines = vec!["id\tname\tvalue\tprocessed".to_string()];
        let mut intermediate_data = Vec::new();

        for record in data {
            let mut processed_record = record.clone();

            // 簡單的數據處理邏輯
            let id = record.data.get("id").and_then(|v| v.as_i64()).unwrap_or(0);

            let name = record
                .data
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");

            let value = record
                .data
                .get("value")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            // 添加處理標記
            processed_record
                .data
                .insert("processed".to_string(), serde_json::Value::Bool(true));

            // 生成CSV行
            csv_lines.push(format!("{},{},{},true", id, name, value));

            // 生成TSV行
            tsv_lines.push(format!("{}\t{}\t{}\ttrue", id, name, value));

            // 如果符合條件，添加到中繼結果
            if value > 20 {
                intermediate_data.push(processed_record.clone());
            }

            processed_records.push(processed_record);
        }

        Ok(TransformResult {
            processed_records,
            csv_output: csv_lines.join("\n"),
            tsv_output: tsv_lines.join("\n"),
            intermediate_data,
        })
    }

    async fn load(&self, result: TransformResult) -> Result<String> {
        let output_path = format!("{}/etl_output.zip", self.config.output_path());

        tracing::debug!(
            "Creating ZIP file with {} files",
            2 + if result.intermediate_data.is_empty() { 0 } else { 1 }
        );

        // 創建ZIP文件
        let zip_data = {
            let mut zip = ZipWriter::new(std::io::Cursor::new(Vec::new()));

            // 添加CSV文件
            zip.start_file::<_, ()>("output.csv", FileOptions::default())?;
            zip.write_all(result.csv_output.as_bytes())?;

            // 添加TSV文件
            zip.start_file::<_, ()>("output.tsv", FileOptions::default())?;
            zip.write_all(result.tsv_output.as_bytes())?;

            // 添加中繼結果JSON
            if !result.intermediate_data.is_empty() {
                zip.start_file::<_, ()>("intermediate.json", FileOptions::default())?;
                let json_data = serde_json::to_string_pretty(&result.intermediate_data)?;
                zip.write_all(json_data.as_bytes())?;
            }

            // 完成並取回底層 Vec<u8>
            let cursor = zip.finish()?;
            cursor.into_inner()
        };

        // 保存ZIP文件
        tracing::debug!("Writing ZIP file ({} bytes) to storage", zip_data.len());
        self.storage.write_file("etl_output.zip", &zip_data).await?;

        tracing::debug!("ZIP file saved successfully");
        Ok(output_path)
    }
}
