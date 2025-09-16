use crate::config::sequence_config::PipelineDefinition;
use crate::core::{
    pipeline_sequence::{ContextualPipeline, PipelineContext},
    Record, Storage, TransformResult,
};
use crate::utils::error::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::io::Write;
use zip::write::{FileOptions, ZipWriter};

/// 基於序列配置的上下文感知 Pipeline
pub struct SequenceAwarePipeline<S: Storage> {
    name: String,
    storage: S,
    config: PipelineDefinition,
    client: Client,
}

impl<S: Storage> SequenceAwarePipeline<S> {
    pub fn new(name: String, storage: S, config: PipelineDefinition) -> Self {
        Self {
            name,
            storage,
            config,
            client: Client::new(),
        }
    }

    /// 決定數據來源：API、前一個 Pipeline 或合併
    async fn determine_data_source(&self, context: &PipelineContext) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        // 檢查是否使用前一個 Pipeline 的輸出
        if let Some(data_source) = &self.config.source.data_source {
            if data_source.use_previous_output.unwrap_or(false) {
                if let Some(from_pipeline) = &data_source.from_pipeline {
                    // 使用指定 Pipeline 的輸出
                    if let Some(pipeline_result) = context.get_result_by_name(from_pipeline) {
                        records.extend(pipeline_result.records.clone());
                        tracing::info!(
                            "📂 {}: Using {} records from pipeline '{}'",
                            self.name,
                            records.len(),
                            from_pipeline
                        );
                    }
                } else {
                    // 使用前一個 Pipeline 的輸出
                    if let Some(previous_result) = context.get_previous_result() {
                        records.extend(previous_result.records.clone());
                        tracing::info!(
                            "📂 {}: Using {} records from previous pipeline",
                            self.name,
                            records.len()
                        );
                    }
                }

                // 如果設定為合併，還需要獲取 API 數據
                // 但對於參數化 API（含 {param}），即使 merge_with_api = false 也需要執行 API 呼叫
                if !data_source.merge_with_api.unwrap_or(false) && !self.config.source.endpoint.contains("{") {
                    return Ok(records);
                }
            }
        }

        // 獲取 API 數據 - 檢查是否需要參數化呼叫
        let api_records = if self.config.source.endpoint.contains("{") {
            // 參數化 API 呼叫 - 替換前一個 pipeline 的數據
            return self.fetch_parameterized_api(context).await;
        } else {
            // 標準 API 呼叫
            self.fetch_api_data().await?
        };
        records.extend(api_records);

        Ok(records)
    }

    /// 處理參數化 API 呼叫（為每個前一個記錄分別呼叫）
    async fn fetch_parameterized_api(&self, context: &PipelineContext) -> Result<Vec<Record>> {
        let mut all_records = Vec::new();

        // 獲取前一個 Pipeline 的記錄作為參數源
        let param_records = if let Some(data_source) = &self.config.source.data_source {
            if data_source.use_previous_output.unwrap_or(false) {
                if let Some(from_pipeline) = &data_source.from_pipeline {
                    context.get_result_by_name(from_pipeline)
                        .map(|r| r.records.clone())
                        .unwrap_or_default()
                } else {
                    context.get_previous_result()
                        .map(|r| r.records.clone())
                        .unwrap_or_default()
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        tracing::info!("📡 {}: Making parameterized API calls for {} records", self.name, param_records.len());

        // 為每個記錄構建並呼叫 API
        for (index, record) in param_records.iter().enumerate() {
            let endpoint = self.build_parameterized_endpoint(&record.data)?;
            tracing::debug!("📡 {}: API call {}/{}: {}", self.name, index + 1, param_records.len(), endpoint);

            let api_records = self.fetch_single_api_call(&endpoint).await?;
            all_records.extend(api_records);

            // 可選：添加延遲避免請求過於頻繁
            if index < param_records.len() - 1 {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }

        tracing::info!("📡 {}: Total records fetched from parameterized APIs: {}", self.name, all_records.len());
        Ok(all_records)
    }

    /// 構建參數化端點 URL
    fn build_parameterized_endpoint(&self, data: &HashMap<String, serde_json::Value>) -> Result<String> {
        let mut endpoint = self.config.source.endpoint.clone();

        tracing::debug!("📡 {}: Building endpoint from template: {}", self.name, endpoint);
        tracing::debug!("📡 {}: Available data fields: {:?}", self.name, data.keys().collect::<Vec<_>>());

        // 替換 URL 中的參數佔位符
        for (key, value) in data {
            let placeholder = format!("{{{}}}", key);
            if endpoint.contains(&placeholder) {
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    _ => value.to_string().trim_matches('"').to_string(),
                };
                endpoint = endpoint.replace(&placeholder, &value_str);
                tracing::info!("📡 {}: Replaced {} with {}", self.name, placeholder, value_str);
            }
        }

        tracing::debug!("📡 {}: Final endpoint: {}", self.name, endpoint);

        // 檢查是否還有未替換的參數
        if endpoint.contains("{") && endpoint.contains("}") {
            tracing::error!("📡 {}: Unresolved parameters in endpoint: {}", self.name, endpoint);
            tracing::error!("📡 {}: Available fields were: {:?}", self.name, data);
            return Err(crate::utils::error::EtlError::ProcessingError {
                message: format!("Unresolved parameters in endpoint: {}. Available fields: {:?}", endpoint, data.keys().collect::<Vec<_>>())
            });
        }

        Ok(endpoint)
    }

    /// 執行單一 API 呼叫
    async fn fetch_single_api_call(&self, endpoint: &str) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        // 構建請求
        let mut request = self.client.get(endpoint);

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

        // 執行請求
        let response = request.send().await?;

        if response.status().is_success() {
            let json_data: serde_json::Value = response.json().await?;

            // 處理 API 回應（支持單一物件回應）
            if let serde_json::Value::Object(obj) = json_data {
                let mut data = HashMap::new();

                // 應用字段映射
                if let Some(field_mapping) = &self.config.extract.field_mapping {
                    for (original_key, value) in obj {
                        let mapped_key = field_mapping
                            .get(&original_key)
                            .unwrap_or(&original_key);
                        data.insert(mapped_key.clone(), value);
                    }
                } else {
                    // 沒有映射就直接使用原始字段
                    for (key, value) in obj {
                        data.insert(key, value);
                    }
                }

                records.push(Record { data });
            } else if let serde_json::Value::Array(items) = json_data {
                // 處理陣列回應
                for item in items {
                    if let serde_json::Value::Object(obj) = item {
                        let mut data = HashMap::new();

                        if let Some(field_mapping) = &self.config.extract.field_mapping {
                            for (original_key, value) in obj {
                                let mapped_key = field_mapping
                                    .get(&original_key)
                                    .unwrap_or(&original_key);
                                data.insert(mapped_key.clone(), value);
                            }
                        } else {
                            for (key, value) in obj {
                                data.insert(key, value);
                            }
                        }

                        records.push(Record { data });
                    }
                }
            }
        } else {
            let error_msg = format!("API request failed with status: {}", response.status());
            return Err(crate::utils::error::EtlError::ProcessingError {
                message: error_msg
            });
        }

        Ok(records)
    }

    /// 從 API 獲取數據
    async fn fetch_api_data(&self) -> Result<Vec<Record>> {
        let mut records = Vec::new();

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

        tracing::debug!("📡 {}: Making API request to: {}", self.name, self.config.source.endpoint);

        // 執行請求
        let response = request.send().await?;
        tracing::debug!("📡 {}: API response status: {}", self.name, response.status());

        if response.status().is_success() {
            let json_data: serde_json::Value = response.json().await?;

            // 處理 API 回應
            if let serde_json::Value::Array(items) = json_data {
                let max_records = self.config.extract.max_records.unwrap_or(items.len());

                for item in items.into_iter().take(max_records) {
                    if let serde_json::Value::Object(obj) = item {
                        let mut data = HashMap::new();

                        // 應用字段映射
                        if let Some(field_mapping) = &self.config.extract.field_mapping {
                            for (original_key, value) in obj {
                                let mapped_key = field_mapping
                                    .get(&original_key)
                                    .unwrap_or(&original_key);
                                data.insert(mapped_key.clone(), value);
                            }
                        } else {
                            // 沒有映射就直接使用原始字段
                            for (key, value) in obj {
                                data.insert(key, value);
                            }
                        }

                        records.push(Record { data });
                    }
                }
            } else if let serde_json::Value::Object(obj) = json_data {
                // 單一物件回應 - 應用字段映射
                let mut data = HashMap::new();

                if let Some(field_mapping) = &self.config.extract.field_mapping {
                    for (original_key, value) in obj {
                        let mapped_key = field_mapping
                            .get(&original_key)
                            .unwrap_or(&original_key);
                        data.insert(mapped_key.clone(), value);
                    }
                } else {
                    // 沒有映射就直接使用原始字段
                    for (key, value) in obj {
                        data.insert(key, value);
                    }
                }

                records.push(Record { data });
            } else {
                // 其他類型的回應
                let mut data = HashMap::new();
                data.insert("response".to_string(), json_data);
                records.push(Record { data });
            }
        } else {
            // API 回應失敗
            let error_msg = format!("API request failed with status: {}", response.status());
            tracing::error!("❌ {}: {}", self.name, error_msg);
            return Err(crate::utils::error::EtlError::ProcessingError {
                message: error_msg
            });
        }

        tracing::info!("📡 {}: Fetched {} records from API", self.name, records.len());
        Ok(records)
    }

    /// 應用數據處理操作
    fn apply_data_processing(&self, mut records: Vec<Record>) -> Vec<Record> {
        if let Some(processing) = &self.config.extract.data_processing {
            // 去重
            if processing.deduplicate.unwrap_or(false) {
                let original_count = records.len();
                if let Some(dedup_fields) = &processing.deduplicate_fields {
                    // 基於指定字段去重
                    let mut seen = std::collections::HashSet::new();
                    records.retain(|record| {
                        let key: Vec<String> = dedup_fields
                            .iter()
                            .map(|field| {
                                record.data.get(field)
                                    .map(|v| v.to_string())
                                    .unwrap_or_default()
                            })
                            .collect();
                        seen.insert(key)
                    });
                } else {
                    // 基於整個記錄去重
                    let mut seen = std::collections::HashSet::new();
                    records.retain(|record| {
                        let key = serde_json::to_string(&record.data).unwrap_or_default();
                        seen.insert(key)
                    });
                }
                tracing::info!(
                    "🔄 {}: Deduplicated {} -> {} records",
                    self.name,
                    original_count,
                    records.len()
                );
            }

            // 排序
            if let Some(sort_field) = &processing.sort_by {
                let ascending = processing.sort_order.as_deref() != Some("desc");
                records.sort_by(|a, b| {
                    let a_val = a.data.get(sort_field);
                    let b_val = b.data.get(sort_field);

                    let comparison = match (a_val, b_val) {
                        (Some(a), Some(b)) => a.to_string().cmp(&b.to_string()),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    };

                    if ascending { comparison } else { comparison.reverse() }
                });
                tracing::info!("🔄 {}: Sorted {} records by '{}'", self.name, records.len(), sort_field);
            }
        }

        records
    }
}

#[async_trait::async_trait]
impl<S: Storage> ContextualPipeline for SequenceAwarePipeline<S> {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn extract_with_context(&self, context: &PipelineContext) -> Result<Vec<Record>> {
        tracing::info!("📥 {}: Starting contextual extract", self.name);

        // 決定數據來源並獲取原始數據
        let raw_records = self.determine_data_source(context).await?;

        // 應用數據處理操作
        let processed_records = self.apply_data_processing(raw_records);

        tracing::info!("📥 {}: Extracted {} records", self.name, processed_records.len());
        Ok(processed_records)
    }

    async fn transform_with_context(
        &self,
        data: Vec<Record>,
        context: &PipelineContext,
    ) -> Result<TransformResult> {
        let mut processed_records = Vec::new();
        let mut csv_lines = vec!["id,data,pipeline,processed".to_string()];
        let mut tsv_lines = vec!["id\tdata\tpipeline\tprocessed".to_string()];
        let mut intermediate_data = Vec::new();

        tracing::info!("🔄 {}: Starting contextual transform for {} records", self.name, data.len());

        for (index, mut record) in data.into_iter().enumerate() {
            // 應用轉換操作
            if let Some(operations) = &self.config.transform.operations {
                // 文本清理
                if operations.clean_text.unwrap_or(false) {
                    for (_, value) in record.data.iter_mut() {
                        if let serde_json::Value::String(s) = value {
                            *s = s.trim().replace('\n', " ");
                        }
                    }
                }

                // 標準化字段
                if let Some(normalize_fields) = &operations.normalize_fields {
                    for field in normalize_fields {
                        if let Some(value) = record.data.get_mut(field) {
                            if let serde_json::Value::String(s) = value {
                                *s = s.to_lowercase();
                            }
                        }
                    }
                }
            }

            // 數據豐富化
            if let Some(enrichment) = &self.config.transform.data_enrichment {
                // 查找數據
                if let Some(lookup_data) = &enrichment.lookup_data {
                    for (lookup_field, target_field) in lookup_data {
                        if let Some(lookup_value) = record.data.get(lookup_field) {
                            // 這裡可以實作更複雜的查找邏輯
                            record.data.insert(
                                target_field.clone(),
                                serde_json::Value::String(format!("enriched_{}", lookup_value))
                            );
                        }
                    }
                }

                // 計算字段
                if let Some(computed_fields) = &enrichment.computed_fields {
                    for (field_name, expression) in computed_fields {
                        // 簡單的計算邏輯示例
                        let computed_value = match expression.as_str() {
                            "record_index" => serde_json::Value::Number(index.into()),
                            "pipeline_name" => serde_json::Value::String(self.name.clone()),
                            "execution_id" => serde_json::Value::String(context.execution_id.clone()),
                            _ => serde_json::Value::String(expression.clone()),
                        };
                        record.data.insert(field_name.clone(), computed_value);
                    }
                }
            }

            // 添加處理標記
            record.data.insert("processed".to_string(), serde_json::Value::Bool(true));
            record.data.insert("processed_by".to_string(), serde_json::Value::String(self.name.clone()));

            // 生成輸出格式
            let id = record.data.get("id")
                .and_then(|v| v.as_i64())
                .unwrap_or(index as i64);
            let data_summary = format!("record_{}", index);

            csv_lines.push(format!("{},{},{},true", id, data_summary, self.name));
            tsv_lines.push(format!("{}\t{}\t{}\ttrue", id, data_summary, self.name));

            // 檢查中繼數據條件
            if let Some(intermediate_config) = &self.config.transform.intermediate {
                let mut meets_conditions = true;

                if let Some(conditions) = &intermediate_config.conditions {
                    for (field, expected_value) in conditions {
                        if let Some(actual_value) = record.data.get(field) {
                            if actual_value != expected_value {
                                meets_conditions = false;
                                break;
                            }
                        } else {
                            meets_conditions = false;
                            break;
                        }
                    }
                }

                if meets_conditions {
                    intermediate_data.push(record.clone());

                    // 導出到共享數據
                    if intermediate_config.export_to_shared.unwrap_or(false) {
                        if let Some(shared_key) = &intermediate_config.shared_key {
                            // 這裡需要修改 context 來更新共享數據
                            // 目前的實作中 context 是不可變的，需要重新設計
                            tracing::debug!("📤 {}: Would export to shared data with key '{}'", self.name, shared_key);
                        }
                    }
                }
            }

            processed_records.push(record);
        }

        tracing::info!(
            "🔄 {}: Transform complete: {} processed, {} intermediate",
            self.name,
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

    async fn load_with_context(
        &self,
        result: TransformResult,
        context: &PipelineContext,
    ) -> Result<String> {
        let filename = if let Some(pattern) = &self.config.load.filename_pattern {
            // 簡單的模板替換
            pattern
                .replace("{pipeline_name}", &self.name)
                .replace("{execution_id}", &context.execution_id)
                .replace("{timestamp}", &chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string())
        } else {
            format!("{}_output.zip", self.name)
        };

        let output_path = format!("{}/{}", self.config.load.output_path, filename);

        tracing::info!("💾 {}: Starting contextual load to: {}", self.name, output_path);

        // 創建 ZIP 文件
        let zip_data = {
            let mut zip = ZipWriter::new(std::io::Cursor::new(Vec::new()));

            // 根據配置的輸出格式添加文件
            for format in &self.config.load.output_formats {
                match format.as_str() {
                    "csv" => {
                        zip.start_file::<_, ()>("output.csv", FileOptions::default())?;
                        zip.write_all(result.csv_output.as_bytes())?;
                    }
                    "tsv" => {
                        zip.start_file::<_, ()>("output.tsv", FileOptions::default())?;
                        zip.write_all(result.tsv_output.as_bytes())?;
                    }
                    "json" => {
                        zip.start_file::<_, ()>("processed_data.json", FileOptions::default())?;
                        let json_data = serde_json::to_string_pretty(&result.processed_records)?;
                        zip.write_all(json_data.as_bytes())?;
                    }
                    _ => {
                        tracing::warn!("🔶 {}: Unsupported output format: {}", self.name, format);
                    }
                }
            }

            // 添加中繼結果 JSON
            if !result.intermediate_data.is_empty() {
                zip.start_file::<_, ()>("intermediate.json", FileOptions::default())?;
                let json_data = serde_json::to_string_pretty(&result.intermediate_data)?;
                zip.write_all(json_data.as_bytes())?;
            }

            // 添加元數據
            if let Some(compression) = &self.config.load.compression {
                if compression.include_metadata.unwrap_or(false) {
                    zip.start_file::<_, ()>("metadata.json", FileOptions::default())?;
                    let mut metadata = HashMap::new();
                    metadata.insert("pipeline_name".to_string(), serde_json::Value::String(self.name.clone()));
                    metadata.insert("execution_id".to_string(), serde_json::Value::String(context.execution_id.clone()));
                    metadata.insert("timestamp".to_string(), serde_json::Value::String(chrono::Utc::now().to_rfc3339()));
                    let metadata_json = serde_json::to_string_pretty(&metadata)?;
                    zip.write_all(metadata_json.as_bytes())?;
                }
            }

            // 完成並取回底層 Vec<u8>
            let cursor = zip.finish()?;
            cursor.into_inner()
        };

        // 保存 ZIP 文件
        self.storage.write_file(&filename, &zip_data).await?;

        tracing::info!("💾 {}: Load completed successfully", self.name);
        Ok(output_path)
    }

    fn should_execute(&self, context: &PipelineContext) -> bool {
        // 檢查是否啟用
        if !self.config.enabled.unwrap_or(true) {
            return false;
        }

        // 檢查執行條件
        if let Some(conditions) = &self.config.conditions {
            // 檢查前一個 Pipeline 是否成功
            if let Some(when_previous_succeeded) = conditions.when_previous_succeeded {
                if when_previous_succeeded && context.get_previous_result().is_none() {
                    return false;
                }
            }

            // 檢查記錄數條件
            if let Some(record_condition) = &conditions.when_records_count {
                let record_count = if let Some(from_pipeline) = &record_condition.from_pipeline {
                    context.get_result_by_name(from_pipeline)
                        .map(|r| r.records.len())
                        .unwrap_or(0)
                } else {
                    context.get_previous_result()
                        .map(|r| r.records.len())
                        .unwrap_or(0)
                };

                if let Some(min) = record_condition.min {
                    if record_count < min {
                        return false;
                    }
                }

                if let Some(max) = record_condition.max {
                    if record_count > max {
                        return false;
                    }
                }
            }

            // 檢查共享數據條件
            if let Some(shared_conditions) = &conditions.when_shared_data {
                for (key, expected_value) in shared_conditions {
                    if let Some(actual_value) = context.get_shared_data(key) {
                        if actual_value != expected_value {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
        }

        true
    }

}