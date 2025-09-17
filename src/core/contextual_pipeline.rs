use crate::config::sequence_config::PipelineDefinition;
use crate::core::{
    pipeline_sequence::{ContextualPipeline, PipelineContext},
    Record, Storage, TransformResult,
};
use crate::utils::error::{EtlError, Result};
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
                let endpoint = self.config.source.endpoint.as_deref().unwrap_or("");
                if !data_source.merge_with_api.unwrap_or(false) && !endpoint.contains("{") {
                    return Ok(records);
                }
            }
        }

        // 獲取 API 數據 - 檢查是否需要參數化呼叫
        let endpoint = self.config.source.endpoint.as_deref().unwrap_or("");

        // 對於 "previous" 和 "combined" 類型，不進行 API 呼叫
        if self.config.source.r#type == "previous" || self.config.source.r#type == "combined" {
            return Ok(records);
        }

        // 如果沒有端點，也不進行 API 呼叫
        if endpoint.is_empty() {
            return Ok(records);
        }

        let api_records = if endpoint.contains("{") {
            // 參數化 API 呼叫 - 替換前一個 pipeline 的數據
            return self.fetch_parameterized_api(context).await;
        } else {
            // 標準 API 呼叫
            self.fetch_api_data(context).await?
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
                    context
                        .get_result_by_name(from_pipeline)
                        .map(|r| r.records.clone())
                        .unwrap_or_default()
                } else {
                    context
                        .get_previous_result()
                        .map(|r| r.records.clone())
                        .unwrap_or_default()
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        tracing::info!(
            "📡 {}: Making parameterized API calls for {} records",
            self.name,
            param_records.len()
        );

        // 為每個記錄構建並呼叫 API
        for (index, record) in param_records.iter().enumerate() {
            let endpoint = self.build_parameterized_endpoint(&record.data)?;
            tracing::debug!(
                "📡 {}: API call {}/{}: {}",
                self.name,
                index + 1,
                param_records.len(),
                endpoint
            );

            let api_records = self
                .fetch_single_api_call_with_data(&endpoint, Some(&record.data), context)
                .await?;
            all_records.extend(api_records);

            // 可選：添加延遲避免請求過於頻繁
            if index < param_records.len() - 1 {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }

        tracing::info!(
            "📡 {}: Total records fetched from parameterized APIs: {}",
            self.name,
            all_records.len()
        );
        Ok(all_records)
    }

    /// 處理 header 模板，支援共享數據和記錄數據替換
    fn process_header_template(
        &self,
        template: &str,
        record_data: Option<&HashMap<String, serde_json::Value>>,
        context: &PipelineContext,
    ) -> Result<String> {
        let mut processed = template.to_string();

        // 替換共享數據中的參數 {{key}}
        if processed.contains("{{") && processed.contains("}}") {
            let re = regex::Regex::new(r"\{\{([^}]+)\}\}").unwrap();
            processed = re
                .replace_all(&processed, |caps: &regex::Captures| {
                    let key = &caps[1];
                    if let Some(shared_value) = context.get_shared_data(key) {
                        match shared_value {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            serde_json::Value::Null => "null".to_string(),
                            _ => serde_json::to_string(shared_value)
                                .unwrap_or_default()
                                .trim_matches('"')
                                .to_string(),
                        }
                    } else {
                        // 嘗試從記錄數據中查找
                        if let Some(record_data) = record_data {
                            if let Some(record_value) = record_data.get(key) {
                                match record_value {
                                    serde_json::Value::String(s) => s.clone(),
                                    serde_json::Value::Number(n) => n.to_string(),
                                    serde_json::Value::Bool(b) => b.to_string(),
                                    serde_json::Value::Null => "null".to_string(),
                                    _ => serde_json::to_string(record_value)
                                        .unwrap_or_default()
                                        .trim_matches('"')
                                        .to_string(),
                                }
                            } else {
                                format!("{{{{{}}}}}", key) // 保持原樣，未找到替換值
                            }
                        } else {
                            format!("{{{{{}}}}}", key) // 保持原樣，未找到替換值
                        }
                    }
                })
                .to_string();
        }

        // 檢查是否還有未替換的參數
        if processed.contains("{{") && processed.contains("}}") {
            tracing::warn!(
                "📡 {}: Unresolved template parameters in header: {}",
                self.name,
                processed
            );
        }

        Ok(processed)
    }

    /// 處理 payload 模板，替換參數 (支援 shared data 和 record data)
    fn process_payload_template(
        &self,
        template: &str,
        record_data: Option<&HashMap<String, serde_json::Value>>,
        context: &PipelineContext,
    ) -> Result<String> {
        let mut processed = template.to_string();

        // 替換共享數據中的參數 {{key}}
        if processed.contains("{{") && processed.contains("}}") {
            let re = regex::Regex::new(r"\{\{([^}]+)\}\}").unwrap();
            processed = re
                .replace_all(&processed, |caps: &regex::Captures| {
                    let key = &caps[1];
                    if let Some(shared_value) = context.get_shared_data(key) {
                        match shared_value {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            serde_json::Value::Null => "null".to_string(),
                            _ => serde_json::to_string(shared_value)
                                .unwrap_or_default()
                                .trim_matches('"')
                                .to_string(),
                        }
                    } else {
                        // 嘗試從記錄數據中查找
                        if let Some(record_data) = record_data {
                            if let Some(record_value) = record_data.get(key) {
                                match record_value {
                                    serde_json::Value::String(s) => s.clone(),
                                    serde_json::Value::Number(n) => n.to_string(),
                                    serde_json::Value::Bool(b) => b.to_string(),
                                    serde_json::Value::Null => "null".to_string(),
                                    _ => serde_json::to_string(record_value)
                                        .unwrap_or_default()
                                        .trim_matches('"')
                                        .to_string(),
                                }
                            } else {
                                caps[0].to_string() // 保持原樣如果找不到
                            }
                        } else {
                            caps[0].to_string() // 保持原樣如果找不到
                        }
                    }
                })
                .to_string();
        }

        // 如果配置了模板參數映射
        if let Some(payload_config) = &self.config.source.payload {
            if let Some(template_params) = &payload_config.template_params {
                for (template_key, data_key) in template_params {
                    let placeholder = format!("{{{{{}}}}}", template_key);
                    if processed.contains(&placeholder) {
                        if let Some(record_data) = record_data {
                            if let Some(value) = record_data.get(data_key) {
                                let value_str = match value {
                                    serde_json::Value::String(s) => s.clone(),
                                    serde_json::Value::Number(n) => n.to_string(),
                                    serde_json::Value::Bool(b) => b.to_string(),
                                    serde_json::Value::Null => "null".to_string(),
                                    _ => serde_json::to_string(value)
                                        .unwrap_or_default()
                                        .trim_matches('"')
                                        .to_string(),
                                };
                                processed = processed.replace(&placeholder, &value_str);
                                tracing::debug!(
                                    "📡 {}: Replaced payload template {} with {}",
                                    self.name,
                                    placeholder,
                                    value_str
                                );
                            }
                        }
                    }
                }
            }

            // 如果啟用了使用前一個 pipeline 資料作為參數
            if payload_config.use_previous_data_as_params.unwrap_or(false) {
                if let Some(record_data) = record_data {
                    for (key, value) in record_data {
                        let placeholder = format!("{{{{{}}}}}", key);
                        if processed.contains(&placeholder) {
                            let value_str = match value {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => "null".to_string(),
                                _ => serde_json::to_string(value)
                                    .unwrap_or_default()
                                    .trim_matches('"')
                                    .to_string(),
                            };
                            processed = processed.replace(&placeholder, &value_str);
                            tracing::debug!(
                                "📡 {}: Replaced payload parameter {} with {}",
                                self.name,
                                placeholder,
                                value_str
                            );
                        }
                    }
                }
            }
        }

        // 檢查是否還有未替換的參數
        if processed.contains("{{") && processed.contains("}}") {
            tracing::warn!(
                "📡 {}: Unresolved template parameters in payload: {}",
                self.name,
                processed
            );
        }

        Ok(processed)
    }

    /// 構建參數化端點 URL
    fn build_parameterized_endpoint(
        &self,
        data: &HashMap<String, serde_json::Value>,
    ) -> Result<String> {
        let mut endpoint = self
            .config
            .source
            .endpoint
            .as_ref()
            .ok_or_else(|| EtlError::ConfigValidationError {
                field: "source.endpoint".to_string(),
                message: "Endpoint is required for parameterized API calls".to_string(),
            })?
            .clone();

        tracing::debug!(
            "📡 {}: Building endpoint from template: {}",
            self.name,
            endpoint
        );
        tracing::debug!(
            "📡 {}: Available data fields: {:?}",
            self.name,
            data.keys().collect::<Vec<_>>()
        );

        // 替換 URL 中的參數佔位符 (支援 {key} 和 {{key}} 格式)
        for (key, value) in data {
            let placeholder_single = format!("{{{}}}", key);
            let placeholder_double = format!("{{{{{}}}}}", key);

            let value_str = match value {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => value.to_string().trim_matches('"').to_string(),
            };

            if endpoint.contains(&placeholder_single) {
                endpoint = endpoint.replace(&placeholder_single, &value_str);
                tracing::info!(
                    "📡 {}: Replaced {} with {}",
                    self.name,
                    placeholder_single,
                    value_str
                );
            } else if endpoint.contains(&placeholder_double) {
                endpoint = endpoint.replace(&placeholder_double, &value_str);
                tracing::info!(
                    "📡 {}: Replaced {} with {}",
                    self.name,
                    placeholder_double,
                    value_str
                );
            }
        }

        tracing::debug!("📡 {}: Final endpoint: {}", self.name, endpoint);

        // 檢查是否還有未替換的參數 (單括號格式)
        if endpoint.contains("{") && endpoint.contains("}") {
            // 找出所有未替換的參數
            let re = regex::Regex::new(r"\{([^}]+)\}").unwrap();
            let unresolved: Vec<&str> = re
                .captures_iter(&endpoint)
                .map(|cap| cap.get(1).unwrap().as_str())
                .collect();

            tracing::error!(
                "📡 {}: Unresolved parameters in endpoint: {}",
                self.name,
                endpoint
            );
            tracing::error!(
                "📡 {}: Unresolved parameter names: {:?}",
                self.name,
                unresolved
            );
            tracing::error!(
                "📡 {}: Available fields were: {:?}",
                self.name,
                data.keys().collect::<Vec<_>>()
            );
            return Err(crate::utils::error::EtlError::ProcessingError {
                message: format!("Unresolved parameters in endpoint: {}. Unresolved: {:?}, Available fields: {:?}",
                    endpoint, unresolved, data.keys().collect::<Vec<_>>())
            });
        }

        Ok(endpoint)
    }

    /// 執行單一 API 呼叫，支援資料參數
    async fn fetch_single_api_call_with_data(
        &self,
        endpoint: &str,
        record_data: Option<&HashMap<String, serde_json::Value>>,
        context: &PipelineContext,
    ) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        // 決定 HTTP 方法
        let method = self
            .config
            .source
            .method
            .as_deref()
            .unwrap_or("GET")
            .to_uppercase();

        // 構建請求
        let mut request = match method.as_str() {
            "GET" => self.client.get(endpoint),
            "POST" => self.client.post(endpoint),
            "PUT" => self.client.put(endpoint),
            "DELETE" => self.client.delete(endpoint),
            "PATCH" => self.client.patch(endpoint),
            "HEAD" => self.client.head(endpoint),
            _ => {
                tracing::warn!(
                    "📡 {}: Unsupported HTTP method '{}', falling back to GET",
                    self.name,
                    method
                );
                self.client.get(endpoint)
            }
        };

        // 添加自定義標頭（支援模板替換）
        if let Some(headers) = &self.config.source.headers {
            for (key, value_template) in headers {
                // 替換 header 值中的模板參數
                let processed_value =
                    self.process_header_template(value_template, record_data, context)?;
                request = request.header(key, &processed_value);
                tracing::debug!("📡 {}: Set header {} = {}", self.name, key, processed_value);
            }
        }

        // 處理 payload
        if let Some(payload_config) = &self.config.source.payload {
            // 設定 Content-Type
            if let Some(content_type) = &payload_config.content_type {
                request = request.header("Content-Type", content_type);
            } else if method != "GET" && method != "HEAD" {
                request = request.header("Content-Type", "application/json");
            }

            // 處理請求體
            if let Some(body_template) = &payload_config.body {
                let processed_body =
                    self.process_payload_template(body_template, record_data, context)?;
                if !processed_body.is_empty() {
                    tracing::debug!("📡 {}: Request body: {}", self.name, processed_body);
                    request = request.body(processed_body);
                }
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

        tracing::debug!(
            "📡 {}: Making {} request to: {}",
            self.name,
            method,
            endpoint
        );

        // 執行請求
        let response = request.send().await?;

        if response.status().is_success() {
            let json_data: serde_json::Value = response.json().await?;

            // 處理 API 回應（支持單一物件回應）
            if let serde_json::Value::Object(obj) = json_data {
                let mut data = HashMap::new();

                // 應用字段映射（支援多階層路徑）
                if let Some(field_mapping) = &self.config.extract.field_mapping {
                    // 先處理簡單的頂層映射
                    for (original_key, value) in &obj {
                        let mapped_key = field_mapping.get(original_key).unwrap_or(original_key);
                        data.insert(mapped_key.clone(), value.clone());
                    }

                    // 再處理多階層路徑映射（如 "user.profile.name" = "user_name"）
                    for (path, mapped_key) in field_mapping {
                        if path.contains('.') {
                            if let Some(nested_value) = self.extract_nested_value(&obj, path) {
                                data.insert(mapped_key.clone(), nested_value);
                            }
                        }
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
                            // 先處理簡單的頂層映射
                            for (original_key, value) in &obj {
                                let mapped_key =
                                    field_mapping.get(original_key).unwrap_or(original_key);
                                data.insert(mapped_key.clone(), value.clone());
                            }

                            // 再處理多階層路徑映射
                            for (path, mapped_key) in field_mapping {
                                if path.contains('.') {
                                    if let Some(nested_value) =
                                        self.extract_nested_value(&obj, path)
                                    {
                                        data.insert(mapped_key.clone(), nested_value);
                                    }
                                }
                            }
                        } else {
                            for (key, value) in &obj {
                                data.insert(key.clone(), value.clone());
                            }
                        }

                        records.push(Record { data });
                    }
                }
            }
        } else {
            let error_msg = format!("API request failed with status: {}", response.status());
            return Err(crate::utils::error::EtlError::ProcessingError { message: error_msg });
        }

        Ok(records)
    }

    /// 從 API 獲取數據
    async fn fetch_api_data(&self, context: &PipelineContext) -> Result<Vec<Record>> {
        let endpoint = self.config.source.endpoint.as_ref().ok_or_else(|| {
            EtlError::ConfigValidationError {
                field: "source.endpoint".to_string(),
                message: "Endpoint is required for API calls".to_string(),
            }
        })?;

        self.fetch_single_api_call_with_data(endpoint, None, context)
            .await
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
                                record
                                    .data
                                    .get(field)
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

                    if ascending {
                        comparison
                    } else {
                        comparison.reverse()
                    }
                });
                tracing::info!(
                    "🔄 {}: Sorted {} records by '{}'",
                    self.name,
                    records.len(),
                    sort_field
                );
            }
        }

        records
    }

    /// 從多階層 JSON 物件中提取巢狀值
    /// 支援路徑如 "user.profile.name" 來存取巢狀欄位
    /// 支援陣列索引如 "user.items[0].name" 和 flat mapping "user.items[*].name"
    fn extract_nested_value(
        &self,
        obj: &serde_json::Map<String, serde_json::Value>,
        path: &str,
    ) -> Option<serde_json::Value> {
        if path.is_empty()
            || path.trim_matches('.').is_empty()
            || path.contains("..")
            || path.ends_with('.')
            || path.starts_with('.')
        {
            return None;
        }

        let mut current: serde_json::Value = serde_json::Value::Object(obj.clone());
        let mut remaining_path = path;

        while !remaining_path.is_empty() {
            // 尋找下一個分隔符（. 或 [）
            let next_delimiter = remaining_path
                .find('.')
                .unwrap_or(remaining_path.len())
                .min(remaining_path.find('[').unwrap_or(remaining_path.len()));

            if next_delimiter == 0 {
                // 路徑以 . 或 [ 開頭，跳過
                remaining_path = &remaining_path[1..];
                continue;
            }

            let part = &remaining_path[..next_delimiter];
            remaining_path = if next_delimiter < remaining_path.len() {
                &remaining_path[next_delimiter..]
            } else {
                ""
            };

            // 處理物件欄位
            match &current {
                serde_json::Value::Object(map) => {
                    if let Some(value) = map.get(part) {
                        current = value.clone();
                    } else {
                        tracing::debug!(
                            "🔍 {}: Nested field '{}' not found in path '{}'",
                            self.name,
                            part,
                            path
                        );
                        return None;
                    }
                }
                _ => {
                    tracing::debug!(
                        "🔍 {}: Expected object at '{}' in path '{}', found: {:?}",
                        self.name,
                        part,
                        path,
                        current
                    );
                    return None;
                }
            }

            // 處理陣列索引
            if remaining_path.starts_with('[') {
                let end_bracket = remaining_path.find(']')?;
                let index_str = &remaining_path[1..end_bracket];
                remaining_path = if end_bracket + 1 < remaining_path.len() {
                    &remaining_path[end_bracket + 1..]
                } else {
                    ""
                };

                // 跳過緊接的點號
                if remaining_path.starts_with('.') {
                    remaining_path = &remaining_path[1..];
                }

                match &current {
                    serde_json::Value::Array(arr) => {
                        if index_str == "*" {
                            // Flat mapping: 提取所有元素的指定欄位
                            if remaining_path.is_empty() {
                                // 如果沒有更多路徑，返回整個陣列
                                return Some(current);
                            } else {
                                // 遞歸提取每個元素的剩餘路徑
                                let mut results = Vec::new();
                                for item in arr {
                                    if let serde_json::Value::Object(item_obj) = item {
                                        if let Some(extracted) =
                                            self.extract_nested_value(item_obj, remaining_path)
                                        {
                                            results.push(extracted);
                                        }
                                    }
                                }
                                return Some(serde_json::Value::Array(results));
                            }
                        } else {
                            // 索引存取
                            let index: std::result::Result<i32, _> = index_str.parse();
                            match index {
                                Ok(idx) => {
                                    let actual_index = if idx < 0 {
                                        (arr.len() as i32 + idx) as usize
                                    } else {
                                        idx as usize
                                    };

                                    if actual_index < arr.len() {
                                        current = arr[actual_index].clone();
                                    } else {
                                        tracing::debug!(
                                            "🔍 {}: Array index {} out of bounds (length: {}) in path '{}'",
                                            self.name,
                                            idx,
                                            arr.len(),
                                            path
                                        );
                                        return None;
                                    }
                                }
                                Err(_) => {
                                    tracing::debug!(
                                        "🔍 {}: Invalid array index '{}' in path '{}'",
                                        self.name,
                                        index_str,
                                        path
                                    );
                                    return None;
                                }
                            }
                        }
                    }
                    _ => {
                        tracing::debug!(
                            "🔍 {}: Expected array for indexing in path '{}', found: {:?}",
                            self.name,
                            path,
                            current
                        );
                        return None;
                    }
                }
            }

            // 跳過路徑開頭的點號
            if remaining_path.starts_with('.') {
                remaining_path = &remaining_path[1..];
            }
        }

        Some(current)
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

        tracing::info!(
            "📥 {}: Extracted {} records",
            self.name,
            processed_records.len()
        );
        Ok(processed_records)
    }

    async fn transform_with_context(
        &self,
        data: Vec<Record>,
        context: &mut PipelineContext,
    ) -> Result<TransformResult> {
        let mut processed_records = Vec::new();
        let mut csv_lines = Vec::new();
        let mut tsv_lines = Vec::new();
        let mut intermediate_data = Vec::new();
        let mut headers_generated = false;

        tracing::info!(
            "🔄 {}: Starting contextual transform for {} records",
            self.name,
            data.len()
        );

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
                        if let Some(serde_json::Value::String(s)) = record.data.get_mut(field) {
                            *s = s.to_lowercase();
                        }
                    }
                }

                // 欄位過濾：只保留指定欄位
                if let Some(keep_only_fields) = &operations.keep_only_fields {
                    let mut filtered_data = HashMap::new();
                    for field in keep_only_fields {
                        if let Some(value) = record.data.get(field) {
                            filtered_data.insert(field.clone(), value.clone());
                        } else {
                            tracing::debug!(
                                "🔄 {}: Field '{}' specified in keep_only_fields not found",
                                self.name,
                                field
                            );
                        }
                    }

                    let original_count = record.data.len();
                    record.data = filtered_data;
                    tracing::debug!(
                        "🔄 {}: Filtered fields {} -> {} (keeping only: {:?})",
                        self.name,
                        original_count,
                        record.data.len(),
                        keep_only_fields
                    );
                }
                // 欄位過濾：排除指定欄位
                else if let Some(exclude_fields) = &operations.exclude_fields {
                    for field in exclude_fields {
                        if record.data.remove(field).is_some() {
                            tracing::debug!("🔄 {}: Excluded field '{}'", self.name, field);
                        } else {
                            tracing::debug!(
                                "🔄 {}: Field '{}' specified in exclude_fields not found",
                                self.name,
                                field
                            );
                        }
                    }

                    tracing::debug!(
                        "🔄 {}: Excluded {} fields, {} fields remaining",
                        self.name,
                        exclude_fields.len(),
                        record.data.len()
                    );
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
                                serde_json::Value::String(format!("enriched_{}", lookup_value)),
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
                            "execution_id" => {
                                serde_json::Value::String(context.execution_id.clone())
                            }
                            _ => serde_json::Value::String(expression.clone()),
                        };
                        record.data.insert(field_name.clone(), computed_value);
                    }
                }
            }

            // 添加處理標記
            record
                .data
                .insert("processed".to_string(), serde_json::Value::Bool(true));
            record.data.insert(
                "processed_by".to_string(),
                serde_json::Value::String(self.name.clone()),
            );

            // 生成動態 CSV/TSV 輸出格式
            if !headers_generated {
                // 根據第一筆記錄生成標頭
                let mut field_names: Vec<String> = record.data.keys().cloned().collect();
                field_names.sort(); // 確保一致的欄位順序

                // 生成 CSV 標頭
                csv_lines.push(field_names.join(","));

                // 生成 TSV 標頭
                tsv_lines.push(field_names.join("\t"));

                headers_generated = true;

                tracing::debug!(
                    "🔄 {}: Generated headers for {} fields: {:?}",
                    self.name,
                    field_names.len(),
                    field_names
                );
            }

            // 根據欄位順序生成資料行
            if headers_generated {
                let header_line = csv_lines[0].clone(); // 複製標頭行避免借用衝突
                let field_names: Vec<&str> = header_line.split(',').collect();

                // 生成 CSV 資料行
                let csv_values: Vec<String> = field_names
                    .iter()
                    .map(|field_name| {
                        record
                            .data
                            .get(*field_name)
                            .map(|value| match value {
                                serde_json::Value::String(s) => {
                                    // CSV 欄位轉義：包含逗號、引號或換行的字串用引號包圍
                                    if s.contains(',') || s.contains('"') || s.contains('\n') {
                                        format!("\"{}\"", s.replace('"', "\"\""))
                                    } else {
                                        s.clone()
                                    }
                                }
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => "".to_string(),
                                _ => serde_json::to_string(value).unwrap_or_default().trim_matches('"').to_string(),
                            })
                            .unwrap_or_else(|| "".to_string())
                    })
                    .collect();

                csv_lines.push(csv_values.join(","));

                // 生成 TSV 資料行
                let tsv_values: Vec<String> = field_names
                    .iter()
                    .map(|field_name| {
                        record
                            .data
                            .get(*field_name)
                            .map(|value| match value {
                                serde_json::Value::String(s) => s.replace('\t', " ").replace('\n', " "),
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => "".to_string(),
                                _ => serde_json::to_string(value).unwrap_or_default().trim_matches('"').to_string().replace('\t', " ").replace('\n', " "),
                            })
                            .unwrap_or_else(|| "".to_string())
                    })
                    .collect();

                tsv_lines.push(tsv_values.join("\t"));
            }

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
                            // 從記錄中提取需要的值（例如 token）
                            for (key, value) in &record.data {
                                let full_key = if shared_key.is_empty() {
                                    key.clone()
                                } else {
                                    format!("{}_{}", shared_key, key)
                                };

                                // 特殊處理 token 字段
                                if key == "token" || key == "access_token" {
                                    context.add_shared_data("token".to_string(), value.clone());
                                    tracing::info!(
                                        "📤 {}: Exported {} to shared data as 'token'",
                                        self.name,
                                        key
                                    );
                                } else {
                                    let full_key_clone = full_key.clone();
                                    context.add_shared_data(full_key, value.clone());
                                    tracing::debug!(
                                        "📤 {}: Exported {} to shared data as '{}'",
                                        self.name,
                                        key,
                                        full_key_clone
                                    );
                                }
                            }
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
                .replace(
                    "{timestamp}",
                    &chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string(),
                )
        } else {
            format!("{}_output.zip", self.name)
        };

        let output_path = format!("{}/{}", self.config.load.output_path, filename);

        tracing::info!(
            "💾 {}: Starting contextual load to: {}",
            self.name,
            output_path
        );

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
                    metadata.insert(
                        "pipeline_name".to_string(),
                        serde_json::Value::String(self.name.clone()),
                    );
                    metadata.insert(
                        "execution_id".to_string(),
                        serde_json::Value::String(context.execution_id.clone()),
                    );
                    metadata.insert(
                        "timestamp".to_string(),
                        serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
                    );
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
                    context
                        .get_result_by_name(from_pipeline)
                        .map(|r| r.records.len())
                        .unwrap_or(0)
                } else {
                    context
                        .get_previous_result()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalStorage;
    use serde_json::json;
    use tempfile::TempDir;

    // 創建測試用的 SequenceAwarePipeline
    fn create_test_pipeline() -> SequenceAwarePipeline<LocalStorage> {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap().to_string());

        let config = PipelineDefinition {
            name: "test_pipeline".to_string(),
            description: None,
            enabled: Some(true),
            source: crate::config::sequence_config::SourceConfig {
                r#type: "api".to_string(),
                endpoint: Some("http://test.com".to_string()),
                method: None,
                timeout_seconds: None,
                retry_attempts: None,
                retry_delay_seconds: None,
                headers: None,
                parameters: None,
                payload: None,
                data_source: None,
            },
            extract: crate::config::sequence_config::ExtractConfig {
                max_records: None,
                concurrent_requests: None,
                field_mapping: None,
                filters: None,
                data_processing: None,
            },
            transform: crate::config::sequence_config::TransformConfig {
                operations: None,
                validation: None,
                intermediate: None,
                data_enrichment: None,
            },
            load: crate::config::sequence_config::LoadConfig {
                output_path: temp_dir.path().to_str().unwrap().to_string(),
                output_formats: vec!["json".to_string()],
                filename_pattern: None,
                compression: None,
                append_to_sequence: None,
            },
            dependencies: None,
            conditions: None,
        };

        SequenceAwarePipeline::new("test_pipeline".to_string(), storage, config)
    }

    #[test]
    fn test_extract_nested_value_simple_path() {
        let pipeline = create_test_pipeline();

        let obj = json!({
            "user": {
                "name": "John Doe",
                "email": "john@example.com"
            }
        })
        .as_object()
        .unwrap()
        .clone();

        // 測試簡單的兩層路徑
        let result = pipeline.extract_nested_value(&obj, "user.name");
        assert_eq!(result, Some(json!("John Doe")));

        let result = pipeline.extract_nested_value(&obj, "user.email");
        assert_eq!(result, Some(json!("john@example.com")));
    }

    #[test]
    fn test_extract_nested_value_deep_path() {
        let pipeline = create_test_pipeline();

        let obj = json!({
            "user": {
                "profile": {
                    "personal": {
                        "name": "Jane Smith",
                        "age": 30
                    },
                    "settings": {
                        "theme": "dark",
                        "notifications": {
                            "email": true,
                            "sms": false
                        }
                    }
                }
            }
        })
        .as_object()
        .unwrap()
        .clone();

        // 測試深層巢狀路徑
        let result = pipeline.extract_nested_value(&obj, "user.profile.personal.name");
        assert_eq!(result, Some(json!("Jane Smith")));

        let result = pipeline.extract_nested_value(&obj, "user.profile.personal.age");
        assert_eq!(result, Some(json!(30)));

        let result = pipeline.extract_nested_value(&obj, "user.profile.settings.theme");
        assert_eq!(result, Some(json!("dark")));

        let result =
            pipeline.extract_nested_value(&obj, "user.profile.settings.notifications.email");
        assert_eq!(result, Some(json!(true)));

        let result = pipeline.extract_nested_value(&obj, "user.profile.settings.notifications.sms");
        assert_eq!(result, Some(json!(false)));
    }

    #[test]
    fn test_extract_nested_value_different_types() {
        let pipeline = create_test_pipeline();

        let obj = json!({
            "data": {
                "string_value": "test string",
                "number_value": 42,
                "float_value": 3.14,
                "boolean_value": true,
                "null_value": null,
                "array_value": [1, 2, 3],
                "object_value": {
                    "nested": "value"
                }
            }
        })
        .as_object()
        .unwrap()
        .clone();

        // 測試不同資料類型
        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.string_value"),
            Some(json!("test string"))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.number_value"),
            Some(json!(42))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.float_value"),
            Some(json!(3.14))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.boolean_value"),
            Some(json!(true))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.null_value"),
            Some(json!(null))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.array_value"),
            Some(json!([1, 2, 3]))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.object_value"),
            Some(json!({"nested": "value"}))
        );
    }

    #[test]
    fn test_extract_nested_value_nonexistent_paths() {
        let pipeline = create_test_pipeline();

        let obj = json!({
            "user": {
                "name": "John Doe",
                "profile": {
                    "email": "john@example.com"
                }
            }
        })
        .as_object()
        .unwrap()
        .clone();

        // 測試不存在的路徑
        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.nonexistent"),
            None
        );
        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.profile.nonexistent"),
            None
        );
        assert_eq!(
            pipeline.extract_nested_value(&obj, "nonexistent.path"),
            None
        );
        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.name.invalid"),
            None
        );
    }

    #[test]
    fn test_extract_nested_value_edge_cases() {
        let pipeline = create_test_pipeline();

        let obj = json!({
            "user": {
                "name": "John Doe"
            }
        })
        .as_object()
        .unwrap()
        .clone();

        // 測試邊界情況
        assert_eq!(pipeline.extract_nested_value(&obj, ""), None);
        assert_eq!(pipeline.extract_nested_value(&obj, "."), None);
        assert_eq!(pipeline.extract_nested_value(&obj, "user."), None);
        assert_eq!(pipeline.extract_nested_value(&obj, ".user"), None);
        assert_eq!(pipeline.extract_nested_value(&obj, "user..name"), None);
    }

    #[test]
    fn test_extract_nested_value_single_level() {
        let pipeline = create_test_pipeline();

        let obj = json!({
            "name": "Direct Value",
            "count": 123
        })
        .as_object()
        .unwrap()
        .clone();

        // 測試單層欄位（雖然方法主要用於多層，但應該也能處理單層）
        assert_eq!(
            pipeline.extract_nested_value(&obj, "name"),
            Some(json!("Direct Value"))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "count"),
            Some(json!(123))
        );

        assert_eq!(pipeline.extract_nested_value(&obj, "nonexistent"), None);
    }

    #[test]
    fn test_extract_nested_value_array_in_path() {
        let pipeline = create_test_pipeline();

        let obj = json!({
            "users": [
                {"name": "User 1"},
                {"name": "User 2"}
            ],
            "data": {
                "items": ["a", "b", "c"],
                "metadata": {
                    "tags": ["tag1", "tag2"]
                }
            }
        })
        .as_object()
        .unwrap()
        .clone();

        // 當路徑中遇到陣列時，應該返回 None（因為我們期望物件）
        assert_eq!(pipeline.extract_nested_value(&obj, "users.name"), None);
        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.items.length"),
            None
        );

        // 但可以提取陣列本身
        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.items"),
            Some(json!(["a", "b", "c"]))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "data.metadata.tags"),
            Some(json!(["tag1", "tag2"]))
        );
    }

    #[test]
    fn test_extract_nested_value_complex_real_world_example() {
        let pipeline = create_test_pipeline();

        // 模擬真實 API 回應結構
        let obj = json!({
            "id": "user_123",
            "user": {
                "personal": {
                    "first_name": "John",
                    "last_name": "Doe",
                    "birth_date": "1990-01-15"
                },
                "contact": {
                    "email": "john.doe@example.com",
                    "phone": {
                        "primary": "+1-555-0123",
                        "secondary": "+1-555-0124"
                    },
                    "address": {
                        "street": "123 Main St",
                        "city": "New York",
                        "state": "NY",
                        "postal_code": "10001",
                        "country": "USA"
                    }
                },
                "preferences": {
                    "language": "en-US",
                    "timezone": "America/New_York",
                    "notifications": {
                        "email": true,
                        "push": false,
                        "sms": true
                    }
                },
                "account": {
                    "created_at": "2020-01-15T10:30:00Z",
                    "subscription": {
                        "plan": "premium",
                        "expires_at": "2024-12-31T23:59:59Z",
                        "features": ["api_access", "priority_support"]
                    }
                }
            },
            "metadata": {
                "version": "1.0",
                "last_updated": "2024-03-20T14:45:00Z"
            }
        })
        .as_object()
        .unwrap()
        .clone();

        // 測試各種真實路徑
        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.personal.first_name"),
            Some(json!("John"))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.contact.email"),
            Some(json!("john.doe@example.com"))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.contact.phone.primary"),
            Some(json!("+1-555-0123"))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.contact.address.city"),
            Some(json!("New York"))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.preferences.notifications.email"),
            Some(json!(true))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.account.subscription.plan"),
            Some(json!("premium"))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "user.account.subscription.features"),
            Some(json!(["api_access", "priority_support"]))
        );

        assert_eq!(
            pipeline.extract_nested_value(&obj, "metadata.last_updated"),
            Some(json!("2024-03-20T14:45:00Z"))
        );
    }

    #[test]
    fn test_extract_nested_value_array_indexing() {
        let pipeline = create_test_pipeline();

        // 模擬包含陣列的 JSON 結構
        let obj = json!({
            "users": [
                {
                    "id": 1,
                    "name": "Alice",
                    "email": "alice@example.com",
                    "tags": ["admin", "developer"]
                },
                {
                    "id": 2,
                    "name": "Bob",
                    "email": "bob@example.com",
                    "tags": ["user", "designer"]
                },
                {
                    "id": 3,
                    "name": "Charlie",
                    "email": "charlie@example.com",
                    "tags": ["user"]
                }
            ],
            "company": {
                "departments": [
                    {
                        "name": "Engineering",
                        "employees": [
                            {"name": "Dave", "role": "senior"},
                            {"name": "Eve", "role": "junior"}
                        ]
                    },
                    {
                        "name": "Design",
                        "employees": [
                            {"name": "Frank", "role": "lead"}
                        ]
                    }
                ]
            }
        })
        .as_object()
        .unwrap()
        .clone();

        // 測試正常索引存取
        assert_eq!(
            pipeline.extract_nested_value(&obj, "users[0].name"),
            Some(json!("Alice"))
        );
        assert_eq!(
            pipeline.extract_nested_value(&obj, "users[1].email"),
            Some(json!("bob@example.com"))
        );
        assert_eq!(
            pipeline.extract_nested_value(&obj, "users[2].id"),
            Some(json!(3))
        );

        // 測試負數索引（從後往前）
        assert_eq!(
            pipeline.extract_nested_value(&obj, "users[-1].name"),
            Some(json!("Charlie"))
        );
        assert_eq!(
            pipeline.extract_nested_value(&obj, "users[-2].name"),
            Some(json!("Bob"))
        );

        // 測試嵌套陣列索引
        assert_eq!(
            pipeline.extract_nested_value(&obj, "company.departments[0].name"),
            Some(json!("Engineering"))
        );
        assert_eq!(
            pipeline.extract_nested_value(&obj, "company.departments[0].employees[1].name"),
            Some(json!("Eve"))
        );
        assert_eq!(
            pipeline.extract_nested_value(&obj, "company.departments[1].employees[0].role"),
            Some(json!("lead"))
        );

        // 測試越界情況
        assert_eq!(pipeline.extract_nested_value(&obj, "users[5].name"), None);
        assert_eq!(pipeline.extract_nested_value(&obj, "users[-5].name"), None);

        // 測試非陣列上使用索引
        assert_eq!(
            pipeline.extract_nested_value(&obj, "users[0].name[0]"),
            None
        );
    }

    #[test]
    fn test_extract_nested_value_flat_mapping() {
        let pipeline = create_test_pipeline();

        // 模擬包含陣列的 JSON 結構
        let obj = json!({
            "products": [
                {
                    "id": 101,
                    "name": "Laptop",
                    "price": 999.99,
                    "category": "electronics",
                    "specs": {
                        "cpu": "Intel i7",
                        "ram": "16GB"
                    }
                },
                {
                    "id": 102,
                    "name": "Mouse",
                    "price": 29.99,
                    "category": "electronics",
                    "specs": {
                        "dpi": "1600",
                        "wireless": true
                    }
                },
                {
                    "id": 103,
                    "name": "Desk",
                    "price": 299.99,
                    "category": "furniture",
                    "specs": {
                        "material": "wood",
                        "adjustable": false
                    }
                }
            ],
            "orders": [
                {
                    "order_id": "A001",
                    "items": [
                        {"product_id": 101, "quantity": 1},
                        {"product_id": 102, "quantity": 2}
                    ]
                },
                {
                    "order_id": "A002",
                    "items": [
                        {"product_id": 103, "quantity": 1}
                    ]
                }
            ]
        })
        .as_object()
        .unwrap()
        .clone();

        // 測試 flat mapping 提取所有產品名稱
        assert_eq!(
            pipeline.extract_nested_value(&obj, "products[*].name"),
            Some(json!(["Laptop", "Mouse", "Desk"]))
        );

        // 測試 flat mapping 提取所有產品 ID
        assert_eq!(
            pipeline.extract_nested_value(&obj, "products[*].id"),
            Some(json!([101, 102, 103]))
        );

        // 測試 flat mapping 提取所有價格
        assert_eq!(
            pipeline.extract_nested_value(&obj, "products[*].price"),
            Some(json!([999.99, 29.99, 299.99]))
        );

        // 測試 flat mapping 提取巢狀欄位
        assert_eq!(
            pipeline.extract_nested_value(&obj, "products[*].specs.cpu"),
            Some(json!(["Intel i7"])) // 只有第一個產品有 CPU
        );

        // 測試 flat mapping 提取訂單ID
        assert_eq!(
            pipeline.extract_nested_value(&obj, "orders[*].order_id"),
            Some(json!(["A001", "A002"]))
        );

        // 測試返回整個陣列（沒有更多路徑）
        let products_array = pipeline.extract_nested_value(&obj, "products[*]");
        assert!(products_array.is_some());
        if let Some(serde_json::Value::Array(arr)) = products_array {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_extract_nested_value_array_edge_cases() {
        let pipeline = create_test_pipeline();

        // 空陣列
        let obj1 = json!({
            "empty_array": []
        })
        .as_object()
        .unwrap()
        .clone();

        assert_eq!(pipeline.extract_nested_value(&obj1, "empty_array[0]"), None);
        assert_eq!(
            pipeline.extract_nested_value(&obj1, "empty_array[*].name"),
            Some(json!([])) // 空陣列的 flat mapping 應返回空陣列
        );

        // 包含 null 值的陣列
        let obj2 = json!({
            "mixed_array": [
                {"name": "valid"},
                null,
                {"name": "another_valid"},
                {"other_field": "no_name"}
            ]
        })
        .as_object()
        .unwrap()
        .clone();

        assert_eq!(
            pipeline.extract_nested_value(&obj2, "mixed_array[0].name"),
            Some(json!("valid"))
        );
        assert_eq!(
            pipeline.extract_nested_value(&obj2, "mixed_array[1].name"),
            None
        );

        // Flat mapping 應該跳過無效項目
        assert_eq!(
            pipeline.extract_nested_value(&obj2, "mixed_array[*].name"),
            Some(json!(["valid", "another_valid"]))
        );

        // 無效的索引格式
        let obj3 = json!({
            "array": [{"name": "test"}]
        })
        .as_object()
        .unwrap()
        .clone();

        assert_eq!(
            pipeline.extract_nested_value(&obj3, "array[abc].name"),
            None
        );
        assert_eq!(pipeline.extract_nested_value(&obj3, "array[].name"), None);
        assert_eq!(
            pipeline.extract_nested_value(&obj3, "array[1.5].name"),
            None
        );
    }

    #[test]
    fn test_process_payload_template_with_shared_data() {
        let pipeline = create_test_pipeline();
        let mut context = PipelineContext::new("test_execution".to_string());

        // 添加 shared data
        context.add_shared_data("api_key".to_string(), serde_json::json!("secret_key_123"));
        context.add_shared_data("user_id".to_string(), serde_json::json!(42));
        context.add_shared_data("active".to_string(), serde_json::json!(true));

        // 創建 record data
        let mut record_data = HashMap::new();
        record_data.insert("operation".to_string(), serde_json::json!("create_user"));
        record_data.insert("name".to_string(), serde_json::json!("John Doe"));

        // 測試包含 shared data 和 record data 的模板
        let template = r#"{
    "api_key": "{{api_key}}",
    "user_id": {{user_id}},
    "active": {{active}},
    "operation": "{{operation}}",
    "user_name": "{{name}}",
    "unknown_key": "{{unknown}}"
}"#;

        let result = pipeline.process_payload_template(template, Some(&record_data), &context);
        assert!(result.is_ok());

        let processed = result.unwrap();

        // 驗證 shared data 替換
        assert!(processed.contains(r#""api_key": "secret_key_123""#));
        assert!(processed.contains(r#""user_id": 42"#));
        assert!(processed.contains(r#""active": true"#));

        // 驗證 record data 替換
        assert!(processed.contains(r#""operation": "create_user""#));
        assert!(processed.contains(r#""user_name": "John Doe""#));

        // 驗證未知 key 保持原樣
        assert!(processed.contains(r#""unknown_key": "{{unknown}}""#));

        println!("Processed payload: {}", processed);
    }

    #[test]
    fn test_process_payload_template_shared_data_priority() {
        let pipeline = create_test_pipeline();
        let mut context = PipelineContext::new("test_execution".to_string());

        // 添加 shared data
        context.add_shared_data("key".to_string(), serde_json::json!("shared_value"));

        // 創建 record data 有相同的 key
        let mut record_data = HashMap::new();
        record_data.insert("key".to_string(), serde_json::json!("record_value"));

        let template = r#"{"value": "{{key}}"}"#;

        let result = pipeline.process_payload_template(template, Some(&record_data), &context);
        assert!(result.is_ok());

        let processed = result.unwrap();

        // 驗證 shared data 優先於 record data
        assert!(processed.contains(r#""value": "shared_value""#));
        assert!(!processed.contains("record_value"));

        println!("Processed payload (priority test): {}", processed);
    }
}
