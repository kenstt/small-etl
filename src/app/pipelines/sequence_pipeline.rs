use crate::core::{Record, TransformResult};
use crate::utils::error::{EtlError, Result};
use crate::utils::monitor::SystemMonitor;
use std::collections::HashMap;
use std::time::Instant;

/// Pipeline 執行結果
#[derive(Debug, Clone)]
pub struct PipelineResult {
    pub pipeline_name: String,
    pub records: Vec<Record>,
    pub output_path: String,
    pub duration: std::time::Duration,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Pipeline 執行上下文，用於在 Pipeline 間傳遞數據
#[derive(Debug, Clone)]
pub struct PipelineContext {
    pub previous_results: Vec<PipelineResult>,
    pub shared_data: HashMap<String, serde_json::Value>,
    pub execution_id: String,
    pipeline_data: HashMap<String, Vec<Record>>,
}

impl PipelineContext {
    pub fn new(execution_id: String) -> Self {
        Self {
            previous_results: Vec::new(),
            shared_data: HashMap::new(),
            execution_id,
            pipeline_data: HashMap::new(),
        }
    }

    /// 獲取上一個 Pipeline 的結果
    pub fn get_previous_result(&self) -> Option<&PipelineResult> {
        self.previous_results.last()
    }

    /// 獲取指定名稱的 Pipeline 結果
    pub fn get_result_by_name(&self, name: &str) -> Option<&PipelineResult> {
        self.previous_results.iter().find(|r| r.pipeline_name == name)
    }

    /// 獲取所有之前處理的記錄
    pub fn get_all_previous_records(&self) -> Vec<Record> {
        self.previous_results
            .iter()
            .flat_map(|result| result.records.clone())
            .collect()
    }

    /// 添加 Pipeline 數據
    pub fn add_pipeline_data(&mut self, pipeline_name: String, records: Vec<Record>) {
        self.pipeline_data.insert(pipeline_name, records);
    }

    /// 獲取 Pipeline 數據
    pub fn get_pipeline_data(&self, pipeline_name: &str) -> Option<&Vec<Record>> {
        self.pipeline_data.get(pipeline_name)
    }

    /// 添加共享數據
    pub fn add_shared_data(&mut self, key: String, value: serde_json::Value) {
        self.shared_data.insert(key, value);
    }

    /// 獲取共享數據
    pub fn get_shared_data(&self, key: &str) -> Option<&serde_json::Value> {
        self.shared_data.get(key)
    }

    /// 與前一個 Pipeline 的數據合併
    pub fn merge_with_previous(&self, pipeline_name: &str, api_records: Vec<Record>) -> Vec<Record> {
        if let Some(previous_records) = self.get_pipeline_data(pipeline_name) {
            let mut merged = Vec::new();

            for api_record in api_records {
                let mut merged_data = api_record.data.clone();

                // 嘗試根據 ID 合併數據
                if let Some(api_id) = api_record.data.get("id") {
                    for prev_record in previous_records {
                        if prev_record.data.get("id") == Some(api_id) {
                            // 合併數據，API 數據優先
                            for (key, value) in &prev_record.data {
                                merged_data.entry(key.clone()).or_insert(value.clone());
                            }
                            break;
                        }
                    }
                }

                merged.push(Record { data: merged_data });
            }

            merged
        } else {
            api_records
        }
    }

    /// 添加結果到上下文
    pub fn add_result(&mut self, result: PipelineResult) {
        // 同時添加到 pipeline_data 供後續使用
        self.add_pipeline_data(result.pipeline_name.clone(), result.records.clone());
        self.previous_results.push(result);
    }
}

/// 帶上下文的 Pipeline 介面
#[async_trait::async_trait]
pub trait ContextualPipeline: Send + Sync {
    async fn extract_with_context(&self, context: &PipelineContext) -> Result<Vec<Record>>;
    async fn transform_with_context(
        &self,
        data: Vec<Record>,
        context: &PipelineContext,
    ) -> Result<TransformResult>;
    async fn load_with_context(&self, result: TransformResult, context: &PipelineContext) -> Result<String>;

    /// 用於標識 pipeline 名稱
    fn get_name(&self) -> &str;

    /// 根據上下文決定是否執行
    fn should_execute(&self, _context: &PipelineContext) -> bool {
        true
    }
}

/// Pipeline 序列，負責順序執行多個帶上下文的 Pipeline
pub struct PipelineSequence {
    pipelines: Vec<Box<dyn ContextualPipeline>>, // 使用 trait object 支持多態
    monitor: Option<SystemMonitor>,
    monitor_enabled: bool,
    execution_id: String,
}

impl PipelineSequence {
    pub fn new(execution_id: String) -> Self {
        Self {
            pipelines: Vec::new(),
            monitor: None,
            monitor_enabled: false,
            execution_id,
        }
    }

    /// 啟用或禁用系統監控
    pub fn with_monitoring(mut self, enabled: bool) -> Self {
        self.monitor_enabled = enabled;
        if enabled {
            self.monitor = Some(SystemMonitor::new(enabled));
        }
        self
    }

    /// 添加帶上下文的 Pipeline
    pub fn add_pipeline(&mut self, pipeline: Box<dyn ContextualPipeline>) {
        self.pipelines.push(pipeline);
    }

    /// 執行所有 pipeline
    pub async fn execute_all(&mut self) -> Result<Vec<PipelineResult>> {
        let mut results = Vec::new();
        let mut context = PipelineContext::new(self.execution_id.clone());

        if self.monitor_enabled {
            if let Some(monitor) = &self.monitor {
                monitor.log_stats("Pipeline execution started.");
            }
        }

        for pipeline in &self.pipelines {
            let start_time = Instant::now();

            // 根據上下文決定是否執行
            if !pipeline.should_execute(&context) {
                tracing::info!("⏭️ Skipping pipeline: {} (condition not met)", pipeline.get_name());
                continue;
            }

            // 執行單個 pipeline
            match self.execute_pipeline(pipeline.as_ref(), &context).await {
                Ok(execution_result) => {
                    let end_time = Instant::now();
                    let duration = end_time.duration_since(start_time);

                    let result = PipelineResult {
                        pipeline_name: pipeline.get_name().to_string(),
                        records: execution_result.processed_records.clone(),
                        output_path: execution_result.output_path.clone(),
                        duration,
                        metadata: execution_result.metadata.clone(),
                    };

                    tracing::info!(
                        "✅ Pipeline executed: {} (records: {}, duration: {:?})",
                        result.pipeline_name,
                        result.records.len(),
                        result.duration
                    );

                    // 將結果添加到上下文
                    context.add_result(result.clone());
                    results.push(result);
                }
                Err(e) => {
                    tracing::error!("❌ Pipeline execution failed: {}", e);
                    return Err(EtlError::TransformationError {
                        stage: pipeline.get_name().to_string(),
                        details: format!("Pipeline execution failed: {}", e),
                    });
                }
            }
        }

        if self.monitor_enabled {
            if let Some(monitor) = &self.monitor {
                monitor.log_stats("Pipeline execution completed.");
                {
                    if let Some(metrics) = monitor.get_stats()
                    {
                        tracing::info!("📊 System metrics during execution: {:?}", metrics)
                    };
                }
            }
        }

        Ok(results)
    }

    async fn execute_pipeline(&self, pipeline: &dyn ContextualPipeline, context: &PipelineContext) -> Result<PipelineExecutionResult> {
        // Extract
        let records = pipeline.extract_with_context(context).await?;
        tracing::debug!("📥 Extracted {} records", records.len());

        // Transform
        let transform_result = pipeline.transform_with_context(records, context).await?;
        tracing::debug!("🔄 Transformed {} records", transform_result.processed_records.len());

        // Load
        let output_path = pipeline.load_with_context(transform_result.clone(), context).await?;
        tracing::debug!("💾 Loaded data to: {}", output_path);

        Ok(PipelineExecutionResult {
            processed_records: transform_result.processed_records,
            output_path,
            metadata: HashMap::new(),
        })
    }

    /// 獲取執行摘要
    pub fn get_execution_summary(results: &[PipelineResult]) -> HashMap<String, serde_json::Value> {
        let mut summary = HashMap::new();

        let total_pipelines = results.len();
        let total_records: usize = results.iter().map(|r| r.records.len()).sum();
        let total_duration: std::time::Duration = results.iter().map(|r| r.duration).sum();

        summary.insert("total_pipelines".to_string(), serde_json::Value::Number(total_pipelines.into()));
        summary.insert("total_records".to_string(), serde_json::Value::Number(total_records.into()));
        summary.insert("total_duration_ms".to_string(), serde_json::Value::Number((total_duration.as_millis() as u64).into()));

        let pipeline_names: Vec<serde_json::Value> = results
            .iter()
            .map(|r| serde_json::Value::String(r.pipeline_name.clone()))
            .collect();
        summary.insert("executed_pipelines".to_string(), serde_json::Value::Array(pipeline_names));

        summary
    }
}

/// Pipeline 執行結果內部結構
struct PipelineExecutionResult {
    processed_records: Vec<Record>,
    output_path: String,
    metadata: HashMap<String, serde_json::Value>,
}
