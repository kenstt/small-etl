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

/// 上下文感知的 Pipeline trait
#[async_trait::async_trait]
pub trait ContextualPipeline: Send + Sync {
    async fn extract_with_context(&self, context: &PipelineContext) -> Result<Vec<Record>>;
    async fn transform_with_context(&self, data: Vec<Record>, context: &PipelineContext) -> Result<TransformResult>;
    async fn load_with_context(&self, result: TransformResult, context: &PipelineContext) -> Result<String>;

    fn get_name(&self) -> &str;
    fn should_execute(&self, context: &PipelineContext) -> bool;
}

/// Pipeline 序列執行器
pub struct PipelineSequence {
    pipelines: Vec<Box<dyn ContextualPipeline>>,
    monitor: SystemMonitor,
    execution_id: String,
    monitoring_enabled: bool,
}

impl PipelineSequence {
    pub fn new(execution_id: String) -> Self {
        Self {
            pipelines: Vec::new(),
            monitor: SystemMonitor::new(false),
            execution_id,
            monitoring_enabled: false,
        }
    }

    pub fn with_monitoring(mut self, enabled: bool) -> Self {
        self.monitoring_enabled = enabled;
        self
    }

    pub fn add_pipeline(&mut self, pipeline: Box<dyn ContextualPipeline>) {
        self.pipelines.push(pipeline);
    }

    /// 執行所有 Pipeline
    pub async fn execute_all(&mut self) -> Result<Vec<PipelineResult>> {
        let mut context = PipelineContext::new(self.execution_id.clone());
        let mut results = Vec::new();

        tracing::info!("🎬 Starting pipeline sequence execution: {}", self.execution_id);

        for (index, pipeline) in self.pipelines.iter().enumerate() {
            let pipeline_name = pipeline.get_name();
            tracing::info!("📦 Executing pipeline {}/{}: {}", index + 1, self.pipelines.len(), pipeline_name);

            // 檢查是否應該執行
            if !pipeline.should_execute(&context) {
                tracing::info!("⏭️ Skipping pipeline: {} (conditions not met)", pipeline_name);
                continue;
            }

            let start_time = Instant::now();

            if self.monitoring_enabled {
                self.monitor.log_stats(&format!("Starting {}", pipeline_name));
            }

            // 執行 ETL 流程
            match self.execute_pipeline(pipeline.as_ref(), &context).await {
                Ok(result) => {
                    let duration = start_time.elapsed();
                    let pipeline_result = PipelineResult {
                        pipeline_name: pipeline_name.to_string(),
                        records: result.processed_records,
                        output_path: result.output_path,
                        duration,
                        metadata: result.metadata,
                    };

                    tracing::info!(
                        "✅ Pipeline {} completed successfully in {:?}, {} records processed",
                        pipeline_name,
                        duration,
                        pipeline_result.records.len()
                    );

                    // 添加結果到上下文
                    context.add_result(pipeline_result.clone());
                    results.push(pipeline_result);

                    if self.monitoring_enabled {
                        self.monitor.log_stats(&format!("Completed {}", pipeline_name));
                    }
                }
                Err(e) => {
                    tracing::error!("❌ Pipeline {} failed: {}", pipeline_name, e);

                    if self.monitoring_enabled {
                        self.monitor.log_stats(&format!("Failed {}", pipeline_name));
                    }

                    return Err(EtlError::PipelineExecution(format!(
                        "Pipeline '{}' failed: {}",
                        pipeline_name, e
                    )));
                }
            }
        }

        tracing::info!("🎉 Pipeline sequence completed successfully! {} pipelines executed", results.len());
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MockPipeline {
        name: String,
        should_execute: bool,
        extract_records: Vec<Record>,
        use_previous_data: bool,
    }

    impl MockPipeline {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                should_execute: true,
                extract_records: Vec::new(),
                use_previous_data: false,
            }
        }

        fn with_records(mut self, records: Vec<Record>) -> Self {
            self.extract_records = records;
            self
        }

        fn with_execution_condition(mut self, should_execute: bool) -> Self {
            self.should_execute = should_execute;
            self
        }

        fn with_previous_data(mut self, use_previous: bool) -> Self {
            self.use_previous_data = use_previous;
            self
        }
    }

    #[async_trait::async_trait]
    impl ContextualPipeline for MockPipeline {
        async fn extract_with_context(&self, context: &PipelineContext) -> Result<Vec<Record>> {
            if self.use_previous_data {
                Ok(context.get_all_previous_records())
            } else {
                Ok(self.extract_records.clone())
            }
        }

        async fn transform_with_context(&self, data: Vec<Record>, _context: &PipelineContext) -> Result<TransformResult> {
            Ok(TransformResult {
                processed_records: data,
                csv_output: String::new(),
                tsv_output: String::new(),
                intermediate_data: Vec::new(),
            })
        }

        async fn load_with_context(&self, _result: TransformResult, _context: &PipelineContext) -> Result<String> {
            Ok(format!("/tmp/{}_output.json", self.name))
        }

        fn get_name(&self) -> &str {
            &self.name
        }

        fn should_execute(&self, _context: &PipelineContext) -> bool {
            self.should_execute
        }
    }

    fn create_test_record(id: i64, title: &str) -> Record {
        let mut data = HashMap::new();
        data.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(id)));
        data.insert("title".to_string(), serde_json::Value::String(title.to_string()));
        Record { data }
    }

    #[tokio::test]
    async fn test_pipeline_context_new() {
        let context = PipelineContext::new("test_execution".to_string());
        assert_eq!(context.execution_id, "test_execution");
        assert!(context.previous_results.is_empty());
        assert!(context.shared_data.is_empty());
    }

    #[tokio::test]
    async fn test_pipeline_context_add_and_get_data() {
        let mut context = PipelineContext::new("test".to_string());

        let records = vec![create_test_record(1, "Test")];
        context.add_pipeline_data("pipeline1".to_string(), records.clone());

        let retrieved = context.get_pipeline_data("pipeline1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().len(), 1);
        assert_eq!(retrieved.unwrap()[0].data.get("title").unwrap(), "Test");
    }

    #[tokio::test]
    async fn test_pipeline_context_shared_data() {
        let mut context = PipelineContext::new("test".to_string());

        context.add_shared_data("key1".to_string(), serde_json::Value::String("value1".to_string()));
        context.add_shared_data("key2".to_string(), serde_json::Value::Number(serde_json::Number::from(42)));

        assert_eq!(context.get_shared_data("key1").unwrap(), &serde_json::Value::String("value1".to_string()));
        assert_eq!(context.get_shared_data("key2").unwrap(), &serde_json::Value::Number(serde_json::Number::from(42)));
        assert!(context.get_shared_data("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_pipeline_context_merge_with_previous() {
        let mut context = PipelineContext::new("test".to_string());

        // 添加前一個 pipeline 的數據
        let previous_records = vec![
            create_test_record(1, "Previous Title 1"),
            create_test_record(2, "Previous Title 2"),
        ];
        context.add_pipeline_data("previous".to_string(), previous_records);

        // API 數據
        let mut api_record_data = HashMap::new();
        api_record_data.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
        api_record_data.insert("description".to_string(), serde_json::Value::String("API Description".to_string()));
        let api_records = vec![Record { data: api_record_data }];

        let merged = context.merge_with_previous("previous", api_records);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].data.get("id").unwrap(), &serde_json::Value::Number(serde_json::Number::from(1)));
        assert_eq!(merged[0].data.get("title").unwrap(), "Previous Title 1");
        assert_eq!(merged[0].data.get("description").unwrap(), "API Description");
    }

    #[tokio::test]
    async fn test_pipeline_sequence_execution() {
        let mut sequence = PipelineSequence::new("test_sequence".to_string());

        // 添加第一個 pipeline
        let records1 = vec![create_test_record(1, "First Pipeline")];
        let pipeline1 = MockPipeline::new("pipeline1").with_records(records1);
        sequence.add_pipeline(Box::new(pipeline1));

        // 添加第二個 pipeline（使用前一個的數據）
        let pipeline2 = MockPipeline::new("pipeline2").with_previous_data(true);
        sequence.add_pipeline(Box::new(pipeline2));

        let results = sequence.execute_all().await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].pipeline_name, "pipeline1");
        assert_eq!(results[1].pipeline_name, "pipeline2");
        assert_eq!(results[0].records.len(), 1);
        assert_eq!(results[1].records.len(), 1); // 使用前一個 pipeline 的數據
    }

    #[tokio::test]
    async fn test_pipeline_sequence_conditional_execution() {
        let mut sequence = PipelineSequence::new("conditional_test".to_string());

        // 添加第一個 pipeline
        let records1 = vec![create_test_record(1, "First Pipeline")];
        let pipeline1 = MockPipeline::new("pipeline1").with_records(records1);
        sequence.add_pipeline(Box::new(pipeline1));

        // 添加第二個 pipeline（不應該執行）
        let pipeline2 = MockPipeline::new("pipeline2").with_execution_condition(false);
        sequence.add_pipeline(Box::new(pipeline2));

        // 添加第三個 pipeline
        let records3 = vec![create_test_record(3, "Third Pipeline")];
        let pipeline3 = MockPipeline::new("pipeline3").with_records(records3);
        sequence.add_pipeline(Box::new(pipeline3));

        let results = sequence.execute_all().await.unwrap();

        assert_eq!(results.len(), 2); // 只有 pipeline1 和 pipeline3 執行
        assert_eq!(results[0].pipeline_name, "pipeline1");
        assert_eq!(results[1].pipeline_name, "pipeline3");
    }

    #[tokio::test]
    async fn test_pipeline_sequence_execution_summary() {
        let results = vec![
            PipelineResult {
                pipeline_name: "pipeline1".to_string(),
                records: vec![create_test_record(1, "Test")],
                output_path: "/tmp/output1.json".to_string(),
                duration: std::time::Duration::from_millis(100),
                metadata: HashMap::new(),
            },
            PipelineResult {
                pipeline_name: "pipeline2".to_string(),
                records: vec![create_test_record(2, "Test"), create_test_record(3, "Test")],
                output_path: "/tmp/output2.json".to_string(),
                duration: std::time::Duration::from_millis(200),
                metadata: HashMap::new(),
            },
        ];

        let summary = PipelineSequence::get_execution_summary(&results);

        assert_eq!(summary.get("total_pipelines").unwrap(), &serde_json::Value::Number(2.into()));
        assert_eq!(summary.get("total_records").unwrap(), &serde_json::Value::Number(3.into()));
        assert_eq!(summary.get("total_duration_ms").unwrap(), &serde_json::Value::Number(300.into()));

        let executed_pipelines = summary.get("executed_pipelines").unwrap().as_array().unwrap();
        assert_eq!(executed_pipelines.len(), 2);
        assert_eq!(executed_pipelines[0], serde_json::Value::String("pipeline1".to_string()));
        assert_eq!(executed_pipelines[1], serde_json::Value::String("pipeline2".to_string()));
    }

    #[test]
    fn test_pipeline_context_get_result_by_name() {
        let mut context = PipelineContext::new("test".to_string());

        let result1 = PipelineResult {
            pipeline_name: "pipeline1".to_string(),
            records: vec![create_test_record(1, "Test")],
            output_path: "/tmp/output1.json".to_string(),
            duration: std::time::Duration::from_millis(100),
            metadata: HashMap::new(),
        };

        let result2 = PipelineResult {
            pipeline_name: "pipeline2".to_string(),
            records: vec![create_test_record(2, "Test")],
            output_path: "/tmp/output2.json".to_string(),
            duration: std::time::Duration::from_millis(200),
            metadata: HashMap::new(),
        };

        context.add_result(result1.clone());
        context.add_result(result2.clone());

        let retrieved = context.get_result_by_name("pipeline1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().pipeline_name, "pipeline1");

        let last_result = context.get_previous_result();
        assert!(last_result.is_some());
        assert_eq!(last_result.unwrap().pipeline_name, "pipeline2");

        assert!(context.get_result_by_name("nonexistent").is_none());
    }
}