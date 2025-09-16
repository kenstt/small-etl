use crate::core::{Record, TransformResult};
use crate::utils::error::{EtlError, Result};
use crate::utils::monitor::SystemMonitor;
use std::collections::HashMap;
use std::time::Instant;

/// Pipeline 執行結果
pub use crate::app::pipelines::sequence_pipeline::PipelineResult;

pub use crate::app::pipelines::sequence_pipeline::PipelineContext;

/// 上下文感知的 Pipeline trait
pub use crate::app::pipelines::sequence_pipeline::ContextualPipeline;

/// Pipeline 序列執行器
pub use crate::app::pipelines::sequence_pipeline::PipelineSequence;


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
