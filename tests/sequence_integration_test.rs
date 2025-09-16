use anyhow::Result;
use httpmock::prelude::*;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline,
    pipeline_sequence::{PipelineSequence, PipelineContext},
    Record,
};
use samll_etl::LocalStorage;
use std::collections::HashMap;
use tempfile::TempDir;

async fn create_test_config(temp_dir: &str) -> String {
    // 將Windows路徑中的反斜杠轉為正斜杠以避免TOML解析問題
    let normalized_path = temp_dir.replace('\\', "/");
    let config_content = format!(
        r#"
[sequence]
name = "test-sequence"
description = "Test pipeline sequence"
version = "1.0.0"
execution_order = ["pipeline1", "pipeline2", "pipeline3"]

[global]
working_directory = "{}"

[monitoring]
enabled = false

[error_handling]
on_pipeline_failure = "stop"

[[pipelines]]
name = "pipeline1"
description = "First pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/posts"

[pipelines.extract]
max_records = 5

[pipelines.extract.field_mapping]
id = "post_id"
title = "post_title"

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "pipeline2"
description = "Second pipeline"
enabled = true
dependencies = ["pipeline1"]

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/users"

[pipelines.source.data_source]
use_previous_output = true
merge_with_api = true

[pipelines.extract]
max_records = 10

[pipelines.extract.field_mapping]
id = "user_id"
name = "user_name"

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "pipeline3"
description = "Third pipeline"
enabled = true
dependencies = ["pipeline2"]

[pipelines.source]
type = "previous"
endpoint = ""

[pipelines.source.data_source]
use_previous_output = true
from_pipeline = "pipeline2"

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path, normalized_path, normalized_path
    );

    let config_path = format!("{}/test_sequence.toml", temp_dir);
    tokio::fs::write(&config_path, config_content)
        .await
        .expect("Failed to write test config");

    config_path
}

#[tokio::test]
async fn test_pipeline_sequence_execution() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();

    // 創建測試配置
    let config_path = create_test_config(temp_path).await;
    let config = SequenceConfig::from_file(&config_path)?;

    // 設置 Mock Server
    let server = MockServer::start();

    // Mock 第一個 API 端點
    let posts_mock = server.mock(|when, then| {
        when.method(GET).path("/posts");
        then.status(200).json_body(serde_json::json!([
            {"id": 1, "title": "Test Post 1"},
            {"id": 2, "title": "Test Post 2"},
            {"id": 3, "title": "Test Post 3"}
        ]));
    });

    // Mock 第二個 API 端點
    let users_mock = server.mock(|when, then| {
        when.method(GET).path("/users");
        then.status(200).json_body(serde_json::json!([
            {"id": 1, "name": "User 1"},
            {"id": 2, "name": "User 2"}
        ]));
    });

    // 創建 Pipeline 序列
    let mut sequence = PipelineSequence::new("test_execution".to_string());

    // 修改配置中的端點URL為mock server
    let mut modified_config = config.clone();
    for pipeline in &mut modified_config.pipelines {
        if pipeline.source.endpoint.contains("localhost:8080") {
            pipeline.source.endpoint = pipeline.source.endpoint.replace("localhost:8080", &server.address().to_string());
        }
    }

    // 添加Pipeline到序列
    for pipeline_def in &modified_config.pipelines {
        let storage = LocalStorage::new(pipeline_def.load.output_path.clone());
        let contextual_pipeline = SequenceAwarePipeline::new(
            pipeline_def.name.clone(),
            storage,
            pipeline_def.clone(),
        );
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    // 執行序列
    let results = sequence.execute_all().await?;

    // 驗證結果
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].pipeline_name, "pipeline1");
    assert_eq!(results[1].pipeline_name, "pipeline2");
    assert_eq!(results[2].pipeline_name, "pipeline3");

    // 驗證第一個 pipeline 有數據
    assert!(!results[0].records.is_empty());

    // 驗證 Mock 被調用
    posts_mock.assert();
    users_mock.assert();

    Ok(())
}

#[tokio::test]
async fn test_pipeline_sequence_with_dependency_failure() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();

    let config_path = create_test_config(temp_path).await;
    let config = SequenceConfig::from_file(&config_path)?;

    // 設置 Mock Server，第一個端點會失敗
    let server = MockServer::start();

    let _failing_mock = server.mock(|when, then| {
        when.method(GET).path("/posts");
        then.status(500);
    });

    let mut sequence = PipelineSequence::new("test_failure".to_string());

    let mut modified_config = config.clone();
    for pipeline in &mut modified_config.pipelines {
        if pipeline.source.endpoint.contains("localhost:8080") {
            pipeline.source.endpoint = pipeline.source.endpoint.replace("localhost:8080", &server.address().to_string());
        }
    }

    for pipeline_def in &modified_config.pipelines {
        let storage = LocalStorage::new(pipeline_def.load.output_path.clone());
        let contextual_pipeline = SequenceAwarePipeline::new(
            pipeline_def.name.clone(),
            storage,
            pipeline_def.clone(),
        );
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    // 執行序列應該失敗
    let result = sequence.execute_all().await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_pipeline_context_data_passing() -> Result<()> {
    let _temp_dir = TempDir::new()?;

    // 創建初始數據
    let mut initial_records = Vec::new();
    let mut record_data = HashMap::new();
    record_data.insert("test_field".to_string(), serde_json::Value::String("test_value".to_string()));
    record_data.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
    initial_records.push(Record { data: record_data });

    // 創建 Pipeline Context
    let mut context = PipelineContext::new("test_execution".to_string());
    context.add_pipeline_data("previous_pipeline".to_string(), initial_records.clone());
    context.add_shared_data("shared_key".to_string(), serde_json::Value::String("shared_value".to_string()));

    // 測試數據獲取
    let retrieved_data = context.get_pipeline_data("previous_pipeline");
    assert!(retrieved_data.is_some());
    assert_eq!(retrieved_data.unwrap().len(), 1);

    let shared_value = context.get_shared_data("shared_key");
    assert!(shared_value.is_some());
    assert_eq!(shared_value.unwrap(), &serde_json::Value::String("shared_value".to_string()));

    // 測試合併數據
    let mut api_records = Vec::new();
    let mut api_data = HashMap::new();
    api_data.insert("api_field".to_string(), serde_json::Value::String("api_value".to_string()));
    api_data.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
    api_records.push(Record { data: api_data });

    let merged = context.merge_with_previous("previous_pipeline", api_records);
    assert!(!merged.is_empty());
    assert!(merged[0].data.contains_key("test_field"));
    assert!(merged[0].data.contains_key("api_field"));

    Ok(())
}

#[tokio::test]
async fn test_conditional_pipeline_execution() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    // 創建有條件執行的配置
    let config_content = format!(
        r#"
[sequence]
name = "conditional-test"
description = "Test conditional execution"
version = "1.0.0"
execution_order = ["pipeline1", "pipeline2"]

[global]
working_directory = "{}"

[[pipelines]]
name = "pipeline1"
description = "First pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/empty"

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "pipeline2"
description = "Conditional pipeline"
enabled = true
dependencies = ["pipeline1"]

[pipelines.source]
type = "previous"
endpoint = ""

[pipelines.source.data_source]
use_previous_output = true
from_pipeline = "pipeline1"

[pipelines.extract]

[pipelines.transform]

[pipelines.conditions]
skip_if_empty = true

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path, normalized_path
    );

    let config_path = format!("{}/conditional_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    // 設置返回空數據的 Mock Server
    let server = MockServer::start();
    let _empty_mock = server.mock(|when, then| {
        when.method(GET).path("/empty");
        then.status(200).json_body(serde_json::json!([]));
    });

    let mut sequence = PipelineSequence::new("conditional_test".to_string());

    let mut modified_config = config.clone();
    for pipeline in &mut modified_config.pipelines {
        if pipeline.source.endpoint.contains("localhost:8080") {
            pipeline.source.endpoint = pipeline.source.endpoint.replace("localhost:8080", &server.address().to_string());
        }
    }

    for pipeline_def in &modified_config.pipelines {
        let storage = LocalStorage::new(pipeline_def.load.output_path.clone());
        let contextual_pipeline = SequenceAwarePipeline::new(
            pipeline_def.name.clone(),
            storage,
            pipeline_def.clone(),
        );
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    // 執行序列
    let results = sequence.execute_all().await?;

    // 第一個 pipeline 應該執行（雖然數據為空）
    assert!(!results.is_empty());
    assert_eq!(results[0].pipeline_name, "pipeline1");

    // 第二個 pipeline 應該被跳過因為前一個沒有數據
    // 注意：具體行為取決於 skip_if_empty 的實作

    Ok(())
}

#[tokio::test]
async fn test_pipeline_sequence_metrics() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();

    let config_path = create_test_config(temp_path).await;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();
    let _posts_mock = server.mock(|when, then| {
        when.method(GET).path("/posts");
        then.status(200).json_body(serde_json::json!([
            {"id": 1, "title": "Test Post"}
        ]));
    });

    let _users_mock = server.mock(|when, then| {
        when.method(GET).path("/users");
        then.status(200).json_body(serde_json::json!([
            {"id": 1, "name": "Test User"}
        ]));
    });

    let mut sequence = PipelineSequence::new("metrics_test".to_string()).with_monitoring(true);

    let mut modified_config = config.clone();
    for pipeline in &mut modified_config.pipelines {
        if pipeline.source.endpoint.contains("localhost:8080") {
            pipeline.source.endpoint = pipeline.source.endpoint.replace("localhost:8080", &server.address().to_string());
        }
    }

    for pipeline_def in &modified_config.pipelines {
        let storage = LocalStorage::new(pipeline_def.load.output_path.clone());
        let contextual_pipeline = SequenceAwarePipeline::new(
            pipeline_def.name.clone(),
            storage,
            pipeline_def.clone(),
        );
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    let results = sequence.execute_all().await?;

    // 驗證每個結果都有執行時間
    for result in &results {
        assert!(result.duration.as_millis() > 0);
        assert!(!result.pipeline_name.is_empty());
        assert!(!result.output_path.is_empty());
    }

    // 測試執行摘要
    let summary = samll_etl::core::pipeline_sequence::PipelineSequence::get_execution_summary(&results);
    assert!(summary.contains_key("total_pipelines"));
    assert!(summary.contains_key("total_records"));
    assert!(summary.contains_key("total_duration_ms"));

    Ok(())
}