use anyhow::Result;
use httpmock::prelude::*;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline,
    pipeline_sequence::{PipelineContext, PipelineSequence},
};
use samll_etl::LocalStorage;
use std::collections::HashMap;
use tempfile::TempDir;

/// 測試 header 模板替換功能
#[tokio::test]
async fn test_header_template_replacement() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    // 創建帶有 header 模板的配置
    let config_content = format!(
        r#"
[sequence]
name = "header-template-test"
description = "Test header template replacement"
version = "1.0.0"
execution_order = ["auth_pipeline", "api_pipeline"]

[global]
shared_variables = {{ BASE_URL = "http://localhost:8080" }}

[[pipelines]]
name = "auth_pipeline"
description = "Authentication pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "${{BASE_URL}}/auth/token"
method = "POST"

[pipelines.source.payload]
body = '''{{\"username\": \"test_user\", \"password\": \"test_pass\"}}'''
content_type = "application/json"

[pipelines.extract]
field_mapping = {{ "access_token" = "token" }}

[pipelines.transform.intermediate]
export_to_shared = true
shared_key = "auth"

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "api_pipeline"
description = "API pipeline with token authentication"
enabled = true

[pipelines.source]
type = "api"
endpoint = "${{BASE_URL}}/protected-data"
method = "GET"

[pipelines.source.headers]
Authorization = "Bearer {{{{token}}}}"
X-Custom-Header = "Pipeline-{{{{auth_user_id}}}}"

[pipelines.source.data_source]
use_previous_output = true
from_pipeline = "auth_pipeline"
merge_with_api = true

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path
    );

    let config_path = format!("{}/auth_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    // 設置 Mock Server
    let server = MockServer::start();

    // Mock 授權端點 - 簡化，不檢查 body
    let auth_mock = server.mock(|when, then| {
        when.method(POST).path("/auth/token");
        then.status(200).json_body(serde_json::json!({
            "access_token": "test_bearer_token_12345",
            "token_type": "Bearer",
            "expires_in": 3600
        }));
    });

    // Mock 受保護的數據端點，驗證 Authorization header
    let protected_data_mock = server.mock(|when, then| {
        when.method(GET)
            .path("/protected-data")
            .header("authorization", "Bearer test_bearer_token_12345");
        then.status(200).json_body(serde_json::json!([
            {"id": 1, "data": "Protected Data 1"},
            {"id": 2, "data": "Protected Data 2"}
        ]));
    });

    // 創建 Pipeline 序列
    let mut sequence = PipelineSequence::new("auth_test_execution".to_string());

    // 修改配置中的端點URL為mock server
    let mut modified_config = config.clone();
    for pipeline in &mut modified_config.pipelines {
        if let Some(endpoint) = &mut pipeline.source.endpoint {
            if endpoint.contains("localhost:8080") {
                *endpoint = endpoint.replace("localhost:8080", &server.address().to_string());
            }
        }
    }

    // 添加Pipeline到序列
    for pipeline_def in &modified_config.pipelines {
        let storage = LocalStorage::new(pipeline_def.load.output_path.clone());
        let contextual_pipeline =
            SequenceAwarePipeline::new(pipeline_def.name.clone(), storage, pipeline_def.clone());
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    // 執行序列
    let results = sequence.execute_all().await?;

    // 驗證結果
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].pipeline_name, "auth_pipeline");
    assert_eq!(results[1].pipeline_name, "api_pipeline");

    // 驗證第一個 pipeline 有授權數據
    assert!(!results[0].records.is_empty());
    let auth_record = &results[0].records[0];
    assert!(auth_record.data.contains_key("token"));

    // 驗證第二個 pipeline 有受保護的數據
    assert!(!results[1].records.is_empty());

    // 驗證 Mock 被正確調用
    auth_mock.assert();
    protected_data_mock.assert();

    Ok(())
}

/// 測試共享數據導出功能
#[tokio::test]
async fn test_shared_data_export() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "shared-data-test"
description = "Test shared data export"
version = "1.0.0"
execution_order = ["data_producer", "data_consumer"]

[[pipelines]]
name = "data_producer"
description = "Produces data for sharing"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/token-data"

[pipelines.extract]
field_mapping = {{ "access_token" = "token", "user_id" = "user_id" }}

[pipelines.transform.intermediate]
export_to_shared = true
shared_key = "producer"

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "data_consumer"
description = "Consumes shared data"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/user-data"

[pipelines.source.headers]
Authorization = "Bearer {{{{token}}}}"
X-User-ID = "{{{{producer_user_id}}}}"

[pipelines.source.data_source]
use_previous_output = true
from_pipeline = "data_producer"
merge_with_api = true

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path
    );

    let config_path = format!("{}/shared_data_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    // Mock 數據生產端點
    let producer_mock = server.mock(|when, then| {
        when.method(GET).path("/token-data");
        then.status(200).json_body(serde_json::json!({
            "access_token": "shared_token_67890",
            "user_id": "user_123",
            "other_data": "some_value"
        }));
    });

    // Mock 數據消費端點，驗證共享數據在 header 中
    let consumer_mock = server.mock(|when, then| {
        when.method(GET)
            .path("/user-data")
            .header("authorization", "Bearer shared_token_67890")
            .header("x-user-id", "user_123");
        then.status(200).json_body(serde_json::json!([
            {"id": 1, "name": "User Data"}
        ]));
    });

    let mut sequence = PipelineSequence::new("shared_data_test".to_string());

    let mut modified_config = config.clone();
    for pipeline in &mut modified_config.pipelines {
        if let Some(endpoint) = &mut pipeline.source.endpoint {
            if endpoint.contains("localhost:8080") {
                *endpoint = endpoint.replace("localhost:8080", &server.address().to_string());
            }
        }
    }

    for pipeline_def in &modified_config.pipelines {
        let storage = LocalStorage::new(pipeline_def.load.output_path.clone());
        let contextual_pipeline =
            SequenceAwarePipeline::new(pipeline_def.name.clone(), storage, pipeline_def.clone());
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    let results = sequence.execute_all().await?;

    assert_eq!(results.len(), 2);
    assert!(!results[0].records.is_empty());
    assert!(!results[1].records.is_empty());

    producer_mock.assert();
    consumer_mock.assert();

    Ok(())
}

/// 測試單元：header 模板處理邏輯
#[tokio::test]
async fn test_header_template_processing_unit() -> Result<()> {
    let mut context = PipelineContext::new("test_execution".to_string());

    // 添加共享數據
    context.add_shared_data(
        "token".to_string(),
        serde_json::Value::String("bearer_token_123".to_string()),
    );
    context.add_shared_data(
        "api_key".to_string(),
        serde_json::Value::String("key_456".to_string()),
    );

    // 創建記錄數據
    let mut record_data = HashMap::new();
    record_data.insert(
        "user_id".to_string(),
        serde_json::Value::String("user_789".to_string()),
    );
    record_data.insert(
        "session".to_string(),
        serde_json::Value::String("session_abc".to_string()),
    );

    // 模擬 header 模板替換邏輯
    let test_cases = vec![
        ("Bearer {{token}}", "Bearer bearer_token_123"),
        ("API-Key {{api_key}}", "API-Key key_456"),
        (
            "User-{{user_id}}-Session-{{session}}",
            "User-user_789-Session-session_abc",
        ),
        (
            "Mixed {{token}} and {{user_id}}",
            "Mixed bearer_token_123 and user_789",
        ),
        ("No template", "No template"),
        ("Unknown {{unknown_key}}", "Unknown {{unknown_key}}"), // 應保持未替換
    ];

    for (template, expected) in test_cases {
        let result = process_test_template(template, Some(&record_data), &context);
        assert_eq!(
            result, expected,
            "Template: {} should become: {}",
            template, expected
        );
    }

    Ok(())
}

// 輔助函數模擬模板處理邏輯
fn process_test_template(
    template: &str,
    record_data: Option<&HashMap<String, serde_json::Value>>,
    context: &PipelineContext,
) -> String {
    let mut processed = template.to_string();

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

    processed
}

/// 測試 base URL 共享變數功能
#[tokio::test]
async fn test_base_url_shared_variables() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "base-url-test"
description = "Test base URL shared variables"
version = "1.0.0"
execution_order = ["auth_pipeline", "api_pipeline"]

[global]
shared_variables = {{ BASE_URL = "http://localhost:8080", API_VERSION = "v1" }}

[[pipelines]]
name = "auth_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "${{BASE_URL}}/${{API_VERSION}}/auth"
method = "POST"

[pipelines.source.payload]
body = '''{{\"grant_type\": \"client_credentials\"}}'''

[pipelines.extract]
field_mapping = {{ "access_token" = "token" }}

[pipelines.transform.intermediate]
export_to_shared = true
shared_key = ""

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "api_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "${{BASE_URL}}/${{API_VERSION}}/data"

[pipelines.source.headers]
Authorization = "Bearer {{{{token}}}}"

[pipelines.source.data_source]
use_previous_output = true
merge_with_api = true

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path
    );

    let config_path = format!("{}/base_url_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    let auth_mock = server.mock(|when, then| {
        when.method(POST).path("/v1/auth");
        then.status(200).json_body(serde_json::json!({
            "access_token": "base_url_token_999"
        }));
    });

    let data_mock = server.mock(|when, then| {
        when.method(GET)
            .path("/v1/data")
            .header("authorization", "Bearer base_url_token_999");
        then.status(200).json_body(serde_json::json!([
            {"message": "Base URL test successful"},
            {"message2": "Base URL test successful"}
        ]));
    });

    let mut sequence = PipelineSequence::new("base_url_test".to_string());

    let mut modified_config = config.clone();
    for pipeline in &mut modified_config.pipelines {
        if let Some(endpoint) = &mut pipeline.source.endpoint {
            if endpoint.contains("localhost:8080") {
                *endpoint = endpoint.replace("localhost:8080", &server.address().to_string());
            }
        }
    }

    for pipeline_def in &modified_config.pipelines {
        let storage = LocalStorage::new(pipeline_def.load.output_path.clone());
        let contextual_pipeline =
            SequenceAwarePipeline::new(pipeline_def.name.clone(), storage, pipeline_def.clone());
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    let results = sequence.execute_all().await?;

    assert_eq!(results.len(), 2);
    assert!(!results[0].records.is_empty());
    assert!(!results[1].records.is_empty());

    auth_mock.assert();
    data_mock.assert();

    Ok(())
}
