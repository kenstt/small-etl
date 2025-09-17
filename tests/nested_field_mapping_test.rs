use anyhow::Result;
use httpmock::prelude::*;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline,
    pipeline_sequence::PipelineSequence,
};
use samll_etl::LocalStorage;
use tempfile::TempDir;

/// 測試多階層 JSON field mapping 功能
#[tokio::test]
async fn test_nested_field_mapping() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    // 創建包含多階層 field mapping 的配置
    let config_content = format!(
        r#"
[sequence]
name = "nested-field-test"
description = "Test nested field mapping"
version = "1.0.0"
execution_order = ["nested_data_pipeline"]

[[pipelines]]
name = "nested_data_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/user-details"
method = "GET"

[pipelines.extract]
# 測試多階層映射
field_mapping = {{ "id" = "user_id", "user.profile.name" = "full_name", "user.profile.email" = "email_address", "user.preferences.theme" = "ui_theme", "user.account.subscription.plan" = "plan_type", "metadata.created" = "created_date" }}

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path
    );

    let config_path = format!("{}/nested_field_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    // Mock API 回應包含多階層 JSON 結構
    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/user-details");
        then.status(200).json_body(serde_json::json!({
            "id": 123,
            "user": {
                "profile": {
                    "name": "John Doe",
                    "email": "john.doe@example.com",
                    "age": 30
                },
                "preferences": {
                    "theme": "dark",
                    "language": "en"
                },
                "account": {
                    "subscription": {
                        "plan": "premium",
                        "expires": "2024-12-31"
                    },
                    "status": "active"
                }
            },
            "metadata": {
                "created": "2024-01-15T10:30:00Z",
                "updated": "2024-03-20T14:45:00Z"
            },
            "tags": ["customer", "premium"]
        }));
    });

    let mut sequence = PipelineSequence::new("nested_field_test".to_string());

    // 修改端點 URL 為 mock server
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
        let contextual_pipeline = SequenceAwarePipeline::new(
            pipeline_def.name.clone(),
            storage,
            pipeline_def.clone(),
        );
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    println!("🔧 Starting nested field mapping test...");
    let results = sequence.execute_all().await?;

    println!("📊 Results: {} pipelines executed", results.len());
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].pipeline_name, "nested_data_pipeline");
    assert!(!results[0].records.is_empty());

    let record = &results[0].records[0];
    println!("🔍 Record data keys: {:?}", record.data.keys().collect::<Vec<_>>());

    // 驗證頂層映射
    assert!(record.data.contains_key("user_id"));
    assert_eq!(record.data.get("user_id").unwrap(), &serde_json::json!(123));

    // 驗證多階層映射
    assert!(record.data.contains_key("full_name"));
    assert_eq!(record.data.get("full_name").unwrap(), &serde_json::json!("John Doe"));

    assert!(record.data.contains_key("email_address"));
    assert_eq!(record.data.get("email_address").unwrap(), &serde_json::json!("john.doe@example.com"));

    assert!(record.data.contains_key("ui_theme"));
    assert_eq!(record.data.get("ui_theme").unwrap(), &serde_json::json!("dark"));

    assert!(record.data.contains_key("plan_type"));
    assert_eq!(record.data.get("plan_type").unwrap(), &serde_json::json!("premium"));

    assert!(record.data.contains_key("created_date"));
    assert_eq!(record.data.get("created_date").unwrap(), &serde_json::json!("2024-01-15T10:30:00Z"));

    // 驗證原始頂層欄位也存在（因為沒有被多階層路徑覆蓋）
    assert!(record.data.contains_key("user"));
    assert!(record.data.contains_key("metadata"));
    assert!(record.data.contains_key("tags"));

    api_mock.assert();

    println!("✅ Nested field mapping test passed!");

    Ok(())
}

/// 測試多階層 field mapping 在陣列回應中的應用
#[tokio::test]
async fn test_nested_field_mapping_array() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "nested-array-test"
description = "Test nested field mapping with array response"
version = "1.0.0"
execution_order = ["array_pipeline"]

[[pipelines]]
name = "array_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/users"
method = "GET"

[pipelines.extract]
field_mapping = {{ "contact.email" = "email", "contact.phone" = "phone", "address.city" = "city" }}

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path
    );

    let config_path = format!("{}/nested_array_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/users");
        then.status(200).json_body(serde_json::json!([
            {
                "id": 1,
                "name": "Alice",
                "contact": {
                    "email": "alice@example.com",
                    "phone": "+1-555-0101"
                },
                "address": {
                    "city": "New York",
                    "country": "USA"
                }
            },
            {
                "id": 2,
                "name": "Bob",
                "contact": {
                    "email": "bob@example.com",
                    "phone": "+1-555-0102"
                },
                "address": {
                    "city": "Los Angeles",
                    "country": "USA"
                }
            }
        ]));
    });

    let mut sequence = PipelineSequence::new("nested_array_test".to_string());

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
        let contextual_pipeline = SequenceAwarePipeline::new(
            pipeline_def.name.clone(),
            storage,
            pipeline_def.clone(),
        );
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    let results = sequence.execute_all().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].records.len(), 2);

    // 檢查第一個記錄
    let record1 = &results[0].records[0];
    assert_eq!(record1.data.get("email").unwrap(), &serde_json::json!("alice@example.com"));
    assert_eq!(record1.data.get("phone").unwrap(), &serde_json::json!("+1-555-0101"));
    assert_eq!(record1.data.get("city").unwrap(), &serde_json::json!("New York"));

    // 檢查第二個記錄
    let record2 = &results[0].records[1];
    assert_eq!(record2.data.get("email").unwrap(), &serde_json::json!("bob@example.com"));
    assert_eq!(record2.data.get("phone").unwrap(), &serde_json::json!("+1-555-0102"));
    assert_eq!(record2.data.get("city").unwrap(), &serde_json::json!("Los Angeles"));

    api_mock.assert();

    Ok(())
}