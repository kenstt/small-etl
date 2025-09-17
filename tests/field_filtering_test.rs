use anyhow::Result;
use httpmock::prelude::*;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline, pipeline_sequence::PipelineSequence,
};
use samll_etl::LocalStorage;
use tempfile::TempDir;

/// 測試 keep_only_fields 功能：只保留指定欄位
#[tokio::test]
async fn test_keep_only_fields() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "keep-only-fields-test"
description = "Test keep_only_fields functionality"
version = "1.0.0"
execution_order = ["filter_pipeline"]

[[pipelines]]
name = "filter_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/user-data"
method = "GET"

[pipelines.extract]

[pipelines.transform.operations]
keep_only_fields = ["id", "name", "email"]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path
    );

    let config_path = format!("{}/keep_only_fields_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    // Mock API 回應包含多個欄位
    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/user-data");
        then.status(200).json_body(serde_json::json!([
            {
                "id": 1,
                "name": "John Doe",
                "email": "john@example.com",
                "password": "secret123",
                "ssn": "123-45-6789",
                "internal_notes": "VIP customer",
                "created_at": "2024-01-15T10:30:00Z",
                "last_login": "2024-03-20T14:45:00Z",
                "preferences": {
                    "theme": "dark",
                    "language": "en"
                }
            },
            {
                "id": 2,
                "name": "Jane Smith",
                "email": "jane@example.com",
                "password": "secret456",
                "ssn": "987-65-4321",
                "internal_notes": "Regular customer",
                "created_at": "2024-02-10T11:15:00Z",
                "last_login": "2024-03-21T09:30:00Z",
                "preferences": {
                    "theme": "light",
                    "language": "es"
                }
            }
        ]));
    });

    let mut sequence = PipelineSequence::new("keep_only_fields_test".to_string());

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

    println!("🔧 Starting keep_only_fields test...");
    let results = sequence.execute_all().await?;

    println!("📊 Results: {} pipelines executed", results.len());
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].pipeline_name, "filter_pipeline");
    assert_eq!(results[0].records.len(), 2);

    // 驗證第一個記錄只包含指定欄位
    let record1 = &results[0].records[0];
    println!(
        "🔍 Record 1 keys: {:?}",
        record1.data.keys().collect::<Vec<_>>()
    );

    // 應該包含的欄位
    assert!(record1.data.contains_key("id"));
    assert!(record1.data.contains_key("name"));
    assert!(record1.data.contains_key("email"));
    assert_eq!(record1.data.get("id").unwrap(), &serde_json::json!(1));
    assert_eq!(
        record1.data.get("name").unwrap(),
        &serde_json::json!("John Doe")
    );
    assert_eq!(
        record1.data.get("email").unwrap(),
        &serde_json::json!("john@example.com")
    );

    // 不應該包含的敏感欄位
    assert!(!record1.data.contains_key("password"));
    assert!(!record1.data.contains_key("ssn"));
    assert!(!record1.data.contains_key("internal_notes"));
    assert!(!record1.data.contains_key("created_at"));
    assert!(!record1.data.contains_key("last_login"));
    assert!(!record1.data.contains_key("preferences"));

    // 驗證第二個記錄
    let record2 = &results[0].records[1];
    assert!(record2.data.contains_key("id"));
    assert!(record2.data.contains_key("name"));
    assert!(record2.data.contains_key("email"));
    assert_eq!(record2.data.get("id").unwrap(), &serde_json::json!(2));
    assert_eq!(
        record2.data.get("name").unwrap(),
        &serde_json::json!("Jane Smith")
    );
    assert_eq!(
        record2.data.get("email").unwrap(),
        &serde_json::json!("jane@example.com")
    );

    // 確保敏感資料被過濾掉
    assert!(!record2.data.contains_key("password"));
    assert!(!record2.data.contains_key("ssn"));

    api_mock.assert();
    println!("✅ keep_only_fields test passed!");

    Ok(())
}

/// 測試 exclude_fields 功能：排除指定欄位
#[tokio::test]
async fn test_exclude_fields() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "exclude-fields-test"
description = "Test exclude_fields functionality"
version = "1.0.0"
execution_order = ["exclude_pipeline"]

[[pipelines]]
name = "exclude_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/user-data"
method = "GET"

[pipelines.extract]

[pipelines.transform.operations]
exclude_fields = ["password", "ssn", "internal_notes"]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path
    );

    let config_path = format!("{}/exclude_fields_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/user-data");
        then.status(200).json_body(serde_json::json!({
            "id": 123,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "password": "topsecret",
            "ssn": "555-66-7777",
            "internal_notes": "High value customer",
            "phone": "+1-555-0199",
            "address": "123 Main St",
            "subscription": "premium"
        }));
    });

    let mut sequence = PipelineSequence::new("exclude_fields_test".to_string());

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

    println!("🔧 Starting exclude_fields test...");
    let results = sequence.execute_all().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].records.len(), 1);

    let record = &results[0].records[0];
    println!(
        "🔍 Record keys after exclusion: {:?}",
        record.data.keys().collect::<Vec<_>>()
    );

    // 應該保留的欄位
    assert!(record.data.contains_key("id"));
    assert!(record.data.contains_key("name"));
    assert!(record.data.contains_key("email"));
    assert!(record.data.contains_key("phone"));
    assert!(record.data.contains_key("address"));
    assert!(record.data.contains_key("subscription"));

    // 應該被排除的敏感欄位
    assert!(!record.data.contains_key("password"));
    assert!(!record.data.contains_key("ssn"));
    assert!(!record.data.contains_key("internal_notes"));

    // 驗證具體值
    assert_eq!(record.data.get("id").unwrap(), &serde_json::json!(123));
    assert_eq!(
        record.data.get("name").unwrap(),
        &serde_json::json!("Alice Johnson")
    );
    assert_eq!(
        record.data.get("email").unwrap(),
        &serde_json::json!("alice@example.com")
    );

    api_mock.assert();
    println!("✅ exclude_fields test passed!");

    Ok(())
}

/// 測試欄位過濾與多階層 field mapping 的組合使用
#[tokio::test]
async fn test_field_filtering_with_mapping() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "filtering-with-mapping-test"
description = "Test field filtering with nested field mapping"
version = "1.0.0"
execution_order = ["combined_pipeline"]

[[pipelines]]
name = "combined_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/complex-data"
method = "GET"

[pipelines.extract]
# 先透過 field mapping 提取和重命名欄位
field_mapping = {{ "id" = "user_id", "user.profile.name" = "full_name", "user.contact.email" = "email_address", "user.personal.ssn" = "ssn", "user.preferences.theme" = "theme" }}

[pipelines.transform.operations]
# 然後只保留指定的欄位（包括映射後的欄位名）
keep_only_fields = ["user_id", "full_name", "email_address", "theme"]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path
    );

    let config_path = format!("{}/filtering_with_mapping_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/complex-data");
        then.status(200).json_body(serde_json::json!({
            "id": 999,
            "user": {
                "profile": {
                    "name": "Bob Wilson",
                    "age": 35
                },
                "contact": {
                    "email": "bob@example.com",
                    "phone": "+1-555-0100"
                },
                "personal": {
                    "ssn": "999-88-7777",
                    "birth_date": "1989-05-20"
                },
                "preferences": {
                    "theme": "dark",
                    "language": "en",
                    "notifications": true
                }
            },
            "metadata": {
                "created": "2024-01-01T00:00:00Z",
                "updated": "2024-03-20T12:00:00Z"
            },
            "internal": {
                "classification": "restricted",
                "notes": "Important customer"
            }
        }));
    });

    let mut sequence = PipelineSequence::new("filtering_with_mapping_test".to_string());

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

    println!("🔧 Starting field filtering with mapping test...");
    let results = sequence.execute_all().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].records.len(), 1);

    let record = &results[0].records[0];
    println!(
        "🔍 Final record keys: {:?}",
        record.data.keys().collect::<Vec<_>>()
    );

    // 應該包含的映射後且被保留的欄位
    assert!(record.data.contains_key("user_id"));
    assert!(record.data.contains_key("full_name"));
    assert!(record.data.contains_key("email_address"));
    assert!(record.data.contains_key("theme"));

    // 驗證值
    assert_eq!(record.data.get("user_id").unwrap(), &serde_json::json!(999));
    assert_eq!(
        record.data.get("full_name").unwrap(),
        &serde_json::json!("Bob Wilson")
    );
    assert_eq!(
        record.data.get("email_address").unwrap(),
        &serde_json::json!("bob@example.com")
    );
    assert_eq!(
        record.data.get("theme").unwrap(),
        &serde_json::json!("dark")
    );

    // 不應該包含的欄位（被過濾掉的敏感資料）
    assert!(!record.data.contains_key("ssn")); // 雖然被映射了，但不在 keep_only_fields 中
    assert!(!record.data.contains_key("user")); // 原始巢狀結構
    assert!(!record.data.contains_key("metadata")); // 元資料
    assert!(!record.data.contains_key("internal")); // 內部資料

    api_mock.assert();
    println!("✅ Field filtering with mapping test passed!");

    Ok(())
}
