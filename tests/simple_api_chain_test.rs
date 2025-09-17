use anyhow::Result;
use httpmock::prelude::*;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline, pipeline_sequence::PipelineSequence,
};
use samll_etl::LocalStorage;
use tempfile::TempDir;

#[tokio::test]
#[ignore] // Complex parameterized API chain test - core functionality tested elsewhere
async fn test_simple_api_chain() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    // 創建簡化的 API 鏈配置
    let config_content = format!(
        r#"
[sequence]
name = "simple-api-chain"
description = "Simple API chain test"
version = "1.0.0"
execution_order = ["get-users", "get-user-details"]

[global]
working_directory = "{}"

# Pipeline 1: 取得用戶列表
[[pipelines]]
name = "get-users"
description = "Get user list"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/users"

[pipelines.extract]
max_records = 2

[pipelines.extract.field_mapping]
id = "user_id"
name = "user_name"

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

# Pipeline 2: 使用用戶 ID 取得詳細資訊
[[pipelines]]
name = "get-user-details"
description = "Get user details by ID"
enabled = true
dependencies = ["get-users"]

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/users/{{user_id}}"

[pipelines.source.data_source]
use_previous_output = true
from_pipeline = "get-users"
merge_with_api = false

[pipelines.extract]

[pipelines.extract.field_mapping]
id = "detail_user_id"
name = "detail_name"
email = "detail_email"

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path, normalized_path
    );

    let config_path = format!("{}/simple_api_chain.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    // 設置 Mock Server
    let server = MockServer::start();

    // Mock 用戶列表 API
    let users_mock = server.mock(|when, then| {
        when.method(GET).path("/users");
        then.status(200).json_body(serde_json::json!([
            {"id": 1, "name": "John Doe"},
            {"id": 2, "name": "Jane Smith"},
            {"id": 3, "name": "Bob Wilson"}
        ]));
    });

    // Mock 用戶詳細資訊 API - 為每個用戶 ID
    let user_1_mock = server.mock(|when, then| {
        when.method(GET).path("/users/1");
        then.status(200).json_body(serde_json::json!({
            "id": 1,
            "name": "John Doe",
            "email": "john@example.com",
            "phone": "123-456-7890"
        }));
    });

    let user_2_mock = server.mock(|when, then| {
        when.method(GET).path("/users/2");
        then.status(200).json_body(serde_json::json!({
            "id": 2,
            "name": "Jane Smith",
            "email": "jane@example.com",
            "phone": "098-765-4321"
        }));
    });

    // 創建 Pipeline 序列
    let mut sequence = PipelineSequence::new("simple_chain_test".to_string());

    // 修改配置中的端點 URL 為 mock server
    let mut modified_config = config.clone();
    for pipeline in &mut modified_config.pipelines {
        if let Some(endpoint) = &mut pipeline.source.endpoint {
            if endpoint.contains("localhost:8080") {
                *endpoint = endpoint.replace("localhost:8080", &server.address().to_string());
            }
        }
    }

    // 添加 Pipeline 到序列
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
    assert_eq!(results[0].pipeline_name, "get-users");
    assert_eq!(results[1].pipeline_name, "get-user-details");

    // 驗證第一個 pipeline 有 2 個用戶（因為 max_records = 2）
    assert_eq!(results[0].records.len(), 2);

    // 驗證第二個 pipeline 有 2 個詳細記錄（對應每個用戶）
    assert_eq!(results[1].records.len(), 2);

    // 調試：輸出詳細記錄的內容
    println!("Debug: User details records:");
    for (i, record) in results[1].records.iter().enumerate() {
        println!("Record {}: {:?}", i, record.data);
    }

    // 驗證詳細記錄包含正確的字段映射
    for record in &results[1].records {
        assert!(
            record.data.contains_key("detail_user_id"),
            "Missing detail_user_id in record: {:?}",
            record.data
        );
        assert!(
            record.data.contains_key("detail_name"),
            "Missing detail_name in record: {:?}",
            record.data
        );
        assert!(
            record.data.contains_key("detail_email"),
            "Missing detail_email in record: {:?}",
            record.data
        );
    }

    // 驗證 Mock 被正確調用
    users_mock.assert(); // 用戶列表被調用一次
    user_1_mock.assert(); // 用戶 1 詳情被調用一次
    user_2_mock.assert(); // 用戶 2 詳情被調用一次

    println!("✅ Simple API chain test completed successfully!");
    println!(
        "📊 First pipeline (users): {} records",
        results[0].records.len()
    );
    println!(
        "📊 Second pipeline (details): {} records",
        results[1].records.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_api_chain_with_missing_parameter() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    // 創建有缺失參數的配置
    let config_content = format!(
        r#"
[sequence]
name = "missing-param-chain"
description = "Test missing parameter handling"
version = "1.0.0"
execution_order = ["get-users", "get-user-details"]

[global]
working_directory = "{}"

[[pipelines]]
name = "get-users"
description = "Get user list"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/users"

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "get-user-details"
description = "Get user details with missing param"
enabled = true
dependencies = ["get-users"]

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/users/{{missing_param}}"

[pipelines.source.data_source]
use_previous_output = true
from_pipeline = "get-users"

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path, normalized_path
    );

    let config_path = format!("{}/missing_param_chain.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    // Mock 用戶列表 API（返回沒有預期參數的數據）
    let _users_mock = server.mock(|when, then| {
        when.method(GET).path("/users");
        then.status(200).json_body(serde_json::json!([
            {"name": "John Doe", "email": "john@example.com"}  // 缺少 'id' 字段
        ]));
    });

    let mut sequence = PipelineSequence::new("missing_param_test".to_string());

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

    // 執行序列應該失敗因為缺少參數
    let result = sequence.execute_all().await;
    assert!(result.is_err());

    println!("✅ Missing parameter test completed - correctly failed as expected");

    Ok(())
}
