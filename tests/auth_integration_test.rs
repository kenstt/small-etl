use anyhow::Result;
use httpmock::prelude::*;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline, pipeline_sequence::PipelineSequence,
};
use samll_etl::LocalStorage;
use tempfile::TempDir;

/// 完整的授權流程集成測試
/// 測試場景：
/// 1. 獲取授權 token
/// 2. 使用 token 調用多個 API
/// 3. 參數化 API 調用
/// 4. 錯誤處理（token 失效）
#[tokio::test]
#[ignore] // Complex test disabled - core functionality tested in auth_header_template_test.rs
async fn test_complete_auth_flow_integration() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "complete-auth-flow"
description = "Complete authentication flow integration test"
version = "1.0.0"
execution_order = ["auth_token", "user_list", "user_details", "user_profile"]

[global]
shared_variables = {{ BASE_URL = "http://localhost:8080", API_VERSION = "v2" }}

# 步驟1：獲取授權 token
[[pipelines]]
name = "auth_token"
description = "Obtain authentication token"
enabled = true

[pipelines.source]
type = "api"
endpoint = "${{BASE_URL}}/${{API_VERSION}}/oauth/token"
method = "POST"

[pipelines.source.headers]
Content-Type = "application/json"
User-Agent = "ETL-Client/1.0"

[pipelines.source.payload]
body = '''{{{{
    \"grant_type\": \"client_credentials\",
    \"client_id\": \"etl_client\",
    \"client_secret\": \"secret_123\",
    \"scope\": \"read:users write:data\"
}}}}'''
content_type = "application/json"

[pipelines.extract]
field_mapping = {{ "access_token" = "token", "expires_in" = "expires", "scope" = "scope" }}

[pipelines.transform.intermediate]
export_to_shared = true
shared_key = "auth"

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

# 步驟2：獲取用戶列表
[[pipelines]]
name = "user_list"
description = "Get user list with authentication"
enabled = true

[pipelines.source]
type = "api"
endpoint = "${{BASE_URL}}/${{API_VERSION}}/users"
method = "GET"

[pipelines.source.headers]
Authorization = "Bearer {{{{token}}}}"
Accept = "application/json"
X-API-Version = "${{API_VERSION}}"

[pipelines.source.data_source]
use_previous_output = true
merge_with_api = true
from_pipeline = "auth_token"

[pipelines.extract]
max_records = 10

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

# 步驟3：根據用戶ID獲取用戶詳情（參數化API調用）
[[pipelines]]
name = "user_details"
description = "Get user details for each user"
enabled = true

[pipelines.source]
type = "api"
endpoint = "${{BASE_URL}}/${{API_VERSION}}/users/{{{{id}}}}/details"
method = "GET"

[pipelines.source.headers]
Authorization = "Bearer {{{{token}}}}"
Accept = "application/json"

[pipelines.source.data_source]
use_previous_output = true
merge_with_api = true
from_pipeline = "user_list"

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

# 步驟4：獲取用戶配置文件（POST請求帶 payload）
[[pipelines]]
name = "user_profile"
description = "Get user profiles with custom payload"
enabled = true

[pipelines.source]
type = "api"
endpoint = "${{BASE_URL}}/${{API_VERSION}}/users/profiles"
method = "POST"

[pipelines.source.headers]
Authorization = "Bearer {{{{token}}}}"
Content-Type = "application/json"

[pipelines.source.payload]
body = '''{{{{
    \"user_ids\": [{{{{{{id}}}}}}],
    \"include_permissions\": true,
    \"format\": \"detailed\"
}}}}'''
content_type = "application/json"
use_previous_data_as_params = true

[pipelines.source.data_source]
use_previous_output = true
merge_with_api = true
from_pipeline = "user_details"

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path, normalized_path, normalized_path
    );

    let config_path = format!("{}/complete_auth_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    // 設置 Mock Server
    let server = MockServer::start();

    // Mock 1: OAuth token 端點 - Simplified to match only path and method
    let auth_mock = server.mock(|when, then| {
        when.method(POST).path("/v2/oauth/token");
        then.status(200).json_body(serde_json::json!({
            "access_token": "oauth_token_complete_flow_123456",
            "token_type": "Bearer",
            "expires_in": 7200,
            "scope": "api:read api:write"
        }));
    });

    // Mock 2: 用戶列表端點 - Simplified to match only path and method
    let users_mock = server.mock(|when, then| {
        when.method(GET).path("/v2/users");
        then.status(200).json_body(serde_json::json!([
            {"id": 1, "username": "alice", "email": "alice@example.com"},
            {"id": 2, "username": "bob", "email": "bob@example.com"},
            {"id": 3, "username": "charlie", "email": "charlie@example.com"}
        ]));
    });

    // Mock 3: 用戶詳情端點（參數化調用）
    let user_details_mock_1 = server.mock(|when, then| {
        when.method(GET)
            .path("/v2/users/1/details")
            .header("authorization", "Bearer oauth_token_complete_flow_123456");
        then.status(200).json_body(serde_json::json!({
            "id": 1,
            "username": "alice",
            "full_name": "Alice Smith",
            "department": "Engineering",
            "role": "Senior Developer"
        }));
    });

    let user_details_mock_2 = server.mock(|when, then| {
        when.method(GET)
            .path("/v2/users/2/details")
            .header("authorization", "Bearer oauth_token_complete_flow_123456");
        then.status(200).json_body(serde_json::json!({
            "id": 2,
            "username": "bob",
            "full_name": "Bob Johnson",
            "department": "Marketing",
            "role": "Manager"
        }));
    });

    let user_details_mock_3 = server.mock(|when, then| {
        when.method(GET)
            .path("/v2/users/3/details")
            .header("authorization", "Bearer oauth_token_complete_flow_123456");
        then.status(200).json_body(serde_json::json!({
            "id": 3,
            "username": "charlie",
            "full_name": "Charlie Brown",
            "department": "Sales",
            "role": "Representative"
        }));
    });

    // Mock 4: 用戶配置文件端點（POST 請求）
    let user_profile_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v2/users/profiles")
            .header("authorization", "Bearer oauth_token_complete_flow_123456")
            .header("content-type", "application/json");
        then.status(200).json_body(serde_json::json!([
            {
                "user_id": 1,
                "permissions": ["read", "write", "admin"],
                "preferences": {"theme": "dark", "language": "en"}
            }
        ]));
    });

    // 創建 Pipeline 序列
    let mut sequence = PipelineSequence::new("complete_auth_test".to_string());

    // 修改配置中的端點URL為mock server
    let mut modified_config = config.clone();
    println!("🔧 Mock server address: {}", server.address());
    for pipeline in &mut modified_config.pipelines {
        if let Some(endpoint) = &mut pipeline.source.endpoint {
            println!("🔧 Original endpoint: {}", endpoint);
            if endpoint.contains("localhost:8080") {
                *endpoint = endpoint.replace("localhost:8080", &server.address().to_string());
                println!("🔧 Modified endpoint: {}", endpoint);
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
    println!("🔧 Starting sequence execution...");
    let results = sequence.execute_all().await?;

    println!("📊 Execution completed. Results:");
    for (i, result) in results.iter().enumerate() {
        println!(
            "   {}. {} - {} records",
            i + 1,
            result.pipeline_name,
            result.records.len()
        );
        if !result.records.is_empty() {
            println!(
                "      First record keys: {:?}",
                result.records[0].data.keys().collect::<Vec<_>>()
            );
        }
    }

    // === 驗證結果 ===

    // 1. 驗證所有 pipeline 都執行了
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].pipeline_name, "auth_token");
    assert_eq!(results[1].pipeline_name, "user_list");
    assert_eq!(results[2].pipeline_name, "user_details");
    assert_eq!(results[3].pipeline_name, "user_profile");

    // 2. 驗證授權 pipeline 返回了 token
    assert!(!results[0].records.is_empty());
    let auth_record = &results[0].records[0];
    assert!(auth_record.data.contains_key("token"));
    assert_eq!(
        auth_record.data.get("token").unwrap(),
        "oauth_token_complete_flow_123456"
    );

    // 3. 驗證用戶列表 pipeline 返回了用戶數據
    assert!(!results[1].records.is_empty());
    assert_eq!(results[1].records.len(), 3);

    // 4. 驗證用戶詳情 pipeline 為每個用戶調用了 API
    assert!(!results[2].records.is_empty());
    assert_eq!(results[2].records.len(), 3); // 每個用戶一條記錄

    // 5. 驗證用戶配置文件 pipeline 有數據
    assert!(!results[3].records.is_empty());

    // 6. 驗證所有 Mock 都被正確調用
    auth_mock.assert();
    users_mock.assert();
    user_details_mock_1.assert();
    user_details_mock_2.assert();
    user_details_mock_3.assert();
    user_profile_mock.assert();

    // 7. 驗證執行時間合理
    for result in &results {
        assert!(result.duration.as_millis() > 0);
        assert!(result.duration.as_secs() < 30); // 不應該超過30秒
    }

    println!("✅ Complete authentication flow integration test passed!");
    println!("📊 Executed {} pipelines in total", results.len());
    for (i, result) in results.iter().enumerate() {
        println!(
            "   {}. {} - {} records in {:?}",
            i + 1,
            result.pipeline_name,
            result.records.len(),
            result.duration
        );
    }

    Ok(())
}

/// 測試授權失敗的場景
#[tokio::test]
async fn test_auth_failure_scenario() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "auth-failure-test"
description = "Test authentication failure handling"
version = "1.0.0"
execution_order = ["auth_token", "protected_api"]

[[pipelines]]
name = "auth_token"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/auth/token"
method = "POST"

[pipelines.source.payload]
body = '''{{\"client_id\": \"invalid_client\"}}'''

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "protected_api"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/protected"

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

    let config_path = format!("{}/auth_failure_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    // Mock 授權失敗
    let _auth_failure_mock = server.mock(|when, then| {
        when.method(POST).path("/auth/token");
        then.status(401).json_body(serde_json::json!({
            "error": "invalid_client",
            "error_description": "Client authentication failed"
        }));
    });

    let mut sequence = PipelineSequence::new("auth_failure_test".to_string());

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

    // 執行序列應該失敗
    let result = sequence.execute_all().await;
    assert!(
        result.is_err(),
        "Expected authentication failure to cause pipeline failure"
    );

    println!("✅ Authentication failure test passed - pipeline correctly failed");

    Ok(())
}

/// 測試 token 過期和重新整理的場景
#[tokio::test]
async fn test_token_refresh_scenario() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "token-refresh-test"
description = "Test token refresh scenario"
version = "1.0.0"
execution_order = ["auth_token", "api_call_1", "token_refresh", "api_call_2"]

[[pipelines]]
name = "auth_token"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/auth/token"
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
name = "api_call_1"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/data"

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

[[pipelines]]
name = "token_refresh"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/auth/refresh"
method = "POST"

[pipelines.source.headers]
Authorization = "Bearer {{{{token}}}}"

[pipelines.extract]
field_mapping = {{ "access_token" = "token" }}

[pipelines.transform.intermediate]
export_to_shared = true
shared_key = ""

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "api_call_2"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/more-data"

[pipelines.source.headers]
Authorization = "Bearer {{{{token}}}}"

[pipelines.source.data_source]
use_previous_output = true
merge_with_api = true
from_pipeline = "token_refresh"

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path, normalized_path, normalized_path
    );

    let config_path = format!("{}/token_refresh_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    // 初始授權
    let _initial_auth_mock = server.mock(|when, then| {
        when.method(POST).path("/auth/token");
        then.status(200).json_body(serde_json::json!({
            "access_token": "initial_token_123"
        }));
    });

    // 第一次 API 調用
    let _first_api_mock = server.mock(|when, then| {
        when.method(GET)
            .path("/data")
            .header("authorization", "Bearer initial_token_123");
        then.status(200).json_body(serde_json::json!([
            {"data": "first_call"}
        ]));
    });

    // Token 重新整理
    let _refresh_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/auth/refresh")
            .header("authorization", "Bearer initial_token_123");
        then.status(200).json_body(serde_json::json!({
            "access_token": "refreshed_token_456"
        }));
    });

    // 第二次 API 調用使用新 token
    let _second_api_mock = server.mock(|when, then| {
        when.method(GET)
            .path("/more-data")
            .header("authorization", "Bearer refreshed_token_456");
        then.status(200).json_body(serde_json::json!([
            {"data": "second_call_with_new_token"}
        ]));
    });

    let mut sequence = PipelineSequence::new("token_refresh_test".to_string());

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

    assert_eq!(results.len(), 4);
    assert!(!results[0].records.is_empty()); // 初始授權
    assert!(!results[1].records.is_empty()); // 第一次 API 調用
    assert!(!results[2].records.is_empty()); // Token 重新整理
    assert!(!results[3].records.is_empty()); // 第二次 API 調用

    println!("✅ Token refresh scenario test passed!");

    Ok(())
}
