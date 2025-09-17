use anyhow::Result;
use httpmock::prelude::*;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline, pipeline_sequence::PipelineSequence,
};
use samll_etl::LocalStorage;
use tempfile::TempDir;

#[tokio::test]
async fn debug_simple_auth_flow() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    // 簡化的配置，只測試基本的授權和使用
    let config_content = format!(
        r#"
[sequence]
name = "debug-auth"
description = "Debug auth flow"
version = "1.0.0"
execution_order = ["auth", "api"]

[[pipelines]]
name = "auth"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/auth"
method = "POST"

[pipelines.extract]

[pipelines.transform.intermediate]
export_to_shared = true
shared_key = ""

[pipelines.load]
output_path = "{}"
output_formats = ["json"]

[[pipelines]]
name = "api"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/data"

[pipelines.source.headers]
Authorization = "Bearer {{{{token}}}}"

[pipelines.source.data_source]
use_previous_output = true
from_pipeline = "auth"
merge_with_api = true

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path, normalized_path
    );

    let config_path = format!("{}/debug_auth.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    // Mock 授權端點 - 返回簡單的 token
    let auth_mock = server.mock(|when, then| {
        when.method(POST).path("/auth");
        then.status(200).json_body(serde_json::json!({
            "access_token": "debug_token_123"
        }));
    });

    // Mock 數據端點 - 檢查是否有正確的 Authorization header
    let data_mock = server.mock(|when, then| {
        when.method(GET)
            .path("/data")
            .header("authorization", "Bearer debug_token_123");
        then.status(200).json_body(serde_json::json!([
            {"message": "success"}
        ]));
    });

    let mut sequence = PipelineSequence::new("debug_auth".to_string());

    // 修改端點為 mock server
    let mut modified_config = config.clone();
    for pipeline in &mut modified_config.pipelines {
        if let Some(endpoint) = &mut pipeline.source.endpoint {
            if endpoint.contains("localhost:8080") {
                *endpoint = endpoint.replace("localhost:8080", &server.address().to_string());
            }
        }
    }

    // 添加 pipelines
    for pipeline_def in &modified_config.pipelines {
        let storage = LocalStorage::new(pipeline_def.load.output_path.clone());
        let contextual_pipeline =
            SequenceAwarePipeline::new(pipeline_def.name.clone(), storage, pipeline_def.clone());
        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    // 執行序列
    println!("🔧 Starting debug auth flow...");
    let results = sequence.execute_all().await?;

    // 調試輸出
    println!("📊 Results: {} pipelines executed", results.len());
    for (i, result) in results.iter().enumerate() {
        println!(
            "   {}. {} - {} records",
            i + 1,
            result.pipeline_name,
            result.records.len()
        );

        if !result.records.is_empty() {
            println!("      First record: {:?}", result.records[0].data);
        }
    }

    // 基本驗證
    assert_eq!(results.len(), 2, "Should have 2 pipeline results");
    assert_eq!(results[0].pipeline_name, "auth");
    assert_eq!(results[1].pipeline_name, "api");

    println!("✅ Debug test basic structure passed");

    // 檢查第一個 pipeline 是否有數據
    if results[0].records.is_empty() {
        println!("❌ First pipeline (auth) has no records!");
        return Err(anyhow::anyhow!("Auth pipeline returned no records"));
    }

    // 檢查第二個 pipeline 是否有數據
    if results[1].records.is_empty() {
        println!("❌ Second pipeline (api) has no records!");
        return Err(anyhow::anyhow!("API pipeline returned no records"));
    }

    // 檢查 mock 是否被調用
    println!("🔍 Checking mock calls...");
    auth_mock.assert();
    data_mock.assert();

    println!("✅ All debug tests passed!");
    Ok(())
}
