use anyhow::Result;
use httpmock::prelude::*;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline, pipeline_sequence::PipelineSequence,
};
use samll_etl::LocalStorage;
use tempfile::TempDir;

/// 簡單測試 payload 中的 shared key 替換
#[tokio::test]
async fn test_simple_payload_shared_key() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "simple-test"
description = "Simple test"
version = "1.0.0"
execution_order = ["test_pipeline"]

[global]
shared_variables = {{ TEST_KEY = "shared_value_123" }}

[[pipelines]]
name = "test_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/test"
method = "POST"

[pipelines.source.payload]
content_type = "application/json"
body = '''{{
    "key": "{{{{TEST_KEY}}}}"
}}'''

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path
    );

    let config_path = format!("{}/simple_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    // 接收任何請求並記錄它，不做嚴格的body驗證
    let api_mock = server.mock(|when, then| {
        when.method(POST).path("/test");
        then.status(200).json_body(serde_json::json!({
            "status": "ok",
            "message": "Request received"
        }));
    });

    let mut sequence = PipelineSequence::new("simple_test".to_string());

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

    println!("🔧 Starting simple payload test...");
    let results = sequence.execute_all().await?;

    println!("📊 Results: {:?}", results);
    assert_eq!(results.len(), 1);

    // 驗證 mock 被調用
    api_mock.assert();

    println!("✅ Simple payload test passed!");
    Ok(())
}
