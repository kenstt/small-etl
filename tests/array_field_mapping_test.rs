use anyhow::Result;
use httpmock::prelude::*;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline,
    pipeline_sequence::PipelineSequence,
};
use samll_etl::LocalStorage;
use tempfile::TempDir;

/// 測試陣列索引功能：選擇特定位置的元素
#[tokio::test]
async fn test_array_indexing() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "array-indexing-test"
description = "Test array indexing functionality"
version = "1.0.0"
execution_order = ["array_pipeline"]

[[pipelines]]
name = "array_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/team-data"
method = "GET"

[pipelines.extract]
field_mapping = {{ "team.name" = "team_name", "employees[0].name" = "team_lead", "employees[1].email" = "second_member_email", "employees[-1].name" = "newest_member" }}

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path
    );

    let config_path = format!("{}/array_indexing_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    // Mock API 回應包含陣列資料
    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/team-data");
        then.status(200).json_body(serde_json::json!({
            "team": {
                "name": "Engineering Team",
                "department": "Technology"
            },
            "employees": [
                {
                    "id": 1,
                    "name": "Alice Johnson",
                    "email": "alice@company.com",
                    "role": "Team Lead"
                },
                {
                    "id": 2,
                    "name": "Bob Smith",
                    "email": "bob@company.com",
                    "role": "Senior Developer"
                },
                {
                    "id": 3,
                    "name": "Charlie Brown",
                    "email": "charlie@company.com",
                    "role": "Junior Developer"
                }
            ]
        }));
    });

    let mut sequence = PipelineSequence::new("array_indexing_test".to_string());

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

    println!("🔧 Starting array indexing test...");
    let results = sequence.execute_all().await?;

    println!("📊 Results: {} pipelines executed", results.len());
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].pipeline_name, "array_pipeline");
    assert_eq!(results[0].records.len(), 1);

    let record = &results[0].records[0];
    println!("🔍 Record keys: {:?}", record.data.keys().collect::<Vec<_>>());

    // 驗證陣列索引映射結果
    assert_eq!(record.data.get("team_name").unwrap(), &serde_json::json!("Engineering Team"));
    assert_eq!(record.data.get("team_lead").unwrap(), &serde_json::json!("Alice Johnson"));
    assert_eq!(record.data.get("second_member_email").unwrap(), &serde_json::json!("bob@company.com"));
    assert_eq!(record.data.get("newest_member").unwrap(), &serde_json::json!("Charlie Brown"));

    api_mock.assert();
    println!("✅ Array indexing test passed!");

    Ok(())
}

/// 測試 flat mapping 功能：提取所有元素的欄位
#[tokio::test]
async fn test_flat_mapping() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "flat-mapping-test"
description = "Test flat mapping functionality"
version = "1.0.0"
execution_order = ["flat_mapping_pipeline"]

[[pipelines]]
name = "flat_mapping_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/products"
method = "GET"

[pipelines.extract]
field_mapping = {{ "products[*].name" = "all_product_names", "products[*].price" = "all_prices", "products[*].category.type" = "all_categories", "store.name" = "store_name" }}

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path
    );

    let config_path = format!("{}/flat_mapping_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/products");
        then.status(200).json_body(serde_json::json!({
            "store": {
                "name": "Tech Store",
                "location": "Downtown"
            },
            "products": [
                {
                    "id": 101,
                    "name": "Laptop",
                    "price": 999.99,
                    "category": {
                        "type": "electronics",
                        "subcategory": "computers"
                    }
                },
                {
                    "id": 102,
                    "name": "Mouse",
                    "price": 29.99,
                    "category": {
                        "type": "electronics",
                        "subcategory": "accessories"
                    }
                },
                {
                    "id": 103,
                    "name": "Desk",
                    "price": 299.99,
                    "category": {
                        "type": "furniture",
                        "subcategory": "office"
                    }
                }
            ]
        }));
    });

    let mut sequence = PipelineSequence::new("flat_mapping_test".to_string());

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

    println!("🔧 Starting flat mapping test...");
    let results = sequence.execute_all().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].records.len(), 1);

    let record = &results[0].records[0];
    println!("🔍 Record keys: {:?}", record.data.keys().collect::<Vec<_>>());

    // 驗證 flat mapping 結果
    assert_eq!(record.data.get("store_name").unwrap(), &serde_json::json!("Tech Store"));

    let all_names = record.data.get("all_product_names").unwrap();
    assert_eq!(all_names, &serde_json::json!(["Laptop", "Mouse", "Desk"]));

    let all_prices = record.data.get("all_prices").unwrap();
    assert_eq!(all_prices, &serde_json::json!([999.99, 29.99, 299.99]));

    let all_categories = record.data.get("all_categories").unwrap();
    assert_eq!(all_categories, &serde_json::json!(["electronics", "electronics", "furniture"]));

    api_mock.assert();
    println!("✅ Flat mapping test passed!");

    Ok(())
}

/// 測試混合使用陣列索引和 flat mapping
#[tokio::test]
async fn test_mixed_array_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_str().unwrap();
    let normalized_path = temp_path.replace('\\', "/");

    let config_content = format!(
        r#"
[sequence]
name = "mixed-array-test"
description = "Test mixed array operations"
version = "1.0.0"
execution_order = ["mixed_pipeline"]

[[pipelines]]
name = "mixed_pipeline"
enabled = true

[pipelines.source]
type = "api"
endpoint = "http://localhost:8080/department"
method = "GET"

[pipelines.extract]
field_mapping = {{ "department.name" = "dept_name", "teams[0].name" = "first_team_name", "teams[-1].name" = "last_team_name", "teams[*].name" = "all_team_names", "teams[0].members[*].name" = "first_team_members", "teams[*].members[0].name" = "all_team_leads" }}

[pipelines.transform]

[pipelines.load]
output_path = "{}"
output_formats = ["json"]
"#,
        normalized_path
    );

    let config_path = format!("{}/mixed_array_test.toml", temp_path);
    tokio::fs::write(&config_path, config_content).await?;
    let config = SequenceConfig::from_file(&config_path)?;

    let server = MockServer::start();

    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/department");
        then.status(200).json_body(serde_json::json!({
            "department": {
                "name": "Engineering",
                "id": "ENG001"
            },
            "teams": [
                {
                    "name": "Backend Team",
                    "members": [
                        {"name": "Alice", "role": "Lead"},
                        {"name": "Bob", "role": "Senior"}
                    ]
                },
                {
                    "name": "Frontend Team",
                    "members": [
                        {"name": "Charlie", "role": "Lead"},
                        {"name": "Diana", "role": "Junior"}
                    ]
                },
                {
                    "name": "DevOps Team",
                    "members": [
                        {"name": "Eve", "role": "Lead"}
                    ]
                }
            ]
        }));
    });

    let mut sequence = PipelineSequence::new("mixed_array_test".to_string());

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

    println!("🔧 Starting mixed array operations test...");
    let results = sequence.execute_all().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].records.len(), 1);

    let record = &results[0].records[0];
    println!("🔍 Record keys: {:?}", record.data.keys().collect::<Vec<_>>());

    // 驗證混合操作結果
    assert_eq!(record.data.get("dept_name").unwrap(), &serde_json::json!("Engineering"));
    assert_eq!(record.data.get("first_team_name").unwrap(), &serde_json::json!("Backend Team"));
    assert_eq!(record.data.get("last_team_name").unwrap(), &serde_json::json!("DevOps Team"));

    assert_eq!(
        record.data.get("all_team_names").unwrap(),
        &serde_json::json!(["Backend Team", "Frontend Team", "DevOps Team"])
    );

    assert_eq!(
        record.data.get("first_team_members").unwrap(),
        &serde_json::json!(["Alice", "Bob"])
    );

    assert_eq!(
        record.data.get("all_team_leads").unwrap(),
        &serde_json::json!(["Alice", "Charlie", "Eve"])
    );

    api_mock.assert();
    println!("✅ Mixed array operations test passed!");

    Ok(())
}