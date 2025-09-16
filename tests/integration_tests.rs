use httpmock::prelude::*;
use samll_etl::{CliConfig, EtlEngine, LocalStorage, SimplePipeline};
use std::collections::HashMap;
use tempfile::TempDir;

#[tokio::test]
async fn test_end_to_end_etl_with_real_http() {
    // Setup temporary directory for output
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_str().unwrap().to_string();

    // Setup mock HTTP server
    let server = MockServer::start();
    let mock_data = serde_json::json!([
        {"id": 1, "name": "Product A", "price": 29.99, "category": "Electronics"},
        {"id": 2, "name": "Product B", "price": 49.99, "category": "Books"},
        {"id": 3, "name": "Product C", "price": 79.99, "category": "Electronics"}
    ]);

    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/products");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body(mock_data);
    });

    // Create configuration for CLI
    let mut config = CliConfig {
        api_endpoint: server.url("/products"),
        output_path: output_path.clone(),
        lookup_files: vec![],
        concurrent_requests: 5,
        verbose: false,
        monitor: false,
    };

    // Create storage and pipeline
    let storage = LocalStorage::new(output_path.clone());
    let pipeline = SimplePipeline::new(storage, config);

    // Create and run ETL engine
    let engine = EtlEngine::new_with_monitoring(pipeline, false);
    let result = engine.run().await;

    // Verify results
    assert!(result.is_ok());
    api_mock.assert();

    let output_file_path = result.unwrap();
    assert!(output_file_path.contains("etl_output.zip"));

    // Verify output file exists
    let full_path = std::path::Path::new(&output_path).join("etl_output.zip");
    assert!(full_path.exists());

    // Verify ZIP content
    let zip_data = std::fs::read(&full_path).unwrap();
    let cursor = std::io::Cursor::new(zip_data);
    let mut archive = zip::ZipArchive::new(cursor).unwrap();

    // Should have at least CSV and TSV files
    assert!(archive.len() >= 2);

    let file_names: Vec<String> = (0..archive.len())
        .map(|i| archive.by_index(i).unwrap().name().to_string())
        .collect();

    assert!(file_names.contains(&"output.csv".to_string()));
    assert!(file_names.contains(&"output.tsv".to_string()));

    // Verify CSV content structure
    let mut csv_file = archive.by_name("output.csv").unwrap();
    let mut csv_content = String::new();
    std::io::Read::read_to_string(&mut csv_file, &mut csv_content).unwrap();

    assert!(csv_content.contains("id,name,value,processed"));
    assert!(csv_content.contains("Product A"));
    assert!(csv_content.contains("Product B"));
    assert!(csv_content.contains("Product C"));
}

#[tokio::test]
async fn test_end_to_end_with_api_failure() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_str().unwrap().to_string();

    // Setup mock HTTP server that returns 500 error
    let server = MockServer::start();
    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/failed");
        then.status(500);
    });

    let config = CliConfig {
        api_endpoint: server.url("/failed"),
        output_path: output_path.clone(),
        lookup_files: vec![],
        concurrent_requests: 5,
        verbose: false,
        monitor: false,
    };

    let storage = LocalStorage::new(output_path.clone());
    let pipeline = SimplePipeline::new(storage, config);
    let engine = EtlEngine::new(pipeline);

    let result = engine.run().await;

    // Should still succeed because pipeline falls back to sample data
    assert!(result.is_ok());
    api_mock.assert();

    // Verify output file exists (with sample data)
    let output_file_path = result.unwrap();
    let full_path = std::path::Path::new(&output_path).join("etl_output.zip");
    assert!(full_path.exists());
}

#[tokio::test]
async fn test_end_to_end_with_monitoring() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_str().unwrap().to_string();

    let server = MockServer::start();
    let mock_data = serde_json::json!([
        {"id": 1, "name": "Test Item", "value": 100}
    ]);

    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/test");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body(mock_data);
    });

    let config = CliConfig {
        api_endpoint: server.url("/test"),
        output_path: output_path.clone(),
        lookup_files: vec![],
        concurrent_requests: 5,
        verbose: true,
        monitor: true, // Enable monitoring
    };

    let storage = LocalStorage::new(output_path.clone());
    let pipeline = SimplePipeline::new(storage, config);
    let engine = EtlEngine::new_with_monitoring(pipeline, true);

    let result = engine.run().await;

    assert!(result.is_ok());
    api_mock.assert();
}

#[tokio::test]
async fn test_intermediate_data_generation() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_str().unwrap().to_string();

    let server = MockServer::start();
    // Create data where some values > 20 (will be in intermediate data)
    let mock_data = serde_json::json!([
        {"id": 1, "name": "Low Value", "value": 10},   // Won't be in intermediate
        {"id": 2, "name": "High Value", "value": 50},  // Will be in intermediate
        {"id": 3, "name": "Higher Value", "value": 75} // Will be in intermediate
    ]);

    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/data");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body(mock_data);
    });

    let config = CliConfig {
        api_endpoint: server.url("/data"),
        output_path: output_path.clone(),
        lookup_files: vec![],
        concurrent_requests: 5,
        verbose: false,
        monitor: false,
    };

    let storage = LocalStorage::new(output_path.clone());
    let pipeline = SimplePipeline::new(storage, config);
    let engine = EtlEngine::new(pipeline);

    let result = engine.run().await;
    assert!(result.is_ok());
    api_mock.assert();

    // Verify ZIP contains intermediate.json
    let full_path = std::path::Path::new(&output_path).join("etl_output.zip");
    let zip_data = std::fs::read(&full_path).unwrap();
    let cursor = std::io::Cursor::new(zip_data);
    let mut archive = zip::ZipArchive::new(cursor).unwrap();

    let file_names: Vec<String> = (0..archive.len())
        .map(|i| archive.by_index(i).unwrap().name().to_string())
        .collect();

    assert!(file_names.contains(&"intermediate.json".to_string()));

    // Verify intermediate data content
    let mut json_file = archive.by_name("intermediate.json").unwrap();
    let mut json_content = String::new();
    std::io::Read::read_to_string(&mut json_file, &mut json_content).unwrap();

    let intermediate_data: Vec<serde_json::Value> = serde_json::from_str(&json_content).unwrap();
    assert_eq!(intermediate_data.len(), 2); // Only records with value > 20
}

#[tokio::test]
async fn test_concurrent_requests_parameter() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_str().unwrap().to_string();

    let server = MockServer::start();
    let mock_data = serde_json::json!([{"id": 1, "name": "Test"}]);

    let api_mock = server.mock(|when, then| {
        when.method(GET).path("/concurrent");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body(mock_data);
    });

    let config = CliConfig {
        api_endpoint: server.url("/concurrent"),
        output_path: output_path.clone(),
        lookup_files: vec![],
        concurrent_requests: 10, // Different value
        verbose: false,
        monitor: false,
    };

    let storage = LocalStorage::new(output_path.clone());
    let pipeline = SimplePipeline::new(storage, config);
    let engine = EtlEngine::new(pipeline);

    let result = engine.run().await;
    assert!(result.is_ok());
    api_mock.assert();
}
