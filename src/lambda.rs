use std::fs::File;
use std::io::Read;
#[cfg(feature = "lambda")]
use aws_config::BehaviorVersion;
#[cfg(feature = "lambda")]
use aws_sdk_s3::config::Region;
#[cfg(feature = "lambda")]
use aws_sdk_s3::Client as S3Client;
#[cfg(feature = "lambda")]
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
#[cfg(feature = "lambda")]
use samll_etl::config::lambda::{LambdaConfig, S3Storage};
#[cfg(feature = "lambda")]
use samll_etl::core::{etl::EtlEngine, pipeline::SimplePipeline};
#[cfg(feature = "lambda")]
use samll_etl::utils::logger;
#[cfg(feature = "lambda")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "lambda")]
#[derive(Debug, Deserialize)]
pub struct Request {
    pub api_endpoint: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_prefix: Option<String>,
}

#[cfg(feature = "lambda")]
#[derive(Debug, Serialize)]
pub struct Response {
    pub message: String,
    pub output_path: String,
    pub records_processed: usize,
}

#[cfg(feature = "lambda")]
async fn function_handler(event: LambdaEvent<Request>) -> Result<Response, Error> {
    tracing::info!("Starting ETL Lambda function");
    tracing::debug!("Lambda event: {:?}", event.payload);

    // 設置環境變量 (如果事件中有的話)
    if let Some(endpoint) = &event.payload.api_endpoint {
        std::env::set_var("API_ENDPOINT", endpoint);
    }
    if let Some(bucket) = &event.payload.s3_bucket {
        std::env::set_var("S3_BUCKET", bucket);
    }
    if let Some(prefix) = &event.payload.s3_prefix {
        std::env::set_var("S3_PREFIX", prefix);
    }

    // 創建Lambda配置
    let lambda_config = LambdaConfig::from_env()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    tracing::info!(
        "Lambda config - bucket: {}, region: {}, prefix: {}",
        lambda_config.s3_bucket,
        lambda_config.s3_region,
        lambda_config.s3_prefix
    );

    // 創建AWS配置和S3客戶端
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let region = Region::new(lambda_config.s3_region.clone());
    // let s3_client = S3Client::new(&config);
    let config = aws_sdk_s3::config::Builder::from(&config)
        .region(region)
        .force_path_style(true)
        .build();
    let s3_client = S3Client::from_conf(config);

    // 創建存儲和管道
    let storage = S3Storage::new(s3_client, lambda_config.s3_bucket.clone());
    let pipeline = SimplePipeline::new(storage, lambda_config);

    // 運行ETL
    let engine = EtlEngine::new(pipeline);
    let output_path = engine
        .run()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    let response = Response {
        message: "ETL process completed successfully".to_string(),
        output_path: output_path.clone(),
        records_processed: 0, // TODO: 實際記錄處理數量
    };

    tracing::info!("ETL Lambda function completed successfully");
    tracing::info!("Response: {:?}", response);
    Ok(response)
}

#[cfg(feature = "lambda")]
#[tokio::main]
async fn main() -> Result<(), Error> {
    // 初始化Lambda日誌
    logger::init_lambda_logger();

    tracing::info!("Lambda runtime starting...");
    run(service_fn(function_handler)).await
}
