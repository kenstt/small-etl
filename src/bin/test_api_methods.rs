use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::Storage;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline, pipeline_sequence::PipelineSequence,
};
use samll_etl::utils::error::Result;
use std::path::Path;

/// 基於檔案系統的存儲實現（用於測試）
#[derive(Clone)]
pub struct FileSystemStorage {
    base_path: String,
}

impl FileSystemStorage {
    pub fn new(base_path: String) -> Self {
        Self { base_path }
    }
}

impl Storage for FileSystemStorage {
    fn write_file(
        &self,
        filename: &str,
        data: &[u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let path = format!("{}/{}", self.base_path, filename);
        async move {
            // 確保目錄存在
            if let Some(parent) = Path::new(&path).parent() {
                std::fs::create_dir_all(parent)
                    .map_err(samll_etl::utils::error::EtlError::IoError)?;
            }

            // 寫入文件
            std::fs::write(&path, data).map_err(samll_etl::utils::error::EtlError::IoError)?;
            Ok(())
        }
    }

    fn read_file(
        &self,
        filename: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        let path = format!("{}/{}", self.base_path, filename);
        async move { std::fs::read(&path).map_err(samll_etl::utils::error::EtlError::IoError) }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    println!("🚀 測試 API HTTP 方法和 Payload 功能");

    // 載入測試配置
    let config_path = "test_api_config.toml";
    let config = SequenceConfig::from_file(config_path)?;

    // 驗證配置
    config.validate()?;

    println!("✅ 配置驗證通過");
    println!("📋 將執行以下 pipelines:");
    for pipeline_name in &config.sequence.execution_order {
        if let Some(pipeline_def) = config.get_pipeline(pipeline_name) {
            let method = pipeline_def.source.method.as_deref().unwrap_or("GET");
            let endpoint = pipeline_def.source.endpoint.as_deref().unwrap_or("N/A");
            println!("  - {}: {} {}", pipeline_name, method, endpoint);

            if let Some(payload) = &pipeline_def.source.payload {
                if let Some(body) = &payload.body {
                    println!("    Payload: {}", body);
                }
            }
        }
    }

    // 創建 Pipeline 序列
    let mut sequence = PipelineSequence::new("test-api-methods".to_string()).with_monitoring(true);

    // 為每個 Pipeline 定義創建 SequenceAwarePipeline
    for pipeline_name in &config.sequence.execution_order {
        if let Some(pipeline_def) = config.get_pipeline(pipeline_name) {
            if pipeline_def.enabled.unwrap_or(true) {
                let storage = FileSystemStorage::new(pipeline_def.load.output_path.clone());
                let pipeline = SequenceAwarePipeline::new(
                    pipeline_name.clone(),
                    storage,
                    pipeline_def.clone(),
                );
                sequence.add_pipeline(Box::new(pipeline));
            }
        }
    }

    // 執行 Pipeline 序列
    println!("\n🔄 開始執行 Pipeline 序列...");
    let results = sequence.execute_all().await?;

    // 顯示結果
    println!("\n✅ Pipeline 序列執行完成！");
    println!("📊 執行摘要:");

    for result in &results {
        println!(
            "  - {}: {} 筆記錄, 耗時 {:?}",
            result.pipeline_name,
            result.records.len(),
            result.duration
        );
    }

    let summary = PipelineSequence::get_execution_summary(&results);
    println!(
        "📈 總計: {} 個 pipelines, {} 筆記錄",
        summary.get("total_pipelines").unwrap(),
        summary.get("total_records").unwrap()
    );

    println!("\n🎉 測試完成！檢查 ./test-output 資料夾中的輸出文件。");

    Ok(())
}
