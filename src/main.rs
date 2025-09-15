use clap::Parser;
use samll_etl::{CliConfig, EtlEngine, LocalStorage, SimplePipeline};
use samll_etl::utils::logger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = CliConfig::parse();

    // 初始化日誌
    logger::init_cli_logger(config.verbose);

    tracing::info!("Starting samll-etl CLI");
    if config.verbose {
        tracing::debug!("CLI config: {:?}", config);
    }

    let monitor_enabled = config.monitor;
    if monitor_enabled {
        tracing::info!("🔍 System monitoring enabled");
    }

    // 創建存儲和管道
    let storage = LocalStorage::new(config.output_path.clone());
    let pipeline = SimplePipeline::new(storage, config);

    // 創建ETL引擎並運行
    let engine = EtlEngine::new_with_monitoring(pipeline, monitor_enabled);

    match engine.run().await {
        Ok(output_path) => {
            tracing::info!("✅ ETL process completed successfully!");
            tracing::info!("📁 Output saved to: {}", output_path);
            println!("✅ ETL process completed successfully!");
            println!("📁 Output saved to: {}", output_path);
        }
        Err(e) => {
            tracing::error!("❌ ETL process failed: {}", e);
            eprintln!("❌ ETL process failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
