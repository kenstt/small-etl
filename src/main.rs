use clap::Parser;
use samll_etl::utils::{logger, validation::Validate};
use samll_etl::{CliConfig, EtlEngine, LocalStorage, SimplePipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = CliConfig::parse();

    // 初始化日誌
    logger::init_cli_logger(config.verbose);

    tracing::info!("Starting samll-etl CLI");
    if config.verbose {
        tracing::debug!("CLI config: {:?}", config);
    }

    // 驗證配置
    if let Err(e) = config.validate() {
        tracing::error!("❌ Configuration validation failed: {}", e);
        tracing::error!("💡 Suggestion: {}", e.recovery_suggestion());
        eprintln!("❌ {}", e.user_friendly_message());
        std::process::exit(1);
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
            // 記錄詳細錯誤信息
            tracing::error!(
                "❌ ETL process failed: {} (Category: {:?}, Severity: {:?})",
                e,
                e.category(),
                e.severity()
            );
            tracing::error!("💡 Recovery suggestion: {}", e.recovery_suggestion());

            // 輸出用戶友好的錯誤信息
            eprintln!("❌ {}", e.user_friendly_message());
            eprintln!("💡 建議: {}", e.recovery_suggestion());

            // 根據錯誤嚴重程度決定退出碼
            let exit_code = match e.severity() {
                samll_etl::utils::error::ErrorSeverity::Low => 0, // 警告，但成功
                samll_etl::utils::error::ErrorSeverity::Medium => 2, // 重試錯誤
                samll_etl::utils::error::ErrorSeverity::High => 1, // 處理錯誤
                samll_etl::utils::error::ErrorSeverity::Critical => 3, // 系統錯誤
            };

            if exit_code > 0 {
                std::process::exit(exit_code);
            }
        }
    }

    Ok(())
}
