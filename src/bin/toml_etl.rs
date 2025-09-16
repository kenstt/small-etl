use clap::Parser;
use samll_etl::config::toml_config::TomlConfig;
use samll_etl::core::mvp_pipeline::MvpPipeline;
use samll_etl::utils::{logger, validation::Validate};
use samll_etl::EtlEngine;
use samll_etl::LocalStorage;

#[derive(Parser)]
#[command(name = "toml-etl")]
#[command(about = "ETL tool with TOML configuration support")]
struct Args {
    /// Path to TOML configuration file
    #[arg(short, long, default_value = "etl-config.toml")]
    config: String,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Override monitoring setting from config
    #[arg(long)]
    monitor: Option<bool>,

    /// Override MVP mode setting from config
    #[arg(long)]
    mvp: Option<bool>,

    /// Dry run - show what would be processed without executing
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // 初始化日誌
    logger::init_cli_logger(args.verbose);

    tracing::info!("🚀 Starting TOML-based ETL tool ");
    tracing::info!("📁 Loading configuration from: {}", args.config);

    // 載入 TOML 配置
    let mut config = match TomlConfig::from_file(&args.config)    {
        Ok(config) => config,
        Err(e) => {
            eprintln!("❌ Failed to load config file '{}': {}", args.config, e);
            eprintln!("💡 Make sure the file exists and is valid TOML format");
            std::process::exit(1);
        }
    };


    // 應用命令列覆蓋設定
    if let Some(mvp) = args.mvp {
        config.extract.first_record_only = Some(mvp);
        tracing::info!("🔧 MVP mode overridden to: {}", mvp);
    }

    // 驗證配置
    if let Err(e) = config.validate() {
        tracing::error!("❌ Configuration validation failed: {}", e);
        tracing::error!("💡 Suggestion: {}", e.recovery_suggestion());
        eprintln!("❌ {}", e.user_friendly_message());
        std::process::exit(1);
    }

    tracing::info!("✅ Configuration loaded and validated successfully");

    // 顯示配置摘要
    display_config_summary(&config, &args);

    if args.dry_run {
        tracing::info!("🔍 DRY RUN MODE - No actual processing will occur");
        perform_dry_run(&config).await?;
        return Ok(());
    }

    // 決定監控設定
    let monitor_enabled = args.monitor.unwrap_or_else(|| config.monitoring_enabled());

    if monitor_enabled {
        tracing::info!("🔍 System monitoring enabled");
    }

    // 創建存儲和 MVP 管道
    let storage = LocalStorage::new(config.output_path().to_string());
    let pipeline = MvpPipeline::new(storage, config);

    // 創建 ETL 引擎並運行
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

fn display_config_summary(config: &TomlConfig, args: &Args) {
    println!("📋 Configuration Summary:");
    println!(
        "  Pipeline: {} v{}",
        config.pipeline.name, config.pipeline.version
    );
    println!("  Source: {}", config.source.endpoint);
    println!("  Output: {}", config.output_path());
    println!("  MVP Mode: {}", config.is_mvp_mode());
    println!("  Formats: {}", config.load.output_formats.join(", "));

    if let Some(max_records) = config.max_records() {
        println!("  Max Records: {}", max_records);
    }

    println!("  Concurrent Requests: {}", config.concurrent_requests());

    if args.dry_run {
        println!("  🔍 DRY RUN MODE ENABLED");
    }

    println!();
}

async fn perform_dry_run(config: &TomlConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Dry Run Analysis:");
    println!();

    // API 端點分析
    println!("📡 Data Source Analysis:");
    println!("  Endpoint: {}", config.source.endpoint);
    println!(
        "  Method: {}",
        config.source.method.as_deref().unwrap_or("GET")
    );

    if let Some(headers) = &config.source.headers {
        println!("  Headers: {} custom headers", headers.len());
    }

    if let Some(params) = &config.source.parameters {
        println!("  Parameters: {} query parameters", params.len());
    }

    // 處理模式分析
    println!();
    println!("⚙️ Processing Mode:");
    if config.is_mvp_mode() {
        println!("  🎯 MVP Mode: Will process ONLY the first record");
        println!("  📊 Expected records: 1");
    } else {
        println!("  📊 Normal Mode: Will process all available records");
        if let Some(max) = config.max_records() {
            println!("  📊 Max records limit: {}", max);
        }
    }

    // 輸出分析
    println!();
    println!("💾 Output Configuration:");
    println!("  Path: {}", config.output_path());
    println!("  Formats: {}", config.load.output_formats.join(", "));

    if let Some(compression) = &config.load.compression {
        if compression.enabled {
            println!("  Compression: {} (ZIP)", compression.filename);
        }
    }

    // 字段映射分析
    if let Some(mapping) = &config.extract.field_mapping {
        println!();
        println!("🔄 Field Mapping:");
        for (from, to) in mapping {
            println!("  {} -> {}", from, to);
        }
    }

    // 轉換操作分析
    if let Some(ops) = &config.transform.operations {
        println!();
        println!("🛠️ Transform Operations:");
        if ops.clean_text.unwrap_or(false) {
            println!("  ✅ Text cleaning enabled");
        }
        if ops.trim_whitespace.unwrap_or(false) {
            println!("  ✅ Whitespace trimming enabled");
        }
        if ops.remove_html_tags.unwrap_or(false) {
            println!("  ✅ HTML tag removal enabled");
        }
    }

    println!();
    println!("✅ Dry run analysis complete. Use --verbose for more details during actual run.");

    Ok(())
}
