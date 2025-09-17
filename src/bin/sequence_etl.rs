use clap::Parser;
use samll_etl::config::sequence_config::SequenceConfig;
use samll_etl::core::{
    contextual_pipeline::SequenceAwarePipeline, pipeline_sequence::PipelineSequence,
};
use samll_etl::utils::logger;
use samll_etl::LocalStorage;
use std::collections::HashMap;

#[derive(Parser)]
#[command(name = "sequence-etl")]
#[command(about = "ETL tool with pipeline sequence support")]
struct Args {
    /// Path to sequence configuration file
    #[arg(short, long, default_value = "configs/sequence-example.toml")]
    config: String,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Override monitoring setting from config
    #[arg(long)]
    monitor: Option<bool>,

    /// Dry run - show execution plan without executing
    #[arg(long)]
    dry_run: bool,

    /// Execution ID for this run
    #[arg(long)]
    execution_id: Option<String>,

    /// Execute only specific pipelines (comma-separated)
    #[arg(long)]
    only: Option<String>,

    /// Skip specific pipelines (comma-separated)
    #[arg(long)]
    skip: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // 初始化日誌
    logger::init_cli_logger(args.verbose);

    tracing::info!("🚀 Starting Pipeline Sequence ETL tool");
    tracing::info!("📁 Loading sequence configuration from: {}", args.config);

    // 載入序列配置
    let config = match SequenceConfig::from_file(&args.config) {
        Ok(config) => config,
        Err(e) => {
            eprintln!(
                "❌ Failed to load sequence config file '{}': {}",
                args.config, e
            );
            eprintln!("💡 Make sure the file exists and is valid TOML format");
            std::process::exit(1);
        }
    };

    // 驗證配置
    if let Err(e) = config.validate() {
        tracing::error!("❌ Sequence configuration validation failed: {}", e);
        eprintln!("❌ {}", e);
        std::process::exit(1);
    }

    tracing::info!("✅ Sequence configuration loaded and validated successfully");

    // 生成執行 ID
    let execution_id = args
        .execution_id
        .clone()
        .unwrap_or_else(|| format!("seq_{}", chrono::Utc::now().format("%Y%m%d_%H%M%S")));

    // 顯示序列摘要
    display_sequence_summary(&config, &args, &execution_id);

    if args.dry_run {
        tracing::info!("🔍 DRY RUN MODE - No actual processing will occur");
        perform_dry_run(&config, &args).await?;
        return Ok(());
    }

    // 決定監控設定
    let monitor_enabled = args.monitor.unwrap_or_else(|| {
        config
            .monitoring
            .as_ref()
            .map(|m| m.enabled)
            .unwrap_or(false)
    });

    // 創建序列執行器
    let mut sequence = PipelineSequence::new(execution_id.clone()).with_monitoring(monitor_enabled);

    // 獲取要執行的 Pipeline 列表
    let pipelines_to_execute = determine_pipelines_to_execute(&config, &args);

    // 為每個要執行的 Pipeline 創建 ContextualPipeline
    for pipeline_def in pipelines_to_execute {
        tracing::info!("📦 Setting up pipeline: {}", pipeline_def.name);

        // 創建存儲（每個 Pipeline 使用獨立的存儲）
        let storage = LocalStorage::new(pipeline_def.load.output_path.clone());

        // 創建 SequenceAwarePipeline
        let contextual_pipeline =
            SequenceAwarePipeline::new(pipeline_def.name.clone(), storage, pipeline_def.clone());

        sequence.add_pipeline(Box::new(contextual_pipeline));
    }

    // 執行序列
    tracing::info!("🎬 Starting pipeline sequence execution");
    match sequence.execute_all().await {
        Ok(results) => {
            tracing::info!("🎉 Pipeline sequence completed successfully!");

            // 顯示執行結果摘要
            display_execution_results(&results, &execution_id);

            // 匯出執行摘要
            if let Some(monitoring) = &config.monitoring {
                if monitoring.export_metrics.unwrap_or(false) {
                    export_execution_metrics(&results, &execution_id, monitoring).await?;
                }
            }

            println!("✅ Pipeline sequence completed successfully!");
            println!("🆔 Execution ID: {}", execution_id);
            println!("📊 Pipelines executed: {}", results.len());
        }
        Err(e) => {
            tracing::error!("❌ Pipeline sequence failed: {}", e);
            eprintln!("❌ Pipeline sequence failed: {}", e);

            // 根據錯誤處理配置決定處理方式
            if let Some(error_config) = &config.error_handling {
                match error_config.on_pipeline_failure.as_deref() {
                    Some("continue") => {
                        tracing::info!("⚠️ Continuing despite failure (configured behavior)");
                        return Ok(());
                    }
                    Some("retry") => {
                        tracing::info!("🔄 Retry logic would be implemented here");
                        // 這裡可以實作重試邏輯
                    }
                    _ => {
                        // 預設是停止
                        std::process::exit(1);
                    }
                }
            }

            std::process::exit(1);
        }
    }

    Ok(())
}

fn display_sequence_summary(config: &SequenceConfig, args: &Args, execution_id: &str) {
    println!("📋 Pipeline Sequence Summary:");
    println!(
        "  Name: {} v{}",
        config.sequence.name, config.sequence.version
    );
    println!("  Description: {}", config.sequence.description);
    println!("  Execution ID: {}", execution_id);
    println!("  Total Pipelines: {}", config.pipelines.len());

    if args.dry_run {
        println!("  🔍 DRY RUN MODE ENABLED");
    }

    if let Some(only) = &args.only {
        println!("  🎯 Only executing: {}", only);
    }

    if let Some(skip) = &args.skip {
        println!("  ⏭️ Skipping: {}", skip);
    }

    println!();
    println!("📝 Execution Order:");
    for (index, pipeline_name) in config.sequence.execution_order.iter().enumerate() {
        if let Some(pipeline) = config.get_pipeline(pipeline_name) {
            let status = if pipeline.enabled.unwrap_or(true) {
                "✅"
            } else {
                "⏸️"
            };
            println!(
                "  {}. {} {} - {}",
                index + 1,
                status,
                pipeline_name,
                pipeline.description.as_deref().unwrap_or("No description")
            );

            if let Some(deps) = &pipeline.dependencies {
                println!("     Dependencies: {}", deps.join(", "));
            }
        }
    }
    println!();
}

fn determine_pipelines_to_execute<'a>(
    config: &'a SequenceConfig,
    args: &'a Args,
) -> Vec<&'a samll_etl::config::sequence_config::PipelineDefinition> {
    let mut pipelines = config.get_enabled_pipelines();

    // 處理 --only 參數
    if let Some(only_list) = &args.only {
        let only_names: std::collections::HashSet<&str> =
            only_list.split(',').map(|s| s.trim()).collect();
        pipelines.retain(|p| only_names.contains(p.name.as_str()));
    }

    // 處理 --skip 參數
    if let Some(skip_list) = &args.skip {
        let skip_names: std::collections::HashSet<&str> =
            skip_list.split(',').map(|s| s.trim()).collect();
        pipelines.retain(|p| !skip_names.contains(p.name.as_str()));
    }

    pipelines
}

async fn perform_dry_run(
    config: &SequenceConfig,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Dry Run Analysis:");
    println!();

    let pipelines_to_execute = determine_pipelines_to_execute(config, args);

    for (index, pipeline) in pipelines_to_execute.iter().enumerate() {
        println!("📦 Pipeline {}: {}", index + 1, pipeline.name);
        println!(
            "  📡 Source: {}",
            pipeline
                .source
                .endpoint
                .as_deref()
                .unwrap_or("Previous pipeline output")
        );

        if let Some(data_source) = &pipeline.source.data_source {
            if data_source.use_previous_output.unwrap_or(false) {
                println!("  📂 Uses previous pipeline output");
                if let Some(from_pipeline) = &data_source.from_pipeline {
                    println!("  📂 Specifically from: {}", from_pipeline);
                }
                if data_source.merge_with_api.unwrap_or(false) {
                    println!("  🔄 Merges with API data");
                }
            }
        }

        if let Some(max_records) = pipeline.extract.max_records {
            println!("  📊 Max records: {}", max_records);
        }

        println!("  💾 Output: {}", pipeline.load.output_path);
        println!("  📄 Formats: {}", pipeline.load.output_formats.join(", "));

        if let Some(conditions) = &pipeline.conditions {
            println!("  ⚙️ Execution conditions:");
            if let Some(when_prev) = conditions.when_previous_succeeded {
                println!("    - Requires previous success: {}", when_prev);
            }
            if conditions.skip_if_empty.unwrap_or(false) {
                println!("    - Skip if no data");
            }
        }

        if let Some(deps) = &pipeline.dependencies {
            println!("  🔗 Dependencies: {}", deps.join(", "));
        }

        println!();
    }

    println!("📊 Summary:");
    println!(
        "  Total pipelines to execute: {}",
        pipelines_to_execute.len()
    );
    println!("  Estimated total time: Variable (depends on data size and API response time)");
    println!();
    println!("✅ Dry run analysis complete.");

    Ok(())
}

fn display_execution_results(
    results: &[samll_etl::core::pipeline_sequence::PipelineResult],
    execution_id: &str,
) {
    println!();
    println!("📊 Execution Results Summary:");
    println!("  Execution ID: {}", execution_id);
    println!("  Completed Pipelines: {}", results.len());

    let total_records: usize = results.iter().map(|r| r.records.len()).sum();
    let total_duration: std::time::Duration = results.iter().map(|r| r.duration).sum();

    println!("  Total Records Processed: {}", total_records);
    println!("  Total Execution Time: {:?}", total_duration);
    println!();

    println!("📝 Pipeline Details:");
    for (index, result) in results.iter().enumerate() {
        println!(
            "  {}. {} - {} records in {:?}",
            index + 1,
            result.pipeline_name,
            result.records.len(),
            result.duration
        );
        println!("     Output: {}", result.output_path);
    }
    println!();
}

async fn export_execution_metrics(
    results: &[samll_etl::core::pipeline_sequence::PipelineResult],
    execution_id: &str,
    monitoring_config: &samll_etl::config::sequence_config::MonitoringConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics_file = monitoring_config
        .metrics_file
        .as_deref()
        .unwrap_or("sequence_metrics.json");

    let mut metrics = HashMap::new();
    metrics.insert(
        "execution_id",
        serde_json::Value::String(execution_id.to_string()),
    );
    metrics.insert(
        "timestamp",
        serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
    );

    let summary = PipelineSequence::get_execution_summary(results);
    metrics.insert(
        "summary",
        serde_json::Value::Object(summary.into_iter().collect()),
    );

    let pipeline_metrics: Vec<serde_json::Value> = results
        .iter()
        .map(|result| {
            let mut pipeline_data = HashMap::new();
            pipeline_data.insert(
                "name".to_string(),
                serde_json::Value::String(result.pipeline_name.clone()),
            );
            pipeline_data.insert(
                "records_count".to_string(),
                serde_json::Value::Number(result.records.len().into()),
            );
            pipeline_data.insert(
                "duration_ms".to_string(),
                serde_json::Value::Number((result.duration.as_millis() as u64).into()),
            );
            pipeline_data.insert(
                "output_path".to_string(),
                serde_json::Value::String(result.output_path.clone()),
            );

            for (key, value) in &result.metadata {
                pipeline_data.insert(key.clone(), value.clone());
            }

            serde_json::Value::Object(pipeline_data.into_iter().collect())
        })
        .collect();

    metrics.insert("pipelines", serde_json::Value::Array(pipeline_metrics));

    let metrics_json = serde_json::to_string_pretty(&metrics)?;
    tokio::fs::write(metrics_file, metrics_json).await?;

    tracing::info!("📊 Execution metrics exported to: {}", metrics_file);
    println!("📊 Metrics exported to: {}", metrics_file);

    Ok(())
}
