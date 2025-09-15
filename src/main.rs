use clap::Parser;
use samll_etl::{CliConfig, EtlEngine, LocalStorage, SimplePipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = CliConfig::parse();

    if config.verbose {
        println!("Starting ETL with config: {:?}", config);
    }

    // 創建存儲和管道
    let storage = LocalStorage::new(config.output_path.clone());
    let pipeline = SimplePipeline::new(storage, config);

    // 創建ETL引擎並運行
    let engine = EtlEngine::new(pipeline);

    match engine.run().await {
        Ok(output_path) => {
            println!("✅ ETL process completed successfully!");
            println!("📁 Output saved to: {}", output_path);
        }
        Err(e) => {
            eprintln!("❌ ETL process failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
