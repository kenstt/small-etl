use crate::core::Pipeline;
use crate::utils::error::Result;

pub struct EtlEngine<P: Pipeline> {
    pipeline: P,
}

impl<P: Pipeline> EtlEngine<P> {
    pub fn new(pipeline: P) -> Self {
        Self { pipeline }
    }

    pub async fn run(&self) -> Result<String> {
        tracing::info!("Starting ETL process");

        // Extract
        tracing::info!("Phase 1: Extracting data");
        let start_time = std::time::Instant::now();
        let raw_data = self.pipeline.extract().await?;
        let extract_duration = start_time.elapsed();
        tracing::info!(
            "✅ Extracted {} records in {:?}",
            raw_data.len(),
            extract_duration
        );

        // Transform
        tracing::info!("Phase 2: Transforming data");
        let start_time = std::time::Instant::now();
        let transformed_result = self.pipeline.transform(raw_data).await?;
        let transform_duration = start_time.elapsed();
        tracing::info!(
            "✅ Transformed {} records, {} intermediate records in {:?}",
            transformed_result.processed_records.len(),
            transformed_result.intermediate_data.len(),
            transform_duration
        );

        // Load
        tracing::info!("Phase 3: Loading data");
        let start_time = std::time::Instant::now();
        let output_path = self.pipeline.load(transformed_result).await?;
        let load_duration = start_time.elapsed();
        tracing::info!("✅ Data loaded to: {} in {:?}", output_path, load_duration);

        tracing::info!("🎉 ETL process completed successfully");
        Ok(output_path)
    }
}
