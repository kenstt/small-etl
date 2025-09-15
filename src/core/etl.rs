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
        println!("Starting ETL process...");

        // Extract
        println!("Extracting data...");
        let raw_data = self.pipeline.extract().await?;
        println!("Extracted {} records", raw_data.len());

        // Transform
        println!("Transforming data...");
        let transformed_result = self.pipeline.transform(raw_data).await?;
        println!(
            "Transformed {} records",
            transformed_result.processed_records.len()
        );

        // Load
        println!("Loading data...");
        let output_path = self.pipeline.load(transformed_result).await?;
        println!("Output saved to: {}", output_path);

        Ok(output_path)
    }
}
