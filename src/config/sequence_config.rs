use crate::core::ConfigProvider;
use crate::utils::error::{EtlError, Result};
use crate::utils::validation::Validate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceConfig {
    pub sequence: SequenceInfo,
    pub pipelines: Vec<PipelineDefinition>,
    pub global: Option<GlobalConfig>,
    pub monitoring: Option<MonitoringConfig>,
    pub error_handling: Option<ErrorHandlingConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceInfo {
    pub name: String,
    pub description: String,
    pub version: String,
    pub execution_order: Vec<String>, // Pipeline 執行順序
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDefinition {
    pub name: String,
    pub description: Option<String>,
    pub enabled: Option<bool>,
    pub source: SourceConfig,
    pub extract: ExtractConfig,
    pub transform: TransformConfig,
    pub load: LoadConfig,
    pub dependencies: Option<Vec<String>>, // 依賴的其他 Pipeline
    pub conditions: Option<ExecutionConditions>, // 執行條件
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub r#type: String,
    pub endpoint: String,
    pub method: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub retry_attempts: Option<u32>,
    pub retry_delay_seconds: Option<u64>,
    pub headers: Option<HashMap<String, String>>,
    pub parameters: Option<HashMap<String, String>>,
    pub data_source: Option<DataSource>, // 數據來源設定
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSource {
    pub use_previous_output: Option<bool>, // 使用前一個 Pipeline 的輸出
    pub from_pipeline: Option<String>,     // 指定來源 Pipeline
    pub merge_with_api: Option<bool>,      // 是否與 API 數據合併
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractConfig {
    pub max_records: Option<usize>,
    pub concurrent_requests: Option<usize>,
    pub field_mapping: Option<HashMap<String, String>>,
    pub filters: Option<HashMap<String, serde_json::Value>>,
    pub data_processing: Option<DataProcessing>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataProcessing {
    pub deduplicate: Option<bool>,
    pub deduplicate_fields: Option<Vec<String>>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>, // "asc" or "desc"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    pub operations: Option<TransformOperations>,
    pub validation: Option<ValidationConfig>,
    pub intermediate: Option<IntermediateConfig>,
    pub data_enrichment: Option<DataEnrichment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformOperations {
    pub clean_text: Option<bool>,
    pub trim_whitespace: Option<bool>,
    pub remove_html_tags: Option<bool>,
    pub normalize_fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    pub required_fields: Option<Vec<String>>,
    pub field_types: Option<HashMap<String, String>>,
    pub min_records: Option<usize>,
    pub max_records: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntermediateConfig {
    pub conditions: Option<HashMap<String, serde_json::Value>>,
    pub export_to_shared: Option<bool>, // 是否導出到共享數據
    pub shared_key: Option<String>,     // 共享數據的 key
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataEnrichment {
    pub lookup_data: Option<HashMap<String, String>>,
    pub computed_fields: Option<HashMap<String, String>>, // 計算字段
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadConfig {
    pub output_path: String,
    pub output_formats: Vec<String>,
    pub filename_pattern: Option<String>, // 例如: "{pipeline_name}_{timestamp}"
    pub compression: Option<CompressionConfig>,
    pub append_to_sequence: Option<bool>, // 是否追加到序列輸出
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub filename: String,
    pub include_metadata: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConditions {
    pub when_previous_succeeded: Option<bool>,
    pub when_records_count: Option<RecordCountCondition>,
    pub when_shared_data: Option<HashMap<String, serde_json::Value>>,
    pub skip_if_empty: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordCountCondition {
    pub min: Option<usize>,
    pub max: Option<usize>,
    pub from_pipeline: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub working_directory: Option<String>,
    pub shared_variables: Option<HashMap<String, String>>,
    pub timeout_minutes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub enabled: bool,
    pub log_level: Option<String>,
    pub export_metrics: Option<bool>,
    pub metrics_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorHandlingConfig {
    pub on_pipeline_failure: Option<String>, // "stop", "continue", "retry"
    pub retry_attempts: Option<u32>,
    pub retry_delay_seconds: Option<u64>,
    pub fallback_pipeline: Option<String>,
}

impl SequenceConfig {
    /// 從 TOML 檔案載入序列配置
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(&path).map_err(EtlError::IoError)?;
        Self::from_str(&content)
    }

    /// 從 TOML 字串解析序列配置
    pub fn from_str(content: &str) -> Result<Self> {
        // 處理環境變數替換
        let processed_content = Self::substitute_env_vars(content)?;

        toml::from_str(&processed_content).map_err(|e| EtlError::ConfigValidationError {
            field: "sequence_toml_parsing".to_string(),
            message: format!("Sequence TOML parsing error: {}", e),
        })
    }

    /// 替換環境變數
    fn substitute_env_vars(content: &str) -> Result<String> {
        use regex::Regex;
        let re = Regex::new(r"\$\{([^}]+)\}").unwrap();

        let result = re.replace_all(content, |caps: &regex::Captures| {
            let var_name = &caps[1];
            std::env::var(var_name).unwrap_or_else(|_| format!("${{{}}}", var_name))
        });

        Ok(result.to_string())
    }

    /// 驗證序列配置
    pub fn validate(&self) -> Result<()> {
        // 驗證執行順序中的 Pipeline 都存在
        let pipeline_names: std::collections::HashSet<String> =
            self.pipelines.iter().map(|p| p.name.clone()).collect();

        for pipeline_name in &self.sequence.execution_order {
            if !pipeline_names.contains(pipeline_name) {
                return Err(EtlError::ConfigValidationError {
                    field: "sequence.execution_order".to_string(),
                    message: format!("Pipeline '{}' in execution order not found in pipelines definition", pipeline_name),
                });
            }
        }

        // 驗證每個 Pipeline 的配置
        for pipeline in &self.pipelines {
            self.validate_pipeline(pipeline)?;
        }

        // 驗證依賴關係
        self.validate_dependencies()?;

        Ok(())
    }

    fn validate_pipeline(&self, pipeline: &PipelineDefinition) -> Result<()> {
        // 驗證 API 端點
        crate::utils::validation::validate_url("source.endpoint", &pipeline.source.endpoint)?;

        // 驗證輸出路徑
        crate::utils::validation::validate_path("load.output_path", &pipeline.load.output_path)?;

        // 驗證並發請求數
        if let Some(concurrent) = pipeline.extract.concurrent_requests {
            crate::utils::validation::validate_positive_number(
                "extract.concurrent_requests",
                concurrent,
                1
            )?;
        }

        // 驗證依賴的 Pipeline 存在
        if let Some(dependencies) = &pipeline.dependencies {
            let pipeline_names: std::collections::HashSet<String> =
                self.pipelines.iter().map(|p| p.name.clone()).collect();

            for dep in dependencies {
                if !pipeline_names.contains(dep) {
                    return Err(EtlError::ConfigValidationError {
                        field: format!("pipelines.{}.dependencies", pipeline.name),
                        message: format!("Dependency pipeline '{}' not found", dep),
                    });
                }
            }
        }

        Ok(())
    }

    fn validate_dependencies(&self) -> Result<()> {
        // 檢查循環依賴
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();

        for pipeline in &self.pipelines {
            if !visited.contains(&pipeline.name) {
                if self.has_circular_dependency(&pipeline.name, &mut visited, &mut rec_stack)? {
                    return Err(EtlError::ConfigValidationError {
                        field: "pipelines.dependencies".to_string(),
                        message: "Circular dependency detected in pipeline configuration".to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    fn has_circular_dependency(
        &self,
        pipeline_name: &str,
        visited: &mut std::collections::HashSet<String>,
        rec_stack: &mut std::collections::HashSet<String>,
    ) -> Result<bool> {
        visited.insert(pipeline_name.to_string());
        rec_stack.insert(pipeline_name.to_string());

        if let Some(pipeline) = self.pipelines.iter().find(|p| p.name == pipeline_name) {
            if let Some(dependencies) = &pipeline.dependencies {
                for dep in dependencies {
                    if !visited.contains(dep) {
                        if self.has_circular_dependency(dep, visited, rec_stack)? {
                            return Ok(true);
                        }
                    } else if rec_stack.contains(dep) {
                        return Ok(true);
                    }
                }
            }
        }

        rec_stack.remove(pipeline_name);
        Ok(false)
    }

    /// 獲取指定名稱的 Pipeline 定義
    pub fn get_pipeline(&self, name: &str) -> Option<&PipelineDefinition> {
        self.pipelines.iter().find(|p| p.name == name)
    }

    /// 獲取啟用的 Pipeline 列表（按執行順序）
    pub fn get_enabled_pipelines(&self) -> Vec<&PipelineDefinition> {
        self.sequence.execution_order
            .iter()
            .filter_map(|name| self.get_pipeline(name))
            .filter(|pipeline| pipeline.enabled.unwrap_or(true))
            .collect()
    }
}

impl ConfigProvider for PipelineDefinition {
    fn api_endpoint(&self) -> &str {
        &self.source.endpoint
    }

    fn output_path(&self) -> &str {
        &self.load.output_path
    }

    fn lookup_files(&self) -> &[String] {
        // 序列配置中暫不支援 lookup files
        &[]
    }

    fn concurrent_requests(&self) -> usize {
        self.extract.concurrent_requests.unwrap_or(5)
    }
}

impl Validate for SequenceConfig {
    fn validate(&self) -> Result<()> {
        self.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_config_parsing() {
        let toml_content = r#"
[sequence]
name = "test-sequence"
description = "Test sequence"
version = "1.0.0"
execution_order = ["pipeline1", "pipeline2"]

[[pipelines]]
name = "pipeline1"
enabled = true

[pipelines.source]
type = "api"
endpoint = "https://api1.example.com"

[pipelines.extract]
max_records = 100

[pipelines.transform]

[pipelines.load]
output_path = "./output1"
output_formats = ["csv"]

[[pipelines]]
name = "pipeline2"
enabled = true

[pipelines.source]
type = "api"
endpoint = "https://api2.example.com"

[pipelines.extract]
max_records = 50

[pipelines.transform]

[pipelines.load]
output_path = "./output2"
output_formats = ["json"]
"#;

        let config = SequenceConfig::from_str(toml_content).unwrap();
        assert_eq!(config.sequence.name, "test-sequence");
        assert_eq!(config.pipelines.len(), 2);
        assert_eq!(config.sequence.execution_order.len(), 2);
    }

    #[test]
    fn test_circular_dependency_detection() {
        let toml_content = r#"
[sequence]
name = "circular-test"
description = "Test circular dependency"
version = "1.0.0"
execution_order = ["pipeline1", "pipeline2"]

[[pipelines]]
name = "pipeline1"
dependencies = ["pipeline2"]

[pipelines.source]
type = "api"
endpoint = "https://api1.example.com"

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "./output1"
output_formats = ["csv"]

[[pipelines]]
name = "pipeline2"
dependencies = ["pipeline1"]

[pipelines.source]
type = "api"
endpoint = "https://api2.example.com"

[pipelines.extract]

[pipelines.transform]

[pipelines.load]
output_path = "./output2"
output_formats = ["csv"]
"#;

        let config = SequenceConfig::from_str(toml_content).unwrap();
        assert!(config.validate().is_err());
    }
}