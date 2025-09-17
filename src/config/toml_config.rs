use crate::core::ConfigProvider;
use crate::utils::error::{EtlError, Result};
use crate::utils::validation::Validate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TomlConfig {
    pub pipeline: PipelineConfig,
    pub source: SourceConfig,
    pub extract: ExtractConfig,
    pub transform: TransformConfig,
    pub load: LoadConfig,
    pub monitoring: Option<MonitoringConfig>,
    pub error_handling: Option<ErrorHandlingConfig>,
    pub performance: Option<PerformanceConfig>,
    pub environment: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub name: String,
    pub description: String,
    pub version: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractConfig {
    pub first_record_only: Option<bool>,
    pub concurrent_requests: Option<usize>,
    pub max_records: Option<usize>,
    pub field_mapping: Option<HashMap<String, String>>,
    pub filters: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    pub operations: Option<TransformOperations>,
    pub validation: Option<ValidationConfig>,
    pub intermediate: Option<IntermediateConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformOperations {
    pub clean_text: Option<bool>,
    pub trim_whitespace: Option<bool>,
    pub remove_html_tags: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    pub required_fields: Option<Vec<String>>,
    pub max_title_length: Option<usize>,
    pub max_content_length: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntermediateConfig {
    pub title_length_threshold: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadConfig {
    pub output_path: String,
    pub output_formats: Vec<String>,
    pub compression: Option<CompressionConfig>,
    pub filenames: Option<FilenameConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub filename: String,
    pub include_intermediate: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilenameConfig {
    pub csv: Option<String>,
    pub tsv: Option<String>,
    pub json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub enabled: bool,
    pub log_level: Option<String>,
    pub system_stats: Option<bool>,
    pub performance_metrics: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorHandlingConfig {
    pub on_api_failure: Option<String>,
    pub on_transform_error: Option<String>,
    pub on_load_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub request_timeout: Option<u64>,
    pub memory_limit_mb: Option<usize>,
    pub disk_cache_enabled: Option<bool>,
}

impl TomlConfig {
    /// 從 TOML 檔案載入配置
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(&path).map_err(EtlError::IoError)?;
        Self::from_toml_str(&content)
    }

    /// 從 TOML 字串解析配置
    pub fn from_toml_str(content: &str) -> Result<Self> {
        // 處理環境變數替換
        let processed_content = Self::substitute_env_vars(content)?;

        toml::from_str(&processed_content).map_err(|e| EtlError::ConfigValidationError {
            field: "toml_parsing".to_string(),
            message: format!("TOML parsing error: {}", e),
        })
    }

    /// 替換環境變數 (例如 ${API_KEY})
    fn substitute_env_vars(content: &str) -> Result<String> {
        use regex::Regex;
        // 使用正規表達式匹配 ${VAR_NAME} 格式
        let re = Regex::new(r"\$\{([^}]+)\}").unwrap();

        let result = re.replace_all(content, |caps: &regex::Captures| {
            let var_name = &caps[1];
            std::env::var(var_name).unwrap_or_else(|_| format!("${{{}}}", var_name))
        });

        Ok(result.to_string())
    }

    /// 驗證配置的合理性
    pub fn validate_config(&self) -> Result<()> {
        // 驗證 API 端點
        crate::utils::validation::validate_url("source.endpoint", &self.source.endpoint)?;

        // 驗證輸出路徑
        crate::utils::validation::validate_path("load.output_path", &self.load.output_path)?;

        // 驗證並發請求數
        if let Some(concurrent) = self.extract.concurrent_requests {
            crate::utils::validation::validate_positive_number(
                "extract.concurrent_requests",
                concurrent,
                1,
            )?;
        }

        // 驗證輸出格式
        let valid_formats = ["csv", "tsv", "json"];
        for format in &self.load.output_formats {
            if !valid_formats.contains(&format.as_str()) {
                return Err(EtlError::InvalidConfigValueError {
                    field: "load.output_formats".to_string(),
                    value: format.clone(),
                    reason: format!(
                        "Unsupported format. Valid formats: {}",
                        valid_formats.join(", ")
                    ),
                });
            }
        }

        Ok(())
    }

    /// 取得 API 端點
    pub fn api_endpoint(&self) -> &str {
        &self.source.endpoint
    }

    /// 取得輸出路徑
    pub fn output_path(&self) -> &str {
        &self.load.output_path
    }

    /// 取得並發請求數
    pub fn concurrent_requests(&self) -> usize {
        self.extract.concurrent_requests.unwrap_or(5)
    }

    /// 是否啟用 MVP 模式 (只處理第一筆記錄)
    pub fn is_mvp_mode(&self) -> bool {
        self.extract.first_record_only.unwrap_or(false)
    }

    /// 取得最大記錄數
    pub fn max_records(&self) -> Option<usize> {
        self.extract.max_records
    }

    /// 取得監控設定
    pub fn monitoring_enabled(&self) -> bool {
        self.monitoring.as_ref().map(|m| m.enabled).unwrap_or(false)
    }
}

impl ConfigProvider for TomlConfig {
    fn api_endpoint(&self) -> &str {
        &self.source.endpoint
    }

    fn output_path(&self) -> &str {
        &self.load.output_path
    }

    fn lookup_files(&self) -> &[String] {
        // TOML 配置目前不支援 lookup files，返回空陣列
        &[]
    }

    fn concurrent_requests(&self) -> usize {
        self.concurrent_requests()
    }
}

impl Validate for TomlConfig {
    fn validate(&self) -> Result<()> {
        self.validate_config()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_basic_toml_config() {
        let toml_content = r#"
[pipeline]
name = "test-pipeline"
description = "Test pipeline"
version = "1.0.0"

[source]
type = "api"
endpoint = "https://api.example.com/data"

[extract]
first_record_only = true
concurrent_requests = 1

[transform]

[load]
output_path = "./test-output"
output_formats = ["csv", "json"]
"#;

        let config = TomlConfig::from_toml_str(toml_content).unwrap();

        assert_eq!(config.pipeline.name, "test-pipeline");
        assert_eq!(config.source.endpoint, "https://api.example.com/data");
        assert!(config.is_mvp_mode());
        assert_eq!(config.concurrent_requests(), 1);
    }

    #[test]
    fn test_env_var_substitution() {
        std::env::set_var("TEST_API_ENDPOINT", "https://test.api.com");

        let toml_content = r#"
[pipeline]
name = "test"
description = "test"
version = "1.0"

[source]
type = "api"
endpoint = "${TEST_API_ENDPOINT}"

[extract]

[transform]

[load]
output_path = "./output"
output_formats = ["csv"]
"#;

        let config = TomlConfig::from_toml_str(toml_content).unwrap();
        assert_eq!(config.source.endpoint, "https://test.api.com");

        std::env::remove_var("TEST_API_ENDPOINT");
    }

    #[test]
    fn test_config_validation() {
        let toml_content = r#"
[pipeline]
name = "test"
description = "test"
version = "1.0"

[source]
type = "api"
endpoint = "invalid-url"

[extract]

[transform]

[load]
output_path = "./output"
output_formats = ["csv"]
"#;

        let config = TomlConfig::from_toml_str(toml_content).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_from_file() {
        let mut temp_file = NamedTempFile::new().unwrap();

        let toml_content = r#"
[pipeline]
name = "file-test"
description = "File test"
version = "1.0"

[source]
type = "api"
endpoint = "https://api.example.com"

[extract]

[transform]

[load]
output_path = "./output"
output_formats = ["csv"]
"#;

        temp_file.write_all(toml_content.as_bytes()).unwrap();

        let config = TomlConfig::from_file(temp_file.path()).unwrap();
        assert_eq!(config.pipeline.name, "file-test");
    }
}
