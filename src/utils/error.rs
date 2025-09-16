use thiserror::Error;

#[derive(Error, Debug)]
pub enum EtlError {
    // Infrastructure errors
    #[error("Zip operation failed: {0}")]
    ZipError(#[from] zip::result::ZipError),

    #[error("API request failed: {source}")]
    ApiError {
        #[from]
        source: reqwest::Error,
    },

    #[error("CSV processing error: {0}")]
    CsvError(#[from] csv::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    // Configuration errors
    #[error("Configuration validation failed: {field} - {message}")]
    ConfigValidationError { field: String, message: String },

    #[error("Missing required configuration: {field}")]
    MissingConfigError { field: String },

    #[error("Invalid configuration value: {field} = '{value}' - {reason}")]
    InvalidConfigValueError {
        field: String,
        value: String,
        reason: String,
    },

    // Legacy config error (keeping for backward compatibility)
    #[error("Configuration error: {message}")]
    ConfigError { message: String },

    // Data processing errors
    #[error("Data validation failed: {message}")]
    DataValidationError { message: String },

    #[error("Data processing error: {message}")]
    ProcessingError { message: String },

    #[error("Data transformation failed: {stage} - {details}")]
    TransformationError { stage: String, details: String },

    // Network and connectivity errors
    #[error("Network timeout: {operation} took longer than {timeout_seconds}s")]
    TimeoutError {
        operation: String,
        timeout_seconds: u64,
    },

    #[error("Rate limit exceeded: {api} - retry after {retry_after_seconds}s")]
    RateLimitError {
        api: String,
        retry_after_seconds: u64,
    },

    #[error("Authentication failed: {details}")]
    AuthenticationError { details: String },

    // Business logic errors
    #[error("Insufficient data: expected at least {expected} records, got {actual}")]
    InsufficientDataError { expected: usize, actual: usize },

    #[error("Data quality check failed: {check} - {message}")]
    DataQualityError { check: String, message: String },

    // System errors
    #[error("Resource exhausted: {resource} - {details}")]
    ResourceExhaustedError { resource: String, details: String },

    #[error("External service unavailable: {service}")]
    ServiceUnavailableError { service: String },

    // Legacy validation error (keeping for backward compatibility)
    #[error("Validation error: {message}")]
    ValidationError { message: String },

    // Pipeline execution errors
    #[error("Pipeline execution failed: {0}")]
    PipelineExecution(String),
}

pub type Result<T> = std::result::Result<T, EtlError>;

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorSeverity {
    Low,      // Warning level, process can continue
    Medium,   // Error level, process should retry
    High,     // Critical error, process should fail
    Critical, // System-level error, immediate attention required
}

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorCategory {
    Configuration,
    Network,
    DataProcessing,
    Infrastructure,
    Authentication,
    BusinessLogic,
    System,
}

impl EtlError {
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            // Low severity - warnings
            EtlError::DataQualityError { .. } => ErrorSeverity::Low,
            EtlError::InsufficientDataError { .. } => ErrorSeverity::Low,

            // Medium severity - retryable errors
            EtlError::ApiError { .. } => ErrorSeverity::Medium,
            EtlError::TimeoutError { .. } => ErrorSeverity::Medium,
            EtlError::RateLimitError { .. } => ErrorSeverity::Medium,
            EtlError::ServiceUnavailableError { .. } => ErrorSeverity::Medium,

            // High severity - process-level errors
            EtlError::ConfigValidationError { .. } => ErrorSeverity::High,
            EtlError::MissingConfigError { .. } => ErrorSeverity::High,
            EtlError::InvalidConfigValueError { .. } => ErrorSeverity::High,
            EtlError::AuthenticationError { .. } => ErrorSeverity::High,
            EtlError::DataValidationError { .. } => ErrorSeverity::High,
            EtlError::TransformationError { .. } => ErrorSeverity::High,

            // Critical severity - system errors
            EtlError::ResourceExhaustedError { .. } => ErrorSeverity::Critical,
            EtlError::IoError(_) => ErrorSeverity::Critical,
            EtlError::PipelineExecution(_) => ErrorSeverity::High,

            // Default mappings
            _ => ErrorSeverity::Medium,
        }
    }

    pub fn category(&self) -> ErrorCategory {
        match self {
            EtlError::ConfigValidationError { .. }
            | EtlError::MissingConfigError { .. }
            | EtlError::InvalidConfigValueError { .. }
            | EtlError::ConfigError { .. } => ErrorCategory::Configuration,

            EtlError::ApiError { .. }
            | EtlError::TimeoutError { .. }
            | EtlError::RateLimitError { .. }
            | EtlError::ServiceUnavailableError { .. } => ErrorCategory::Network,

            EtlError::DataValidationError { .. }
            | EtlError::ProcessingError { .. }
            | EtlError::TransformationError { .. }
            | EtlError::CsvError(_)
            | EtlError::SerializationError(_) => ErrorCategory::DataProcessing,

            EtlError::ZipError(_) | EtlError::IoError(_) => ErrorCategory::Infrastructure,

            EtlError::AuthenticationError { .. } => ErrorCategory::Authentication,

            EtlError::InsufficientDataError { .. } | EtlError::DataQualityError { .. } => {
                ErrorCategory::BusinessLogic
            }

            EtlError::ResourceExhaustedError { .. } => ErrorCategory::System,

            EtlError::ValidationError { .. } => ErrorCategory::DataProcessing,
            EtlError::PipelineExecution(_) => ErrorCategory::DataProcessing,
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            EtlError::ApiError { .. }
                | EtlError::TimeoutError { .. }
                | EtlError::RateLimitError { .. }
                | EtlError::ServiceUnavailableError { .. }
                | EtlError::ResourceExhaustedError { .. }
        )
    }

    pub fn recovery_suggestion(&self) -> &'static str {
        match self {
            EtlError::ConfigValidationError { .. } => "Check configuration values and restart",
            EtlError::MissingConfigError { .. } => "Set required configuration and restart",
            EtlError::InvalidConfigValueError { .. } => "Fix configuration value and restart",
            EtlError::AuthenticationError { .. } => "Check API credentials and permissions",
            EtlError::ApiError { .. } => "Check network connectivity and API service status",
            EtlError::TimeoutError { .. } => "Increase timeout values or check network latency",
            EtlError::RateLimitError { .. } => "Reduce request rate or implement backoff",
            EtlError::ServiceUnavailableError { .. } => "Wait for service to become available",
            EtlError::DataValidationError { .. } => "Check input data format and quality",
            EtlError::TransformationError { .. } => "Review data transformation logic",
            EtlError::ResourceExhaustedError { .. } => "Increase system resources or reduce load",
            EtlError::InsufficientDataError { .. } => "Check data source availability",
            EtlError::DataQualityError { .. } => "Review data quality rules and input data",
            EtlError::PipelineExecution(_) => "Check pipeline configuration and data dependencies",
            _ => "Check logs for detailed error information",
        }
    }

    pub fn user_friendly_message(&self) -> String {
        match self {
            EtlError::ConfigValidationError { field, .. } => {
                format!("配置參數 '{}' 驗證失敗", field)
            }
            EtlError::MissingConfigError { field } => {
                format!("缺少必要配置參數 '{}'", field)
            }
            EtlError::ApiError { .. } => "API請求失敗，請檢查網路連線".to_string(),
            EtlError::TimeoutError { operation, .. } => {
                format!("操作 '{}' 逾時", operation)
            }
            EtlError::DataValidationError { .. } => "數據驗證失敗，請檢查輸入數據格式".to_string(),
            EtlError::AuthenticationError { .. } => "認證失敗，請檢查API憑證".to_string(),
            EtlError::PipelineExecution(msg) => format!("Pipeline執行失敗: {}", msg),
            _ => "處理過程中發生錯誤".to_string(),
        }
    }
}
