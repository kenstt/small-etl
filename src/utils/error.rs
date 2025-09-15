use thiserror::Error;

#[derive(Error, Debug)]
pub enum EtlError {
    #[error("Zip operation failed: {0}")]
    ZipError(#[from] zip::result::ZipError),

    #[error("API request failed: {0}")]
    ApiError(#[from] reqwest::Error),

    #[error("CSV processing error: {0}")]
    CsvError(#[from] csv::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Configuration error: {message}")]
    ConfigError { message: String },

    #[error("Data processing error: {message}")]
    ProcessingError { message: String },

    #[error("Validation error: {message}")]
    ValidationError { message: String },
}

pub type Result<T> = std::result::Result<T, EtlError>;
