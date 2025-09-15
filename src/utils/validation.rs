use crate::utils::error::{EtlError, Result};
use std::collections::HashSet;
use url::Url;

pub trait Validate {
    fn validate(&self) -> Result<()>;
}

pub fn validate_url(field_name: &str, url_str: &str) -> Result<()> {
    if url_str.is_empty() {
        return Err(EtlError::InvalidConfigValueError {
            field: field_name.to_string(),
            value: url_str.to_string(),
            reason: "URL cannot be empty".to_string(),
        });
    }

    match Url::parse(url_str) {
        Ok(url) => {
            match url.scheme() {
                "http" | "https" => Ok(()),
                scheme => Err(EtlError::InvalidConfigValueError {
                    field: field_name.to_string(),
                    value: url_str.to_string(),
                    reason: format!("Unsupported URL scheme: {}", scheme),
                }),
            }
        }
        Err(e) => Err(EtlError::InvalidConfigValueError {
            field: field_name.to_string(),
            value: url_str.to_string(),
            reason: format!("Invalid URL format: {}", e),
        }),
    }
}

pub fn validate_path(field_name: &str, path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(EtlError::InvalidConfigValueError {
            field: field_name.to_string(),
            value: path.to_string(),
            reason: "Path cannot be empty".to_string(),
        });
    }

    if path.contains('\0') {
        return Err(EtlError::InvalidConfigValueError {
            field: field_name.to_string(),
            value: path.to_string(),
            reason: "Path contains null bytes".to_string(),
        });
    }

    Ok(())
}

pub fn validate_positive_number(field_name: &str, value: usize, min_value: usize) -> Result<()> {
    if value < min_value {
        return Err(EtlError::InvalidConfigValueError {
            field: field_name.to_string(),
            value: value.to_string(),
            reason: format!("Value must be at least {}", min_value),
        });
    }
    Ok(())
}

pub fn validate_file_extensions(field_name: &str, files: &[String], allowed_extensions: &[&str]) -> Result<()> {
    let allowed_set: HashSet<&str> = allowed_extensions.iter().copied().collect();

    for file in files {
        if let Some(extension) = std::path::Path::new(file)
            .extension()
            .and_then(|ext| ext.to_str())
        {
            if !allowed_set.contains(extension) {
                return Err(EtlError::InvalidConfigValueError {
                    field: field_name.to_string(),
                    value: file.clone(),
                    reason: format!(
                        "Unsupported file extension: {}. Allowed extensions: {}",
                        extension,
                        allowed_extensions.join(", ")
                    ),
                });
            }
        } else {
            return Err(EtlError::InvalidConfigValueError {
                field: field_name.to_string(),
                value: file.clone(),
                reason: "File has no extension or invalid filename".to_string(),
            });
        }
    }

    Ok(())
}

pub fn validate_required_field<'a, T>(field_name: &str, value: &'a Option<T>) -> Result<&'a T> {
    value.as_ref().ok_or_else(|| EtlError::MissingConfigError {
        field: field_name.to_string(),
    })
}

pub fn validate_non_empty_string(field_name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(EtlError::InvalidConfigValueError {
            field: field_name.to_string(),
            value: value.to_string(),
            reason: "Value cannot be empty or whitespace-only".to_string(),
        });
    }
    Ok(())
}

pub fn validate_range<T: PartialOrd + std::fmt::Display + Copy>(
    field_name: &str,
    value: T,
    min: T,
    max: T,
) -> Result<()> {
    if value < min || value > max {
        return Err(EtlError::InvalidConfigValueError {
            field: field_name.to_string(),
            value: value.to_string(),
            reason: format!("Value must be between {} and {}", min, max),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_url() {
        assert!(validate_url("api_endpoint", "https://example.com").is_ok());
        assert!(validate_url("api_endpoint", "http://example.com").is_ok());
        assert!(validate_url("api_endpoint", "").is_err());
        assert!(validate_url("api_endpoint", "invalid-url").is_err());
        assert!(validate_url("api_endpoint", "ftp://example.com").is_err());
    }

    #[test]
    fn test_validate_positive_number() {
        assert!(validate_positive_number("concurrent_requests", 5, 1).is_ok());
        assert!(validate_positive_number("concurrent_requests", 0, 1).is_err());
    }

    #[test]
    fn test_validate_file_extensions() {
        let files = vec!["data.csv".to_string(), "lookup.tsv".to_string()];
        assert!(validate_file_extensions("lookup_files", &files, &["csv", "tsv"]).is_ok());

        let invalid_files = vec!["data.txt".to_string()];
        assert!(validate_file_extensions("lookup_files", &invalid_files, &["csv", "tsv"]).is_err());
    }
}