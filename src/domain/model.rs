use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub data: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct TransformResult {
    pub processed_records: Vec<Record>,
    pub csv_output: String,
    pub tsv_output: String,
    pub intermediate_data: Vec<Record>,
}
