use crate::core::Storage;
use crate::utils::error::Result;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct LocalStorage {
    base_path: String,
}

impl LocalStorage {
    pub fn new(base_path: String) -> Self {
        Self { base_path }
    }
}

impl Storage for LocalStorage {
    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let full_path = Path::new(&self.base_path).join(path);
        let data = fs::read(full_path)?;
        Ok(data)
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        let full_path = Path::new(&self.base_path).join(path);

        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(full_path, data)?;
        Ok(())
    }
}
