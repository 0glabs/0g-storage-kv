use crate::LogStorageConfig;
use std::path::PathBuf;

#[derive(Clone)]
pub struct Config {
    pub log_config: LogStorageConfig,
    pub kv_db_file: PathBuf,
}
