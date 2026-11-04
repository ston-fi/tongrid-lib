//! Component settings; application composition and private presets remain caller-owned.
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct FileManagerConfig {
    pub s3_enabled: bool,
    pub s3_endpoint: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
}

impl Debug for FileManagerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileManagerConfig")
            .field("s3_enabled", &self.s3_enabled)
            .field("s3_endpoint", &"[redacted]")
            .field("s3_bucket", &self.s3_bucket)
            .field("s3_access_key", &"[redacted]")
            .field("s3_secret_key", &"[redacted]")
            .finish()
    }
}
