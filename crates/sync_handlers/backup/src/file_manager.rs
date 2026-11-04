use anyhow::ensure;
use async_trait::async_trait;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fmt::Debug;
use std::io::Read;
use std::io::Write;
use std::ops::Deref;
use std::sync::Arc;
use tokio::task::spawn_blocking;
pub mod local;
pub mod s3;

#[async_trait]
pub trait FsOps: Debug + Send + Sync {
    async fn create_dir_all(&self, path: &str) -> anyhow::Result<()>;
    async fn write_compressed(&self, file_path: &str, plain_data: Vec<u8>) -> anyhow::Result<()> {
        let compressed = spawn_blocking(move || compress_sync(plain_data)).await??;
        log::info!(
            "[FILE_MANAGER] writing compressed {} bytes to path: {file_path}",
            compressed.len()
        );
        self.write_verified(file_path, compressed).await
    }
    /// Publish already encoded bytes and verify the stored contents before reporting success.
    async fn write_verified(&self, file_path: &str, data: Vec<u8>) -> anyhow::Result<()> {
        self.write(file_path, data.clone()).await?;
        let written = self.read(file_path).await?;
        ensure!(
            written == data,
            "File verification failed after write: {file_path} (expected {} bytes, got {} bytes)",
            data.len(),
            written.len()
        );
        Ok(())
    }
    async fn write(&self, file_path: &str, data: Vec<u8>) -> anyhow::Result<()>;
    async fn ls(
        &self,
        dir_path: &str,
        include_dir: bool,
        include_file: bool,
    ) -> anyhow::Result<Vec<String>>;
    async fn read_and_decompress(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let compressed = self.read(path).await?;
        spawn_blocking(move || decompress_sync(compressed)).await?
    }
    async fn read(&self, path: &str) -> anyhow::Result<Vec<u8>>;
    async fn remove(&self, path: &str) -> anyhow::Result<()>;
}

pub(crate) fn compress_sync(plain_data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let mut compressed = vec![];
    {
        let mut gz = GzEncoder::new(&mut compressed, Compression::default());
        gz.write_all(&plain_data)?;
    }
    Ok(compressed)
}

pub(crate) fn decompress_sync(compressed: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let mut decompressed = vec![];
    let mut decoder = GzDecoder::new(compressed.as_slice());
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

#[derive(Clone, Debug)]
pub struct FileManager(Arc<dyn FsOps>);

impl Deref for FileManager {
    type Target = dyn FsOps;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl FileManager {
    pub fn new(ops: Arc<dyn FsOps>) -> Self {
        Self(ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[derive(Debug)]
    struct MissingAfterWriteFsOps;

    #[async_trait]
    impl FsOps for MissingAfterWriteFsOps {
        async fn create_dir_all(&self, _path: &str) -> anyhow::Result<()> {
            Ok(())
        }

        async fn write(&self, _file_path: &str, _data: Vec<u8>) -> anyhow::Result<()> {
            Ok(())
        }

        async fn ls(
            &self,
            _dir_path: &str,
            _include_dir: bool,
            _include_file: bool,
        ) -> anyhow::Result<Vec<String>> {
            Ok(vec![])
        }

        async fn read(&self, _path: &str) -> anyhow::Result<Vec<u8>> {
            Err(anyhow!("file is missing"))
        }

        async fn remove(&self, _path: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_write_compressed_fails_when_written_file_is_not_readable() {
        let err = MissingAfterWriteFsOps
            .write_compressed("/backup/test.datgz", b"payload".to_vec())
            .await
            .unwrap_err();

        assert!(err.to_string().contains("file is missing"));
    }
}

pub mod config;
