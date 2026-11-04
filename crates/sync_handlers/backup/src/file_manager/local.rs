use async_trait::async_trait;
use std::collections::HashSet;
use std::path::Path;

use super::FsOps;

#[derive(Debug)]
pub struct LocalFsOps;

#[async_trait]
impl FsOps for LocalFsOps {
    async fn create_dir_all(&self, path: &str) -> anyhow::Result<()> {
        if tokio::fs::metadata(path).await.is_err() {
            tokio::fs::create_dir_all(path).await?;
        }
        Ok(())
    }

    async fn write(&self, file_path: &str, data: Vec<u8>) -> anyhow::Result<()> {
        let file_path_obj = Path::new(file_path);
        if let Some(dir) = file_path_obj.parent() {
            self.create_dir_all(
                dir.to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid directory encoding"))?,
            )
            .await?;
        }
        let tmp_path = format!("{file_path}-tmp");
        tokio::fs::write(&tmp_path, data).await?;
        tokio::fs::rename(&tmp_path, file_path).await?;
        Ok(())
    }

    async fn ls(
        &self,
        dir_path: &str,
        include_dir: bool,
        include_file: bool,
    ) -> anyhow::Result<Vec<String>> {
        let mut paths = HashSet::new();
        let mut dir_reader = tokio::fs::read_dir(dir_path).await?;
        while let Some(entry) = dir_reader.next_entry().await? {
            let entry_meta = entry.metadata().await?;
            if include_dir && entry_meta.is_dir() {
                paths.insert(entry.path().display().to_string());
            }
            if include_file && entry_meta.is_file() {
                paths.insert(entry.path().display().to_string());
            }
        }
        let mut paths = paths.into_iter().collect::<Vec<_>>();
        paths.sort();
        log::trace!("ls found the following files: {paths:?}");
        Ok(paths)
    }

    async fn read(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        Ok(tokio::fs::read(path).await?)
    }

    async fn remove(&self, path: &str) -> anyhow::Result<()> {
        if !tokio::fs::try_exists(path).await? {
            return Ok(());
        }
        let path_meta = tokio::fs::metadata(path).await?;
        if path_meta.is_file() {
            Ok(tokio::fs::remove_file(path).await?)
        } else {
            Ok(tokio::fs::remove_dir_all(path).await?)
        }
    }
}
