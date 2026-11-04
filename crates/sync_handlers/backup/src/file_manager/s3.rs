use anyhow::bail;
use async_trait::async_trait;
use futures::stream;
use futures::StreamExt;
use s3::creds::Credentials;
use s3::{Bucket, Region};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use super::FsOps;

pub struct S3FsOps {
    client: Box<Bucket>,
    bucket_name: String,
    prefetched: Arc<Mutex<Option<PrefetchedFile>>>,
}

impl std::fmt::Debug for S3FsOps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The dependency's bucket contains credentials; do not format it.
        f.debug_struct("S3FsOps")
            .field("bucket_name", &self.bucket_name)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct PrefetchedFile {
    path: String,
    handle: JoinHandle<anyhow::Result<Vec<u8>>>,
}

impl Drop for PrefetchedFile {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl S3FsOps {
    pub async fn new(
        endpoint: &str,
        bucket_name: &str,
        access_key: &str,
        secret_key: &str,
    ) -> anyhow::Result<Self> {
        let bucket_name = bucket_name.to_string();
        let client = init_s3_client(endpoint, access_key, secret_key, &bucket_name).await?;
        Ok(Self {
            client,
            bucket_name,
            prefetched: Arc::new(Mutex::new(None)),
        })
    }

    fn parent_dir(path: &str) -> Option<String> {
        Path::new(path).parent().map(|p| p.display().to_string())
    }

    async fn prefetch_next(&self, path: &str) {
        let Some(dir) = Self::parent_dir(path) else {
            return;
        };
        let Ok(siblings) = self.ls(&dir, false, true).await else {
            return;
        };
        let pos = siblings.iter().position(|p| p == path);
        let Some(idx) = pos else { return };

        let Some(sib) = siblings.get(idx + 1) else {
            return;
        };
        let key = adjust_path_to_s3(sib);
        let mut guard = self.prefetched.lock().await;
        if guard
            .as_ref()
            .map(|prefetched| prefetched.path == *sib)
            .unwrap_or(false)
        {
            return;
        }
        let cli = self.client.clone();
        let handle = tokio::spawn(async move {
            let obj = cli.get_object(&key).await?;
            Ok::<Vec<u8>, anyhow::Error>(obj.bytes().to_vec())
        });
        *guard = Some(PrefetchedFile {
            path: sib.to_string(),
            handle,
        });
    }

    async fn wait_for_prefetch(&self, path: &str) -> Option<Vec<u8>> {
        let mut prefetched = {
            let mut guard = self.prefetched.lock().await;
            if guard
                .as_ref()
                .map(|prefetched| prefetched.path == path)
                .unwrap_or(false)
            {
                guard.take()?
            } else {
                return None;
            }
        };

        match (&mut prefetched.handle).await {
            Ok(Ok(bytes)) => Some(bytes),
            Ok(Err(err)) => {
                log::warn!("[S3 prefetch] failed to download {path}: {err}");
                None
            }
            Err(join_err) => {
                log::warn!("[S3 prefetch] join error for {path}: {join_err}");
                None
            }
        }
    }

    async fn discard_prefetch(&self, path: &str) {
        let mut guard = self.prefetched.lock().await;
        if guard
            .as_ref()
            .map(|prefetched| prefetched.path == path)
            .unwrap_or(false)
        {
            *guard = None;
        }
    }
}

#[async_trait]
impl FsOps for S3FsOps {
    async fn create_dir_all(&self, _path: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write(&self, file_path: &str, data: Vec<u8>) -> anyhow::Result<()> {
        log::trace!("s3: writing file: {file_path}");
        self.discard_prefetch(file_path).await;
        let file_path = adjust_path_to_s3(file_path);
        let cli = &self.client;
        log::trace!(
            "writing to s3: bucket={}, path={}, data_len: {}",
            self.bucket_name,
            file_path,
            data.len()
        );
        let c_type = "application/octet-stream";
        let upload_init = cli.initiate_multipart_upload(&file_path, c_type).await?;
        let mut parts = vec![];
        let chunk_size = 10 * 1024 * 1024; // 10mb
        for (part_num, chunk) in (1..).zip(data.chunks(chunk_size)) {
            let part = cli
                .put_multipart_chunk(
                    chunk.to_vec(),
                    &file_path,
                    part_num,
                    &upload_init.upload_id,
                    c_type,
                )
                .await?;
            parts.push(part);
            if part_num % 10 == 0 {
                log::trace!(
                    "[FILE_MANAGER] uploaded {}/{} parts",
                    part_num,
                    data.len() / chunk_size
                );
            }
        }
        cli.complete_multipart_upload(&file_path, &upload_init.upload_id, parts)
            .await?;
        log::trace!("s3: writing file: {file_path} completed");
        Ok(())
    }

    async fn ls(
        &self,
        dir_path: &str,
        include_dir: bool,
        include_file: bool,
    ) -> anyhow::Result<Vec<String>> {
        if !include_dir && !include_file {
            return Ok(vec![]);
        }
        let s3_prefix = {
            let mut prefix = adjust_path_to_s3(dir_path);
            if !prefix.ends_with('/') {
                prefix.push('/');
            }
            prefix
        };
        let response = self
            .client
            .list(s3_prefix.clone(), Some("/".to_string()))
            .await?;
        let mut paths = HashSet::new();
        for list_result in response {
            if include_dir {
                if let Some(common_prefixes) = list_result.common_prefixes {
                    for cp in common_prefixes {
                        let p = cp.prefix;
                        if let Some(suffix) = p.strip_prefix(&s3_prefix) {
                            let first = suffix.trim_end_matches('/');
                            if !first.is_empty() {
                                let entry = Path::new(dir_path).join(first);
                                paths.insert(entry.display().to_string());
                            }
                        }
                    }
                }
            }
            if include_file {
                for obj in list_result.contents {
                    if let Some(suffix) = obj.key.strip_prefix(&s3_prefix) {
                        if !suffix.contains('/') && !suffix.is_empty() {
                            let entry = Path::new(dir_path).join(suffix);
                            paths.insert(entry.display().to_string());
                        }
                    }
                }
            }
        }
        let mut paths = paths.into_iter().collect::<Vec<_>>();
        paths.sort();
        log::trace!("ls found the following entries: {paths:?}");
        Ok(paths)
    }

    async fn read(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        log::trace!("s3: reading file: {path}");
        let key = adjust_path_to_s3(path);

        if let Some(data) = self.wait_for_prefetch(path).await {
            self.prefetch_next(path).await;
            log::trace!("s3: reading {path} from prefetched data");
            return Ok(data);
        }

        let response = self.client.get_object(&key).await?;
        let bytes = response.bytes().to_vec();
        self.prefetch_next(path).await;
        log::trace!("s3: reading file: {path} completed");
        Ok(bytes)
    }

    async fn remove(&self, path: &str) -> anyhow::Result<()> {
        let key = adjust_path_to_s3(path);
        let mut keys: Vec<String> = Vec::new();
        keys.push(key.clone());

        let mut prefix = key.clone();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        let pages = self.client.list(prefix.clone(), None).await?; // all objects under prefix
        for page in pages {
            for obj in page.contents {
                keys.push(obj.key);
            }
        }
        let client = &self.client;
        let mut stream = stream::iter(
            keys.into_iter()
                .map(|k| async move { client.delete_object(&k).await }),
        )
        .buffer_unordered(32);

        while let Some(res) = stream.next().await {
            res?;
        }

        Ok(())
    }
}

async fn init_s3_client(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    bucket_name: &str,
) -> anyhow::Result<Box<Bucket>> {
    let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None)?;
    let region = Region::Custom {
        region: "eu-central-1".to_owned(),
        endpoint: endpoint.to_string(),
    };
    let client = Bucket::new(bucket_name, region, credentials)?
        .with_path_style()
        .with_request_timeout(Duration::from_secs(60 * 60))?; // 1 hour timeout

    match client.exists().await {
        Ok(true) => log::info!("S3 bucket `{bucket_name}` found"),
        Ok(false) => bail!("S3 bucket `{bucket_name}` does not exist"),
        // Dependency errors can retain the configured endpoint, including credentials.
        Err(_) => bail!("Failed to check S3 bucket existence"),
    }
    Ok(client)
}

fn adjust_path_to_s3(path: &str) -> String {
    let s3_path = if let Some(stripped) = path.strip_prefix("./") {
        stripped
    } else {
        path
    };
    s3_path
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string()
}

#[cfg(test)]
#[path = "../tests/test_s3.rs"]
mod tests;
