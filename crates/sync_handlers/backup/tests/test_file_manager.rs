use std::fs::create_dir_all;
use std::sync::Arc;
use tongrid_backup_handler::file_manager::{local::LocalFsOps, s3::S3FsOps, FileManager, FsOps};

async fn build_file_manager(use_s3: bool) -> anyhow::Result<FileManager> {
    let ops: Arc<dyn FsOps> = if use_s3 {
        Arc::new(
            S3FsOps::new(
                "http://127.0.0.1:19000",
                "tongrid-test",
                "tongrid-local-test",
                "tongrid-local-test-password",
            )
            .await?,
        )
    } else {
        Arc::new(LocalFsOps)
    };
    Ok(FileManager::new(ops))
}

// Requires the disposable S3 emulator from docker-compose.yml.
#[ignore = "requires explicitly provisioned disposable loopback S3"]
#[tokio::test]
async fn test_file_manager_create_read_delete() -> anyhow::Result<()> {
    for use_s3 in [false, true] {
        let file_manager = build_file_manager(use_s3).await?;
        let file_path = "/tmp/test1_file_manager_create_read_delete/file1.txt";
        file_manager.remove(file_path).await?;
        let file_data = b"test".to_vec();
        file_manager.write(file_path, file_data.clone()).await?;
        let read_data = file_manager.read(file_path).await?;
        assert_eq!(file_data, read_data);
        file_manager.remove(file_path).await?;
        assert!(file_manager.read(file_path).await.is_err());

        // remove by directory
        let dir_path = "/tmp/test2_file_manager_create_read_delete";
        let file_path = format!("{dir_path}/file1.txt");
        file_manager.remove(&file_path).await?;
        let file_data = b"test".to_vec();
        file_manager.write(&file_path, file_data.clone()).await?;
        let read_data = file_manager.read(&file_path).await?;
        assert_eq!(file_data, read_data);
        file_manager.remove(dir_path).await?;
        assert!(file_manager.read(&file_path).await.is_err());
    }
    Ok(())
}

#[ignore = "requires explicitly provisioned disposable loopback S3"]
#[tokio::test]
async fn test_file_manager_compressing() -> anyhow::Result<()> {
    for use_s3 in [false, true] {
        let file_manager = build_file_manager(use_s3).await?;
        let file_path = "/tmp/test1_file_manager_create_read_delete/file1.txt.gz";
        let _ = file_manager.remove(file_path).await; // cleanup after previous run

        let file_data = b"test".to_vec();
        file_manager
            .write_compressed(file_path, file_data.clone())
            .await?;
        let read_data = file_manager.read_and_decompress(file_path).await?;
        assert_eq!(file_data, read_data);
        file_manager.remove(file_path).await?;
        assert!(file_manager.read(file_path).await.is_err());
    }
    Ok(())
}

#[ignore = "requires explicitly provisioned disposable loopback S3"]
#[tokio::test]
async fn test_file_manager_ls() -> anyhow::Result<()> {
    for use_s3 in [false, true] {
        let file_manager = build_file_manager(use_s3).await?;
        let dir_path = "/tmp/test_file_manager_ls";
        for path in ["file1.txt", "file2.txt"] {
            let full_path = format!("{dir_path}/{path}");
            file_manager.remove(&full_path).await?;
            file_manager.write(&full_path, vec![0]).await?;
        }

        let files = file_manager.ls(dir_path, true, true).await?;
        assert_eq!(files.len(), 2);

        let dir_path = "/tmp/one_more_dir/test_file_manager_ls";
        for path in ["file1.txt", "file2.txt"] {
            let full_path = format!("{dir_path}/{path}");
            file_manager.remove(&full_path).await?;
            file_manager.write(&full_path, vec![1]).await?;
        }

        let files = file_manager.ls("/tmp/one_more_dir/", true, false).await?;
        log::info!("files: {files:?}");
        assert_eq!(files.len(), 1);
    }
    Ok(())
}

#[ignore = "requires explicitly provisioned disposable loopback S3"]
#[tokio::test]
async fn test_s3_fair_binary() -> anyhow::Result<()> {
    let file_manager = build_file_manager(true).await?;

    let file_path = "/tmp/test_s3_fair_binary";
    file_manager.remove(file_path).await?;

    let file_data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    file_manager.write(file_path, file_data.clone()).await?;
    let read_data = file_manager.read(file_path).await?;
    assert_eq!(file_data, read_data);
    file_manager.remove(file_path).await?;
    Ok(())
}

#[ignore = "requires explicitly provisioned disposable loopback S3"]
#[tokio::test]
async fn test_s3_bad_prefix() -> anyhow::Result<()> {
    let file_manager = build_file_manager(true).await?;

    for (counter, file_path) in (1..).zip([
        "/tmp/test_s3_bad_prefix",
        "./tmp/test_s3_bad_prefix",
        ".//tmp/test_s3_bad_prefix",
        ".///tmp/test_s3_bad_prefix",
    ]) {
        file_manager.remove(file_path).await?;
        let file_data = vec![counter];
        file_manager.write(file_path, file_data.clone()).await?;
        let read_data = file_manager.read(file_path).await?;
        assert_eq!(file_data, read_data);
        file_manager.remove(file_path).await?;
    }
    Ok(())
}

#[ignore = "requires explicitly provisioned disposable loopback S3"]
#[tokio::test]
async fn test_ls_many_dirs() -> anyhow::Result<()> {
    let test_dir = "/tmp/test_ls_many_dirs";
    create_dir_all(test_dir)?;
    for use_s3 in [false, true] {
        let file_manager = build_file_manager(use_s3).await?;
        file_manager.remove(test_dir).await?;
        // s3 by default return 1000 files. So we need to create more than 1000 files
        // let's create 200 folders with 10 files in each
        let dir_cnt = 200;
        let files_cnt = 10;
        let paths: Vec<String> = (0..dir_cnt)
            .flat_map(|dir| {
                let dir_path = format!("{test_dir}/dir{dir}");
                (0..files_cnt).map(move |file| format!("{dir_path}/file{file}"))
            })
            .collect();

        // Create the full 2,000-object listing fixture without stressing the
        // local emulator with concurrent multipart uploads.
        for path in paths {
            file_manager.write(&path, vec![0]).await?;
        }

        let dirs = file_manager.ls(test_dir, true, false).await?;
        assert_eq!(dirs.len(), dir_cnt);
    }

    Ok(())
}
