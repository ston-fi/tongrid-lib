use crate::file_manager::FileManager;
use crate::util::format_inner_dir;
use crate::BLOCKS_IN_SUBFOLDER;
use anyhow::bail;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct BackupIterator {
    file_manager: FileManager,
    backup_inner_dirs: Vec<String>,
    cur_dir_position: usize,
    cur_files: Vec<String>,
    cur_file_position: usize,
    cur_last_seqno: u32,
    from_seqno: u32,
    to_seqno: u32,
    pub data_first_seqno: u32,
    pub data_last_seqno: u32,
}

/// Expect backup_tree in the following format:
/// backup_dir/{NUMBERS}/{START_SEQNO1}_{END_SEQNO1}.datgz
impl BackupIterator {
    pub async fn new(
        backup_dir: &str,
        file_manager: &FileManager,
        from_seqno: Option<u32>,
        to_seqno: Option<u32>,
    ) -> anyhow::Result<Self> {
        let backup_inner_dirs = file_manager.ls(backup_dir, true, false).await?;
        if backup_inner_dirs.is_empty() {
            bail!("No backup dirs found");
        }

        // detect first and last seqno in backup files
        let first_file = {
            let first_inner_dir = backup_inner_dirs
                .first()
                .ok_or_else(|| anyhow::anyhow!("No backup dirs found"))?;
            let files = ls_backup_files(file_manager, first_inner_dir).await?;
            if files.is_empty() {
                bail!("No backup files found in {}", first_inner_dir);
            }
            PathBuf::from(&files[0])
        };

        let last_file = {
            let last_inner_dir = backup_inner_dirs
                .last()
                .ok_or_else(|| anyhow::anyhow!("No backup dirs found"))?;
            let files = ls_backup_files(file_manager, last_inner_dir).await?;
            if files.is_empty() {
                bail!("No backup files found in {}", last_inner_dir);
            }
            PathBuf::from(
                &files
                    .last()
                    .ok_or_else(|| anyhow::anyhow!("No backup files found"))?,
            )
        };

        let (data_first_seqno, _) = parse_backup_file_seqnos(&first_file)?;
        let (_, data_last_seqno) = parse_backup_file_seqnos(&last_file)?;

        let from_seqno = from_seqno.unwrap_or(data_first_seqno);
        let to_seqno = to_seqno.unwrap_or(data_last_seqno);

        if from_seqno > to_seqno {
            bail!("Invalid seqno range: {} > {}", from_seqno, to_seqno);
        }
        if from_seqno < data_first_seqno || to_seqno > data_last_seqno {
            bail!(
                "Invalid seqno range: [{}, {}] (data has only [{}, {}])",
                from_seqno,
                to_seqno,
                data_first_seqno,
                data_last_seqno
            );
        }

        let cur_dir_position = {
            let cur_inner_dir = format_inner_dir(from_seqno as usize / BLOCKS_IN_SUBFOLDER);
            backup_inner_dirs
                .iter()
                .enumerate()
                .find(|(_, dir)| dir.split('/').next_back() == Some(cur_inner_dir.as_str()))
                .map(|(pos, _)| pos)
        };

        let cur_dir_position = match cur_dir_position {
            Some(pos) => pos,
            None => bail!("No backup dirs found for seqno: {}", from_seqno),
        };

        let cur_dir = backup_inner_dirs[cur_dir_position].clone();
        let cur_files = ls_backup_files(file_manager, &cur_dir).await?;
        log::info!("BackupIterator for backup_dir={backup_dir} ready");
        Ok(BackupIterator {
            file_manager: file_manager.clone(),
            backup_inner_dirs,
            cur_dir_position,
            cur_files,
            cur_file_position: 0,
            cur_last_seqno: from_seqno,
            from_seqno,
            to_seqno,
            data_first_seqno,
            data_last_seqno,
        })
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<String>> {
        if !self.cur_positions_are_valid().await? {
            return Ok(None);
        }

        loop {
            let file = &self.cur_files[self.cur_file_position];
            self.cur_file_position += 1;
            let (file_start_seqno, file_end_seqno) =
                parse_backup_file_seqnos(&PathBuf::from(file))?;
            if file_start_seqno > self.to_seqno {
                return Ok(None);
            }
            if file_end_seqno < self.from_seqno {
                continue;
            }
            if file_start_seqno != self.cur_last_seqno {
                log::warn!(
                    "[BackupIterator] Seqnos skipped in backup files before file {file}, previous seqno: {}",
                    self.cur_last_seqno
                );
            }
            self.cur_last_seqno = file_end_seqno + 1;
            return Ok(Some(file.clone()));
        }
    }

    // move to the next dir if all files in the current dir were checked
    // return false if no more backup folders to iterate
    async fn cur_positions_are_valid(&mut self) -> anyhow::Result<bool> {
        if self.cur_file_position >= self.cur_files.len() {
            self.cur_dir_position += 1;
            if self.cur_dir_position >= self.backup_inner_dirs.len() {
                return Ok(false);
            }
            let new_dir = &self.backup_inner_dirs[self.cur_dir_position];
            self.cur_files = ls_backup_files(&self.file_manager, new_dir).await?;
            self.cur_file_position = 0;
        }
        Ok(true)
    }
}

fn parse_backup_file_seqnos(path: &Path) -> anyhow::Result<(u32, u32)> {
    let file_name_no_ext = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| anyhow::anyhow!("Invalid backup filename"))?;
    let mut seqnos = vec![];
    for seqno_str in file_name_no_ext.split('_') {
        seqnos.push(seqno_str.parse::<u32>()?)
    }
    if seqnos.len() != 2 {
        bail!("Invalid file name in file: {}", path.display());
    }
    Ok((seqnos[0], seqnos[1]))
}

async fn ls_backup_files(
    file_manager: &FileManager,
    backup_dir: &str,
) -> anyhow::Result<Vec<String>> {
    let paths = file_manager
        .ls(backup_dir, false, true)
        .await?
        .into_iter()
        .filter(|f| f.ends_with(".datgz"))
        .collect::<Vec<_>>();
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_manager::local::LocalFsOps;
    use crate::util::make_backup_path_from_parts;
    use tempfile::TempDir;
    use tokio::fs::File;
    use tokio_test::{assert_err, assert_ok};

    #[tokio::test]
    async fn test_parse_backup_files_seqnos() -> anyhow::Result<()> {
        let (from, to) = assert_ok!(parse_backup_file_seqnos(&PathBuf::from(
            "003055_3055123.datgz"
        )));
        assert_eq!(from, 3055);
        assert_eq!(to, 3055123);

        assert_err!(parse_backup_file_seqnos(&PathBuf::from("_3055123.datgz")));
        assert_err!(parse_backup_file_seqnos(&PathBuf::from("3055_.datgz")));
        assert_err!(parse_backup_file_seqnos(&PathBuf::from(
            "3055_3055123_222.datgz"
        )));
        Ok(())
    }

    #[tokio::test]
    async fn test_iterate_works() -> anyhow::Result<()> {
        let backup_dir = TempDir::new()?;
        let backup_dir_str = backup_dir.path().to_str().unwrap().to_string();
        let file_manager = FileManager::new(std::sync::Arc::new(LocalFsOps));

        // create 5 dir, each contains 5 files for seqnos from 1*10^6 to 2*10^6
        let mut backup_files = vec![];
        for mln in 1..=5 {
            let up_folder = backup_dir.path().join(format_inner_dir(mln));
            tokio::fs::create_dir(&up_folder).await?;

            for thousand in 0..=4 {
                let start_seqno = mln as u32 * 1_000_000 + thousand * 100_000;
                let end_seqno = start_seqno + 99_999;
                let file = make_backup_path_from_parts(&backup_dir_str, start_seqno, end_seqno)?;
                File::create(&file).await?;
                backup_files.push(file);
            }
            // one extra file, will be ignored
            let file = up_folder.join("0000_0001.meta");
            File::create(&file).await?;
        }

        // no backup folder
        assert_err!(BackupIterator::new("unknown_folder", &file_manager, None, None).await);

        // check from_seqno works correctly
        for from_seqno in [2, 3, 5] {
            let mut iterator = assert_ok!(
                BackupIterator::new(
                    backup_dir_str.as_str(),
                    &file_manager,
                    Some(from_seqno * 1_000_000),
                    None
                )
                .await
            );
            assert_eq!(iterator.data_first_seqno, 1_000_000);
            assert_eq!(iterator.data_last_seqno, 5_499_999);
            let mut given = vec![];
            while let Some(path) = assert_ok!(iterator.next().await) {
                given.push(path);
            }
            let source_start_pos = (from_seqno as usize - 1) * 5;
            assert_eq!(backup_files[source_start_pos..], given);
        }

        // incorrect input
        assert_err!(
            BackupIterator::new(
                backup_dir_str.as_str(),
                &file_manager,
                None,
                Some(500_000) // too low
            )
            .await
        );
        assert_err!(
            BackupIterator::new(
                backup_dir_str.as_str(),
                &file_manager,
                None,
                Some(6_000_000) // too high
            )
            .await
        );
        assert_err!(
            BackupIterator::new(backup_dir_str.as_str(), &file_manager, Some(200), Some(100)).await
        );

        // check to_seqno works correctly
        for to_seqno in [2, 4, 5] {
            let mut iterator = assert_ok!(
                BackupIterator::new(
                    backup_dir_str.as_str(),
                    &file_manager,
                    None,
                    Some(to_seqno * 1_000_000)
                )
                .await
            );
            assert_eq!(iterator.data_first_seqno, 1_000_000);
            assert_eq!(iterator.data_last_seqno, 5_499_999);
            let mut given = vec![];
            while let Some(path) = iterator.next().await? {
                given.push(path);
            }
            let source_end_pos = (to_seqno - 1) as usize * 5 + 1;
            assert_eq!(backup_files[0..source_end_pos], given);
        }
        Ok(())
    }
}
