use crate::BLOCKS_IN_SUBFOLDER;
use anyhow::bail;
use tongrid_db::entity::StateID;

const INNER_DIR_DIGITS: usize = 4;

pub fn make_state_backup_dir(all_backup_dir: &str, state_id: StateID) -> String {
    format!("{all_backup_dir}/{state_id}")
}

pub fn make_backup_path_from_parts(
    backup_dir: &str,
    start_seqno: u32,
    end_seqno: u32,
) -> anyhow::Result<String> {
    let subfolder = start_seqno as usize / BLOCKS_IN_SUBFOLDER;
    if subfolder != end_seqno as usize / BLOCKS_IN_SUBFOLDER {
        bail!("Start and end seqno must be in the same subfolder");
    }
    let seqno_digits = usize::ilog10(BLOCKS_IN_SUBFOLDER) as usize + INNER_DIR_DIGITS;
    Ok(format!(
        "{}/{}/{:0>seqno_digits$}_{:0>seqno_digits$}.datgz",
        backup_dir,
        format_inner_dir(subfolder),
        start_seqno,
        end_seqno
    ))
}

pub(crate) fn format_inner_dir(subfolder: usize) -> String {
    format!("{subfolder:0>INNER_DIR_DIGITS$}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::assert_err;

    #[test]
    fn test_from_parts() -> anyhow::Result<()> {
        assert_err!(make_backup_path_from_parts(
            "/backup_data",
            1_000_000,
            2_000_100
        ));

        let path = make_backup_path_from_parts("/backup_data", 0, 15)?;
        assert_eq!(path, "/backup_data/0000/0000000000_0000000015.datgz");

        let path = make_backup_path_from_parts("/backup_data", 1_000_000, 1_000_100)?;
        assert_eq!(path, "/backup_data/0001/0001000000_0001000100.datgz");

        let path = make_backup_path_from_parts("/backup_data", 1_000_000, 1_200_000)?;
        assert_eq!(path, "/backup_data/0001/0001000000_0001200000.datgz");

        let path = make_backup_path_from_parts("/backup_data", 3_000_000, 3_200_000)?;
        assert_eq!(path, "/backup_data/0003/0003000000_0003200000.datgz");

        let path = make_backup_path_from_parts("/backup_data", 23_000_000, 23_200_000)?;
        assert_eq!(path, "/backup_data/0023/0023000000_0023200000.datgz");
        assert_err!(make_backup_path_from_parts(
            "/backup_data",
            23_000_000,
            24_200_000
        ));

        Ok(())
    }
}
