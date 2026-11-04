use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("tongrid_backup_descriptor.bin"))
        .compile_protos(
            &[
                "proto/backup/v1/account.proto",
                "proto/backup/v1/backup_unit.proto",
                "proto/backup/v1/block.proto",
                "proto/backup/v1/simple_lib.proto",
                "proto/backup/v1/tvm_code.proto",
                "proto/backup/v1/tvm_code_traits.proto",
                "proto/backup/v1/tx.proto",
                "proto/backup/v1/jetton.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
