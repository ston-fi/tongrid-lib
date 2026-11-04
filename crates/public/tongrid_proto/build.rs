use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .message_attribute(".", "#[derive(::derive_setters::Setters)]")
        .message_attribute(".", "#[setters(prefix = \"with_\", strip_option)]")
        .file_descriptor_set_path(out_dir.join("tongrid_descriptor.bin"))
        .compile_protos(
            &[
                "proto/tongrid/v3/services/block.proto",
                "proto/tongrid/v3/services/jetton.proto",
                "proto/tongrid/v3/services/library.proto",
                "proto/tongrid/v3/services/send_msg.proto",
                "proto/tongrid/v3/services/sync_status.proto",
                "proto/tongrid/v3/services/emulation.proto",
                "proto/tongrid/v3/services/tx.proto",
                "proto/tongrid/v3/filter.proto",
                "proto/tongrid/v3/pagination.proto",
                "proto/tongrid/v3/response_params.proto",
                "proto/tongrid/v3/types.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
