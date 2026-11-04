use prost::Message;
use prost_types::FileDescriptorSet;

use crate::{v3, TONGRID_API_GRPC_DESCRIPTOR};

/// Reserved tags are a compatibility boundary for clients built before removal.
#[test]
fn test_descriptor_preserves_library_and_emulation_wire_contract(
) -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = FileDescriptorSet::decode(TONGRID_API_GRPC_DESCRIPTOR)?;
    assert!(descriptor
        .file
        .iter()
        .all(|file| file.package.as_deref() == Some("tongrid.v3")
            || file.package.as_deref() == Some("google.protobuf")));
    let messages = descriptor
        .file
        .iter()
        .flat_map(|file| &file.message_type)
        .collect::<Vec<_>>();
    let emulation = messages
        .iter()
        .find(|message| message.name.as_deref() == Some("EmulateGetMethodRequest"))
        .ok_or("missing emulation descriptor")?;
    assert!(emulation
        .reserved_name
        .iter()
        .any(|name| name == "immortal_libs"));
    assert!(emulation
        .reserved_range
        .iter()
        .any(|range| range.start == Some(30) && range.end == Some(31)));
    let library = descriptor
        .file
        .iter()
        .flat_map(|file| &file.service)
        .find(|service| service.name.as_deref() == Some("LibraryGrpc"))
        .ok_or("missing library service")?;
    let method = library
        .method
        .iter()
        .find(|method| method.name.as_deref() == Some("GetLibraries"))
        .ok_or("missing library method")?;
    assert_eq!(
        method.input_type.as_deref(),
        Some(".tongrid.v3.GetLibrariesRequest")
    );
    assert_eq!(
        method.output_type.as_deref(),
        Some(".tongrid.v3.GetLibrariesResponse")
    );
    Ok(())
}

/// Fixed bytes protect the existing LT and hash field numbers across code generation.
#[test]
fn test_transaction_identity_decodes_existing_wire_bytes() -> Result<(), Box<dyn std::error::Error>>
{
    let tx = v3::Tx::decode(&[0x3a, 0x02, 0xaa, 0xbb, 0x40, 0x96, 0x01][..])?;
    assert_eq!(tx.tx_hash, [0xaa, 0xbb]);
    assert_eq!(tx.tx_lt, 150);
    assert_eq!(
        tx.encode_to_vec(),
        [0x3a, 0x02, 0xaa, 0xbb, 0x40, 0x96, 0x01]
    );
    Ok(())
}
