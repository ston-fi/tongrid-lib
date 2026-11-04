use anyhow::bail;
use ton::ton_core::cell::{CellType, TonCell, TonHash};

pub fn make_data_cell(data: &[u8]) -> anyhow::Result<TonCell> {
    if data.len() > 127 {
        bail!("Data is too long: {}", data.len());
    }
    let mut builder = TonCell::builder();
    builder.write_bits(data, data.len() * 8)?;
    Ok(builder.build()?)
}

pub fn make_lib_cell(lib_id: &TonHash) -> anyhow::Result<TonCell> {
    let mut builder = TonCell::builder_extra(CellType::LibraryRef, 1024);
    builder.write_num(&2, 2)?;
    builder.write_bits(lib_id.as_slice(), 256)?;
    Ok(builder.build()?)
}

/// Copies up to 32 fixture bytes into a hash, padding with trailing zeros.
///
/// # Panics
/// Panics if `data` is longer than 32 bytes.
pub fn make_db_ton_hash(data: &[u8]) -> TonHash {
    assert!(
        data.len() <= 32,
        "Hash fixture data is too long: {} > 32",
        data.len()
    );
    let mut hash = [0u8; 32];
    hash[..data.len()].copy_from_slice(data);
    TonHash::from(&hash)
}
