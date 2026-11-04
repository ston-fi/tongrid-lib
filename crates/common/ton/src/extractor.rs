use anyhow::Result;
use num_bigint::BigUint;
use ton::block_tlb::{Block, ConfigParam18, StorageInfo};
use ton::tlb_adapters::{DictKeyAdapterUint, DictValAdapterTLB, TLBHashMap};
use ton::ton_core::cell::TonCell;
use ton::ton_core::errors::TonCoreError;

// Utility class
pub struct TonExtractor;

impl TonExtractor {
    // return price in units per second
    // ported https://github.com/ton-blockchain/unfreezer/blob/main/src/lib/calculateAmountForDelta.ts
    pub fn storage_price_ps(
        storage_info: &StorageInfo,
        is_masterchain: bool,
        config_param: &ConfigParam18,
    ) -> Result<BigUint> {
        let storage_prices = config_param.get_first()?;
        let one_cell_pps = if is_masterchain {
            storage_prices.mc_cell_price_ps
        } else {
            storage_prices.cell_price_ps
        };

        let one_bit_pps = if is_masterchain {
            storage_prices.mc_bit_price_ps
        } else {
            storage_prices.bit_price_ps
        };

        // https://docs.ton.org/v3/documentation/smart-contracts/transaction-fees/fees-low-level#formula
        let cell_pps = BigUint::from(one_cell_pps) * storage_info.used.cells.data;
        let bit_pps = BigUint::from(one_bit_pps) * storage_info.used.bits.data;
        Ok((cell_pps + bit_pps) / 2u32.pow(16))
    }

    pub fn msg_query_id(msg_body: &TonCell) -> Result<Option<u64>> {
        let mut parser = msg_body.parser();
        if parser.read_bits(32).is_err() {
            return Ok(None);
        }
        match parser.read_num(64) {
            Ok(val) => Ok(Some(val)),
            Err(_) => Ok(None),
        }
    }

    pub fn config_ton_cell(key_block: &Block) -> Result<TonCell, TonCoreError> {
        if !key_block.info.key_block {
            return Err(TonCoreError::Custom(format!(
                "Block {} is not a key block",
                key_block.info.seqno
            )));
        }
        let mc_extra = key_block.extra.mc_block_extra.as_ref().ok_or_else(|| {
            TonCoreError::Custom(format!(
                "config not found in mc_block={}",
                key_block.info.seqno
            ))
        })?;
        let config = mc_extra.config.as_ref().ok_or_else(|| {
            TonCoreError::Custom(format!(
                "config_params not found in mc_block={}",
                key_block.info.seqno
            ))
        })?;

        let mut builder = TonCell::builder();
        TLBHashMap::<DictKeyAdapterUint<_>, DictValAdapterTLB<_>>::new(32)
            .write(&mut builder, &config.config)?;
        builder.build()
    }
}
