use crate::ser_de::block::block_serdb;
use crate::ton_db_adapter::TonDBAdapter;
use anyhow::bail;
use ton::block_tlb::{Block, ConfigParams};
use ton::emulators::emul_bc_config::EmulBCConfig;
use ton::tlb_adapters::{DictKeyAdapterUint, DictValAdapterTLB, TLBHashMap};
use ton::ton_core::cell::TonCell;
use ton::ton_core::traits::tlb::TLB;
use tongrid_ton::tg_ton::block_stream::TongridBlock;

/// Builds TON's canonical zero-state emulation configuration from the upstream constant.
/// Callers may pass this to `TonDBAdapter::new`, or supply a custom configuration.
///
/// # Errors
/// Returns an error if the upstream base64 string cannot be represented for the emulator.
pub fn genesis_config() -> anyhow::Result<EmulBCConfig> {
    Ok(EmulBCConfig::from_boc_base64(
        ton::ton_core::constants::TON_ZERO_CONFIG_BOC_B64,
    )?)
}

pub struct ConfigAdapter<'a> {
    tda: &'a TonDBAdapter,
}

impl<'a> From<&'a TonDBAdapter> for ConfigAdapter<'a> {
    fn from(tda: &'a TonDBAdapter) -> Self {
        Self { tda }
    }
}

impl<'a> ConfigAdapter<'a> {
    // Get config which was used to build specified block
    pub async fn get_config_for_mc_block(&self, mc_seqno: u32) -> anyhow::Result<EmulBCConfig> {
        let block = self
            .tda
            .inner
            .db
            .block()
            .get_master_by_mc_seqno(mc_seqno)
            .await?;
        self.get_config(block.key_block_seqno).await
    }

    pub async fn get_config(&self, key_seqno: u32) -> anyhow::Result<EmulBCConfig> {
        if key_seqno == 0 {
            return Ok(self.tda.inner.genesis_config.clone());
        }
        self.tda
            .inner
            .config_cache
            .try_get_with(key_seqno as i32, self.load_or_store_config(key_seqno))
            .await
            .map_err(|err| anyhow::anyhow!("{err}"))
    }

    async fn load_or_store_config(&self, key_seqno: u32) -> anyhow::Result<EmulBCConfig> {
        let key_block = match self
            .tda
            .inner
            .db
            .block()
            .get_master_by_mc_seqno_opt(key_seqno)
            .await?
        {
            Some(block) => Block::from_boc(block.boc_stripped.clone())?,
            None => {
                log::warn!("[CONF_KEEPER] no block for key_seqno={key_seqno} in db, fetching and inserting...");

                let ton = &self.tda.inner.ton.lite_client;
                let block_id_ext = ton.lookup_mc_block(key_seqno).await?;
                let mc_block = ton.get_block(block_id_ext.clone(), None).await?.data;
                let tongrid_block = TongridBlock::new(mc_block.clone(), block_id_ext)?;
                let db_block = block_serdb(key_seqno, &tongrid_block)?;
                self.tda.inner.db.block().insert(&db_block).await?;

                Block::from_boc(db_block.boc_stripped)?
            }
        };
        extract_config(&key_block)
    }
}

fn extract_config(key_block: &Block) -> anyhow::Result<EmulBCConfig> {
    let extra = &key_block.extra;
    let mc_extra = match &extra.mc_block_extra {
        Some(extra) => extra,
        None => bail!(
            "[CONF_KEEPER][extract_config] config not found in mc_block={}",
            key_block.info.seqno
        ),
    };
    let config = match &mc_extra.config {
        Some(config) => config,
        None => bail!(
            "[CONF_KEEPER][extract_config] config_params not found in mc_block={}",
            key_block.info.seqno
        ),
    };
    match extract_config_boc_b64(config) {
        Ok(config) => Ok(config),
        Err(err) => bail!(
            "[CONF_KEEPER][extract_config] error while extracting config from mc_block={}: {}",
            key_block.info.seqno,
            err
        ),
    }
}

fn extract_config_boc_b64(config: &ConfigParams) -> anyhow::Result<EmulBCConfig> {
    let mut builder = TonCell::builder();
    TLBHashMap::<DictKeyAdapterUint<_>, DictValAdapterTLB<_>>::new(32)
        .write(&mut builder, &config.config)?;
    Ok(EmulBCConfig::from_boc(&builder.build()?.to_boc()?)?)
}
