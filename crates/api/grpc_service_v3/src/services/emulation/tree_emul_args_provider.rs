use async_trait::async_trait;
use ton::block_tlb::{Msg, ShardAccount};
use ton::emulators::tx_emulator::{TXEmulArgs, TXEmulOrdArgs};
use ton::ton_core::cell::TonCellUtils;
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;
use tongrid_db::entity::TxKeyLookup;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

#[async_trait]
pub trait TreeEmulArgsProvider: Send + Sync {
    async fn build_args(
        &self,
        mc_seqno: u32,
        src_address: TonAddress,
        dst_address: TonAddress,
        in_msg: Msg,
        dst_state: Option<&ShardAccount>,
        adjust_msg: bool,
    ) -> anyhow::Result<TXEmulOrdArgs>;
}

pub struct TreeEmulArgsProviderScylla {
    tda: TonDBAdapter,
    ignore_chksig: bool,
    utime: u32,
}

impl TreeEmulArgsProviderScylla {
    pub(crate) fn new(tda: TonDBAdapter, ignore_chksig: bool, utime: u32) -> Self {
        Self {
            tda,
            ignore_chksig,
            utime,
        }
    }

    async fn get_shard_account(
        &self,
        address: &TonAddress,
        mc_seqno: u32,
    ) -> anyhow::Result<ShardAccount> {
        let dst_addr_str = address.to_string();
        let tx_key_lookup = TxKeyLookup::address_latest(&dst_addr_str, mc_seqno);
        let tx_key = match self.tda.db().tx().lookup_key(tx_key_lookup).await? {
            None => return Ok(ShardAccount::NON_EXIST),
            Some(key) => key,
        };
        let db_account = self.tda.db().account().get(&tx_key).await?;

        let (shard_account, _lib_ids) = self.tda.account().build_shard_account(&db_account).await?;
        Ok(shard_account)
    }
}

#[async_trait]
impl TreeEmulArgsProvider for TreeEmulArgsProviderScylla {
    async fn build_args(
        &self,
        mc_seqno: u32,
        _src_addr: TonAddress,
        dst_addr: TonAddress,
        in_msg: Msg,
        dst_state: Option<&ShardAccount>,
        _adjust_msg: bool,
    ) -> anyhow::Result<TXEmulOrdArgs> {
        let config = self.tda.conf().get_config_for_mc_block(mc_seqno).await?;
        let block = self
            .tda
            .db()
            .block()
            .get_master_by_mc_seqno(mc_seqno)
            .await?;
        let rand_seed = block.rand_seed;

        let shard_account = match dst_state {
            Some(x) => x,
            None => &self.get_shard_account(&dst_addr, mc_seqno).await?,
        };

        let in_msg_cell = in_msg.to_cell()?;
        let cells_with_libs = [
            shard_account.account.get_code(),
            shard_account.account.get_data(),
            Some(&in_msg_cell),
        ]
        .into_iter()
        .flatten();

        let lib_ids = TonCellUtils::extract_lib_ids(cells_with_libs)?;

        let libs_boc = self
            .tda
            .libs()
            .get_libs_dict(&lib_ids)
            .await?
            .map(Into::into);

        let emul_args = TXEmulArgs {
            shard_account_boc: shard_account.to_boc()?.into(),
            rand_seed,
            utime: self.utime,
            lt: shard_account.last_tx_lt + 1,
            ignore_chksig: self.ignore_chksig,
            c7_prev_blocks_info_boc: None,
            libs_boc,
            bc_config: config,
        };

        Ok(TXEmulOrdArgs {
            in_msg_boc: in_msg.to_boc()?.into(),
            emul_args,
        })
    }
}
