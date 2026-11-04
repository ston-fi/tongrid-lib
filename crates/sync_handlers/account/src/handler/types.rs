use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use ton::block_tlb::{BlockExtra, ShardAccount, Tx};
use ton::emulators::emul_bc_config::EmulBCConfig;
use ton::emulators::tx_emulator::C7PrevBlocksInfo;
use ton::ton_core::cell::TonHash;
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;
use tongrid_db::entity::block::block_shard_key::DBBlockKey;
use tongrid_db::entity::DBTx;

#[derive(Debug)]
pub struct TxEmulData {
    pub blocks: Arc<HashMap<DBBlockKey, Arc<EmulBlockInfo>>>,
    pub txs_sorted: HashMap<TonAddress, Vec<EmulTxInfo>>,
    pub accs_cnt: usize,
    pub txs_cnt: usize,
}

#[derive(Debug)]
pub struct EmulBlockInfo {
    pub block_key: DBBlockKey,
    pub random_seed: TonHash,
    pub c7_prev_blocks_info_boc: Arc<Vec<u8>>,
    pub config: EmulBCConfig,
}

impl EmulBlockInfo {
    pub async fn new(
        block_key: DBBlockKey,
        block_extra: &BlockExtra,
        config: EmulBCConfig,
        prev_blocks_info: C7PrevBlocksInfo,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            block_key,
            random_seed: block_extra.rand_seed,
            c7_prev_blocks_info_boc: Arc::new(prev_blocks_info.to_boc()?),
            config,
        })
    }
}

pub struct EmulTxInfo {
    pub addr: TonAddress,
    pub ton_tx: Arc<Tx>,
    pub db_tx: DBTx,
    pub tx_lib_ids: HashSet<TonHash>,
    pub tx_hash: TonHash,
    pub old_acc_hash: TonHash,
    pub new_acc_hash: TonHash,
    pub block_info: Arc<EmulBlockInfo>,
    pub emulated_state: Option<Arc<ShardAccount>>, // filled during emulation
    pub prev_emulated_state: Option<Arc<ShardAccount>>,
    // we must not remove this handler even if it affect performance:
    // there is a chance with new emulator version to fail emulate state again
    // so if we got it once, we must keep it
    pub state_already_saved: bool,
}

impl Display for EmulTxInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}, mc_seqno: {}, addr: {}",
            self.ton_tx.lt, self.tx_hash, self.block_info.block_key.mc_seqno, self.addr
        )
    }
}

impl EmulTxInfo {
    pub fn new(
        addr: TonAddress,
        db_tx: DBTx,
        ton_tx: Tx,
        tx_lib_ids: HashSet<TonHash>,
        block_info: Arc<EmulBlockInfo>,
        emulated_state: Option<ShardAccount>,
        prev_emulated_state: Option<ShardAccount>,
    ) -> anyhow::Result<Self> {
        let hashes = ton_tx.state_update.clone().into_inner();
        let tx_hash = db_tx.tx_hash;
        let emulated_state = emulated_state.map(Arc::new);
        let prev_emulated_state = prev_emulated_state.map(Arc::new);
        let state_already_saved = emulated_state.is_some();

        Ok(Self {
            addr,
            ton_tx: Arc::new(ton_tx),
            db_tx,
            tx_lib_ids,
            tx_hash,
            old_acc_hash: hashes.old,
            new_acc_hash: hashes.new,
            block_info,
            emulated_state,
            state_already_saved,
            prev_emulated_state,
        })
    }
}

impl Debug for EmulTxInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tx_id: {self}, prev_tx_id: {}:{}, old_acc_hash: {:X}, new_acc_hash: {}, old_acc_status: {:?}, new_acc_status: {:?}",
            self.ton_tx.prev_tx_lt,
            self.ton_tx.prev_tx_hash,
            self.old_acc_hash,
            self.new_acc_hash,
            self.ton_tx.orig_status,
            self.ton_tx.end_status
        )
    }
}
