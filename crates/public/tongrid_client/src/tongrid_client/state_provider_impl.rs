use std::str::FromStr;
use std::sync::Arc;

use ton::block_tlb::ShardAccount;
use ton::ton_core::{
    cell::TonHash,
    errors::{TonCoreError, TonCoreResult},
    traits::{
        state_provider::{ContractState, StateProvider},
        tlb::TLB,
    },
    types::{TonAddress, TxLTHash},
};
use tonic::async_trait;

use tongrid_proto::v3::{
    AddressLt, GetBlocksTxsRequest, GetLastTxAtMcSeqnoRequest, GetTxRequest, McSeqnoRange,
    TxFilter, TxResponseParams,
};

use super::{to_ton_core_boxed, TongridClient};

#[async_trait]
impl StateProvider for TongridClient {
    async fn last_mc_seqno(&self) -> TonCoreResult<u32> {
        Ok(self.last_sync_status().full_synced_seqno)
    }

    async fn load_state(
        &self,
        address: TonAddress,
        tx_id: Option<TxLTHash>,
    ) -> TonCoreResult<ContractState> {
        let tx_response_params = Some(TxResponseParams {
            account_required: true,
            ..Default::default()
        });
        let (tx, latest_mc_seqno) = match tx_id.as_ref() {
            Some(tx_id) => {
                let lt = u64::try_from(tx_id.lt).map_err(|_| {
                    TonCoreError::Custom(format!("negative transaction LT: {}", tx_id.lt))
                })?;
                let tx = self
                    .tx()
                    .get_tx(GetTxRequest {
                        address_lt: Some(AddressLt {
                            address: address.to_string(),
                            lt,
                        }),
                        tx_response_params,
                    })
                    .await
                    .map_err(to_ton_core_boxed)?
                    .into_inner()
                    .tx;
                (tx, None)
            }
            None => {
                let full_synced_seqno = self.last_sync_status().full_synced_seqno;
                let tx = self
                    .tx()
                    .get_last_tx_at_mc_seqno(GetLastTxAtMcSeqnoRequest {
                        mc_seqno: full_synced_seqno,
                        address: address.to_string(),
                        tx_response_params,
                    })
                    .await
                    .map_err(to_ton_core_boxed)?
                    .into_inner()
                    .tx;
                (tx, Some(full_synced_seqno))
            }
        };

        let tx = tx.ok_or_else(|| {
            TonCoreError::Custom(format!(
                "tx not found in Tongrid response for address={address}"
            ))
        })?;
        let account_state = tx.account_state.ok_or_else(|| {
            TonCoreError::Custom(format!(
                "account_state not found in Tongrid tx response for address={address}, tx_lt={}",
                tx.tx_lt
            ))
        })?;

        let shard_account = ShardAccount::from_boc(account_state.shard_account_boc)?;
        let account = &shard_account.account;
        let code_boc = account
            .get_code()
            .map(|code| code.to_boc().map(Arc::new))
            .transpose()?;
        let data_boc = account
            .get_data()
            .map(|data| data.to_boc().map(Arc::new))
            .transpose()?;
        let frozen_hash = account.as_frozen().map(|frozen| frozen.state_hash);
        let balance = match account.get_balance() {
            Some(balance) => i64::try_from(balance.to_u64()?).map_err(|_| {
                TonCoreError::Custom(format!("contract {address} balance exceeds i64"))
            })?,
            None => 0,
        };
        let last_tx_lt = i64::try_from(shard_account.last_tx_lt).map_err(|_| {
            TonCoreError::Custom(format!("contract {address} transaction LT exceeds i64"))
        })?;
        let mut state = ContractState::new(
            address,
            TxLTHash::new(last_tx_lt, shard_account.last_tx_hash),
            balance,
        )
        .with_mc_seqno(latest_mc_seqno.unwrap_or(tx.mc_seqno));
        state.code_boc = code_boc;
        state.data_boc = data_boc;
        state.frozen_hash = frozen_hash;

        if let Some(expected_tx_id) = tx_id {
            if state.last_tx_id != expected_tx_id {
                return Err(TonCoreError::Custom(format!(
                    "Tongrid returned transaction {} for requested transaction {}",
                    state.last_tx_id, expected_tx_id
                )));
            }
        }

        Ok(state)
    }

    async fn load_latest_tx_per_address(
        &self,
        mc_seqno: u32,
    ) -> TonCoreResult<Vec<(TonAddress, TxLTHash)>> {
        self.wait_seqno(mc_seqno, None)
            .await
            .map_err(to_ton_core_boxed)?;
        let req = GetBlocksTxsRequest {
            seqno_range: Some(McSeqnoRange {
                from_mc_seqno: mc_seqno,
                to_mc_seqno: mc_seqno,
            }),
            tx_response_params: None,
            filter: Some(TxFilter {
                latest_per_address: Some(true),
                ..Default::default()
            }),
        };
        let txs = self
            .tx()
            .get_blocks_txs(req)
            .await
            .map_err(to_ton_core_boxed)?
            .into_inner()
            .txs;

        let mut res = Vec::with_capacity(txs.len());
        for tx in txs {
            let address = TonAddress::from_str(&tx.address)?;
            let lt = i64::try_from(tx.tx_lt).map_err(|_| {
                TonCoreError::Custom(format!("contract {address} transaction LT exceeds i64"))
            })?;
            let hash = TonHash::from_vec(tx.tx_hash)?;
            res.push((address, TxLTHash::new(lt, hash)));
        }
        Ok(res)
    }
}
