use crate::proto_serializer::ProtoSerializer;
use std::sync::LazyLock;

use tongrid_db::entity::jetton_balance::DBJettonBalance;
use tongrid_proto::v3::{
    BalanceHistoryEntry, BalanceHistoryResponse, JettonBalance, JettonBalancesResponseParams,
    JettonWalletId as JettonWalletProto, TxPaginationParams,
};

static DEFAULT_RSP_PARAMS: LazyLock<JettonBalancesResponseParams> =
    LazyLock::new(JettonBalancesResponseParams::default);

pub(crate) struct JettonBalanceSerializer;

impl JettonBalanceSerializer {
    pub fn new(_serial: &ProtoSerializer) -> Self {
        Self
    }

    pub fn build_balance_history_response(
        &self,
        jetton_balances: &[DBJettonBalance],
        pagination_params: Option<TxPaginationParams>,
    ) -> BalanceHistoryResponse {
        let entries = jetton_balances
            .iter()
            .map(|b| self.build_balance_history_entry(b))
            .collect();
        BalanceHistoryResponse {
            entries,
            next_pagination_params: pagination_params,
        }
    }

    pub fn build_jetton_balance(
        &self,
        jetton_wallet: JettonWalletProto,
        jetton_balance_opt: Option<DBJettonBalance>,
        response_params: Option<&JettonBalancesResponseParams>,
    ) -> JettonBalance {
        let Some(jetton_balance) = jetton_balance_opt else {
            return JettonBalance {
                master_address: Some(jetton_wallet.master_address),
                owner_address: Some(jetton_wallet.owner_address),
                wallet_address: None,
                balance: None,
                prev_balance: None,
                tx_mc_seqno: None,
                tx_lt: None,
                tx_utime: None,
            };
        };

        let response_params = response_params.unwrap_or(&DEFAULT_RSP_PARAMS);
        let balance = jetton_balance.balance.map(|x| x.to_string());
        let prev_balance = response_params
            .with_prev_balance
            .then_some(jetton_balance.prev_balance.map(|x| x.to_string()))
            .flatten();
        let tx_lt = response_params.with_tx_lt.then_some(jetton_balance.tx_lt);
        let tx_utime = response_params
            .with_tx_utime
            .then_some(jetton_balance.tx_utime);
        let master_address = response_params
            .with_master_address
            .then_some(jetton_wallet.master_address);
        let owner_address = response_params
            .with_owner_address
            .then_some(jetton_wallet.owner_address);
        let wallet_address = response_params
            .with_jetton_wallet_address
            .then_some(jetton_balance.wallet_address);
        let tx_mc_seqno = response_params
            .with_mc_seqno
            .then_some(jetton_balance.mc_seqno);

        JettonBalance {
            master_address,
            owner_address,
            wallet_address,
            balance,
            prev_balance,
            tx_mc_seqno,
            tx_lt,
            tx_utime,
        }
    }

    fn build_balance_history_entry(&self, jetton_balance: &DBJettonBalance) -> BalanceHistoryEntry {
        BalanceHistoryEntry {
            balance: jetton_balance
                .balance
                .as_ref()
                .map(|x| x.to_string())
                .unwrap_or("".to_string()),
            prev_balance: jetton_balance
                .prev_balance
                .as_ref()
                .map(|x| x.to_string())
                .unwrap_or("".to_string()),
            tx_lt: jetton_balance.tx_lt,
            tx_utime: jetton_balance.tx_utime,
            mc_seqno: jetton_balance.mc_seqno,
        }
    }
}
