use std::{cmp::min, sync::Arc, time::Duration};

use tongrid_db::{
    db::DB,
    entity::{jetton_balance::DBJettonBalance, tx::tx_key_lookup::MAX_LIMIT},
    paginator::{LTPage, LTPageLoader, LTPaginator},
};

use futures::stream::{self, StreamExt, TryStreamExt};
use tongrid_proto::v3::{
    AddressPaginationParams, BalanceHistoryResponse, BalancesResponse, GetBalanceHistoryRequest,
    GetBalancesByMasterRequest, GetBalancesByOwnerRequest, GetBalancesRequest,
    JettonBalance as JettonBalanceProto, JettonBalancesResponseParams,
    JettonWalletId as JettonWalletProto,
};
use tonic::async_trait;

use crate::{
    context::ServiceContext,
    error::GrpcServiceError,
    utils::{normalize_address, ResponseWithWaitTime},
};

pub struct JettonService {
    ctx: Arc<ServiceContext>,
}

const ADDRESS_PAGE_LIMIT: i32 = MAX_LIMIT as i32;
enum GetBalanceBy {
    Master(GetBalancesByMasterRequest),
    Owner(GetBalancesByOwnerRequest),
}

impl JettonService {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }

    pub async fn get_balances_by_owner(
        &self,
        mut req: GetBalancesByOwnerRequest,
    ) -> Result<ResponseWithWaitTime<BalancesResponse>, GrpcServiceError> {
        req.owner_address = normalize_address(&req.owner_address)?;
        normalize_address_pagination(req.address_pagination_params.as_mut())?;
        self.get_balances_by(GetBalanceBy::Owner(req)).await
    }

    pub async fn get_balances_by_master(
        &self,
        mut req: GetBalancesByMasterRequest,
    ) -> Result<ResponseWithWaitTime<BalancesResponse>, GrpcServiceError> {
        req.master_address = normalize_address(&req.master_address)?;
        normalize_address_pagination(req.address_pagination_params.as_mut())?;
        self.get_balances_by(GetBalanceBy::Master(req)).await
    }

    pub async fn get_balances(
        &self,
        mut req: GetBalancesRequest,
    ) -> Result<ResponseWithWaitTime<BalancesResponse>, GrpcServiceError> {
        if req.jetton_wallets.len() > self.ctx.config.jetton_service.max_batch_size {
            return Err(GrpcServiceError::InvalidArgument(format!(
                "Too many jetton_wallets: {} (max: {})",
                req.jetton_wallets.len(),
                self.ctx.config.jetton_service.max_batch_size
            )));
        }
        for wallet in &mut req.jetton_wallets {
            normalize_wallet_id(wallet)?;
        }
        let db = self.ctx.tda.db();
        let jetton_balance_serial = &self.ctx.serial.jetton_balance();
        let rsp_params = req.response_params.as_ref();

        let jetton_balances_futs = req
            .jetton_wallets
            .into_iter()
            .map(|wallet_proto| async move {
                // lookup wallet_address by (owner, master)
                let maybe_db_wallet = db
                    .jetton_wallet()
                    .get_legit_by_owner_and_master(
                        &wallet_proto.owner_address,
                        &wallet_proto.master_address,
                    )
                    .await?;

                let Some(db_wallet) = maybe_db_wallet else {
                    let proto_balance =
                        jetton_balance_serial.build_jetton_balance(wallet_proto, None, rsp_params);
                    return Ok(proto_balance);
                };

                // get latest balance for wallet_address
                self.get_wallet_balance_proto(
                    wallet_proto,
                    &db_wallet.wallet_address,
                    req.mc_seqno,
                    rsp_params,
                )
                .await
            });
        let jetton_balances = stream::iter(jetton_balances_futs)
            .buffered(self.ctx.config.jetton_service.concurrency.get())
            .try_collect()
            .await?;

        Ok(ResponseWithWaitTime {
            response: BalancesResponse {
                jetton_balances,
                address_pagination_params: None,
            },
            time_waited: Default::default(),
        })
    }

    pub async fn get_balance_history(
        &self,
        mut req: GetBalanceHistoryRequest,
    ) -> Result<ResponseWithWaitTime<BalanceHistoryResponse>, GrpcServiceError> {
        if let Some(wallet) = &mut req.jetton_wallet_id {
            normalize_wallet_id(wallet)?;
        }
        let recent_data = self.ctx.store.get_recent_data();
        let mc_seqno = recent_data.sync_status.full_synced_seqno;
        let time_waited = Duration::ZERO;
        let db = self.ctx.tda.db();
        let jetton_balance_serial = &self.ctx.serial.jetton_balance();
        let wallet_id = req.jetton_wallet_id.ok_or_else(|| {
            GrpcServiceError::InvalidArgument("jetton_wallet_id is required".to_string())
        })?;

        let Some(db_jetton_wallet) = db
            .jetton_wallet()
            .get_legit_by_owner_and_master(&wallet_id.owner_address, &wallet_id.master_address)
            .await?
        else {
            let response = jetton_balance_serial.build_balance_history_response(&[], None);
            return Ok(ResponseWithWaitTime {
                response,
                time_waited,
            });
        };

        let mut paginator = LTPaginator::<WalletBalancesLoader>::new(
            db.clone(),
            db_jetton_wallet.wallet_address,
            mc_seqno,
            req.pagination_params.as_ref(),
        )
        .await?;

        let db_balances = paginator.load_page().await?;

        let response = jetton_balance_serial
            .build_balance_history_response(&db_balances, paginator.next_page_proto());

        Ok(ResponseWithWaitTime {
            response,
            time_waited,
        })
    }

    async fn get_balances_by(
        &self,
        by: GetBalanceBy,
    ) -> Result<ResponseWithWaitTime<BalancesResponse>, GrpcServiceError> {
        let mc_seqno = match &by {
            GetBalanceBy::Master(req) => req.mc_seqno,
            GetBalanceBy::Owner(req) => req.mc_seqno,
        };

        let db = self.ctx.tda.db();

        let (db_wallets, response_params, address_pagination_params) = match by {
            GetBalanceBy::Master(req) => {
                let req_address_params = req.address_pagination_params.as_ref();
                let owner = req_address_params.and_then(|p| p.after_address.as_deref());
                let limit = req_address_params
                    .and_then(|p| p.limit)
                    .or(req.limit)
                    .map(|limit| limit.min(ADDRESS_PAGE_LIMIT as u32) as i32);
                let wallets = db
                    .jetton_wallet()
                    .get_legit_by_master(&req.master_address, owner, limit)
                    .await?;
                let next_page_params = if is_full_wallet_page(wallets.len(), limit) {
                    wallets.last().map(|wallet| AddressPaginationParams {
                        after_address: Some(wallet.owner_address.clone()),
                        limit: limit.map(|x| x as u32),
                    })
                } else {
                    None
                };
                (wallets, req.response_params, next_page_params)
            }
            GetBalanceBy::Owner(req) => {
                let req_address_params = req.address_pagination_params.as_ref();
                let master = req_address_params.and_then(|p| p.after_address.as_deref());
                let limit = req_address_params
                    .and_then(|p| p.limit)
                    .or(req.limit)
                    .map(|limit| limit.min(ADDRESS_PAGE_LIMIT as u32) as i32);
                let wallets = db
                    .jetton_wallet()
                    .get_legit_by_owner(&req.owner_address, master, limit)
                    .await?;
                let next_page_params = if is_full_wallet_page(wallets.len(), limit) {
                    wallets.last().map(|wallet| AddressPaginationParams {
                        after_address: Some(wallet.master_address.clone()),
                        limit: limit.map(|x| x as u32),
                    })
                } else {
                    None
                };
                (wallets, req.response_params, next_page_params)
            }
        };

        let jetton_balances_futs = db_wallets.into_iter().map(|wallet| async {
            let wallet_addr = wallet.wallet_address;
            let wallet_proto = JettonWalletProto {
                master_address: wallet.master_address,
                owner_address: wallet.owner_address,
            };
            self.get_wallet_balance_proto(
                wallet_proto,
                &wallet_addr,
                mc_seqno,
                response_params.as_ref(),
            )
            .await
        });
        let jetton_balances = stream::iter(jetton_balances_futs)
            .buffered(self.ctx.config.jetton_service.concurrency.get())
            .try_collect()
            .await?;

        let response = BalancesResponse {
            jetton_balances,
            address_pagination_params,
        };
        Ok(ResponseWithWaitTime {
            response,
            time_waited: Default::default(),
        })
    }

    async fn get_wallet_balance_proto(
        &self,
        wallet_proto: JettonWalletProto,
        wallet_address: &str,
        mc_seqno: u32,
        rsp_params: Option<&JettonBalancesResponseParams>,
    ) -> Result<JettonBalanceProto, GrpcServiceError> {
        let db = self.ctx.tda.db();
        let Some(last_tx) = db
            .tx_lite()
            .lookup_latest_key(wallet_address, mc_seqno)
            .await?
        else {
            let proto_balance = self.ctx.serial.jetton_balance().build_jetton_balance(
                wallet_proto,
                None,
                rsp_params,
            );
            return Ok(proto_balance);
        };
        let db_balance = db
            .jetton_balance()
            .get_latest_valid(wallet_address, last_tx.tx_lt)
            .await?;
        let proto_balance = self.ctx.serial.jetton_balance().build_jetton_balance(
            wallet_proto,
            db_balance,
            rsp_params,
        );
        Ok(proto_balance)
    }
}

fn is_full_wallet_page(wallets_len: usize, limit: Option<i32>) -> bool {
    let effective_limit = min(ADDRESS_PAGE_LIMIT, limit.unwrap_or(ADDRESS_PAGE_LIMIT));
    effective_limit > 0 && wallets_len as i32 == effective_limit
}

struct WalletBalancesLoader;

#[async_trait]
impl LTPageLoader for WalletBalancesLoader {
    type EntityWithLT = DBJettonBalance;

    async fn load_page_sorted(
        db: &DB,
        address: &str,
        page: &LTPage,
    ) -> anyhow::Result<Vec<Self::EntityWithLT>> {
        db.jetton_balance().get_page_valid(address, page).await
    }

    fn read_lt(entity: &DBJettonBalance) -> u64 {
        entity.tx_lt
    }
}

fn normalize_wallet_id(wallet: &mut JettonWalletProto) -> Result<(), GrpcServiceError> {
    wallet.owner_address = normalize_address(&wallet.owner_address)?;
    wallet.master_address = normalize_address(&wallet.master_address)?;
    Ok(())
}

fn normalize_address_pagination(
    params: Option<&mut AddressPaginationParams>,
) -> Result<(), GrpcServiceError> {
    if let Some(AddressPaginationParams {
        after_address: Some(address),
        ..
    }) = params
    {
        *address = normalize_address(address)?;
    }
    Ok(())
}
