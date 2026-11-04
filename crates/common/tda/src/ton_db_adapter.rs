pub mod account;
pub mod config;
pub mod emulation;
pub mod libs;
pub mod msg;
pub mod tx;

use self::tx::TxAdapter;
use crate::ton_db_adapter::account::AccountAdapter;
use crate::ton_db_adapter::config::ConfigAdapter;
use crate::ton_db_adapter::emulation::EmulatorAdapter;
use crate::ton_db_adapter::libs::{LibSearchStats, LibsAdapter};
use crate::ton_db_adapter::msg::MsgAdapter;
use moka::future::Cache;
use std::sync::Arc;
use ton::emulators::emul_bc_config::EmulBCConfig;
use tongrid_db::db::DB;
use tongrid_ton::tg_ton::TGTon;

/// Proxy-class to cache some db data & simplify objects creation
#[derive(Clone)]
pub struct TonDBAdapter {
    inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) db: DB,
    pub(crate) ton: TGTon,
    pub(crate) try_fetch_lib_onchain: bool,
    pub(crate) lib_search_stats: Arc<LibSearchStats>,
    pub(crate) genesis_config: EmulBCConfig,
    pub(crate) config_cache: Cache<i32, EmulBCConfig>,
}

impl TonDBAdapter {
    pub async fn new(
        db: DB,
        ton: TGTon,
        genesis_config: EmulBCConfig,
        config_cache_capacity: u64,
    ) -> anyhow::Result<Self> {
        let initial_seqno = db.progress().load_initial_height().await?.unwrap_or(1);
        let is_litenode_search = initial_seqno != 0;

        Ok(Self {
            inner: Arc::new(Inner {
                db,
                ton,
                try_fetch_lib_onchain: is_litenode_search,
                lib_search_stats: Arc::new(LibSearchStats::default()),
                genesis_config,
                config_cache: Cache::new(config_cache_capacity),
            }),
        })
    }

    pub fn db(&self) -> &DB {
        &self.inner.db
    }

    pub(crate) fn inner(&self) -> &Inner {
        self.inner.as_ref()
    }

    pub fn account(&self) -> AccountAdapter<'_> {
        self.into()
    }
    pub fn tx(&self) -> TxAdapter<'_> {
        self.into()
    }
    pub fn msg(&self) -> MsgAdapter<'_> {
        self.into()
    }
    pub fn emulation(&self) -> EmulatorAdapter<'_> {
        self.into()
    }
    pub fn libs(&self) -> LibsAdapter<'_> {
        self.into()
    }

    pub fn conf(&self) -> ConfigAdapter<'_> {
        self.into()
    }

    pub fn ton(&self) -> &TGTon {
        &self.inner.ton
    }
}
