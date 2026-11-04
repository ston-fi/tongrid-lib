//! Offline composition through the same-name crate exports.
use std::{collections::HashSet, num::NonZeroU32, sync::Arc};
use tongrid_lib::tongrid_ton::ton_msg_sender::TonMsgSender;

struct OfflineSender;
#[async_trait::async_trait]
impl TonMsgSender for OfflineSender {
    async fn send_msg(&self, _boc: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("Offline example does not broadcast")
    }
}

fn main() -> anyhow::Result<()> {
    let _senders: Vec<Arc<dyn TonMsgSender>> = vec![Arc::new(OfflineSender)];
    let _account = tongrid_lib::tongrid_account_handler::config::AccountHandlerConfig::default();
    let services = tongrid_lib::tongrid_api_grpc_service_v3::config::ServicesConfig::default();
    services.send_msg_service.validate()?;
    let _backup = tongrid_lib::tongrid_backup_handler::config::BackupHandlerConfig::default();
    let _block = tongrid_lib::tongrid_block_handler::config::BlockSyncConfig::new(1, Some(2));
    let _classifier =
        tongrid_lib::tongrid_classifier_handler::config::ClassifierHandlerConfig::default();
    let _client =
        std::any::type_name::<tongrid_lib::tongrid_client::tongrid_client::TongridClient>();
    let _sparse = tongrid_lib::tongrid_core::sparse_contracts::config::SparseContractsConfig {
        store_period: NonZeroU32::MIN,
        addresses: HashSet::new(),
    };
    let _cache = tongrid_lib::tongrid_db::config::cache::DBAccountCacheConfig {
        capacity: 0,
        ttl_seconds: 1,
    };
    let _grpc = std::any::type_name::<tongrid_lib::tongrid_grpc_server::config::GrpcServerConfig>();
    let _jetton = tongrid_lib::tongrid_jetton_handler::config::JettonHandlerConfig::default();
    let _store = std::any::type_name::<tongrid_lib::tongrid_mem_store::store::MemStore>();
    let _request = tongrid_lib::tongrid_proto::v3::SendMsgReturnHashRequest::default();
    let _adapter = std::any::type_name::<tongrid_lib::tongrid_tda::ton_db_adapter::TonDBAdapter>();
    let _genesis = tongrid_lib::tongrid_tda::ton_db_adapter::config::genesis_config()?;
    let _test_hash = tongrid_lib::tongrid_test_utils::ton::make_db_ton_hash(&[0; 32]);
    let _verifier = tongrid_lib::tongrid_verifier_handler::config::VerifierHandlerConfig::default();
    Ok(())
}
