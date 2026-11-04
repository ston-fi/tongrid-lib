use crate::config::ServicesConfig;
use crate::proto_serializer::ProtoSerializer;
use std::sync::Arc;
use ton::emulators::emulator_pool::EmulatorPool;
use tongrid_mem_store::store::MemStore;
use tongrid_tda::ton_db_adapter::TonDBAdapter;
use tongrid_ton::ton_msg_sender::TonMsgSender;

pub struct ServiceContext {
    pub(crate) tda: TonDBAdapter,
    pub(crate) msg_senders: Vec<Arc<dyn TonMsgSender>>,
    pub(crate) config: ServicesConfig,
    pub(crate) serial: ProtoSerializer,
    pub(crate) store: MemStore,
    pub(crate) emulator_pool: EmulatorPool,
}

impl ServiceContext {
    /// Accepts initialized dependencies and at least one explicit broadcast transport.
    /// Returns an error for no senders or a zero attempt timeout.
    pub fn new(
        tda: TonDBAdapter,
        store: MemStore,
        msg_senders: Vec<Arc<dyn TonMsgSender>>,
        config: ServicesConfig,
    ) -> anyhow::Result<Self> {
        validate_senders(&msg_senders, &config.send_msg_service)?;
        let serial = ProtoSerializer::new(tda.clone());
        let emulator_pool = tda.ton().emul_pool.clone();
        Ok(Self {
            tda,
            store,
            msg_senders,
            config,
            serial,
            emulator_pool,
        })
    }
}

pub(crate) fn validate_senders(
    senders: &[Arc<dyn TonMsgSender>],
    config: &crate::config::SendMsgServiceConfig,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !senders.is_empty(),
        "At least one TON message sender is required"
    );
    config.validate()
}
