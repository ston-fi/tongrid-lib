use crate::store::updater::StoreUpdater;
use crate::types::recent_data::RecentData;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tongrid_tda::ton_db_adapter::TonDBAdapter;
use updater::getters::{collect_recent_data, get_sync_status};

mod metrics;
mod updater;

#[derive(Clone)]
pub struct MemStore {
    inner: Arc<Inner>,
    _worker: Arc<Worker>,
}

impl MemStore {
    pub async fn new(tda: TonDBAdapter) -> anyhow::Result<Self> {
        let sync_status = get_sync_status(&tda).await?;
        let initial_data = Arc::new(collect_recent_data(None, sync_status, &tda).await?);
        let recent_data = RwLock::new(initial_data.clone());
        let (watch_sender, _) = watch::channel(initial_data);
        let inner = Arc::new(Inner {
            recent_data,
            watch_sender,
        });
        let updater = StoreUpdater::new(tda, inner.clone());
        let worker = Arc::new(Worker(tokio::spawn(update_loop(updater))));
        let store = MemStore {
            inner,
            _worker: worker,
        };
        Ok(store)
    }

    pub fn get_recent_data(&self) -> Arc<RecentData> {
        self.inner.get_recent_data()
    }
    pub fn set_recent_data(&self, recent_data: RecentData) {
        self.inner.set_recent_data(recent_data);
    }
    pub fn subscribe(&self) -> watch::Receiver<Arc<RecentData>> {
        self.inner.subscribe()
    }
}

pub(crate) async fn update_loop(updater: StoreUpdater) {
    let mut full_synced_seqno = updater.full_synced_seqno();
    loop {
        match updater.update_store().await {
            Ok(()) => {
                let new_synced_seqno = updater.full_synced_seqno();
                if full_synced_seqno != new_synced_seqno {
                    full_synced_seqno = new_synced_seqno;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            Err(err) => {
                log::error!("Error during update_loop: {err:?}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

pub(crate) struct Inner {
    recent_data: RwLock<Arc<RecentData>>,
    pub(crate) watch_sender: watch::Sender<Arc<RecentData>>,
}

// Only externally owned stores keep the worker alive; the updater owns data, not its task.
struct Worker(tokio::task::JoinHandle<()>);
impl Drop for Worker {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl Inner {
    pub fn get_recent_data(&self) -> Arc<RecentData> {
        self.recent_data.read().clone()
    }

    pub fn set_recent_data(&self, recent_data: RecentData) {
        let data = Arc::new(recent_data);
        *self.recent_data.write() = data.clone();
        let _ = self.watch_sender.send_replace(data);
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<RecentData>> {
        self.watch_sender.subscribe()
    }
}
