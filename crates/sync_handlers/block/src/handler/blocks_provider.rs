use std::time::Duration;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tongrid_ton::tg_ton::block_stream::{BlockStream, BlockStreamItem};

pub struct BlocksProvider {
    max_next_items: usize,
    receiver: RwLock<Receiver<BlockStreamItem>>,
    producer: tokio::task::JoinHandle<()>,
}

impl BlocksProvider {
    pub async fn new(
        block_stream: impl BlockStream,
        buffer_size: usize,
        max_next_items: usize,
    ) -> anyhow::Result<BlocksProvider> {
        anyhow::ensure!(buffer_size > 0, "Block stream buffer must be positive");
        anyhow::ensure!(max_next_items > 0, "Block batch limit must be positive");
        let (sender, receiver) = mpsc::channel(buffer_size);
        let producer = tokio::spawn(populate_loop(block_stream, sender));
        Ok(BlocksProvider {
            max_next_items,
            receiver: RwLock::new(receiver),
            producer,
        })
    }

    // return None means stream is closed
    pub async fn next(&self) -> Option<Vec<BlockStreamItem>> {
        self.next_up_to(self.max_next_items).await
    }

    // return None means stream is closed
    // returns up to max_items items (possibly less if stream is exhausted)
    pub async fn next_up_to(&self, max_items: usize) -> Option<Vec<BlockStreamItem>> {
        if max_items == 0 {
            return Some(Vec::new());
        }

        let mut items = Vec::with_capacity(max_items);

        // always wait at least 1 element if channel is not closed
        match self.receiver.write().await.recv().await {
            Some(item) => items.push(item),
            None => return None,
        }

        // receive up to max_items if available
        while items.len() < max_items {
            match self.receiver.write().await.try_recv() {
                Ok(item) => items.push(item),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        Some(items)
    }
}

async fn populate_loop(mut block_stream: impl BlockStream, sender: Sender<BlockStreamItem>) {
    loop {
        match block_stream.next().await {
            Ok(Some(item)) => {
                if sender.send(item).await.is_err() {
                    log::info!("Receiver is closed, stopping...");
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                // too big seqno means we're at the top of blockchain and there is nothing to get
                if !err.to_string().contains("too big masterchain block seqno") {
                    log::error!("Error in block stream: {err:?}")
                } else {
                    log::debug!("No new blocks, waiting 1 sec before retry...");
                }
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/test_blocks_provider.rs"]
mod tests;

impl Drop for BlocksProvider {
    fn drop(&mut self) {
        self.producer.abort();
    }
}
