mod error;
mod stream_data_fetcher;
mod stream_replayer;

use crate::StreamConfig;
use anyhow::Result;
use ethereum_types::H256;
use jsonrpsee::http_client::HttpClient;
use ssz::Encode;
use std::{collections::HashSet, sync::Arc};
use storage_with_stream::Store;
use task_executor::TaskExecutor;
use tokio::sync::RwLock;

use self::{stream_data_fetcher::StreamDataFetcher, stream_replayer::StreamReplayer};

pub struct StreamManager;

pub const RETRY_WAIT_MS: u64 = 1000;

impl StreamManager {
    pub async fn initialize(
        config: &StreamConfig,
        store: Arc<RwLock<dyn Store>>,
        clients: Vec<HttpClient>,
        admin_client: Option<HttpClient>,
        task_executor: TaskExecutor,
    ) -> Result<(StreamDataFetcher, StreamReplayer)> {
        // initialize
        let holding_stream_ids = store.read().await.get_holding_stream_ids().await?;
        let holding_stream_set: HashSet<H256> =
            HashSet::from_iter(holding_stream_ids.iter().cloned());
        // ensure current stream id set is a subset of streams maintained in db
        let mut reseted = false;
        for id in config.stream_ids.iter() {
            if !holding_stream_set.contains(id) {
                // new stream id, replay from start
                store
                    .write()
                    .await
                    .reset_stream_sync(config.stream_ids.as_ssz_bytes())
                    .await?;
                reseted = true;
                break;
            }
        }
        // is a subset, update stream ids in db
        if !reseted && config.stream_ids.len() != holding_stream_ids.len() {
            store
                .write()
                .await
                .update_stream_ids(config.stream_ids.as_ssz_bytes())
                .await?;
        }

        // spawn data sync and stream replay threads
        let fetcher = StreamDataFetcher::new(
            config.clone(),
            store.clone(),
            clients,
            admin_client,
            task_executor,
        )
        .await?;
        let replayer = StreamReplayer::new(config.clone(), store.clone()).await?;
        Ok((fetcher, replayer))
    }

    pub fn spawn(
        fetcher: StreamDataFetcher,
        replayer: StreamReplayer,
        executor: TaskExecutor,
    ) -> Result<()> {
        executor.spawn(
            async move { Box::pin(fetcher.run()).await },
            "stream data fetcher",
        );

        executor.spawn(
            async move { Box::pin(replayer.run()).await },
            "stream data replayer",
        );
        Ok(())
    }
}
