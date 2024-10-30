use crate::sync_manager::config::LogSyncConfig;
use crate::sync_manager::log_entry_fetcher::{LogEntryFetcher, LogFetchProgress};
use anyhow::{anyhow, bail, Result};
use ethereum_types::H256;
use ethers::{prelude::Middleware, types::BlockNumber};
use futures::FutureExt;
use jsonrpsee::tracing::{debug, error, warn};
use kv_types::KVTransaction;
use shared_types::Transaction;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::log_store::tx_store::BlockHashAndSubmissionIndex;
use storage_with_stream::Store;
use task_executor::{ShutdownReason, TaskExecutor};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, RwLock};

const RETRY_WAIT_MS: u64 = 500;

// A RPC query can return at most 10000 entries.
// Each tx has less than 10KB, so the cache size should be acceptable.
const BROADCAST_CHANNEL_CAPACITY: usize = 25000;
const CATCH_UP_END_GAP: u64 = 10;

/// Errors while handle data
#[derive(Error, Debug)]
pub enum HandleDataError {
    /// Sequence Error
    #[error("transaction seq is great than expected, expect block number {0}")]
    SeqError(u64),
    /// Other Errors
    #[error("{0}")]
    CommonError(#[from] anyhow::Error),
}

#[derive(Clone, Debug)]
pub enum LogSyncEvent {
    /// Chain reorg detected without any operation yet.
    ReorgDetected { tx_seq: u64 },
    /// Transaction reverted in storage.
    Reverted { tx_seq: u64 },
    /// Synced a transaction from blockchain
    TxSynced { tx: Transaction },
}

pub struct LogSyncManager {
    config: LogSyncConfig,
    log_fetcher: LogEntryFetcher,
    store: Arc<RwLock<dyn Store>>,

    next_tx_seq: u64,

    /// To broadcast events to handle in advance.
    event_send: broadcast::Sender<LogSyncEvent>,

    block_hash_cache: Arc<RwLock<BTreeMap<u64, Option<BlockHashAndSubmissionIndex>>>>,
}

impl LogSyncManager {
    pub async fn spawn(
        config: LogSyncConfig,
        executor: TaskExecutor,
        store: Arc<RwLock<dyn Store>>,
    ) -> Result<(broadcast::Sender<LogSyncEvent>, oneshot::Receiver<()>)> {
        let next_tx_seq = store.read().await.next_tx_seq();

        let executor_clone = executor.clone();
        let mut shutdown_sender = executor.shutdown_sender();

        let (event_send, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        let event_send_cloned = event_send.clone();
        let (catch_up_end_sender, catch_up_end_receiver) = oneshot::channel();

        // Spawn the task to sync log entries from the blockchain.
        executor.spawn(
            run_and_log(
                move || {
                    shutdown_sender
                        .try_send(ShutdownReason::Failure("log sync failure"))
                        .expect("shutdown send error")
                },
                async move {
                    let log_fetcher = LogEntryFetcher::new(
                        &config.rpc_endpoint_url,
                        config.contract_address,
                        config.log_page_size,
                        config.confirmation_block_count,
                        config.rate_limit_retries,
                        config.timeout_retries,
                        config.initial_backoff,
                    )
                    .await?;

                    let block_hash_cache = Arc::new(RwLock::new(
                        store
                            .read()
                            .await
                            .get_block_hashes()?
                            .into_iter()
                            .map(|(x, y)| (x, Some(y)))
                            .collect::<BTreeMap<_, _>>(),
                    ));
                    let mut log_sync_manager = Self {
                        config,
                        log_fetcher,
                        next_tx_seq,
                        store,
                        event_send,
                        block_hash_cache,
                    };

                    let (mut start_block_number, mut start_block_hash) =
                        get_start_block_number_with_hash(&log_sync_manager).await?;

                    let (mut finalized_block_number, mut finalized_block_hash) =
                        match log_sync_manager.get_block(BlockNumber::Finalized).await {
                            Ok(finalized) => finalized,
                            Err(e) => {
                                warn!(?e, "unable to get finalized block");
                                log_sync_manager.get_block(0.into()).await?
                            }
                        };

                    // Load previous progress from db and check if chain reorg happens after restart.
                    let mut need_handle_reorg = false;
                    if start_block_number <= finalized_block_number {
                        let expect_block_hash = log_sync_manager
                            .get_block(start_block_number.into())
                            .await?
                            .1;
                        if expect_block_hash != start_block_hash {
                            need_handle_reorg = true;
                        }
                    }
                    debug!(
                        "current start block number {}, block hash {}, finalized block number {}",
                        start_block_number, start_block_hash, finalized_block_number
                    );

                    if need_handle_reorg {
                        let reorg_rx = log_sync_manager.log_fetcher.handle_reorg(
                            start_block_number,
                            start_block_hash,
                            &executor_clone,
                            log_sync_manager.block_hash_cache.clone(),
                        );
                        log_sync_manager.handle_data(reorg_rx, &None).await?;
                        if let Some((block_number, block_hash)) =
                            log_sync_manager.store.read().await.get_sync_progress()?
                        {
                            start_block_number = block_number;
                            start_block_hash = block_hash;
                        } else {
                            bail!("get log sync progress error");
                        }
                    }

                    // TODO(zz): Rate limit to avoid OOM during recovery.
                    if start_block_number >= finalized_block_number {
                        let block = log_sync_manager
                            .block_hash_cache
                            .read()
                            .await
                            .get(&start_block_number)
                            .cloned();
                        if let Some(b) = block {
                            // special case avoid reorg
                            if let Some(submission_idx) = b.as_ref().unwrap().first_submission_index
                            {
                                log_sync_manager.process_reverted(submission_idx).await;
                            }
                        }
                    }

                    let parent_block_hash = if start_block_number >= finalized_block_number {
                        // No need to catch up data.
                        if start_block_number > 0 {
                            let parent_block_number = start_block_number.saturating_sub(1);
                            match log_sync_manager
                                .block_hash_cache
                                .read()
                                .await
                                .get(&parent_block_number)
                            {
                                Some(b) => b.as_ref().unwrap().block_hash,
                                _ => {
                                    log_sync_manager
                                        .get_block(parent_block_number.into())
                                        .await?
                                        .1
                                }
                            }
                        } else {
                            start_block_hash
                        }
                    } else {
                        // Keep catching-up data until we are close to the latest height.
                        loop {
                            // wait tx receipt is ready
                            if let Ok(Some(block)) = log_sync_manager
                                .log_fetcher
                                .provider()
                                .get_block_with_txs(finalized_block_number)
                                .await
                            {
                                if let Some(tx) = block.transactions.first() {
                                    loop {
                                        match log_sync_manager
                                            .log_fetcher
                                            .provider()
                                            .get_transaction_receipt(tx.hash)
                                            .await
                                        {
                                            Ok(Some(_)) => break,
                                            _ => {
                                                tokio::time::sleep(Duration::from_secs(1)).await;
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }

                            while let Err(e) = log_sync_manager
                                .catch_up_data(
                                    executor_clone.clone(),
                                    start_block_number,
                                    finalized_block_number,
                                )
                                .await
                            {
                                match e {
                                    HandleDataError::SeqError(block_number) => {
                                        warn!("seq error occurred, retry from {}", block_number);
                                        start_block_number = block_number;
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                    }
                                    _ => {
                                        return Err(e.into());
                                    }
                                }
                            }

                            start_block_number = finalized_block_number.saturating_add(1);

                            let new_finalized_block =
                                log_sync_manager.get_block(BlockNumber::Finalized).await?;
                            if new_finalized_block.0.saturating_sub(finalized_block_number)
                                <= CATCH_UP_END_GAP
                            {
                                break finalized_block_hash;
                            }
                            finalized_block_number = new_finalized_block.0;
                            finalized_block_hash = new_finalized_block.1;
                        }
                    };

                    if catch_up_end_sender.send(()).is_err() {
                        warn!("catch_up_end send fails, possibly auto_sync is not enabled");
                    }

                    log_sync_manager
                        .log_fetcher
                        .start_remove_finalized_block_task(
                            &executor_clone,
                            log_sync_manager.store.clone(),
                            log_sync_manager.block_hash_cache.clone(),
                            log_sync_manager.config.default_finalized_block_count,
                            log_sync_manager
                                .config
                                .remove_finalized_block_interval_minutes,
                        );

                    let (watch_progress_tx, watch_progress_rx) =
                        tokio::sync::mpsc::unbounded_channel();
                    let watch_rx = log_sync_manager.log_fetcher.start_watch(
                        start_block_number,
                        parent_block_hash,
                        &executor_clone,
                        log_sync_manager.block_hash_cache.clone(),
                        log_sync_manager.config.watch_loop_wait_time_ms,
                        watch_progress_rx,
                    );
                    // Syncing `watch_rx` is supposed to block forever.
                    log_sync_manager
                        .handle_data(watch_rx, &Some(watch_progress_tx))
                        .await?;
                    Ok::<(), anyhow::Error>(())
                },
            )
            .map(|_| ()),
            "log_sync",
        );
        Ok((event_send_cloned, catch_up_end_receiver))
    }

    async fn put_tx(&mut self, tx: KVTransaction) -> Option<bool> {
        // We call this after process chain reorg, so the sequence number should match.
        match tx.transaction.seq.cmp(&self.next_tx_seq) {
            std::cmp::Ordering::Less => Some(true),
            std::cmp::Ordering::Equal => {
                debug!("log entry sync get entry: {:?}", tx);
                Some(self.put_tx_inner(tx).await)
            }
            std::cmp::Ordering::Greater => {
                error!(
                    "Unexpected transaction seq: next={} get={}",
                    self.next_tx_seq, tx.transaction.seq
                );
                None
            }
        }
    }

    /// `tx_seq` is the first reverted tx seq.
    async fn process_reverted(&mut self, tx_seq: u64) {
        warn!("revert for chain reorg: seq={}", tx_seq);

        let _ = self.event_send.send(LogSyncEvent::ReorgDetected { tx_seq });

        // TODO(zz): `wrapping_sub` here is a hack to handle the case of tx_seq=0.
        if let Err(e) = self
            .store
            .write()
            .await
            .revert_stream(tx_seq.wrapping_sub(1))
            .await
        {
            error!("revert_to fails: e={:?}", e);
            return;
        }
        self.next_tx_seq = tx_seq;

        let _ = self.event_send.send(LogSyncEvent::Reverted { tx_seq });
    }

    async fn handle_data(
        &mut self,
        mut rx: UnboundedReceiver<LogFetchProgress>,
        watch_progress_tx: &Option<UnboundedSender<u64>>,
    ) -> Result<(), HandleDataError> {
        let mut log_latest_block_number =
            if let Some(block_number) = self.store.read().await.get_log_latest_block_number()? {
                block_number
            } else {
                0
            };

        while let Some(data) = rx.recv().await {
            debug!("handle_data: data={:?}", data);
            match data {
                LogFetchProgress::SyncedBlock((
                    block_number,
                    block_hash,
                    first_submission_index,
                )) => {
                    if first_submission_index.is_some() {
                        self.block_hash_cache.write().await.insert(
                            block_number,
                            Some(BlockHashAndSubmissionIndex {
                                block_hash,
                                first_submission_index: first_submission_index.unwrap(),
                            }),
                        );
                    }

                    self.store.write().await.put_sync_progress((
                        block_number,
                        block_hash,
                        first_submission_index,
                    ))?;

                    match self.log_fetcher.provider().get_block(block_number).await {
                        Ok(Some(b)) => {
                            if b.number != Some(block_number.into()) {
                                error!(
                                    "block number not match, reorg possible happened, block number {:?}, received {}", b.number, block_number 
                                );
                            } else if b.hash != Some(block_hash) {
                                error!("block hash not match, reorg possible happened, block hash {:?}, received {}", b.hash, block_hash);
                            }
                        }
                        e => {
                            error!("log put progress check rpc fails, e={:?}", e);
                        }
                    }
                }
                LogFetchProgress::Transaction((tx, block_number)) => {
                    let mut stop = false;
                    match self.put_tx(tx.clone()).await {
                        Some(false) => stop = true,
                        Some(true) => {
                            if let Err(e) = self
                                .store
                                .write()
                                .await
                                .put_log_latest_block_number(block_number)
                            {
                                warn!("failed to put log latest block number, error={:?}", e);
                            }

                            log_latest_block_number = block_number;
                        }
                        _ => {
                            stop = true;
                            if let Some(progress_tx) = watch_progress_tx {
                                if let Err(e) = progress_tx.send(log_latest_block_number) {
                                    error!("failed to send watch progress, error={:?}", e);
                                } else {
                                    continue;
                                }
                            } else {
                                return Err(HandleDataError::SeqError(log_latest_block_number));
                            }
                        }
                    }

                    if stop {
                        // Unexpected error.
                        return Err(anyhow!("log sync write error").into());
                    }
                }
                LogFetchProgress::Reverted(reverted) => {
                    self.process_reverted(reverted).await;
                }
            }
        }
        Ok(())
    }

    async fn put_tx_inner(&mut self, tx: KVTransaction) -> bool {
        let start_time = Instant::now();
        let result = self.store.write().await.put_tx(tx.clone());
        metrics::STORE_PUT_TX.update_since(start_time);

        if let Err(e) = result {
            error!("put_tx error: e={:?}", e);
            false
        } else {
            self.next_tx_seq += 1;

            // Check if the computed data root matches on-chain state.
            // If the call fails, we won't check the root here and return `true` directly.
            let flow_contract = self.log_fetcher.flow_contract();
            match flow_contract
                .get_flow_root_by_tx_seq(tx.transaction.seq.into())
                .call()
                .await
            {
                Ok(contract_root_bytes) => {
                    let contract_root = H256::from_slice(&contract_root_bytes);
                    // contract_root is zero for tx submitted before upgrading.
                    if !contract_root.is_zero() {
                        match self.store.read().await.get_context() {
                            Ok((local_root, _)) => {
                                if contract_root != local_root {
                                    error!(
                                        ?contract_root,
                                        ?local_root,
                                        "local flow root and on-chain flow root mismatch"
                                    );
                                    return false;
                                }
                            }
                            Err(e) => {
                                warn!(?e, "fail to read the local flow root");
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(?e, "fail to read the on-chain flow root");
                }
            }

            true
        }
    }

    async fn get_block(&self, block_number: BlockNumber) -> Result<(u64, H256)> {
        let block = match self.log_fetcher.provider().get_block(block_number).await {
            Ok(Some(block)) => block,
            Ok(None) => {
                bail!("None for block {}", block_number);
            }
            e => {
                bail!("unable to get block: {:?}", e);
            }
        };
        Ok((
            block
                .number
                .ok_or_else(|| anyhow!("None block number for finalized block"))?
                .as_u64(),
            block
                .hash
                .ok_or_else(|| anyhow!("None block hash for finalized block"))?,
        ))
    }

    /// Return the ending block number and the parent block hash.
    async fn catch_up_data(
        &mut self,
        executor_clone: TaskExecutor,
        start_block_number: u64,
        finalized_block_number: u64,
    ) -> Result<(), HandleDataError> {
        if start_block_number < finalized_block_number {
            let recover_rx = self.log_fetcher.start_recover(
                start_block_number,
                finalized_block_number,
                &executor_clone,
                Duration::from_millis(self.config.recover_query_delay),
            );
            self.handle_data(recover_rx, &None).await?;
        }
        Ok(())
    }
}

async fn get_start_block_number_with_hash(
    log_sync_manager: &LogSyncManager,
) -> Result<(u64, H256), anyhow::Error> {
    if log_sync_manager
        .config
        .force_log_sync_from_start_block_number
    {
        let block_number = log_sync_manager.config.start_block_number;
        let block_hash = log_sync_manager.get_block(block_number.into()).await?.1;
        return Ok((block_number, block_hash));
    }

    if let Some(block_number) = log_sync_manager
        .store
        .read()
        .await
        .get_log_latest_block_number()?
    {
        if let Some(Some(val)) = log_sync_manager
            .block_hash_cache
            .read()
            .await
            .get(&block_number)
        {
            return Ok((block_number, val.block_hash));
        } else {
            warn!("get block hash for block {} from RPC", block_number);
            let block_hash = log_sync_manager.get_block(block_number.into()).await?.1;
            return Ok((block_number, block_hash));
        }
    }

    let (start_block_number, start_block_hash) =
        match log_sync_manager.store.read().await.get_sync_progress()? {
            // No previous progress, so just use config.
            None => {
                let block_number = log_sync_manager.config.start_block_number;
                let block_hash = log_sync_manager.get_block(block_number.into()).await?.1;
                (block_number, block_hash)
            }
            Some((block_number, block_hash)) => (block_number, block_hash),
        };

    Ok((start_block_number, start_block_hash))
}

async fn run_and_log<R, E>(
    mut on_error: impl FnMut(),
    f: impl Future<Output = std::result::Result<R, E>> + Send,
) -> Option<R>
where
    E: Debug,
{
    match f.await {
        Err(e) => {
            error!("log sync failure: e={:?}", e);
            on_error();
            None
        }
        Ok(r) => Some(r),
    }
}

pub(crate) mod config;
mod log_entry_fetcher;
mod log_query;
mod metrics;
