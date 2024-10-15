use crate::try_option;
use anyhow::{Error, Result};
use async_trait::async_trait;
use ethereum_types::{H160, H256};
use kv_types::{AccessControlSet, KVTransaction, KeyValuePair, StreamWriteSet};
use shared_types::{
    Chunk, ChunkArray, ChunkArrayWithProof, ChunkWithProof, DataRoot, FlowProof, FlowRangeProof,
};
use std::path::Path;
use std::sync::Arc;

use storage::log_store::config::Configurable;
use storage::log_store::log_manager::LogConfig;
use storage::log_store::tx_store::BlockHashAndSubmissionIndex;
use storage::log_store::{
    LogStoreChunkRead, LogStoreChunkWrite, LogStoreRead as _, LogStoreWrite as _,
};
use storage::LogManager;
use tracing::instrument;

use super::metadata_store::MetadataStore;
use super::stream_store::StreamStore;
use super::{LogStoreRead, LogStoreWrite, StreamRead, StreamWrite};

/// 256 Bytes
pub const ENTRY_SIZE: usize = 256;
/// 1024 Entries.
pub const PORA_CHUNK_SIZE: usize = 1024;

pub struct StoreManager {
    metadata_store: MetadataStore,
    log_store: LogManager,
    stream_store: StreamStore,
}

impl LogStoreChunkWrite for StoreManager {
    fn put_chunks(&self, tx_seq: u64, chunks: ChunkArray) -> Result<()> {
        self.log_store.put_chunks(tx_seq, chunks)
    }

    fn put_chunks_with_tx_hash(
        &self,
        tx_seq: u64,
        tx_hash: H256,
        chunks: ChunkArray,
        maybe_file_proof: Option<FlowProof>,
    ) -> storage::error::Result<bool> {
        self.log_store
            .put_chunks_with_tx_hash(tx_seq, tx_hash, chunks, maybe_file_proof)
    }

    fn remove_chunks_batch(&self, batch_list: &[u64]) -> storage::error::Result<()> {
        self.log_store.remove_chunks_batch(batch_list)
    }
}

impl LogStoreWrite for StoreManager {
    #[instrument(skip(self))]
    fn put_tx(&mut self, tx: KVTransaction) -> Result<()> {
        self.metadata_store
            .put_metadata(tx.transaction.seq, tx.metadata)?;
        self.log_store.put_tx(tx.transaction)
    }

    fn finalize_tx(&mut self, tx_seq: u64) -> Result<()> {
        self.log_store.finalize_tx(tx_seq)
    }

    fn finalize_tx_with_hash(
        &mut self,
        tx_seq: u64,
        tx_hash: H256,
    ) -> storage::error::Result<bool> {
        self.log_store.finalize_tx_with_hash(tx_seq, tx_hash)
    }

    fn put_sync_progress(&self, progress: (u64, H256, Option<Option<u64>>)) -> Result<()> {
        self.log_store.put_sync_progress(progress)
    }

    fn revert_to(&mut self, tx_seq: u64) -> Result<()> {
        self.log_store.revert_to(tx_seq)?;
        Ok(())
    }

    fn validate_and_insert_range_proof(
        &mut self,
        tx_seq: u64,
        data: &ChunkArrayWithProof,
    ) -> storage::error::Result<bool> {
        self.log_store.validate_and_insert_range_proof(tx_seq, data)
    }

    fn delete_block_hash_by_number(&self, block_number: u64) -> Result<()> {
        self.log_store.delete_block_hash_by_number(block_number)
    }

    fn put_log_latest_block_number(&self, block_number: u64) -> Result<()> {
        self.log_store.put_log_latest_block_number(block_number)
    }
}

impl LogStoreChunkRead for StoreManager {
    fn get_chunk_by_tx_and_index(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> crate::error::Result<Option<Chunk>> {
        self.log_store.get_chunk_by_tx_and_index(tx_seq, index)
    }

    fn get_chunks_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> crate::error::Result<Option<ChunkArray>> {
        self.log_store
            .get_chunks_by_tx_and_index_range(tx_seq, index_start, index_end)
    }

    fn get_chunk_by_data_root_and_index(
        &self,
        data_root: &DataRoot,
        index: usize,
    ) -> crate::error::Result<Option<Chunk>> {
        self.log_store
            .get_chunk_by_data_root_and_index(data_root, index)
    }

    fn get_chunks_by_data_root_and_index_range(
        &self,
        data_root: &DataRoot,
        index_start: usize,
        index_end: usize,
    ) -> crate::error::Result<Option<ChunkArray>> {
        self.log_store
            .get_chunks_by_data_root_and_index_range(data_root, index_start, index_end)
    }

    fn get_chunk_index_list(&self, tx_seq: u64) -> crate::error::Result<Vec<usize>> {
        self.log_store.get_chunk_index_list(tx_seq)
    }

    fn get_chunk_by_flow_index(
        &self,
        index: u64,
        length: u64,
    ) -> crate::error::Result<Option<ChunkArray>> {
        self.log_store.get_chunk_by_flow_index(index, length)
    }
}

impl LogStoreRead for StoreManager {
    fn get_tx_by_seq_number(&self, seq: u64) -> crate::error::Result<Option<KVTransaction>> {
        Ok(Some(KVTransaction {
            transaction: try_option!(self.log_store.get_tx_by_seq_number(seq)?),
            metadata: try_option!(self.metadata_store.get_metadata_by_seq_number(seq)?),
        }))
    }

    fn get_tx_seq_by_data_root(&self, data_root: &DataRoot) -> crate::error::Result<Option<u64>> {
        self.log_store.get_tx_seq_by_data_root(data_root)
    }

    fn get_chunk_with_proof_by_tx_and_index(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> crate::error::Result<Option<ChunkWithProof>> {
        self.log_store
            .get_chunk_with_proof_by_tx_and_index(tx_seq, index)
    }

    fn get_chunks_with_proof_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> crate::error::Result<Option<ChunkArrayWithProof>> {
        self.log_store.get_chunks_with_proof_by_tx_and_index_range(
            tx_seq,
            index_start,
            index_end,
            None,
        )
    }

    fn check_tx_completed(&self, tx_seq: u64) -> crate::error::Result<bool> {
        self.log_store.check_tx_completed(tx_seq)
    }

    fn validate_range_proof(&self, tx_seq: u64, data: &ChunkArrayWithProof) -> Result<bool> {
        self.log_store.validate_range_proof(tx_seq, data)
    }

    fn get_sync_progress(&self) -> Result<Option<(u64, H256)>> {
        self.log_store.get_sync_progress()
    }

    fn get_block_hash_by_number(&self, block_number: u64) -> Result<Option<(H256, Option<u64>)>> {
        self.log_store.get_block_hash_by_number(block_number)
    }

    fn get_block_hashes(&self) -> Result<Vec<(u64, BlockHashAndSubmissionIndex)>> {
        self.log_store.get_block_hashes()
    }

    fn next_tx_seq(&self) -> u64 {
        self.log_store.next_tx_seq()
    }

    fn get_proof_at_root(
        &self,
        root: &DataRoot,
        index: u64,
        length: u64,
    ) -> Result<FlowRangeProof> {
        self.log_store.get_proof_at_root(Some(*root), index, length)
    }

    fn get_context(&self) -> Result<(DataRoot, u64)> {
        self.log_store.get_context()
    }

    fn get_log_latest_block_number(&self) -> storage::error::Result<Option<u64>> {
        self.log_store.get_log_latest_block_number()
    }
}

impl Configurable for StoreManager {
    fn get_config(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.log_store.get_config(key)
    }

    fn set_config(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.log_store.set_config(key, value)
    }

    fn remove_config(&self, key: &[u8]) -> Result<()> {
        self.log_store.remove_config(key)
    }

    fn exec_configs(&self, tx: storage::log_store::config::ConfigTx) -> Result<()> {
        self.log_store.exec_configs(tx)
    }
}

#[async_trait]
impl StreamRead for StoreManager {
    async fn get_holding_stream_ids(&self) -> crate::error::Result<Vec<H256>> {
        self.stream_store.get_stream_ids().await
    }

    async fn get_stream_data_sync_progress(&self) -> Result<u64> {
        self.stream_store.get_stream_data_sync_progress().await
    }

    async fn get_stream_replay_progress(&self) -> Result<u64> {
        self.stream_store.get_stream_replay_progress().await
    }

    async fn get_latest_version_before(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        before: u64,
    ) -> Result<u64> {
        self.stream_store
            .get_latest_version_before(stream_id, key, before)
            .await
    }

    async fn has_write_permission(
        &self,
        account: H160,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        version: u64,
    ) -> Result<bool> {
        self.stream_store
            .has_write_permission(account, stream_id, key, version)
            .await
    }

    async fn can_write(&self, account: H160, stream_id: H256, version: u64) -> Result<bool> {
        self.stream_store
            .can_write(account, stream_id, version)
            .await
    }

    async fn is_new_stream(&self, stream_id: H256, version: u64) -> Result<bool> {
        self.stream_store.is_new_stream(stream_id, version).await
    }

    async fn is_admin(&self, account: H160, stream_id: H256, version: u64) -> Result<bool> {
        self.stream_store
            .is_admin(account, stream_id, version)
            .await
    }

    async fn is_special_key(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        version: u64,
    ) -> Result<bool> {
        self.stream_store
            .is_special_key(stream_id, key, version)
            .await
    }

    async fn is_writer_of_key(
        &self,
        account: H160,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        version: u64,
    ) -> Result<bool> {
        self.stream_store
            .is_writer_of_key(account, stream_id, key, version)
            .await
    }

    async fn is_writer_of_stream(
        &self,
        account: H160,
        stream_id: H256,
        version: u64,
    ) -> Result<bool> {
        self.stream_store
            .is_writer_of_stream(account, stream_id, version)
            .await
    }

    async fn get_stream_key_value(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        version: u64,
    ) -> Result<Option<KeyValuePair>> {
        self.stream_store
            .get_stream_key_value(stream_id, key, version)
            .await
    }

    async fn get_next_stream_key_value(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        inclusive: bool,
        version: u64,
    ) -> Result<Option<KeyValuePair>> {
        self.stream_store
            .get_next_stream_key_value(stream_id, key, version, inclusive)
            .await
    }

    async fn get_prev_stream_key_value(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        inclusive: bool,
        version: u64,
    ) -> Result<Option<KeyValuePair>> {
        self.stream_store
            .get_prev_stream_key_value(stream_id, key, version, inclusive)
            .await
    }

    async fn get_first(&self, stream_id: H256, version: u64) -> Result<Option<KeyValuePair>> {
        self.stream_store.get_first(stream_id, version).await
    }

    async fn get_last(&self, stream_id: H256, version: u64) -> Result<Option<KeyValuePair>> {
        self.stream_store.get_last(stream_id, version).await
    }
}

#[async_trait]
impl StreamWrite for StoreManager {
    async fn reset_stream_sync(&self, stream_ids: Vec<u8>) -> Result<()> {
        self.stream_store.reset_stream_sync(stream_ids).await
    }

    async fn update_stream_ids(&self, stream_ids: Vec<u8>) -> Result<()> {
        self.stream_store.update_stream_ids(stream_ids).await
    }

    // update the progress and return the next tx_seq to sync
    async fn update_stream_data_sync_progress(&self, from: u64, progress: u64) -> Result<u64> {
        if self
            .stream_store
            .update_stream_data_sync_progress(from, progress)
            .await?
            > 0
        {
            Ok(progress)
        } else {
            Ok(self.stream_store.get_stream_data_sync_progress().await?)
        }
    }

    // update the progress and return the next tx_seq to replay
    async fn update_stream_replay_progress(&self, from: u64, progress: u64) -> Result<u64> {
        if self
            .stream_store
            .update_stream_replay_progress(from, progress)
            .await?
            > 0
        {
            Ok(progress)
        } else {
            Ok(self.stream_store.get_stream_replay_progress().await?)
        }
    }

    async fn put_stream(
        &self,
        tx_seq: u64,
        data_merkle_root: H256,
        result: String,
        commit_data: Option<(StreamWriteSet, AccessControlSet)>,
    ) -> Result<()> {
        match self.log_store.get_tx_by_seq_number(tx_seq) {
            Ok(Some(tx)) => {
                if tx.data_merkle_root != data_merkle_root {
                    return Err(Error::msg("data merkle root deos not match"));
                }
            }
            _ => {
                return Err(Error::msg("tx does not found"));
            }
        }

        self.stream_store
            .put_stream(tx_seq, result, commit_data)
            .await
    }

    async fn get_tx_result(&self, tx_seq: u64) -> Result<Option<String>> {
        self.stream_store.get_tx_result(tx_seq).await
    }

    async fn revert_stream(&mut self, tx_seq: u64) -> Result<()> {
        self.stream_store.revert_to(tx_seq).await?;
        self.log_store.revert_to(tx_seq)?;
        Ok(())
    }
}

impl StoreManager {
    pub async fn memorydb(
        config: LogConfig,
        executor: task_executor::TaskExecutor,
    ) -> Result<Self> {
        let stream_store = StreamStore::new_in_memory().await?;
        stream_store.create_tables_if_not_exist().await?;
        Ok(Self {
            metadata_store: MetadataStore::memorydb(),
            log_store: LogManager::memorydb(config, executor)?,
            stream_store,
        })
    }

    pub async fn rocks_db(
        config: LogConfig,
        path: impl AsRef<Path>,
        kv_db_file: impl AsRef<Path>,
        executor: task_executor::TaskExecutor,
    ) -> Result<Self> {
        let stream_store = StreamStore::new(kv_db_file.as_ref()).await?;
        stream_store.create_tables_if_not_exist().await?;
        Ok(Self {
            metadata_store: MetadataStore::rocksdb(path.as_ref().join("metadata"))?,
            log_store: LogManager::rocksdb(config, path.as_ref().join("log"), executor)?,
            stream_store,
        })
    }
}

#[macro_export]
macro_rules! try_option {
    ($r: ident) => {
        match $r {
            Some(v) => v,
            None => return Ok(None),
        }
    };
    ($e: expr) => {
        match $e {
            Some(v) => v,
            None => return Ok(None),
        }
    };
}
