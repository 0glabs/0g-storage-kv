use anyhow::{Error, Result};
use async_trait::async_trait;
use ethereum_types::{H160, H256};
use kv_types::{AccessControlSet, KVTransaction, KeyValuePair, StreamWriteSet};
use shared_types::{
    ChunkArray, FlowProof,
};
use std::path::Path;
use std::sync::Arc;


use storage::log_store::tx_store::BlockHashAndSubmissionIndex;
use tracing::instrument;

use super::data_store::DataStore;
use super::stream_store::StreamStore;
use super::{DataStoreRead, DataStoreWrite, StreamRead, StreamWrite};

/// 256 Bytes
pub const ENTRY_SIZE: usize = 256;
/// 1024 Entries.
pub const PORA_CHUNK_SIZE: usize = 1024;

pub struct StoreManager {
    data_store: DataStore,
    stream_store: StreamStore,
}

impl DataStoreWrite for StoreManager {
    #[instrument(skip(self))]
    fn put_tx(&mut self, tx: KVTransaction) -> Result<()> {
        self.data_store.put_tx(tx)
    }

    fn finalize_tx_with_hash(
        &mut self,
        tx_seq: u64,
        tx_hash: H256,
    ) -> storage::error::Result<bool> {
        self.data_store.finalize_tx_with_hash(tx_seq, tx_hash)
    }

    fn put_sync_progress(&self, progress: (u64, H256, Option<Option<u64>>)) -> Result<()> {
        self.data_store.put_sync_progress(progress)
    }

    fn revert_to(&mut self, tx_seq: u64) -> Result<()> {
        self.data_store.revert_to(tx_seq)?;
        Ok(())
    }

    fn delete_block_hash_by_number(&self, block_number: u64) -> Result<()> {
        self.data_store.delete_block_hash_by_number(block_number)
    }

    fn put_log_latest_block_number(&self, block_number: u64) -> Result<()> {
        self.data_store.put_log_latest_block_number(block_number)
    }

    fn put_chunks_with_tx_hash(
        &self,
        tx_seq: u64,
        tx_hash: H256,
        chunks: ChunkArray,
        maybe_file_proof: Option<FlowProof>,
    ) -> Result<bool> {
        self.data_store
            .put_chunks_with_tx_hash(tx_seq, tx_hash, chunks, maybe_file_proof)
    }
}

impl DataStoreRead for StoreManager {
    fn get_tx_by_seq_number(&self, seq: u64) -> Result<Option<KVTransaction>> {
        self.data_store.get_tx_by_seq_number(seq)
    }

    fn check_tx_completed(&self, tx_seq: u64) -> crate::error::Result<bool> {
        self.data_store.check_tx_completed(tx_seq)
    }

    fn get_sync_progress(&self) -> Result<Option<(u64, H256)>> {
        self.data_store.get_sync_progress()
    }

    fn get_block_hashes(&self) -> Result<Vec<(u64, BlockHashAndSubmissionIndex)>> {
        self.data_store.get_block_hashes()
    }

    fn next_tx_seq(&self) -> u64 {
        self.data_store.next_tx_seq()
    }

    fn get_log_latest_block_number(&self) -> storage::error::Result<Option<u64>> {
        self.data_store.get_log_latest_block_number()
    }

    fn get_chunk_by_flow_index(&self, index: u64, length: u64) -> Result<Option<ChunkArray>> {
        self.data_store.get_chunk_by_flow_index(index, length)
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
        match self.data_store.get_tx_by_seq_number(tx_seq) {
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
        self.data_store.revert_to(tx_seq)?;
        Ok(())
    }
}

impl StoreManager {
    pub async fn memorydb() -> Result<Self> {
        let stream_store = StreamStore::new_in_memory().await?;
        stream_store.create_tables_if_not_exist().await?;
        Ok(Self {
            data_store: DataStore::memorydb(),
            stream_store,
        })
    }

    pub async fn rocks_db(path: impl AsRef<Path>, kv_db_file: impl AsRef<Path>) -> Result<Self> {
        let stream_store = StreamStore::new(kv_db_file.as_ref()).await?;
        stream_store.create_tables_if_not_exist().await?;
        Ok(Self {
            data_store: DataStore::rocksdb(path.as_ref())?,
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
