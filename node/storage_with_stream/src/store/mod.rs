use std::sync::Arc;

use async_trait::async_trait;
use ethereum_types::{H160, H256};
use kv_types::{AccessControlSet, KVTransaction, KeyValuePair, StreamWriteSet};
use shared_types::ChunkArrayWithProof;
use shared_types::ChunkWithProof;
use shared_types::DataRoot;
use shared_types::FlowRangeProof;
use storage::log_store::config::Configurable;
use storage::log_store::tx_store::BlockHashAndSubmissionIndex;
use storage::log_store::LogStoreChunkRead;
use storage::log_store::LogStoreChunkWrite;

use crate::error::Result;

mod metadata_store;
mod sqlite_db_statements;
pub mod store_manager;
mod stream_store;

pub use stream_store::to_access_control_op_name;
pub use stream_store::AccessControlOps;

pub trait LogStoreRead: LogStoreChunkRead {
    /// Get a transaction by its global log sequence number.
    fn get_tx_by_seq_number(&self, seq: u64) -> Result<Option<KVTransaction>>;

    /// Get a transaction by the data root of its data.
    fn get_tx_seq_by_data_root(&self, data_root: &DataRoot) -> Result<Option<u64>>;

    fn get_tx_by_data_root(&self, data_root: &DataRoot) -> Result<Option<KVTransaction>> {
        match self.get_tx_seq_by_data_root(data_root)? {
            Some(seq) => self.get_tx_by_seq_number(seq),
            None => Ok(None),
        }
    }

    fn get_chunk_with_proof_by_tx_and_index(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> Result<Option<ChunkWithProof>>;

    fn get_chunks_with_proof_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> Result<Option<ChunkArrayWithProof>>;

    fn check_tx_completed(&self, tx_seq: u64) -> Result<bool>;

    fn next_tx_seq(&self) -> u64;

    fn get_sync_progress(&self) -> Result<Option<(u64, H256)>>;

    fn get_block_hash_by_number(&self, block_number: u64) -> Result<Option<(H256, Option<u64>)>>;

    fn get_block_hashes(&self) -> Result<Vec<(u64, BlockHashAndSubmissionIndex)>>;

    fn validate_range_proof(&self, tx_seq: u64, data: &ChunkArrayWithProof) -> Result<bool>;

    fn get_proof_at_root(&self, root: &DataRoot, index: u64, length: u64)
        -> Result<FlowRangeProof>;

    /// Return flow root and length.
    fn get_context(&self) -> Result<(DataRoot, u64)>;
}

pub trait LogStoreWrite: LogStoreChunkWrite {
    /// Store a data entry metadata.
    fn put_tx(&mut self, tx: KVTransaction) -> Result<()>;

    /// Finalize a transaction storage.
    /// This will compute and the merkle tree, check the data root, and persist a part of the merkle
    /// tree for future queries.
    ///
    /// This will return error if not all chunks are stored. But since this check can be expensive,
    /// the caller is supposed to track chunk statuses and call this after storing all the chunks.
    fn finalize_tx(&mut self, tx_seq: u64) -> Result<()>;
    fn finalize_tx_with_hash(&mut self, tx_seq: u64, tx_hash: H256) -> Result<bool>;

    /// Store the progress of synced block number and its hash.
    fn put_sync_progress(&self, progress: (u64, H256, Option<Option<u64>>)) -> Result<()>;

    /// Revert the log state to a given tx seq.
    /// This is needed when transactions are reverted because of chain reorg.
    ///
    /// Reverted transactions are returned in order.
    fn revert_to(&mut self, tx_seq: u64) -> Result<()>;

    /// If the proof is valid, fill the tree nodes with the new data.
    fn validate_and_insert_range_proof(
        &mut self,
        tx_seq: u64,
        data: &ChunkArrayWithProof,
    ) -> Result<bool>;

    fn delete_block_hash_by_number(&self, block_number: u64) -> Result<()>;
}

pub trait Store:
    LogStoreRead + LogStoreWrite + Configurable + Send + Sync + StreamRead + StreamWrite + 'static
{
}
impl<
        T: LogStoreRead
            + LogStoreWrite
            + Configurable
            + Send
            + Sync
            + StreamRead
            + StreamWrite
            + 'static,
    > Store for T
{
}

#[async_trait]
pub trait StreamRead {
    async fn get_holding_stream_ids(&self) -> Result<Vec<H256>>;

    async fn get_stream_data_sync_progress(&self) -> Result<u64>;

    async fn get_stream_replay_progress(&self) -> Result<u64>;

    async fn get_latest_version_before(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        before: u64,
    ) -> Result<u64>;

    async fn has_write_permission(
        &self,
        account: H160,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        version: u64,
    ) -> Result<bool>;

    async fn is_new_stream(&self, stream_id: H256, version: u64) -> Result<bool>;

    async fn is_admin(&self, account: H160, stream_id: H256, version: u64) -> Result<bool>;

    async fn is_special_key(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        version: u64,
    ) -> Result<bool>;

    async fn is_writer_of_key(
        &self,
        account: H160,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        version: u64,
    ) -> Result<bool>;

    async fn is_writer_of_stream(
        &self,
        account: H160,
        stream_id: H256,
        version: u64,
    ) -> Result<bool>;

    async fn get_stream_key_value(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        version: u64,
    ) -> Result<Option<KeyValuePair>>;

    async fn get_next_stream_key_value(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        inclusive: bool,
        version: u64,
    ) -> Result<Option<KeyValuePair>>;

    async fn get_prev_stream_key_value(
        &self,
        stream_id: H256,
        key: Arc<Vec<u8>>,
        inclusive: bool,
        version: u64,
    ) -> Result<Option<KeyValuePair>>;

    async fn get_first(&self, stream_id: H256, version: u64) -> Result<Option<KeyValuePair>>;

    async fn get_last(&self, stream_id: H256, version: u64) -> Result<Option<KeyValuePair>>;
}

#[async_trait]
pub trait StreamWrite {
    async fn reset_stream_sync(&self, stream_ids: Vec<u8>) -> Result<()>;

    async fn update_stream_ids(&self, stream_ids: Vec<u8>) -> Result<()>;

    async fn update_stream_data_sync_progress(&self, from: u64, progress: u64) -> Result<u64>;

    async fn update_stream_replay_progress(&self, from: u64, progress: u64) -> Result<u64>;

    async fn put_stream(
        &self,
        tx_seq: u64,
        data_merkle_root: H256,
        result: String,
        commit_data: Option<(StreamWriteSet, AccessControlSet)>,
    ) -> Result<()>;

    async fn get_tx_result(&self, tx_seq: u64) -> Result<Option<String>>;

    async fn revert_stream(&mut self, tx_seq: u64) -> Result<()>;
}
