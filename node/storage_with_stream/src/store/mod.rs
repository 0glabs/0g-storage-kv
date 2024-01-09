use std::sync::Arc;

use async_trait::async_trait;
use ethereum_types::{H160, H256};
use shared_types::{AccessControlSet, KeyValuePair, StreamWriteSet, Transaction};
use storage::log_store::config::Configurable;
use storage::log_store::{LogStoreRead, LogStoreWrite};

use crate::error::Result;

mod sqlite_db_statements;
pub mod store_manager;
mod stream_store;

pub use stream_store::to_access_control_op_name;
pub use stream_store::AccessControlOps;

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

    async fn revert_stream(&mut self, tx_seq: u64) -> Result<Vec<Transaction>>;
}
