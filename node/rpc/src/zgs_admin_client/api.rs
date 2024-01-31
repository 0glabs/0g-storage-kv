use crate::types::FileSyncInfo;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use std::collections::HashMap;

#[rpc(client, namespace = "admin")]
pub trait ZgsAdmin {
    #[method(name = "startSyncFile")]
    async fn start_sync_file(&self, tx_seq: u64) -> RpcResult<()>;

    #[method(name = "startSyncChunks")]
    async fn start_sync_chunks(
        &self,
        tx_seq: u64,
        start_index: u64,
        end_index: u64, // exclusive
    ) -> RpcResult<()>;

    #[method(name = "getSyncStatus")]
    async fn get_sync_status(&self, tx_seq: u64) -> RpcResult<String>;

    #[method(name = "getSyncInfo")]
    async fn get_sync_info(&self, tx_seq: Option<u64>) -> RpcResult<HashMap<u64, FileSyncInfo>>;
}
