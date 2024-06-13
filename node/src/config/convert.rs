#![allow(clippy::field_reassign_with_default)]

use std::{collections::HashSet, str::FromStr};

use crate::ZgsKVConfig;
use ethereum_types::H256;
use http::Uri;
use log_entry_sync::{CacheConfig, ContractAddress, LogSyncConfig};
use rpc::RPCConfig;
use storage_with_stream::{LogStorageConfig, StorageConfig};
use stream::StreamConfig;

impl ZgsKVConfig {
    pub fn storage_config(&self) -> Result<StorageConfig, String> {
        Ok(StorageConfig {
            log_config: LogStorageConfig {
                db_dir: self.db_dir.clone().into(),
            },
            kv_db_file: self.kv_db_file.clone().into(),
        })
    }

    pub fn stream_config(&self) -> Result<StreamConfig, String> {
        let mut stream_ids: Vec<H256> = vec![];
        for id in &self.stream_ids {
            stream_ids.push(
                H256::from_str(id)
                    .map_err(|e| format!("Unable to parse stream id: {:?}, error: {:?}", id, e))?,
            );
        }
        stream_ids.sort();
        stream_ids.dedup();
        if stream_ids.is_empty() {
            error!("{}", format!("stream ids is empty"))
        }
        let stream_set = HashSet::from_iter(stream_ids.iter().cloned());
        Ok(StreamConfig {
            stream_ids,
            stream_set,
        })
    }

    pub fn rpc_config(&self) -> Result<RPCConfig, String> {
        let listen_address = self
            .rpc_listen_address
            .parse::<std::net::SocketAddr>()
            .map_err(|e| format!("Unable to parse rpc_listen_address: {:?}", e))?;

        Ok(RPCConfig {
            enabled: self.rpc_enabled,
            listen_address,
            chunks_per_segment: self.rpc_chunks_per_segment,
            zgs_nodes: to_zgs_nodes(self.zgs_node_urls.clone())
                .map_err(|e| format!("failed to parse zgs_node_urls: {}", e))?,
            admin_node_address: if self.zgs_admin_url.is_empty() {
                None
            } else {
                self.zgs_admin_url
                    .parse::<Uri>()
                    .map_err(|e| format!("Invalid URL: {}", e))?;
                Some(self.zgs_admin_url.clone())
            },
            max_query_len_in_bytes: self.max_query_len_in_bytes,
            max_response_body_in_bytes: self.max_response_body_in_bytes,
            zgs_rpc_timeout: self.zgs_rpc_timeout,
        })
    }

    pub fn log_sync_config(&self) -> Result<LogSyncConfig, String> {
        let contract_address = self
            .log_contract_address
            .parse::<ContractAddress>()
            .map_err(|e| format!("Unable to parse log_contract_address: {:?}", e))?;
        let cache_config = CacheConfig {
            // 100 MB.
            max_data_size: self.max_cache_data_size,
            // This should be enough if we have about one Zgs tx per block.
            tx_seq_ttl: self.cache_tx_seq_ttl,
        };
        Ok(LogSyncConfig::new(
            self.blockchain_rpc_endpoint.clone(),
            contract_address,
            self.log_sync_start_block_number,
            self.confirmation_block_count,
            cache_config,
            self.log_page_size,
            self.rate_limit_retries,
            self.timeout_retries,
            self.initial_backoff,
            self.recover_query_delay,
            self.default_finalized_block_count,
            self.remove_finalized_block_interval_minutes,
            self.watch_loop_wait_time_ms,
        ))
    }
}

pub fn to_zgs_nodes(zgs_node_urls: String) -> Result<Vec<String>, String> {
    if zgs_node_urls.is_empty() {
        return Err("zgs_node_urls is empty".to_string());
    }

    zgs_node_urls
        .split(',')
        .map(|url| {
            url.parse::<Uri>()
                .map_err(|e| format!("Invalid URL: {}", e))?;

            Ok(url.to_owned())
        })
        .collect()
}
