use crate::stream_manager::error::ParseError;
use crate::StreamConfig;
use anyhow::{bail, Result};
use ethereum_types::{H160, H256};
use shared_types::{
    AccessControl, AccessControlSet, StreamRead, StreamReadSet, StreamWrite, StreamWriteSet,
    Transaction,
};
use ssz::Decode;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str;
use std::{cmp, sync::Arc, time::Duration};
use storage_with_stream::error::Error;
use storage_with_stream::log_store::log_manager::ENTRY_SIZE;
use storage_with_stream::store::to_access_control_op_name;
use storage_with_stream::AccessControlOps;
use storage_with_stream::Store;
use tokio::sync::RwLock;

use super::RETRY_WAIT_MS;

const MAX_LOAD_ENTRY_SIZE: u64 = 10;
const STREAM_ID_SIZE: u64 = 32;
const STREAM_KEY_LEN_SIZE: u64 = 3;
const SET_LEN_SIZE: u64 = 4;
const DATA_LEN_SIZE: u64 = 8;
const VERSION_SIZE: u64 = 8;
const ACCESS_CONTROL_OP_TYPE_SIZE: u64 = 1;
const ADDRESS_SIZE: u64 = 20;
const MAX_SIZE_LEN: u32 = 65536;

enum ReplayResult {
    Commit(u64, StreamWriteSet, AccessControlSet),
    DataParseError(String),
    VersionConfliction,
    TagsMismatch,
    WritePermissionDenied(H256, Arc<Vec<u8>>),
    AccessControlPermissionDenied(u8, H256, Arc<Vec<u8>>, H160),
    DataUnavailable,
}

fn truncated_key(key: &[u8]) -> String {
    if key.is_empty() {
        return "NONE".to_owned();
    }
    let key_str = str::from_utf8(key).unwrap_or("UNKNOWN");
    match key_str.char_indices().nth(32) {
        None => key_str.to_owned(),
        Some((idx, _)) => key_str[0..idx].to_owned(),
    }
}

impl fmt::Display for ReplayResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReplayResult::Commit(_, _, _) => write!(f, "Commit"),
            ReplayResult::DataParseError(e) => write!(f, "DataParseError: {}", e),
            ReplayResult::VersionConfliction => write!(f, "VersionConfliction"),
            ReplayResult::TagsMismatch => write!(f, "TagsMismatch"),
            ReplayResult::WritePermissionDenied(stream_id, key) => write!(
                f,
                "WritePermissionDenied: stream: {:?}, key: {}",
                stream_id,
                truncated_key(key),
            ),
            ReplayResult::AccessControlPermissionDenied(op_type, stream_id, key, account) => {
                write!(
                    f,
                    "AccessControlPermissionDenied: operation: {}, stream: {:?}, key: {}, account: {:?}",
                    to_access_control_op_name(*op_type),
                    stream_id,
                    truncated_key(key),
                    account
                )
            }
            ReplayResult::DataUnavailable => write!(f, "DataUnavailable"),
        }
    }
}

struct StreamReader<'a> {
    store: Arc<RwLock<dyn Store>>,
    tx: &'a Transaction,
    tx_size_in_entry: u64,
    current_position: u64, // the index of next entry to read
    buffer: Vec<u8>,       // buffered data
}

impl<'a> StreamReader<'a> {
    pub fn new(store: Arc<RwLock<dyn Store>>, tx: &'a Transaction) -> Self {
        Self {
            store,
            tx,
            tx_size_in_entry: if tx.size % ENTRY_SIZE as u64 == 0 {
                tx.size / ENTRY_SIZE as u64
            } else {
                tx.size / ENTRY_SIZE as u64 + 1
            },
            current_position: 0,
            buffer: vec![],
        }
    }

    pub fn current_position_in_bytes(&self) -> u64 {
        (self.current_position + self.tx.start_entry_index) * (ENTRY_SIZE as u64)
            - (self.buffer.len() as u64)
    }

    async fn load(&mut self, length: u64) -> Result<()> {
        match self
            .store
            .read()
            .await
            .get_chunk_by_flow_index(self.current_position + self.tx.start_entry_index, length)?
        {
            Some(mut x) => {
                self.buffer.append(&mut x.data);
                self.current_position += length;
                Ok(())
            }
            None => {
                bail!(ParseError::PartialDataAvailable);
            }
        }
    }

    // read next ${size} bytes from the stream
    pub async fn next(&mut self, size: u64) -> Result<Vec<u8>> {
        if (self.buffer.len() as u64)
            + (self.tx_size_in_entry - self.current_position) * (ENTRY_SIZE as u64)
            < size
        {
            bail!(ParseError::InvalidData);
        }
        while (self.buffer.len() as u64) < size {
            self.load(cmp::min(
                self.tx_size_in_entry - self.current_position,
                MAX_LOAD_ENTRY_SIZE,
            ))
            .await?;
        }
        Ok(self.buffer.drain(0..(size as usize)).collect())
    }

    pub async fn skip(&mut self, mut size: u64) -> Result<()> {
        if (self.buffer.len() as u64) >= size {
            self.buffer.drain(0..(size as usize));
            return Ok(());
        }
        size -= self.buffer.len() as u64;
        self.buffer.clear();
        let entries_to_skip = size / (ENTRY_SIZE as u64);
        self.current_position += entries_to_skip;
        if self.current_position > self.tx_size_in_entry {
            bail!(ParseError::InvalidData);
        }
        size -= entries_to_skip * (ENTRY_SIZE as u64);
        if size > 0 {
            self.next(size).await?;
        }
        Ok(())
    }
}

pub struct StreamReplayer {
    config: StreamConfig,
    store: Arc<RwLock<dyn Store>>,
}

impl StreamReplayer {
    pub async fn new(config: StreamConfig, store: Arc<RwLock<dyn Store>>) -> Result<Self> {
        Ok(Self { config, store })
    }

    async fn parse_version(&self, stream_reader: &mut StreamReader<'_>) -> Result<u64> {
        Ok(u64::from_be_bytes(
            stream_reader.next(VERSION_SIZE).await?.try_into().unwrap(),
        ))
    }

    async fn parse_key(&self, stream_reader: &mut StreamReader<'_>) -> Result<Vec<u8>> {
        let mut key_size_in_bytes = vec![0x0; (8 - STREAM_KEY_LEN_SIZE) as usize];
        key_size_in_bytes.append(&mut stream_reader.next(STREAM_KEY_LEN_SIZE).await?);
        let key_size = u64::from_be_bytes(key_size_in_bytes.try_into().unwrap());
        // key should not be empty
        if key_size == 0 {
            bail!(ParseError::InvalidData);
        }
        stream_reader.next(key_size).await
    }

    async fn parse_stream_read_set(
        &self,
        stream_reader: &mut StreamReader<'_>,
    ) -> Result<StreamReadSet> {
        let size = u32::from_be_bytes(stream_reader.next(SET_LEN_SIZE).await?.try_into().unwrap());
        if size > MAX_SIZE_LEN {
            bail!(ParseError::ListTooLong);
        }
        let mut stream_read_set = StreamReadSet {
            stream_reads: vec![],
        };
        for _ in 0..(size as usize) {
            stream_read_set.stream_reads.push(StreamRead {
                stream_id: H256::from_ssz_bytes(&stream_reader.next(STREAM_ID_SIZE).await?)
                    .map_err(Error::from)?,
                key: Arc::new(self.parse_key(stream_reader).await?),
            });
        }
        Ok(stream_read_set)
    }

    async fn validate_stream_read_set(
        &self,
        stream_read_set: &StreamReadSet,
        tx: &Transaction,
        version: u64,
    ) -> Result<Option<ReplayResult>> {
        for stream_read in stream_read_set.stream_reads.iter() {
            if !self.config.stream_set.contains(&stream_read.stream_id) {
                return Ok(Some(ReplayResult::TagsMismatch));
            }
            // check version confiction
            if self
                .store
                .read()
                .await
                .get_latest_version_before(stream_read.stream_id, stream_read.key.clone(), tx.seq)
                .await?
                > version
            {
                return Ok(Some(ReplayResult::VersionConfliction));
            }
        }
        Ok(None)
    }

    async fn parse_stream_write_set(
        &self,
        stream_reader: &mut StreamReader<'_>,
    ) -> Result<StreamWriteSet> {
        let size = u32::from_be_bytes(stream_reader.next(SET_LEN_SIZE).await?.try_into().unwrap());
        if size > MAX_SIZE_LEN {
            bail!(ParseError::ListTooLong);
        }
        // load metadata
        let mut stream_write_metadata = vec![];
        for _ in 0..(size as usize) {
            let stream_id = H256::from_ssz_bytes(&stream_reader.next(STREAM_ID_SIZE).await?)
                .map_err(Error::from)?;
            let key = Arc::new(self.parse_key(stream_reader).await?);
            let data_size =
                u64::from_be_bytes(stream_reader.next(DATA_LEN_SIZE).await?.try_into().unwrap());
            stream_write_metadata.push((stream_id, key, data_size));
        }
        // use a hashmap to filter out the duplicate writes on same key, only the last one is reserved
        let mut start_index = stream_reader.current_position_in_bytes();
        let mut stream_writes = HashMap::new();
        for (stream_id, key, data_size) in stream_write_metadata.iter() {
            let end_index = start_index + data_size;
            stream_writes.insert(
                (stream_id, key.clone()),
                StreamWrite {
                    stream_id: *stream_id,
                    key: key.clone(),
                    start_index,
                    end_index,
                },
            );
            start_index = end_index;
        }
        // skip the write data
        stream_reader
            .skip(start_index - stream_reader.current_position_in_bytes())
            .await?;
        Ok(StreamWriteSet {
            stream_writes: stream_writes.into_values().collect(),
        })
    }

    async fn validate_stream_write_set(
        &self,
        stream_write_set: &StreamWriteSet,
        tx: &Transaction,
        version: u64,
    ) -> Result<Option<ReplayResult>> {
        let stream_set = HashSet::<H256>::from_iter(tx.stream_ids.iter().cloned());
        let store_read = self.store.read().await;
        for stream_write in stream_write_set.stream_writes.iter() {
            if !stream_set.contains(&stream_write.stream_id) {
                // the write set in data is conflict with tx tags
                return Ok(Some(ReplayResult::TagsMismatch));
            }
            // check version confiction
            if store_read
                .get_latest_version_before(stream_write.stream_id, stream_write.key.clone(), tx.seq)
                .await?
                > version
            {
                return Ok(Some(ReplayResult::VersionConfliction));
            }
            // check write permission
            if !(store_read
                .has_write_permission(
                    tx.sender,
                    stream_write.stream_id,
                    stream_write.key.clone(),
                    tx.seq,
                )
                .await?)
            {
                return Ok(Some(ReplayResult::WritePermissionDenied(
                    stream_write.stream_id,
                    stream_write.key.clone(),
                )));
            }
        }
        Ok(None)
    }

    async fn parse_access_control_data(
        &self,
        tx: &Transaction,
        stream_reader: &mut StreamReader<'_>,
    ) -> Result<AccessControlSet> {
        let size = u32::from_be_bytes(stream_reader.next(SET_LEN_SIZE).await?.try_into().unwrap());
        if size > MAX_SIZE_LEN {
            bail!(ParseError::ListTooLong);
        }
        // use a hashmap to filter out the useless operations
        // all operations can be categorized by op_type & 0xf0
        // for each category, except GRANT_ADMIN_ROLE, only the last operation of each account is reserved
        let mut access_ops = HashMap::new();
        // pad GRANT_ADMIN_ROLE prefix to handle the first write to new stream
        let mut is_admin = HashSet::new();
        let store_read = self.store.read().await;
        for id in &tx.stream_ids {
            if store_read.is_new_stream(*id, tx.seq).await? {
                let op_meta = (
                    AccessControlOps::GRANT_ADMIN_ROLE & 0xf0,
                    *id,
                    Arc::new(vec![]),
                    tx.sender,
                );
                access_ops.insert(
                    op_meta,
                    AccessControl {
                        op_type: AccessControlOps::GRANT_ADMIN_ROLE,
                        stream_id: *id,
                        key: Arc::new(vec![]),
                        account: tx.sender,
                        operator: H160::zero(),
                    },
                );
                is_admin.insert(*id);
            } else if store_read.is_admin(tx.sender, *id, tx.seq).await? {
                is_admin.insert(*id);
            }
        }
        drop(store_read);
        // ops in transaction
        for _ in 0..(size as usize) {
            let op_type = u8::from_be_bytes(
                stream_reader
                    .next(ACCESS_CONTROL_OP_TYPE_SIZE)
                    .await?
                    .try_into()
                    .unwrap(),
            );
            // parse operation data
            let stream_id = H256::from_ssz_bytes(&stream_reader.next(STREAM_ID_SIZE).await?)
                .map_err(Error::from)?;
            let mut account = H160::zero();
            let mut key = Arc::new(vec![]);
            match op_type {
                // stream_id + account
                AccessControlOps::GRANT_ADMIN_ROLE
                | AccessControlOps::GRANT_WRITER_ROLE
                | AccessControlOps::REVOKE_WRITER_ROLE => {
                    account = H160::from_ssz_bytes(&stream_reader.next(ADDRESS_SIZE).await?)
                        .map_err(Error::from)?;
                }
                // stream_id + key
                AccessControlOps::SET_KEY_TO_NORMAL | AccessControlOps::SET_KEY_TO_SPECIAL => {
                    key = Arc::new(self.parse_key(stream_reader).await?);
                }
                // stream_id + key + account
                AccessControlOps::GRANT_SPECIAL_WRITER_ROLE
                | AccessControlOps::REVOKE_SPECIAL_WRITER_ROLE => {
                    key = Arc::new(self.parse_key(stream_reader).await?);
                    account = H160::from_ssz_bytes(&stream_reader.next(ADDRESS_SIZE).await?)
                        .map_err(Error::from)?;
                }
                // renounce type
                AccessControlOps::RENOUNCE_ADMIN_ROLE | AccessControlOps::RENOUNCE_WRITER_ROLE => {
                    account = tx.sender;
                }
                AccessControlOps::RENOUNCE_SPECIAL_WRITER_ROLE => {
                    key = Arc::new(self.parse_key(stream_reader).await?);
                    account = tx.sender;
                }
                // unexpected type
                _ => {
                    bail!(ParseError::InvalidData);
                }
            }
            let op_meta = (op_type & 0xf0, stream_id, key.clone(), account);
            if op_type != AccessControlOps::GRANT_ADMIN_ROLE
                || (!access_ops.contains_key(&op_meta) && account != tx.sender)
            {
                access_ops.insert(
                    op_meta,
                    AccessControl {
                        op_type,
                        stream_id,
                        key: key.clone(),
                        account,
                        operator: tx.sender,
                    },
                );
            }
        }
        Ok(AccessControlSet {
            access_controls: access_ops.into_values().collect(),
            is_admin,
        })
    }

    async fn validate_access_control_set(
        &self,
        access_control_set: &mut AccessControlSet,
        tx: &Transaction,
    ) -> Result<Option<ReplayResult>> {
        // validate
        let stream_set = HashSet::<H256>::from_iter(tx.stream_ids.iter().cloned());
        for access_control in &access_control_set.access_controls {
            if !stream_set.contains(&access_control.stream_id) {
                // the write set in data is conflict with tx tags
                return Ok(Some(ReplayResult::TagsMismatch));
            }
            match access_control.op_type {
                AccessControlOps::GRANT_ADMIN_ROLE
                | AccessControlOps::SET_KEY_TO_NORMAL
                | AccessControlOps::SET_KEY_TO_SPECIAL
                | AccessControlOps::GRANT_WRITER_ROLE
                | AccessControlOps::REVOKE_WRITER_ROLE
                | AccessControlOps::GRANT_SPECIAL_WRITER_ROLE
                | AccessControlOps::REVOKE_SPECIAL_WRITER_ROLE => {
                    if !access_control_set
                        .is_admin
                        .contains(&access_control.stream_id)
                    {
                        return Ok(Some(ReplayResult::AccessControlPermissionDenied(
                            access_control.op_type,
                            access_control.stream_id,
                            access_control.key.clone(),
                            access_control.account,
                        )));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }

    async fn replay(&self, tx: &Transaction) -> Result<ReplayResult> {
        if !self.store.read().await.check_tx_completed(tx.seq)? {
            return Ok(ReplayResult::DataUnavailable);
        }
        let mut stream_reader = StreamReader::new(self.store.clone(), tx);
        // parse and validate
        let version = self.parse_version(&mut stream_reader).await?;
        let stream_read_set = match self.parse_stream_read_set(&mut stream_reader).await {
            Ok(x) => x,
            Err(e) => match e.downcast_ref::<ParseError>() {
                Some(ParseError::InvalidData | ParseError::ListTooLong) => {
                    return Ok(ReplayResult::DataParseError(e.to_string()));
                }
                Some(ParseError::PartialDataAvailable) | None => {
                    return Err(e);
                }
            },
        };
        if let Some(result) = self
            .validate_stream_read_set(&stream_read_set, tx, version)
            .await?
        {
            // validation error in stream read set
            return Ok(result);
        }
        let stream_write_set = match self.parse_stream_write_set(&mut stream_reader).await {
            Ok(x) => x,
            Err(e) => match e.downcast_ref::<ParseError>() {
                Some(ParseError::InvalidData | ParseError::ListTooLong) => {
                    return Ok(ReplayResult::DataParseError(e.to_string()));
                }
                Some(ParseError::PartialDataAvailable) | None => {
                    return Err(e);
                }
            },
        };
        if let Some(result) = self
            .validate_stream_write_set(&stream_write_set, tx, version)
            .await?
        {
            // validation error in stream write set
            return Ok(result);
        }
        let mut access_control_set =
            match self.parse_access_control_data(tx, &mut stream_reader).await {
                Ok(x) => x,
                Err(e) => match e.downcast_ref::<ParseError>() {
                    Some(ParseError::InvalidData | ParseError::ListTooLong) => {
                        return Ok(ReplayResult::DataParseError(e.to_string()));
                    }
                    Some(ParseError::PartialDataAvailable) | None => {
                        return Err(e);
                    }
                },
            };
        if let Some(result) = self
            .validate_access_control_set(&mut access_control_set, tx)
            .await?
        {
            // there is confliction in access control set
            return Ok(result);
        }
        Ok(ReplayResult::Commit(
            tx.seq,
            stream_write_set,
            access_control_set,
        ))
    }

    pub async fn run(&self) {
        let mut tx_seq;
        match self.store.read().await.get_stream_replay_progress().await {
            Ok(progress) => {
                tx_seq = progress;
            }
            Err(e) => {
                error!("get stream replay progress error: e={:?}", e);
                return;
            }
        }
        let mut check_replay_progress = false;
        loop {
            if check_replay_progress {
                match self.store.read().await.get_stream_replay_progress().await {
                    Ok(progress) => {
                        if tx_seq != progress {
                            debug!("reorg happend: tx_seq {}, progress {}", tx_seq, progress);
                            tx_seq = progress;
                        }
                    }
                    Err(e) => {
                        error!("get stream replay progress error: e={:?}", e);
                    }
                }

                check_replay_progress = false;
            }

            info!("checking tx with sequence number {:?}..", tx_seq);
            let maybe_tx = self.store.read().await.get_tx_by_seq_number(tx_seq);
            match maybe_tx {
                Ok(Some(tx)) => {
                    let mut skip = false;
                    if tx.stream_ids.is_empty() {
                        skip = true;
                    } else {
                        for id in tx.stream_ids.iter() {
                            if !self.config.stream_set.contains(id) {
                                skip = true;
                                break;
                            }
                        }
                    }
                    // replay data
                    if !skip {
                        info!("replaying data of tx with sequence number {:?}..", tx.seq);
                        match self.replay(&tx).await {
                            Ok(result) => {
                                let result_str = result.to_string();
                                match result {
                                    ReplayResult::Commit(
                                        tx_seq,
                                        stream_write_set,
                                        access_control_set,
                                    ) => {
                                        match self
                                            .store
                                            .write()
                                            .await
                                            .put_stream(
                                                tx_seq,
                                                tx.data_merkle_root,
                                                result_str.clone(),
                                                Some((stream_write_set, access_control_set)),
                                            )
                                            .await
                                        {
                                            Ok(_) => {
                                                info!(
                                                    "tx with sequence number {:?} commit.",
                                                    tx.seq
                                                );
                                            }
                                            Err(e) => {
                                                error!("stream replay result finalization error: e={:?}", e);
                                                check_replay_progress = true;
                                                continue;
                                            }
                                        }
                                    }
                                    ReplayResult::DataUnavailable => {
                                        // data not available
                                        info!("data of tx with sequence number {:?} is not available yet, wait..", tx.seq);
                                        tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS))
                                            .await;
                                        check_replay_progress = true;
                                        continue;
                                    }
                                    _ => {
                                        match self
                                            .store
                                            .write()
                                            .await
                                            .put_stream(
                                                tx.seq,
                                                tx.data_merkle_root,
                                                result_str.clone(),
                                                None,
                                            )
                                            .await
                                        {
                                            Ok(_) => {
                                                info!(
                                                    "tx with sequence number {:?} reverted with reason {:?}",
                                                    tx.seq, result_str
                                                );
                                            }
                                            Err(e) => {
                                                error!("stream replay result finalization error: e={:?}", e);
                                                check_replay_progress = true;
                                                continue;
                                            }
                                        }
                                    }
                                }

                                if !check_replay_progress {
                                    tx_seq += 1;
                                }
                            }
                            Err(e) => {
                                error!("replay stream data error: e={:?}", e);
                                tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                                check_replay_progress = true;
                                continue;
                            }
                        }
                    } else {
                        info!("tx {:?} is not in stream, skipped.", tx.seq);
                        // parse success
                        // update progress, get next tx_seq to sync
                        match self
                            .store
                            .write()
                            .await
                            .update_stream_replay_progress(tx_seq, tx_seq + 1)
                            .await
                        {
                            Ok(next_tx_seq) => {
                                tx_seq = next_tx_seq;
                            }
                            Err(e) => {
                                error!("update stream replay progress error: e={:?}", e);
                            }
                        }
                    }
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                    check_replay_progress = true;
                }
                Err(e) => {
                    error!("stream replay error: e={:?}", e);
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                    check_replay_progress = true;
                }
            }
        }
    }
}
