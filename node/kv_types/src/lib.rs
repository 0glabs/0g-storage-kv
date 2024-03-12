use ethereum_types::{H160, H256};

use serde::{Deserialize, Serialize};
use shared_types::Transaction;
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use std::collections::HashSet;

use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, DeriveDecode, DeriveEncode, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KVMetadata {
    pub stream_ids: Vec<H256>,
    pub sender: H160,
}

#[derive(Clone, Debug, Eq, PartialEq, DeriveDecode, DeriveEncode, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KVTransaction {
    pub metadata: KVMetadata,
    pub transaction: Transaction,
}

#[derive(Debug)]
pub struct StreamRead {
    pub stream_id: H256,
    pub key: Arc<Vec<u8>>,
}

#[derive(Debug)]
pub struct StreamReadSet {
    pub stream_reads: Vec<StreamRead>,
}

#[derive(Debug)]
pub struct StreamWrite {
    pub stream_id: H256,
    pub key: Arc<Vec<u8>>,
    // start index in bytes
    pub start_index: u64,
    // end index in bytes
    pub end_index: u64,
}

#[derive(Debug)]
pub struct StreamWriteSet {
    pub stream_writes: Vec<StreamWrite>,
}

#[derive(Debug)]
pub struct AccessControlSet {
    pub access_controls: Vec<AccessControl>,
    pub is_admin: HashSet<H256>,
}

#[derive(Debug)]
pub struct AccessControl {
    pub op_type: u8,
    pub stream_id: H256,
    pub key: Arc<Vec<u8>>,
    pub account: H160,
    pub operator: H160,
}

#[derive(Debug)]
pub struct KeyValuePair {
    pub stream_id: H256,
    pub key: Vec<u8>,
    pub start_index: u64,
    pub end_index: u64,
    pub version: u64,
}
