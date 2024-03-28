use ethereum_types::{H160, H256};

use serde::{Deserialize, Serialize};
use shared_types::Transaction;
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use std::collections::HashSet;

use std::sync::Arc;

// SHA256("STREAM")
// df2ff3bb0af36c6384e6206552a4ed807f6f6a26e7d0aa6bff772ddc9d4307aa
pub const STREAM_DOMAIN: H256 = H256([
    223, 47, 243, 187, 10, 243, 108, 99, 132, 230, 32, 101, 82, 164, 237, 128, 127, 111, 106, 38,
    231, 208, 170, 107, 255, 119, 45, 220, 157, 67, 7, 170,
]);

pub fn submission_topic_to_stream_ids(topic: Vec<u8>) -> Vec<H256> {
    if topic.is_empty() || topic.len() % 32 != 0 || H256::from_slice(&topic[..32]) != STREAM_DOMAIN
    {
        return vec![];
    }

    let mut stream_ids = Vec::new();
    for i in (32..topic.len()).step_by(32) {
        stream_ids.push(H256::from_slice(&topic[i..i + 32]))
    }
    stream_ids
}

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
