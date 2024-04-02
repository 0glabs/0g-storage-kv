use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValueSegment {
    // key version
    pub version: u64,
    // data
    #[serde(with = "base64")]
    pub data: Vec<u8>,
    // value total size
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValueSegment {
    // key version
    pub version: u64,
    // key
    #[serde(with = "base64")]
    pub key: Vec<u8>,
    // data
    #[serde(with = "base64")]
    pub data: Vec<u8>,
    // value total size
    pub size: u64,
}

mod base64 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = base64::encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        base64::decode(base64.as_bytes()).map_err(serde::de::Error::custom)
    }
}
