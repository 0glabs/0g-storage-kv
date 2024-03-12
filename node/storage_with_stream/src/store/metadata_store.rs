use std::{path::Path, sync::Arc};

use anyhow::Result;
use kv_types::KVMetadata;
use kvdb_rocksdb::{Database, DatabaseConfig};
use ssz::{Decode, Encode};
use storage::{error::Error, log_store::log_manager::COL_TX, ZgsKeyValueDB};
use tracing::instrument;

use crate::try_option;

const COL_METADATA: u32 = 0;
const COL_NUM: u32 = 1;

pub struct MetadataStore {
    kvdb: Arc<dyn ZgsKeyValueDB>,
}

impl MetadataStore {
    pub fn rocksdb(path: impl AsRef<Path>) -> Result<Self> {
        let mut db_config = DatabaseConfig::with_columns(COL_NUM);
        db_config.enable_statistics = true;
        let db = Arc::new(Database::open(&db_config, path)?);
        Ok(Self { kvdb: db })
    }

    pub fn memorydb() -> Self {
        let db = Arc::new(kvdb_memorydb::create(COL_NUM));
        Self { kvdb: db }
    }

    #[instrument(skip(self))]
    pub fn put_metadata(&self, seq: u64, metadata: KVMetadata) -> Result<()> {
        let mut db_tx = self.kvdb.transaction();
        db_tx.put(COL_METADATA, &seq.to_be_bytes(), &metadata.as_ssz_bytes());
        self.kvdb.write(db_tx)?;
        Ok(())
    }

    pub fn get_metadata_by_seq_number(&self, seq: u64) -> Result<Option<KVMetadata>> {
        let value = try_option!(self.kvdb.get(COL_TX, &seq.to_be_bytes())?);
        let metadata = KVMetadata::from_ssz_bytes(&value).map_err(Error::from)?;
        Ok(Some(metadata))
    }
}
