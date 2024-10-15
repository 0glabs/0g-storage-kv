use super::{Client, RuntimeContext};
use log_entry_sync::{LogSyncConfig, LogSyncEvent, LogSyncManager};
use rpc::HttpClient;
use rpc::RPCConfig;
use std::sync::Arc;
use storage_with_stream::log_store::log_manager::LogConfig;
use storage_with_stream::Store;
use storage_with_stream::{StorageConfig, StoreManager};
use stream::{StreamConfig, StreamManager};
use tokio::sync::broadcast;
use tokio::sync::RwLock;

macro_rules! require {
    ($component:expr, $self:ident, $e:ident) => {
        $self
            .$e
            .as_ref()
            .ok_or(format!("{} requires {}", $component, std::stringify!($e)))?
    };
}

struct LogSyncComponents {
    send: broadcast::Sender<LogSyncEvent>,
}

/// Builds a `Client` instance.
///
/// ## Notes
///
/// The builder may start some services (e.g.., libp2p, http server) immediately after they are
/// initialized, _before_ the `self.build(..)` method has been called.
#[derive(Default)]
pub struct ClientBuilder {
    runtime_context: Option<RuntimeContext>,
    store: Option<Arc<RwLock<dyn Store>>>,
    zgs_clients: Option<Vec<HttpClient>>,
    admin_client: Option<Option<HttpClient>>,
    log_sync: Option<LogSyncComponents>,
}

impl ClientBuilder {
    /// Specifies the runtime context (tokio executor, logger, etc) for client services.
    pub fn with_runtime_context(mut self, context: RuntimeContext) -> Self {
        self.runtime_context = Some(context);
        self
    }

    /// Initializes in-memory storage.
    pub async fn with_memory_store(mut self) -> Result<Self, String> {
        let executor = require!("storage", self, runtime_context).clone().executor;

        // TODO(zz): Set config.
        let store = Arc::new(RwLock::new(
            StoreManager::memorydb(LogConfig::default(), executor)
                .await
                .map_err(|e| format!("Unable to start in-memory store: {:?}", e))?,
        ));

        self.store = Some(store);

        Ok(self)
    }

    /// Initializes RocksDB storage.
    pub async fn with_rocksdb_store(mut self, config: &StorageConfig) -> Result<Self, String> {
        let executor = require!("storage", self, runtime_context).clone().executor;

        let store = Arc::new(RwLock::new(
            StoreManager::rocks_db(
                LogConfig::default(),
                &config.log_config.db_dir,
                &config.kv_db_file,
                executor,
            )
            .await
            .map_err(|e| format!("Unable to start RocksDB store: {:?}", e))?,
        ));

        self.store = Some(store);

        Ok(self)
    }

    pub async fn with_rpc(mut self, rpc_config: RPCConfig) -> Result<Self, String> {
        if !rpc_config.enabled {
            return Ok(self);
        }

        let executor = require!("rpc", self, runtime_context).clone().executor;
        let store = require!("stream", self, store).clone();

        let ctx = rpc::Context {
            config: rpc_config,
            shutdown_sender: executor.shutdown_sender(),
            store,
        };

        self.zgs_clients = Some(
            rpc::zgs_clients(&ctx).map_err(|e| format!("Unable to create rpc client: {:?}", e))?,
        );
        self.admin_client = Some(if let Some(url) = ctx.config.admin_node_address.clone() {
            Some(
                rpc::build_client(&url, ctx.config.zgs_rpc_timeout)
                    .map_err(|e| format!("Unable to create admin client: {:?}", e))?,
            )
        } else {
            None
        });

        let rpc_handle = rpc::run_server(ctx)
            .await
            .map_err(|e| format!("Unable to start HTTP RPC server: {:?}", e))?;

        executor.spawn(rpc_handle, "rpc");

        Ok(self)
    }

    pub async fn with_stream(self, config: &StreamConfig) -> Result<Self, String> {
        let executor = require!("stream", self, runtime_context).clone().executor;
        let store = require!("stream", self, store).clone();
        let zgs_clients = require!("stream", self, zgs_clients).clone();
        let admin_client = require!("stream", self, admin_client).clone();
        let (stream_data_fetcher, stream_replayer) =
            StreamManager::initialize(config, store, zgs_clients, admin_client, executor.clone())
                .await
                .map_err(|e| e.to_string())?;
        StreamManager::spawn(stream_data_fetcher, stream_replayer, executor)
            .map_err(|e| e.to_string())?;
        Ok(self)
    }

    pub async fn with_log_sync(mut self, config: LogSyncConfig) -> Result<Self, String> {
        let executor = require!("log_sync", self, runtime_context).clone().executor;
        let store = require!("log_sync", self, store).clone();
        let (send, _) = LogSyncManager::spawn(config, executor, store)
            .await
            .map_err(|e| e.to_string())?;
        self.log_sync = Some(LogSyncComponents { send });
        Ok(self)
    }

    /// Consumes the builder, returning a `Client` if all necessary components have been
    /// specified.
    pub fn build(self) -> Result<Client, String> {
        require!("client", self, runtime_context);

        Ok(Client {})
    }
}
