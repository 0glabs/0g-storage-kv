use std::net::SocketAddr;

#[derive(Clone)]
pub struct Config {
    pub enabled: bool,
    pub listen_address: SocketAddr,
    pub chunks_per_segment: usize,
    pub zgs_nodes: Vec<String>,
    pub admin_node_address: Option<String>,
    pub max_query_len_in_bytes: u64,
    pub max_response_body_in_bytes: u32,
    pub zgs_rpc_timeout: u64,
}
