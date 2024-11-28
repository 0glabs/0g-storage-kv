FROM rust
VOLUME ["/data"]
COPY . .
RUN apt-get update && apt-get install -y clang cmake build-essential pkg-config libssl-dev
RUN cargo build --release
CMD ["./target/release/zgs_kv", "--config", "run/config.toml", "--log", "run/log_config"]