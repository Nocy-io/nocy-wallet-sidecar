# Nocy Wallet Sidecar

Backend sidecar service for Nocy Wallet providing real-time blockchain data feeds for Midnight Network.

## Features

- **Real-time Block Feed**: Live streaming of blockchain blocks via SSE (Server-Sent Events)
- **Wallet Event Processing**: Filters and processes wallet-relevant events (shielded, unshielded, dust)
- **Merkle Tree Updates**: Generates merkle tree delta updates for efficient wallet sync
- **Session Management**: Per-wallet session handling with viewing key support
- **PostgreSQL Storage**: Persistent storage for session data and caching
- **Prometheus Metrics**: Built-in observability with metrics export

## Prerequisites

- Rust 1.70+ (2021 edition)
- PostgreSQL 14+
- NATS Server (for internal messaging)
- Access to a Midnight Network node

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/Nocy-io/nocy-wallet-sidecar.git
cd nocy-wallet-sidecar

# Build release binary
cargo build --release

# The binary will be at ./target/release/nocy-wallet-feed
```

### Using Docker

```bash
# Build the Docker image
docker build -t nocy-wallet-feed .

# Run with environment variables
docker run -p 8080:8080 \
  -e DATABASE_URL=postgres://user:pass@host/db \
  -e NATS_URL=nats://host:4222 \
  nocy-wallet-feed
```

## Configuration

The service is configured via environment variables or a `.env` file:

```bash
# Database
DATABASE_URL=postgres://user:password@localhost:5432/nocy_sidecar

# NATS messaging
NATS_URL=nats://localhost:4222

# Upstream Midnight node
UPSTREAM_NODE_URL=https://node.midnight.network

# Server settings
HOST=0.0.0.0
PORT=8080
LOG_LEVEL=info
```

## Database Setup

Run the migrations to set up the database schema:

```bash
# Using sqlx-cli
cargo install sqlx-cli
sqlx database create
sqlx migrate run

# Or manually apply migrations from ./migrations/
```

## Running

```bash
# Development
cargo run

# Production
./target/release/nocy-wallet-feed
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/status` | GET | Health check |
| `/session` | POST | Create new sync session |
| `/session/:id` | DELETE | End session |
| `/feed/:session_id` | GET (SSE) | Subscribe to wallet feed |
| `/feed/blocks` | GET | Get block range |
| `/zswap/nullifiers` | POST | Check ZSwap nullifiers |
| `/metrics` | GET | Prometheus metrics |

See [API.md](API.md) for detailed API documentation.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Nocy Wallet    │────▶│  Sidecar        │────▶│  Midnight Node  │
│  Extension/SDK  │ SSE │  (this service) │HTTP │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │   PostgreSQL    │
                        │   (sessions,    │
                        │    caching)     │
                        └─────────────────┘
```

### Components

- **routes/**: HTTP endpoint handlers
  - `feed.rs` - Block feed endpoints
  - `subscribe.rs` - SSE subscription
  - `session.rs` - Session management
  - `zswap.rs` - ZSwap-specific endpoints
  - `status.rs` - Health checks
- **ledger_state_store.rs**: Ledger state management using midnight-ledger
- **upstream.rs**: Communication with Midnight node
- **db.rs**: PostgreSQL database access
- **config.rs**: Configuration loading

## Development

```bash
# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run

# Check formatting
cargo fmt -- --check

# Run clippy
cargo clippy
```

## Dependencies

Key dependencies:

- **axum**: Web framework
- **tokio**: Async runtime
- **sqlx**: PostgreSQL async driver
- **midnight-ledger**: Midnight Network ledger types and utilities
- **tracing**: Structured logging

## Related Projects

- [nocy-wallet-sdk](https://github.com/Nocy-io/nocy-wallet-sdk) - TypeScript wallet SDK
- [nocy-wallet-extension](https://github.com/Nocy-io/nocy-wallet-extension) - Browser extension
- [midnight-ledger](https://github.com/midnightntwrk/midnight-ledger) - Midnight Network ledger library

## License

MIT - see [LICENSE](LICENSE)
