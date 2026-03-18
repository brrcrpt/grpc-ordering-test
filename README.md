# gRPC Message Ordering Test

Tests whether a Yellowstone gRPC endpoint delivers **Account updates before or after TransactionStatus updates** for the same transaction signature.

## Quick start

1. Edit `config.json` with your endpoint(s):

```json
{
  "duration_secs": 30,
  "endpoints": [
    {
      "name": "MyEndpoint",
      "url": "http://my-grpc-server:10000"
    }
  ]
}
```

2. Run:

```bash
cargo run
```

Or specify a custom config path:

```bash
cargo run -- my-config.json
```

## Config format

```json
{
  "duration_secs": 30,
  "endpoints": [
    {
      "name": "EndpointA",
      "url": "http://host:port"
    },
    {
      "name": "EndpointB",
      "url": "https://host:port/",
      "x_token": "optional-auth-token"
    }
  ]
}
```

- `duration_secs` — how long to collect data (all endpoints run in parallel)
- `endpoints` — one or more gRPC endpoints to test
  - `name` — display name
  - `url` — Yellowstone gRPC URL
  - `x_token` — optional x-token for authentication

## Requirements

- Rust 1.75+
- Network access to the gRPC endpoint(s)
