# Sendy

Lightweight Solana transaction sender. Pre warms connections and sends transactions via QUIC directly to leader TPU nodes. Lets get sendy!

No retry logic at the moment.

## Modes

### SSE Mode

Uses SSE endpoint for leader tracking. No RPC required. Shoutout to https://github.com/trustless-engineering/leader-stream/

```bash
sendy --port 1337 sse --sse-url https://areweslotyet.xyz/api/leader-stream
```

### gRPC Mode

Uses Yellowstone gRPC for slot updates and RPC for leader schedule.

```bash
sendy --port 1337 grpc \
  --grpc-url https://your-yellowstone-grpc:443 \
  --rpc-url https://api.mainnet-beta.solana.com
```

## Options

```
--port                    HTTP port (default: 1337)
--identity-keypair-file   Path to keypair file (optional, uses ephemeral if not set)
```

## API

Standard JSON-RPC on `/`.

```bash
curl -X POST http://localhost:1337 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "sendTransaction",
    "params": ["<base64-encoded-tx>", {"encoding": "base64"}]
  }'
```

## Build

```bash
cargo build --release
```
