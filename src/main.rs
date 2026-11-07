mod leader;
mod sender;
mod transaction_v1;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use clap::{Parser, Subcommand};
use memchr::memmem;
use solana_keypair::{read_keypair_file, Keypair};
use tracing::info;

use crate::leader::{GrpcLeaderProvider, LeaderProvider, SseLeaderProvider};
use crate::sender::TxnSender;

#[derive(Parser)]
struct Args {
    #[arg(long, env, default_value = "1337")]
    port: u16,

    #[arg(long, env)]
    identity_keypair_file: Option<String>,

    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    Sse {
        #[arg(
            long,
            env,
            default_value = "https://areweslotyet.xyz/api/leader-stream"
        )]
        sse_url: String,
    },
    Grpc {
        #[arg(long, env)]
        grpc_url: String,

        #[arg(long, env)]
        rpc_url: String,
    },
}

fn json_response(body: String) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

fn json_ok(id: &str, result: &str) -> Response {
    json_response(format!(
        r#"{{"jsonrpc":"2.0","id":{},"result":"{}"}}"#,
        id, result
    ))
}

fn json_error(id: &str, code: i32, message: &str) -> Response {
    json_response(format!(
        r#"{{"jsonrpc":"2.0","id":{},"error":{{"code":{},"message":"{}"}}}}"#,
        id, code, message
    ))
}

#[inline]
fn extract_id(body: &[u8]) -> Option<&str> {
    let start = find_value(body, b"\"id\"")?;
    let len = body[start..]
        .iter()
        .position(|&b| b == b',' || b == b'}' || is_json_whitespace(b))?;
    std::str::from_utf8(&body[start..start + len]).ok()
}

#[inline]
fn extract_method(body: &[u8]) -> Option<&str> {
    string_at(body, find_value(body, b"\"method\"")?)
}

#[inline]
fn extract_tx(body: &[u8]) -> Option<&str> {
    let params = find_value(body, b"\"params\"")?;
    if body.get(params) != Some(&b'[') {
        return None;
    }
    string_at(body, skip_whitespace(body, params + 1))
}

#[inline]
fn extract_encoding(body: &[u8]) -> Option<&str> {
    string_at(body, find_value(body, b"\"encoding\"")?)
}

#[inline]
fn find_value(body: &[u8], key: &[u8]) -> Option<usize> {
    let mut from = 0;
    loop {
        let key_end = from + memmem::find(&body[from..], key)? + key.len();
        let colon = skip_whitespace(body, key_end);
        if body.get(colon) == Some(&b':') {
            return Some(skip_whitespace(body, colon + 1));
        }
        from = key_end;
    }
}

#[inline]
fn string_at(body: &[u8], pos: usize) -> Option<&str> {
    if body.get(pos) != Some(&b'"') {
        return None;
    }
    let start = pos + 1;
    let len = memchr::memchr(b'"', &body[start..])?;
    std::str::from_utf8(&body[start..start + len]).ok()
}

#[inline]
fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

#[inline]
fn skip_whitespace(buf: &[u8], pos: usize) -> usize {
    pos + buf[pos..]
        .iter()
        .take_while(|&&b| is_json_whitespace(b))
        .count()
}

async fn rpc_handler(State(sender): State<Arc<TxnSender>>, body: Bytes) -> Response {
    let id = extract_id(&body).unwrap_or("null");

    let Some(method) = extract_method(&body) else {
        return json_error(id, -32600, "invalid request");
    };

    if method != "sendTransaction" {
        return json_error(id, -32601, "method not found");
    }

    let Some(tx) = extract_tx(&body) else {
        return json_error(id, -32602, "missing transaction");
    };

    let encoding = extract_encoding(&body);

    match sender.send(tx, encoding).await {
        Ok(sig) => json_ok(id, &sig),
        Err(e) => json_error(id, -32000, &e.to_string()),
    }
}

async fn health() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();

    let keypair = match &args.identity_keypair_file {
        Some(path) => {
            info!("loading keypair from {}", path);
            read_keypair_file(path).expect("failed to read keypair")
        }
        None => {
            info!("using ephemeral keypair");
            Keypair::new()
        }
    };

    let leaders: Arc<dyn LeaderProvider> = match args.mode {
        Mode::Sse { sse_url } => {
            info!("using SSE leader provider: {}", sse_url);
            Arc::new(SseLeaderProvider::new(&sse_url))
        }
        Mode::Grpc { grpc_url, rpc_url } => {
            info!(
                "using gRPC leader provider: {} + RPC: {}",
                grpc_url, rpc_url
            );
            Arc::new(GrpcLeaderProvider::new(&grpc_url, &rpc_url))
        }
    };

    let sender = Arc::new(TxnSender::new(&keypair, leaders)?);

    let app = Router::new()
        .route("/", post(rpc_handler))
        .route("/health", get(health))
        .with_state(sender);

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
    info!("listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
