mod leader;
mod sender;

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
    let start = memmem::find(body, b"\"id\":")? + 5;
    // Skip whitespace
    let start = start + body[start..].iter().position(|&b| b != b' ')?;
    // Find end (comma, whitespace, or })
    let end = body[start..]
        .iter()
        .position(|&b| b == b',' || b == b'}' || b == b' ' || b == b'\n')?;
    std::str::from_utf8(&body[start..start + end]).ok()
}

#[inline]
fn extract_method(body: &[u8]) -> Option<&str> {
    let start = memmem::find(body, b"\"method\":\"")? + 10;
    let end = start + memchr::memchr(b'"', &body[start..])?;
    std::str::from_utf8(&body[start..end]).ok()
}

#[inline]
fn extract_tx(body: &[u8]) -> Option<&str> {
    let start = memmem::find(body, b"\"params\":[\"")? + 11;
    let end = start + memchr::memchr(b'"', &body[start..])?;
    std::str::from_utf8(&body[start..end]).ok()
}

#[inline]
fn extract_encoding(body: &[u8]) -> Option<&str> {
    let start = memmem::find(body, b"\"encoding\":\"")? + 12;
    let end = start + memchr::memchr(b'"', &body[start..])?;
    std::str::from_utf8(&body[start..end]).ok()
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
