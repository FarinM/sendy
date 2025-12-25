mod leader;
mod sender;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
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

#[derive(Deserialize)]
struct RpcRequest {
    #[allow(unused)]
    jsonrpc: String,
    id: serde_json::Value,
    method: String,
    params: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct RpcResponse<T> {
    jsonrpc: String,
    id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Serialize)]
struct RpcError {
    code: i32,
    message: String,
}

impl<T> RpcResponse<T> {
    fn ok(id: serde_json::Value, result: T) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(result),
            error: None,
        }
    }

    fn err(id: serde_json::Value, code: i32, message: String) -> RpcResponse<()> {
        RpcResponse {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(RpcError { code, message }),
        }
    }
}

async fn rpc_handler(
    State(sender): State<Arc<TxnSender>>,
    Json(req): Json<RpcRequest>,
) -> Json<serde_json::Value> {
    let response = match req.method.as_str() {
        "sendTransaction" => {
            let tx = req.params.first().and_then(|v| v.as_str());
            let encoding = req
                .params
                .get(1)
                .and_then(|v| v.get("encoding"))
                .and_then(|v| v.as_str());

            match tx {
                Some(tx) => match sender.send(tx, encoding).await {
                    Ok(sig) => serde_json::to_value(RpcResponse::ok(req.id, sig)).unwrap(),
                    Err(e) => {
                        serde_json::to_value(RpcResponse::<()>::err(req.id, -32000, e.to_string()))
                            .unwrap()
                    }
                },
                None => serde_json::to_value(RpcResponse::<()>::err(
                    req.id,
                    -32602,
                    "missing transaction".into(),
                ))
                .unwrap(),
            }
        }
        _ => serde_json::to_value(RpcResponse::<()>::err(
            req.id,
            -32601,
            "method not found".into(),
        ))
        .unwrap(),
    };
    Json(response)
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
