use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use reqwest_eventsource::{Event, EventSource};
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::response::RpcContactInfo;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterSlots,
    SubscribeRequestPing,
};

#[derive(Debug, Clone)]
pub struct Leaders {
    pub current: SocketAddr,
    pub next: SocketAddr,
}

pub trait LeaderProvider: Send + Sync {
    fn get(&self) -> Option<Leaders>;
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SseData {
    current_tpu: String,
    next_tpu: String,
}

#[derive(Clone)]
pub struct SseLeaderProvider {
    leaders: Arc<RwLock<Option<Leaders>>>,
}

impl SseLeaderProvider {
    pub fn new(url: &str) -> Self {
        let leaders = Arc::new(RwLock::new(None));
        let provider = Self {
            leaders: leaders.clone(),
        };

        let url = url.to_string();
        tokio::spawn(async move {
            loop {
                info!("connecting to SSE {}", url);

                let client = reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(10))
                    .build()
                    .unwrap();

                let mut es = EventSource::new(client.get(&url)).unwrap();

                while let Some(event) = es.next().await {
                    match event {
                        Ok(Event::Message(msg)) => {
                            let data = msg.data.trim();
                            if data.is_empty() || !data.starts_with('{') {
                                continue;
                            }

                            match serde_json::from_str::<SseData>(data) {
                                Ok(parsed) => {
                                    if let (Ok(current), Ok(next)) = (
                                        parsed.current_tpu.parse::<SocketAddr>(),
                                        parsed.next_tpu.parse::<SocketAddr>(),
                                    ) {
                                        let current_quic =
                                            SocketAddr::new(current.ip(), current.port() + 6);
                                        let next_quic = SocketAddr::new(next.ip(), next.port() + 6);
                                        *leaders.write().unwrap() = Some(Leaders {
                                            current: current_quic,
                                            next: next_quic,
                                        });
                                    }
                                }
                                Err(e) => warn!("SSE parse error: {}", e),
                            }
                        }
                        Ok(Event::Open) => info!("SSE connected"),
                        Err(e) => {
                            error!("SSE error: {:?}", e);
                            es.close();
                            break;
                        }
                    }
                }

                warn!("SSE disconnected, reconnecting...");
                sleep(Duration::from_secs(1)).await;
            }
        });

        provider
    }
}

impl LeaderProvider for SseLeaderProvider {
    fn get(&self) -> Option<Leaders> {
        self.leaders.read().unwrap().clone()
    }
}

const SLOTS_PER_LEADER: u64 = 4;

#[derive(Clone)]
pub struct GrpcLeaderProvider {
    cur_slot: Arc<AtomicU64>,
    cluster_nodes: Arc<RwLock<HashMap<String, RpcContactInfo>>>,
    leader_schedule: Arc<RwLock<Vec<(u64, String)>>>,
}

impl GrpcLeaderProvider {
    pub fn new(grpc_url: &str, rpc_url: &str) -> Self {
        let provider = Self {
            cur_slot: Arc::new(AtomicU64::new(0)),
            cluster_nodes: Arc::new(RwLock::new(HashMap::new())),
            leader_schedule: Arc::new(RwLock::new(Vec::new())),
        };

        provider.start_slot_subscription(grpc_url.to_string());
        provider.start_leader_polling(rpc_url.to_string());

        provider
    }

    fn start_slot_subscription(&self, grpc_url: String) {
        let cur_slot = self.cur_slot.clone();

        tokio::spawn(async move {
            loop {
                info!("connecting to gRPC {}", grpc_url);

                let result = async {
                    let tls = ClientTlsConfig::new().with_native_roots();
                    let mut client = GeyserGrpcClient::build_from_shared(grpc_url.clone())?
                        .tls_config(tls)?
                        .connect()
                        .await?;

                    let (mut tx, mut rx) = client.subscribe().await?;

                    tx.send(SubscribeRequest {
                        slots: HashMap::from([(
                            "slots".to_string(),
                            SubscribeRequestFilterSlots {
                                filter_by_commitment: Some(true),
                                interslot_updates: Some(false),
                            },
                        )]),
                        ..Default::default()
                    })
                    .await?;

                    while let Some(msg) = rx.next().await {
                        match msg?.update_oneof {
                            Some(UpdateOneof::Slot(slot)) => {
                                cur_slot.store(slot.slot, Ordering::Relaxed);
                            }
                            Some(UpdateOneof::Ping(_)) => {
                                tx.send(SubscribeRequest {
                                    ping: Some(SubscribeRequestPing { id: 1 }),
                                    ..Default::default()
                                })
                                .await?;
                            }
                            _ => {}
                        }
                    }

                    anyhow::Ok(())
                }
                .await;

                if let Err(e) = result {
                    error!("gRPC error: {:?}", e);
                }

                warn!("gRPC disconnected, reconnecting...");
                sleep(Duration::from_secs(1)).await;
            }
        });
    }

    fn start_leader_polling(&self, rpc_url: String) {
        let cur_slot = self.cur_slot.clone();
        let cluster_nodes = self.cluster_nodes.clone();
        let leader_schedule = self.leader_schedule.clone();

        tokio::spawn(async move {
            let rpc = RpcClient::new(rpc_url);

            loop {
                let slot = cur_slot.load(Ordering::Relaxed);
                if slot == 0 {
                    sleep(Duration::from_millis(200)).await;
                    continue;
                }

                match rpc.get_cluster_nodes().await {
                    Ok(nodes) => {
                        let mut map = cluster_nodes.write().unwrap();
                        map.clear();
                        for node in nodes {
                            map.insert(node.pubkey.clone(), node);
                        }
                        debug!("updated {} cluster nodes", map.len());
                    }
                    Err(e) => error!("get_cluster_nodes error: {}", e),
                }

                match rpc.get_slot_leaders(slot, 1000).await {
                    Ok(leaders) => {
                        let mut schedule = leader_schedule.write().unwrap();
                        schedule.clear();
                        for (i, pubkey) in leaders.iter().enumerate() {
                            schedule.push((slot + i as u64, pubkey.to_string()));
                        }
                        info!(
                            "updated leader schedule from slot {} ({} leaders)",
                            slot,
                            leaders.len()
                        );
                    }
                    Err(e) => error!("get_slot_leaders error: {}", e),
                }

                sleep(Duration::from_secs(60)).await;
            }
        });
    }

    fn get_leader_at_slot(&self, slot: u64) -> Option<RpcContactInfo> {
        let schedule = self.leader_schedule.read().unwrap();
        let nodes = self.cluster_nodes.read().unwrap();

        for (s, pubkey) in schedule.iter() {
            if *s == slot {
                return nodes.get(pubkey).cloned();
            }
        }
        None
    }
}

impl LeaderProvider for GrpcLeaderProvider {
    fn get(&self) -> Option<Leaders> {
        let slot = self.cur_slot.load(Ordering::Relaxed);
        if slot == 0 {
            return None;
        }

        let current_leader = self.get_leader_at_slot(slot)?;
        let next_leader = self.get_leader_at_slot(slot + SLOTS_PER_LEADER);

        let current_quic = current_leader.tpu_quic?;
        let next_quic = next_leader.and_then(|l| l.tpu_quic).unwrap_or(current_quic);

        Some(Leaders {
            current: current_quic,
            next: next_quic,
        })
    }
}
