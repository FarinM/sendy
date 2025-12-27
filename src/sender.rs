use bytes::Bytes;
use solana_keypair::Keypair;
use solana_signer::Signer;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use base64::prelude::*;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, TransportConfig};
use rustls::{crypto::CryptoProvider, NamedGroup};
use solana_quic_definitions::{QUIC_KEEP_ALIVE, QUIC_SEND_FAIRNESS};
use solana_tls_utils::{new_dummy_x509_certificate, QuicClientCertificate, SkipServerVerification};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::leader::LeaderProvider;

const MAX_TX_SIZE: usize = 1232;
const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";
const PREWARM_INTERVAL: Duration = Duration::from_millis(100);

pub struct TxnSender {
    endpoint: Endpoint,
    leaders: Arc<dyn LeaderProvider>,
    connections: Arc<RwLock<HashMap<SocketAddr, Connection>>>,
}

fn crypto_provider() -> CryptoProvider {
    let mut provider = rustls::crypto::ring::default_provider();
    provider
        .kx_groups
        .retain(|kx| kx.name() == NamedGroup::X25519);
    provider
}

impl TxnSender {
    pub fn new(keypair: &Keypair, leaders: Arc<dyn LeaderProvider>) -> anyhow::Result<Self> {
        let client_config = create_client_config(keypair)?;

        let mut endpoint = Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))?;
        endpoint.set_default_client_config(client_config);

        let sender = Self {
            endpoint,
            leaders,
            connections: Arc::new(RwLock::new(HashMap::new())),
        };

        sender.spawn_prewarm_task();

        Ok(sender)
    }

    fn spawn_prewarm_task(&self) {
        let endpoint = self.endpoint.clone();
        let leaders = self.leaders.clone();
        let connections = self.connections.clone();

        tokio::spawn(async move {
            let mut last_prewarmed: Option<SocketAddr> = None;

            loop {
                if let Some(leader_info) = leaders.get() {
                    let next_addr = leader_info.next;

                    if last_prewarmed != Some(next_addr) {
                        if let Err(e) = prewarm_connection(&endpoint, &connections, next_addr).await
                        {
                            warn!("prewarm failed for {}: {:?}", next_addr, e);
                        } else {
                            info!("prewarmed connection to {}", next_addr);
                            last_prewarmed = Some(next_addr);
                        }
                    }
                }

                tokio::time::sleep(PREWARM_INTERVAL).await;
            }
        });
    }

    pub async fn send(&self, tx_data: &str, encoding: Option<&str>) -> anyhow::Result<String> {
        let (wire_tx, signature) = decode_tx(tx_data, encoding)?;

        let Some(leaders) = self.leaders.get() else {
            anyhow::bail!("no leader data available");
        };

        if leaders.current != leaders.next {
            tokio::join!(
                self.send_to(leaders.current, &wire_tx),
                self.send_to(leaders.next, &wire_tx)
            );
        } else {
            self.send_to(leaders.current, &wire_tx).await;
        }

        debug!("sent tx {}", signature);

        Ok(signature)
    }

    async fn send_to(&self, addr: SocketAddr, data: &Bytes) {
        let endpoint = self.endpoint.clone();
        let connections = self.connections.clone();

        let conn = match get_or_connect(&endpoint, &connections, addr).await {
            Ok(c) => c,
            Err(e) => {
                debug!("connect failed to {}: {:?}", addr, e);
                return;
            }
        };

        match timeout(Duration::from_millis(500), async {
            let mut stream = conn.open_uni().await?;
            stream.write_all(data).await?;
            stream.finish()?;
            anyhow::Ok(())
        })
        .await
        {
            Ok(Ok(())) => debug!("sent to {}", addr),
            Ok(Err(e)) => {
                debug!("send error to {}: {:?}", addr, e);
                connections.write().await.remove(&addr);
            }
            Err(_) => {
                debug!("send timeout to {}", addr);
                connections.write().await.remove(&addr);
            }
        }
    }
}

async fn prewarm_connection(
    endpoint: &Endpoint,
    connections: &Arc<RwLock<HashMap<SocketAddr, Connection>>>,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    {
        let cache = connections.read().await;
        if let Some(conn) = cache.get(&addr) {
            if conn.close_reason().is_none() {
                return Ok(());
            }
        }
    }

    let _ = get_or_connect(endpoint, connections, addr).await?;
    Ok(())
}

async fn get_or_connect(
    endpoint: &Endpoint,
    connections: &Arc<RwLock<HashMap<SocketAddr, Connection>>>,
    addr: SocketAddr,
) -> anyhow::Result<Connection> {
    {
        let cache = connections.read().await;
        if let Some(conn) = cache.get(&addr) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }
    }

    let server_name = format!("{}.{}.sol", addr.ip(), addr.port());
    let connecting = endpoint.connect(addr, &server_name)?;

    let conn = match connecting.into_0rtt() {
        Ok((conn, accepted)) => {
            tokio::spawn(async move {
                if !accepted.await {
                    warn!("0-RTT rejected by {}", addr);
                }
            });
            conn
        }
        Err(connecting) => match timeout(Duration::from_secs(5), connecting).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => anyhow::bail!("connection error: {:?}", e),
            Err(_) => anyhow::bail!("connect timeout"),
        },
    };

    connections.write().await.insert(addr, conn.clone());
    debug!("connected to {}", addr);

    Ok(conn)
}

fn create_client_config(keypair: &Keypair) -> anyhow::Result<ClientConfig> {
    let (certificate, key) = new_dummy_x509_certificate(keypair);
    let cert = QuicClientCertificate { certificate, key };

    let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .expect("Failed to set QUIC client protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_client_auth_cert(vec![cert.certificate.clone()], cert.key.clone_key())
        .expect("Failed to set QUIC client certificates");

    crypto.enable_early_data = true;
    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

    let mut transport = TransportConfig::default();
    let idle_timeout =
        IdleTimeout::try_from(Duration::from_secs(30)).expect("Failed to set idle timeout");
    transport.max_idle_timeout(Some(idle_timeout));
    transport.keep_alive_interval(Some(QUIC_KEEP_ALIVE));
    transport.send_fairness(QUIC_SEND_FAIRNESS);

    let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    config.transport_config(Arc::new(transport));

    info!("created QUIC client for identity: {}", keypair.pubkey());

    Ok(config)
}

fn decode_tx(data: &str, encoding: Option<&str>) -> anyhow::Result<(Bytes, String)> {
    let bytes = match encoding {
        Some("base64") => BASE64_STANDARD.decode(data)?,
        Some("base58") | None => bs58::decode(data).into_vec()?,
        Some(other) => anyhow::bail!("unsupported encoding: {}", other),
    };

    if bytes.len() > MAX_TX_SIZE {
        anyhow::bail!("transaction too large");
    }

    if bytes.len() < 65 {
        anyhow::bail!("transaction too small");
    }
    let signature = bs58::encode(&bytes[1..65]).into_string();

    Ok((Bytes::from(bytes), signature))
}
