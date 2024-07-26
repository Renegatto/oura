
use core::str;
use std::path::PathBuf;

use gasket::framework;
use serde::Serialize;
use serde::Deserialize;
use tracing::{debug, info};
use gasket::framework::AsWorkError;
use pallas::network::miniprotocols::chainsync::NextResponse;
use pallas::network::miniprotocols::Point;
use hex::ToHex;
use anyhow::Context;
use crate::framework as oura;
use tokio::net;
use net::UnixDatagram;

trait Read<A,E> {
    async fn read(&mut self) -> Result<A,E>;
}
pub struct PointfulTxDatagram {
    source: net::UnixDatagram
}
impl Read<PointfulTx,anyhow::Error> for PointfulTxDatagram {
    async fn read(&mut self) -> Result<PointfulTx,anyhow::Error> {

        let buf: &mut [u8] = &mut [];
        let mut src: &mut net::UnixDatagram = &mut self.source;
        (&mut src).recv(buf).await 
        .context("Couldn't wait for readable")?;
        let str: &mut str = str::from_utf8_mut(buf)
        .context("Can't decode read data")?;
        serde_json::from_str(&str)
        .context("Can't parse data")
    }
}
pub struct OurClient {
   // plexer: RunningPlexer,
   // handshake: pallas::network::miniprotocols::handshake::N2CClient,
    tx_source: PointfulTxDatagram,// pallas::network::miniprotocols::chainsync::Client<PointfulTx>,
   // statequery: pallas::network::miniprotocols::localstate::Client,
}

#[derive(framework::Stage)]
#[stage(
    name = "source",
    unit = "NextResponse<PointfulTx>",
    worker = "Worker"
)]
pub struct Stage {
    config: Config,

    // chain: oura::GenesisValues,

    // intersect: oura::IntersectConfig,

    breadcrumbs: oura::Breadcrumbs,

    pub output: oura::SourceOutputPort,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    chain_tip: gasket::metrics::Gauge,

    #[metric]
    current_slot: gasket::metrics::Gauge,

    #[metric]
    rollback_count: gasket::metrics::Counter,
}

pub struct Worker {
    peer_session: OurClient,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Point")]
pub enum PointDef {
    Origin,
    Specific(u64, Vec<u8>),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PointfulTx {
    parsed_tx: oura::ParsedTx,
    #[serde(with = "PointDef")]
    point: Point,
}

trait ProcessNext<Content> {
    async fn process_next(
        &mut self,
        stage: &mut Stage,
        next: &NextResponse<Content>,
    ) -> Result<(), framework::WorkerError>;
}

// it happens somewhere down there
impl ProcessNext<PointfulTx> for Worker {
    async fn process_next(
        &mut self,
        stage: &mut Stage,
        next: &NextResponse<PointfulTx>,
    ) -> Result<(), framework::WorkerError> {
        match next {
            NextResponse::RollForward(pointful_tx, tip) => {
               // let block = MultiEraBlock::decode(cbor).or_panic()?;
                // let slot = block.slot();
                // let hash = block.hash();
                let point = &pointful_tx.point;
                let tx: &oura::ParsedTx = &pointful_tx.parsed_tx;
                let hash: String = tx.hash.encode_hex();
                debug!(%hash, "chain sync roll forward");

                let evt = oura::ChainEvent::Apply(
                    point.clone(),
                    oura::Record::ParsedTx(tx.clone())
                );

                stage.output.send(evt.into()).await.or_panic()?;

                stage.breadcrumbs.track(point.clone());

                stage.chain_tip.set(tip.0.slot_or_default() as i64);
                // stage.current_slot.set(slot as i64);
                stage.ops_count.inc(1);

                Ok(())
            }
            NextResponse::RollBackward(point, tip) => {
                match &point {
                    Point::Origin => debug!("rollback to origin"),
                    Point::Specific(slot, _) => debug!(slot, "rollback"),
                };

                stage
                    .output
                    .send(oura::ChainEvent::reset(point.clone()))
                    .await
                    .or_panic()?;

                stage.breadcrumbs.track(point.clone());

                stage.chain_tip.set(tip.0.slot_or_default() as i64);
                stage.current_slot.set(point.slot_or_default() as i64);
                stage.rollback_count.inc(1);
                stage.ops_count.inc(1);

                Ok(())
            }
            NextResponse::Await => {
                info!("chain-sync reached the tip of the chain");
                Ok(())
            }
        }
    }
}

async fn connect(socket_path: impl AsRef<std::path::Path>) -> Result<OurClient, anyhow::Error> {
    debug!("Binding unix reader socket");
    let listen_from = UnixDatagram::bind(&socket_path)?;
    debug!("Connected to the socket");
    Ok(OurClient { tx_source: PointfulTxDatagram { source: listen_from } })
}

pub trait CanConnect: Sized {
    #[cfg(unix)]
    async fn connect(
        path: impl AsRef<std::path::Path>,
       // magic: u64,
    ) -> Result<Self, pallas::network::facades::Error>;
}
impl CanConnect for OurClient {
    #[cfg(unix)]
    async fn connect(path: impl AsRef<std::path::Path>) -> Result<Self, pallas::network::facades::Error> {
        // let bearer = pallas::network::multiplexer::Bearer::connect_unix(path)
        //     .await
        //     .map_err(pallas::network::facades::Error::ConnectFailure)?;

        // let mut client: OurClient = super::node_client_impl::CanNew::new(bearer);

        // let versions = pallas::network::miniprotocols::handshake::n2c::VersionTable::v10_and_above(magic);

        // let handshake = client
        //     .handshake()
        //     .handshake(versions)
        //     .await
        //     .map_err(pallas::network::facades::Error::HandshakeProtocol)?;

        // if let pallas::network::miniprotocols::handshake::Confirmation::Rejected(reason) = handshake {
        //     tracing::error!(?reason, "handshake refused");
        //     return Err(pallas::network::facades::Error::IncompatibleVersion);
        // }

      connect(path).await.map_err(|err|
        pallas::network::facades::Error::ConnectFailure(std::io::Error::other(err))
      )
    }   
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, framework::WorkerError> {
        debug!("connecting");

        let peer_session = OurClient::connect(&stage.config.socket_path)
            .await
            .or_retry()
            ?;
        debug!("Connected");
      
        // if stage.breadcrumbs.is_empty() {
        //     intersect_from_config(&mut peer_session, &stage.intersect).await?;
        // } else {
        //     intersect_from_breadcrumbs(&mut peer_session, &stage.breadcrumbs).await?;
        // }

        let worker = Self { peer_session };

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        _stage: &mut Stage,
    ) -> Result<framework::WorkSchedule<NextResponse<PointfulTx>>, framework::WorkerError> {
        let client = &mut self.peer_session.tx_source;
        debug!("reading next");
        client.read()
        .await
        .and_then(|pointful_tx| {
            debug!("requesting next block");
            let ptx: PointfulTx = pointful_tx.clone();
            Ok(framework::WorkSchedule::Unit::<NextResponse<PointfulTx>>(
                NextResponse::RollForward(
                    pointful_tx,
                    pallas::network::miniprotocols::chainsync::Tip(ptx.point,454)
                )
            ))
        })
        .map_err(|err| {
            println!("Error {err}"); 
            framework::WorkerError::Recv
        })

        // let next = match client.has_agency() {
        //     true => {
        //         info!("requesting next block");
        //         client.request_next().await.or_restart()?
        //     }
        //     false => {
        //         info!("awaiting next block (blocking)");
        //         client.recv_while_must_reply().await.or_restart()?
        //     }
        // };

        // Ok(framework::WorkSchedule::Unit(next))
    }

    async fn execute(
        &mut self,
        unit: &NextResponse<PointfulTx>,
        stage: &mut Stage,
    ) -> Result<(), framework::WorkerError> {
        self.process_next(stage, unit).await
    }
}

#[derive(Deserialize)]
pub struct Config {
    socket_path: PathBuf,
}

impl Config {
    pub fn bootstrapper(self, ctx: &oura::Context) -> Result<Stage, oura::Error> {
        let stage = Stage {
            config: self,
            breadcrumbs: ctx.breadcrumbs.clone(),
            // chain: ctx.chain.clone().into(),
            // intersect: ctx.intersect.clone(),
            output: Default::default(),
            ops_count: Default::default(),
            chain_tip: Default::default(),
            current_slot: Default::default(),
            rollback_count: Default::default(),
        };

        Ok(stage)
    }
}
