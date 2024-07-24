use std::path::PathBuf;

use gasket::framework;
use pallas::network::multiplexer::RunningPlexer;
use serde::Deserialize;
use tracing::{debug, info};
use gasket::framework::AsWorkError;
use super::node_client_impl::CanHandshake;
use super::node_client_impl::CanConnect;
use super::node_client_impl::CanChainsync;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::chainsync::{BlockContent, NextResponse};
use pallas::network::miniprotocols::Point;
use bytes::Bytes;
use hex::ToHex;

use crate::framework as oura;
use pallas::codec::minicbor as minicbor;

pub struct OurClient {
   // plexer: RunningPlexer,
   // handshake: pallas::network::miniprotocols::handshake::N2CClient,
    chainsync: PointfulTx,// pallas::network::miniprotocols::chainsync::Client<PointfulTx>,
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

    chain: oura::GenesisValues,

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

// #[derive(minicbor::Encode, minicbor::Decode)]
// pub struct F {
//     #[n(0)]
//     aaa: u32
// }
// #[derive(minicbor::Encode)]
pub struct PointfulTx {
    // #[n(0)]
    parsed_tx: oura::ParsedTx,
    // #[n(1)]
    point: Point,
}
// impl pallas::codec::minicbor::Encode<()> for PointfulTx {
//     fn encode<W: pallas::codec::minicbor::encode::Write>(&self, e: &mut pallas::codec::minicbor::Encoder<W>, ctx: &mut ()) -> Result<(), pallas::codec::minicbor::encode::Error<W::Error>> {
//         todo!()
//     }
// }
// impl pallas::codec::minicbor::Decode<'_,()> for PointfulTx {
//     fn decode(d: &mut pallas::codec::minicbor::Decoder, ctx: &mut ()) -> Result<Self, pallas::codec::minicbor::decode::Error> {
//         todo!()
//     }
// }
impl From<PointfulTx> for Vec<u8> {
    fn from(other: PointfulTx) -> Self {
        todo!()
    }
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
impl super::node_client_impl::CanNew for OurClient {
    fn new(bearer: pallas::network::multiplexer::Bearer) -> Self {
        // let mut plexer = multiplexer::Plexer::new(bearer);

        // let hs_channel = plexer.subscribe_client(PROTOCOL_N2C_HANDSHAKE);
        // let cs_channel = plexer.subscribe_client(PROTOCOL_N2C_CHAIN_SYNC);
        // let sq_channel = plexer.subscribe_client(PROTOCOL_N2C_STATE_QUERY);

        // let plexer = plexer.spawn();

        Self {
           // plexer,
            //handshake: handshake::Client::new(hs_channel),
            chainsync: pallas::network::miniprotocols::chainsync::Client::new(todo!()/*cs_channel*/),
            //statequery: localstate::Client::new(sq_channel),
        }
    }
}
impl super::node_client_impl::CanHandshake for OurClient {
    fn handshake(&mut self) -> &mut pallas::network::miniprotocols::handshake::N2CClient {
        todo!()
    }
}
impl CanChainsync<PointfulTx> for OurClient {
    fn chainsync(&mut self) -> &mut pallas::network::miniprotocols::chainsync::Client<PointfulTx> {
        todo!()
    }
}
impl super::node_client_impl::CanConnect for OurClient {
    #[cfg(unix)]
    async fn connect(path: impl AsRef<std::path::Path>, magic: u64) -> Result<Self, pallas::network::facades::Error> {
        let bearer = pallas::network::multiplexer::Bearer::connect_unix(path)
            .await
            .map_err(pallas::network::facades::Error::ConnectFailure)?;

        let mut client: OurClient = super::node_client_impl::CanNew::new(bearer);

        let versions = pallas::network::miniprotocols::handshake::n2c::VersionTable::v10_and_above(magic);

        let handshake = client
            .handshake()
            .handshake(versions)
            .await
            .map_err(pallas::network::facades::Error::HandshakeProtocol)?;

        if let pallas::network::miniprotocols::handshake::Confirmation::Rejected(reason) = handshake {
            tracing::error!(?reason, "handshake refused");
            return Err(pallas::network::facades::Error::IncompatibleVersion);
        }

        Ok(client)
    }   
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, framework::WorkerError> {
        debug!("connecting");

        let mut peer_session = OurClient::connect(&stage.config.socket_path, stage.chain.magic)
            .await
            .or_retry()?;

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
        let client = self.peer_session.chainsync();

        let next = match client.has_agency() {
            true => {
                info!("requesting next block");
                client.request_next().await.or_restart()?
            }
            false => {
                info!("awaiting next block (blocking)");
                client.recv_while_must_reply().await.or_restart()?
            }
        };

        Ok(framework::WorkSchedule::Unit(next))
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
            chain: ctx.chain.clone().into(),
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
