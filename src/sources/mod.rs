use gasket::runtime::Tether;
use serde::Deserialize;

use crate::framework::*;

//#[cfg(target_family = "unix")]
//pub mod n2c;

pub mod n2c;
pub mod n2n;
pub mod tx_over_socket;

#[cfg(feature = "u5c")]
pub mod u5c;

#[cfg(feature = "aws")]
pub mod s3;

// we need to add our own case here
pub enum Bootstrapper {
    N2N(n2n::Stage),

    #[cfg(target_family = "unix")]
    N2C(n2c::Stage),

    #[cfg(feature = "u5c")]
    U5C(u5c::Stage),

    #[cfg(feature = "aws")]
    S3(s3::Stage),

    OurCaseB(our::Stage),
}

impl Bootstrapper {
    pub fn borrow_output(&mut self) -> &mut SourceOutputPort {
        match self {
            Bootstrapper::N2N(p) => &mut p.output,

            // #[cfg(target_family = "our")]
            Bootstrapper::OurCaseB(p) => &mut p.output,

            #[cfg(target_family = "unix")]
            Bootstrapper::N2C(p) => &mut p.output,

            #[cfg(feature = "u5c")]
            Bootstrapper::U5C(p) => &mut p.output,

            #[cfg(feature = "aws")]
            Bootstrapper::S3(p) => &mut p.output,
        }
    }

    pub fn spawn(self, policy: gasket::runtime::Policy) -> Tether {
        match self {
            Bootstrapper::N2N(x) => gasket::runtime::spawn_stage(x, policy),

            #[cfg(target_family = "unix")]
            Bootstrapper::N2C(x) => gasket::runtime::spawn_stage(x, policy),

            #[cfg(feature = "u5c")]
            Bootstrapper::U5C(x) => gasket::runtime::spawn_stage(x, policy),

            #[cfg(feature = "aws")]
            Bootstrapper::S3(x) => gasket::runtime::spawn_stage(x, policy),

            Bootstrapper::OurCaseB(x) => gasket::runtime::spawn_stage(x, policy),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    N2N(n2n::Config),

    #[cfg(target_family = "unix")]
    N2C(n2c::Config),

   //#[cfg(target_family = "our")]
    OurCaseC(our::Config),

    #[cfg(feature = "u5c")]
    U5C(u5c::Config),

    #[cfg(feature = "aws")]
    S3(s3::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Bootstrapper, Error> {
        match self {
            Config::N2N(c) => Ok(Bootstrapper::N2N(c.bootstrapper(ctx)?)),

            #[cfg(target_family = "unix")]
            Config::N2C(c) => Ok(Bootstrapper::N2C(c.bootstrapper(ctx)?)),

            #[cfg(feature = "u5c")]
            Config::U5C(c) => Ok(Bootstrapper::U5C(c.bootstrapper(ctx)?)),

            #[cfg(feature = "aws")]
            Config::S3(c) => Ok(Bootstrapper::S3(c.bootstrapper(ctx)?)),

            Config::OurCaseC(c) => Ok(Bootstrapper::OurCaseB(c.bootstrapper(ctx)?)),

        }
    }
}
