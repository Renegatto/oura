use pallas::network::miniprotocols::chainsync;

pub trait CanConnect: Sized {
  #[cfg(unix)]
  async fn connect(
      path: impl AsRef<std::path::Path>,
      magic: u64,
  ) -> Result<Self, pallas::network::facades::Error>;
  #[cfg(windows)]
  pub async fn connect(
      pipe_name: impl AsRef<std::ffi::OsStr>,
      magic: u64,
  ) -> Result<Self, Error>;
}
pub trait CanHandshake {
  fn handshake(&mut self) -> &mut pallas::network::miniprotocols::handshake::N2CClient;
}
pub trait CanNew {
  fn new(bearer: pallas::network::multiplexer::Bearer) -> Self;
}
pub trait CanChainsync<T> where chainsync::Message<T>: pallas::codec::Fragment {
  fn chainsync(&mut self) -> &mut chainsync::Client<T>;
}
