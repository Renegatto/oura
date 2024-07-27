use std::{fs::File, io::{Read, Write}};
use anyhow::Context;
use tracing::debug;

#[derive(clap::Args)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(long, value_parser)]
    writable_socket: std::path::PathBuf,
    #[clap(long, value_parser)]
    coordination_socket: std::path::PathBuf,
}

fn log_line(msg: String) -> () {
  let line = format!("{msg}\n");
  let buf: &[u8] = line.as_bytes();
  debug!(line);
  File::options()
  .append(true)
  .open("make_socket.log")
  .context("Could not open log file")
  .and_then(|mut log_file|
    log_file
    .write(buf)
    .map_err(|e| 
      anyhow::Error::new(
        std::io::Error::other(
          format!("Couldn not write log: {}", e.to_string()))))
    
  )
  .map(|_| ())
  .unwrap_or_else(|e|
    panic!("Logging failed {}",e)
  )
}

pub fn run(args: Args) -> Result<(), oura::framework::Error> {
  let run_or_throw = || async {
    run_socket(args)
    .await
    .map(|_|
      std::thread::sleep(std::time::Duration::from_secs(15))
    )
    .unwrap_or_else(|err: anyhow::Error| {
      log_line(format!("Error! {}", err.to_string()));
    });
    log_line("Done".to_string())
  };
  let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

  rt.block_on(
    async { run_or_throw().await }
  ); 
  Ok(())
}

fn read_first_tx() -> anyhow::Result<String> {
  let buf: &mut String = &mut String::new();
  File::open("./request1.json")
  .and_then(|mut file|
    file.read_to_string(buf))
  .context("Can not read tx from the file")
  .map(|_| buf.trim().to_string())
}

async fn run_socket(args: Args) -> anyhow::Result<()> {
  log_line("Binding sockets".to_string());
  let coordination_socket = tokio::net::UnixDatagram::bind(&args.coordination_socket)
        .context("Could not bind coordination socket")?;

  let first_tx = read_first_tx()?;
  log_line(format!("Json length is {}",&first_tx.len().to_string()));
  let _tx: oura::sources::tx_over_socket::PointfulTx = serde_json::from_str(&first_tx)?;

  log_line(format!("Can parse the Tx from the JSON. Writing it to the socket: {first_tx}"));
  coordination_socket.send_to(first_tx.as_bytes(),&args.writable_socket).await?;
  log_line("Created socket and wrote Tx into the writable".to_string());
  Ok(())
}

