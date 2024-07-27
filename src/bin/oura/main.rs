use clap::Parser;
use std::process;
mod make_socket;
mod console;
mod daemon;

#[derive(Parser)]
#[clap(name = "Oura")]
#[clap(bin_name = "oura")]
#[clap(author, version, about, long_about = None)]
enum Oura {
    Daemon(daemon::Args),
    Socket(make_socket::Args),
}

fn main() {
    let args = Oura::parse();

    let result = match args {
        Oura::Daemon(x) => daemon::run(&x),
        Oura::Socket(args) => make_socket::run(args),
    };

    if let Err(err) = &result {
        eprintln!("ERROR: {err:#?}");
        process::exit(1);
    }

    process::exit(0);
}
