use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Context;

use clap::Parser;

use codecrafters_redis::server::{Server, ServerRole};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The path to the directory where the RDB file is stored (example: /tmp/redis-data)
    #[arg(long)]
    dir: Option<String>,
    /// The name of the RDB file (example: rdbfile)
    #[arg(long)]
    dbfilename: Option<String>,
    /// The port where server will be running
    #[arg(long)]
    port: Option<u32>,
    /// When the --replicaof flag is passed, the server assumes the "slave" role instead.
    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli_args = Args::parse();
    let port: u16 = u16::try_from(cli_args.port.unwrap_or(6379)).expect("Invalid port");
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    // By default, a Redis server assumes the "master" role.
    let server_role = match cli_args.replicaof {
        Some(value) => {
            let (host, port) = value.split_once(" ").expect("Invalid replicaof value;");
            let host = if host == "localhost" {
                "127.0.0.1"
            } else {
                host
            };

            let addr: SocketAddr = format!("{}:{}", host, port)
                .parse()
                .expect("Invalid replica url");

            ServerRole::Slave(addr)
        }
        _ => ServerRole::Master,
    };

    let mut server = Server::new(addr, server_role);

    if let Some(dir) = cli_args.dir {
        server
            .with_dir(&dir)
            .with_context(|| format!("Failed to read {} directory", dir))?;
    }

    if let Some(dbfilename) = cli_args.dbfilename {
        server
            .with_dbfilename(&dbfilename)
            .with_context(|| format!("Failed to read {} db file", dbfilename))?;
    }

    server
        .listen()
        .await
        .context("Failed to run the Redis server")?;

    Ok(())
}
