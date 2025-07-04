use anyhow::Context;

use clap::Parser;

use codecrafters_redis::server::Server;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The path to the directory where the RDB file is stored (example: /tmp/redis-data)
    #[arg(long)]
    dir: Option<String>,
    /// The name of the RDB file (example: rdbfile)
    #[arg(long)]
    dbfilename: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli_args = Args::parse();

    let mut server = Server::new("127.0.0.1:6379");

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
