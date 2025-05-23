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

fn main() {
    let cli_args = Args::parse();
    let mut server = Server::new("127.0.0.1:6379");

    if let Some(dir) = cli_args.dir {
        server.with_dir(&dir);
    }

    if let Some(dbfilename) = cli_args.dbfilename {
        server.with_dbfilename(&dbfilename);
    }

    server.listen().unwrap();
}
