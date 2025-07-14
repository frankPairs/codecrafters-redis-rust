use std::net::SocketAddr;
use tokio::net::TcpStream;

use crate::commands::{CommandWriter, PingCommand};

pub struct ReplicaConnection {
    addr: SocketAddr,
}

impl ReplicaConnection {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn listen(&self) -> anyhow::Result<()> {
        let mut stream = TcpStream::connect(self.addr)
            .await
            .expect("Replica listener could not be established");

        let mut writer = CommandWriter::new(&mut stream);

        // Init handshake process
        writer.write_request(Box::new(PingCommand)).await?;

        Ok(())
    }
}
