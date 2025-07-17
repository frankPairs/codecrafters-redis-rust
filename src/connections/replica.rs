use std::net::SocketAddr;
use tokio::net::TcpStream;

use crate::commands::{CommandWriter, PingCommand, PsyncCommand, ReplconfCommand};

pub struct ReplicaConnection {
    replica_addr: SocketAddr,
    master_addr: SocketAddr,
}

impl ReplicaConnection {
    pub fn new(replica_addr: SocketAddr, master_addr: SocketAddr) -> Self {
        Self {
            replica_addr,
            master_addr,
        }
    }

    pub async fn listen(&self) -> anyhow::Result<()> {
        let mut stream = TcpStream::connect(self.master_addr)
            .await
            .expect("Replica listener could not be established");

        // Init handshake process
        self.send_handshake(&mut stream).await?;

        Ok(())
    }

    async fn send_handshake(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let mut writer = CommandWriter::new(stream);

        writer.write_request(Box::new(PingCommand)).await?;

        writer
            .write_request(Box::new(ReplconfCommand::new(vec![
                String::from("REPLCONF"),
                String::from("listening-port"),
                self.replica_addr.port().to_string(),
            ])))
            .await?;

        writer
            .write_request(Box::new(ReplconfCommand::new(vec![
                String::from("REPLCONF"),
                String::from("capa"),
                String::from("psync2"),
            ])))
            .await?;

        writer.write_request(Box::new(PsyncCommand)).await?;

        Ok(())
    }
}
