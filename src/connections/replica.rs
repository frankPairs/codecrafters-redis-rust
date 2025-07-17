use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;

use crate::{
    commands::{CommandWriter, PingCommand, PsyncCommand, ReplconfCommand},
    server::ServerInfo,
};

pub struct ReplicaConnection {
    // Represents the information of the current server, which is acting as a replica.
    info: Arc<ServerInfo>,
    master_addr: SocketAddr,
}

impl ReplicaConnection {
    pub fn new(info: Arc<ServerInfo>, master_addr: SocketAddr) -> Self {
        Self { info, master_addr }
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
                self.info.address.port().to_string(),
            ])))
            .await?;

        writer
            .write_request(Box::new(ReplconfCommand::new(vec![
                String::from("REPLCONF"),
                String::from("capa"),
                String::from("psync2"),
            ])))
            .await?;

        writer
            .write_request(Box::new(PsyncCommand::new(self.info.clone())))
            .await?;

        Ok(())
    }
}
