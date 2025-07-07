use std::net::SocketAddr;
use std::{
    error::Error,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio::net::TcpListener;

use crate::data_types::RespDecoder;
use crate::store::Store;
use crate::tcp::TcpStreamReader;
use crate::{commands::CommandWriter, rdb::sync::RdbSync};

#[derive(Debug)]
pub enum ServerError {
    TcpListener(String),
    TcpReader(String),
    InvalidPath(String),
    InvalidCommand(String),
    RdbSync(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::TcpListener(err) => write!(f, "TcpListener Error: {}", err),
            ServerError::InvalidPath(err) => write!(f, "InvalidPath Error: {}", err),
            ServerError::TcpReader(err) => write!(f, "TcpReader Error: {}", err),
            ServerError::InvalidCommand(err) => write!(f, "InvalidCommand Error: {}", err),
            ServerError::RdbSync(err) => write!(f, "RdbSync Error: {}", err),
        }
    }
}

impl Error for ServerError {}

#[derive(Debug, Clone, Copy)]
pub enum ServerRole {
    Slave(SocketAddr),
    Master,
}

impl std::fmt::Display for ServerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerRole::Master => write!(f, "master"),
            ServerRole::Slave(_) => write!(f, "slave"),
        }
    }
}

#[derive(Debug)]
pub struct ServerConfig {
    /// The path to the directory where the RDB file is stored (example: /tmp/redis-data)
    pub dir: Option<PathBuf>,
    /// The name of the RDB file (example: rdbfile)
    pub dbfilename: Option<PathBuf>,
    pub role: ServerRole,
}

impl ServerConfig {
    fn get_rdb_path(&self) -> Option<PathBuf> {
        let mut rdb_path = PathBuf::new();
        let dir = self.dir.clone()?;
        let dbfilename = self.dbfilename.clone()?;

        rdb_path.push(dir);
        rdb_path.push(dbfilename);

        Some(rdb_path)
    }
}

#[derive(Debug)]
pub struct Server {
    address: SocketAddr,
    store: Arc<Mutex<Store>>,
    config: ServerConfig,
}

impl Server {
    pub fn new(address: SocketAddr, role: ServerRole) -> Self {
        Self {
            address,
            config: ServerConfig {
                dir: None,
                dbfilename: None,
                role,
            },
            store: Arc::new(Mutex::new(Store::default())),
        }
    }

    pub fn with_dir(&mut self, dir: &str) -> Result<(), ServerError> {
        let path = PathBuf::from_str(dir)
            .map_err(|_| ServerError::InvalidPath(String::from("Invalid directory path")))?;

        self.config.dir = Some(path);

        Ok(())
    }

    pub fn with_dbfilename(&mut self, dbfilename: &str) -> Result<(), ServerError> {
        let path = PathBuf::from_str(dbfilename)
            .map_err(|_| ServerError::InvalidPath(String::from("Invalid db filename path")))?;

        self.config.dbfilename = Some(path);

        Ok(())
    }

    pub async fn listen(self) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.address).await.map_err(|_| {
            ServerError::TcpListener("Connection could not be established".to_string())
        })?;
        let config = Arc::new(self.config);

        if let Some(rdb_path) = config.get_rdb_path() {
            let mut rdb_sync = RdbSync::new(self.store.clone());

            rdb_sync
                .sync(rdb_path)
                .await
                .map_err(|err| ServerError::RdbSync(err.to_string()))?;
        }

        if let ServerRole::Slave(replica_addr) = config.role {
            tokio::spawn(async move {
                let replica_listener = TcpListener::bind(replica_addr)
                    .await
                    .expect("Replica connection could not be established");

                let (_, _) = replica_listener
                    .accept()
                    .await
                    .expect("Replica connection could not be established");
            });
        }

        loop {
            let (mut socket, _) = listener.accept().await.map_err(|_| {
                ServerError::TcpListener("Connection with could not be established".to_string())
            })?;

            let store_cloned = self.store.clone();
            let config_cloned = config.clone();

            tokio::spawn(async move {
                loop {
                    let mut reader = TcpStreamReader::new(&mut socket);
                    let message = reader
                        .read()
                        .await
                        .expect("Error when reading from TCPStream");

                    if message.is_empty() {
                        break;
                    }

                    let mut lines = message.lines();

                    let resp_data_type = RespDecoder::decode(&mut lines)
                        .expect("Error when decoding string to RESP");

                    CommandWriter::from_resp_data_type(resp_data_type, &mut socket)
                        .expect("Error when decoding a command")
                        .write(store_cloned.clone(), config_cloned.clone())
                        .await
                        .expect("Invalid command")
                }
            });
        }
    }
}
