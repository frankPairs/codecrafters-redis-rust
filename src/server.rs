use std::{
    net::TcpListener,
    sync::{Arc, Mutex},
};

use crate::commands::CommandBuilder;
use crate::data_types::RespDecoder;
use crate::store::Store;
use crate::tcp::TcpStreamReader;

#[derive(Debug)]
pub enum ServerError {
    TcpListener(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::TcpListener(err) => write!(f, "TcpListener Error: {}", err),
        }
    }
}

#[derive(Debug)]
pub struct ServerConfig {
    /// The path to the directory where the RDB file is stored (example: /tmp/redis-data)
    pub dir: Option<String>,
    /// The name of the RDB file (example: rdbfile)
    pub dbfilename: Option<String>,
}

#[derive(Debug)]
pub struct Server {
    address: String,
    store: Arc<Mutex<Store>>,
    config: ServerConfig,
}

impl Server {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
            config: ServerConfig {
                dir: None,
                dbfilename: None,
            },
            store: Arc::new(Mutex::new(Store::default())),
        }
    }

    pub fn with_dir(&mut self, dir: &str) {
        self.config.dir = Some(dir.to_string());
    }

    pub fn with_dbfilename(&mut self, dbfilename: &str) {
        self.config.dbfilename = Some(dbfilename.to_string());
    }

    pub fn listen(self) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.address.as_str()).map_err(|_| {
            ServerError::TcpListener(format!(
                "Connection with {} could not be established",
                self.address
            ))
        })?;
        let config = Arc::new(self.config);

        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    let store_cloned = self.store.clone();
                    let config_cloned = config.clone();

                    std::thread::spawn(move || loop {
                        let mut reader = TcpStreamReader::new(&mut stream);
                        let message = reader.read();

                        if message.is_empty() {
                            break;
                        }

                        let mut lines = message.lines();

                        let resp_data_type = RespDecoder::decode(&mut lines)
                            .expect("Error when decoding string to RESP");

                        let command = CommandBuilder::from_resp_data_type(resp_data_type)
                            .expect("Error when decoding a command")
                            .build(store_cloned.clone(), config_cloned.clone())
                            .expect("Invalid command");

                        command.reply(&mut stream).unwrap();
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }

        Ok(())
    }
}
