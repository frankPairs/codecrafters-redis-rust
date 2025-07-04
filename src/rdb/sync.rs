use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::fs::File;

use crate::store::Store;

use super::decoder::RdbFileDecoder;

pub enum RdbSyncError {
    ReadFile(String),
    DecodeData(String),
    LockStore(String),
}

impl std::fmt::Display for RdbSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbSyncError::ReadFile(err) => {
                write!(f, "ReadFile: {}", err)
            }
            RdbSyncError::DecodeData(err) => {
                write!(f, "DecodeData: {}", err)
            }
            RdbSyncError::LockStore(err) => {
                write!(f, "LockStore: {}", err)
            }
        }
    }
}

pub struct RdbSync {
    store: Arc<Mutex<Store>>,
}

impl RdbSync {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self { store }
    }
    // Sync the values from .rdb file to the redis store
    pub async fn sync(&mut self, rdb_path: PathBuf) -> Result<(), RdbSyncError> {
        let file = File::open(rdb_path).await;
        // When the file does not exists, we do not need to modify the Redis database, neither to throw an error.
        let file = match file {
            Ok(f) => f,
            Err(err) => match err.kind() {
                tokio::io::ErrorKind::NotFound => return Ok(()),
                _ => return Err(RdbSyncError::ReadFile(err.to_string())),
            },
        };

        let mut decoder = RdbFileDecoder::new(file);

        let rdb_data = decoder
            .decode()
            .await
            .map_err(|err| RdbSyncError::DecodeData(err.to_string()))?;

        let mut store = self
            .store
            .lock()
            .map_err(|err| RdbSyncError::LockStore(err.to_string()))?;

        if let Some(databases) = rdb_data.databases {
            for (_, database) in databases.databases {
                for (key, value) in database.data {
                    store.set(&key, value);
                }
            }
        }

        Ok(())
    }
}
