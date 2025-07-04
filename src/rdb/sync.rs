use anyhow::{Context, Ok, Result};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::fs::File;

use crate::store::Store;

use super::decoder::RdbFileDecoder;

pub struct RdbSync {
    store: Arc<Mutex<Store>>,
}

impl RdbSync {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self { store }
    }
    // Sync the values from .rdb file to the redis store
    pub async fn sync(&mut self, rdb_path: PathBuf) -> Result<()> {
        let file = File::open(rdb_path)
            .await
            .context("Failed to read RDB backup file")?;

        let mut decoder = RdbFileDecoder::new(file);

        let rdb_data = decoder
            .decode()
            .await
            .expect("Error while decoding rdb file");

        let mut store = self
            .store
            .lock()
            .expect("Error when trying to lock the store");

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
