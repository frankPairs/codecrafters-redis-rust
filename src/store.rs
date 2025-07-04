use std::collections::HashMap;

use chrono::{DateTime, Utc};

#[derive(Default, Debug)]
pub struct StoreValue {
    pub value: String,
    pub exp: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
pub struct StoreValueBuilder {
    pub value: Option<String>,
    pub exp: Option<DateTime<Utc>>,
}

impl StoreValueBuilder {
    pub fn new() -> Self {
        StoreValueBuilder {
            value: None,
            exp: None,
        }
    }

    pub fn with_exp(&mut self, exp: DateTime<Utc>) {
        self.exp = Some(exp);
    }

    pub fn with_value(&mut self, value: &str) {
        self.value = Some(value.to_string());
    }

    pub fn build(self) -> StoreValue {
        StoreValue {
            value: self.value.unwrap(),
            exp: self.exp,
        }
    }
}

#[derive(Default, Debug)]
pub struct Store {
    data: HashMap<String, StoreValue>,
}

impl Store {
    pub fn set(&mut self, key: &str, value: StoreValue) {
        self.data.insert(key.to_string(), value);
    }

    pub fn get(&self, key: &str) -> Option<&StoreValue> {
        self.data.get(key)
    }

    pub fn get_all_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = Vec::new();

        for key in self.data.keys() {
            keys.push(key.clone());
        }

        keys
    }
}
