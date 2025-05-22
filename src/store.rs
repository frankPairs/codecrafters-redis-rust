use std::collections::HashMap;

use chrono::{DateTime, Utc};

#[derive(Default, Debug)]
pub struct StoreValue {
    pub value: String,
    pub exp: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct StoreValueBuilder {
    pub value: String,
    pub exp: Option<DateTime<Utc>>,
}

impl StoreValueBuilder {
    pub fn new(value: &str) -> Self {
        StoreValueBuilder {
            value: value.to_string(),
            exp: None,
        }
    }

    pub fn with_exp(&mut self, exp: DateTime<Utc>) {
        self.exp = Some(exp);
    }

    pub fn build(self) -> StoreValue {
        StoreValue {
            value: self.value,
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
}
