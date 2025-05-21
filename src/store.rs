use std::collections::HashMap;

#[derive(Default, Debug, Clone)]
pub struct StoreValue {
    pub value: String,
}

#[derive(Default, Debug, Clone)]
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
