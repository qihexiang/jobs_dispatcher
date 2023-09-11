use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Resources {
    countables: HashMap<String, u64>,
    properties: HashMap<String, String>,
}

impl Default for Resources {
    fn default() -> Self {
        Self::new()
    }
}

impl Resources {
    pub fn new() -> Self {
        Self {
            countables: HashMap::new(),
            properties: HashMap::new(),
        }
    }

    pub fn countables(&self) -> &HashMap<String, u64> {
        return &self.countables;
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        return &self.properties;
    }

    pub fn get_countable(&self, key: &str) -> u64 {
        *self.countables.get(key).unwrap_or(&0)
    }

    pub fn set_countable(&self, key: &str, value: u64) -> Self {
        let mut countable = self.countables.clone();
        countable.insert(key.to_string(), value);
        Self {
            countables: countable,
            properties: self.properties.clone(),
        }
    }

    pub fn remove_countable(&self, key: &str) -> Self {
        let mut countable = self.countables.clone();
        countable.remove(key);
        Self {
            countables: countable,
            properties: self.properties.clone(),
        }
    }

    pub fn has_enough(&self, key: &str, value: u64) -> bool {
        let config_value = self.countables.get(key).unwrap_or(&0);
        *config_value >= value
    }

    pub fn not_over(&self, key: &str, value: u64) -> bool {
        let config_value = self.countables.get(key).unwrap_or(&0);
        *config_value <= value
    }

    pub fn set_property(&self, key: &str, value: &str) -> Self {
        let mut properties = self.properties.clone();
        properties.insert(key.to_string(), value.to_string());
        Self {
            countables: self.countables.clone(),
            properties,
        }
    }

    pub fn remove_property(&self, key: &str) -> Self {
        let mut properties = self.properties.clone();
        properties.remove(key);
        Self {
            countables: self.countables.clone(),
            properties,
        }
    }

    pub fn property_is(&self, key: &str, value: &str) -> bool {
        self.properties
            .get(key)
            .map(|property| property == value)
            .unwrap_or(false)
    }
}
