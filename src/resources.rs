use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Nodes {
    Select(HashSet<usize>),
    Use(usize),
    Auto
}

impl Nodes {
    pub fn to_hashset(&self) -> HashSet<usize> {
        match self {
            Self::Select(set) => set.clone(),
            Self::Use(size) => (0..size - 1).collect::<_>(),
            Self::Auto => HashSet::new()
        }
    }
}

impl ToString for Nodes {
    fn to_string(&self) -> String {
        let mut string = String::new();
        for item in self.to_hashset().iter() {
            string.push_str(&format!("{},", item))
        };
        string
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Resources {
    countables: HashMap<String, usize>,
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

    pub fn countables(&self) -> &HashMap<String, usize> {
        &self.countables
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn property(&self, key: &str) -> Option<&String> {
        self.properties.get(key)
    }

    pub fn get_countable(&self, key: &str) -> usize {
        *self.countables.get(key).unwrap_or(&0)
    }

    pub fn set_countable(&self, key: &str, value: usize) -> Self {
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

    pub fn has_enough(&self, key: &str, value: usize) -> bool {
        let config_value = self.countables.get(key).unwrap_or(&0);
        *config_value >= value
    }

    pub fn not_over(&self, key: &str, value: usize) -> bool {
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
