use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct Countables(HashMap<String, usize>);

impl PartialOrd for Countables {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self == other {
            Some(std::cmp::Ordering::Equal)
        } else {
            for (k, v) in self.get_all() {
                if v > &other.get(k) {
                    return Some(std::cmp::Ordering::Greater);
                }
            }
            Some(std::cmp::Ordering::Less)
        }
    }
}

impl Countables {
    fn get_all_mut(&mut self) -> &mut HashMap<String, usize> {
        &mut self.0
    }

    pub fn get_all(&self) -> &HashMap<String, usize> {
        &self.0
    }

    pub fn set(&mut self, k: &str, v: usize) {
        self.get_all_mut().insert(k.to_string(), v);
    }

    pub fn get(&self, k: &str) -> usize {
        *self.get_all().get(k).unwrap_or(&0)
    }

    pub fn enough(&self, k: &str, usage: usize) -> bool {
        self.get(k) >= usage
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct Properties(HashMap<String, String>);

impl PartialOrd for Properties {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self == other {
            Some(std::cmp::Ordering::Equal)
        } else {
            for (k, v) in self.get_all() {
                if let Some(other_value) = other.get(k) {
                    if v != other_value {
                        return Some(std::cmp::Ordering::Greater);
                    }
                } else {
                    return Some(std::cmp::Ordering::Greater);
                }
            }
            Some(std::cmp::Ordering::Less)
        }
    }
}

impl Properties {
    pub fn get_all(&self) -> &HashMap<String, String> {
        &self.0
    }

    pub fn get(&self, k: &str) -> Option<&String> {
        self.get_all().get(k)
    }

    pub fn matches(&self, k: &str, v: &str) -> bool {
        self.get(k).map(|value| value == v).unwrap_or(false)
    }

    pub fn extend(&mut self, Self(other): &Self) {
        self.0.extend(other.clone())
    }

    pub fn conflict(&self, Self(other): &Self) -> bool {
        self.0.keys().any(|key| {
            if let Some(other_value) = other.get(key) {
                other_value != &self.0[key]
            } else {
                false
            }
        })
    }
}

pub type NodeSet = HashSet<usize>;

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum NodesRequirement {
    Select(HashSet<usize>),
    Use(usize),
    Auto,
}

impl NodesRequirement {
    fn is_zero(&self) -> bool {
        match self {
            Self::Select(set) => set.len() == 0,
            Self::Use(size) => *size == 0,
            Self::Auto => false,
        }
    }

    // for Select only
    pub fn to_string(&self) -> Option<String> {
        if let Self::Select(set) = self {
            set.iter()
                .map(|item| item.to_string())
                .reduce(|acc, next| format!("{},{}", acc, next))
        } else {
            None
        }
    }

    pub fn take_set(&self) -> &HashSet<usize> {
        if let Self::Select(set) = self {
            set
        } else {
            panic!("Invalid usage: Not NodesRequirement::Select")
        }
    }
}

impl PartialOrd for NodesRequirement {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self == other {
            Some(std::cmp::Ordering::Equal)
        } else {
            match self {
                Self::Auto => {
                    if other.is_zero() {
                        Some(std::cmp::Ordering::Less)
                    } else {
                        Some(std::cmp::Ordering::Greater)
                    }
                }
                Self::Select(set) => {
                    if let Self::Select(other_set) = other {
                        if set.is_subset(other_set) {
                            Some(std::cmp::Ordering::Less)
                        } else {
                            Some(std::cmp::Ordering::Greater)
                        }
                    } else {
                        Some(std::cmp::Ordering::Greater)
                    }
                }
                Self::Use(size) => {
                    if let Self::Select(other_set) = other {
                        size.partial_cmp(&other_set.len())
                    } else if let Self::Use(other_size) = other {
                        size.partial_cmp(other_size)
                    } else {
                        Some(std::cmp::Ordering::Greater)
                    }
                }
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct ResourcesRequirement {
    pub cpus: NodesRequirement,
    pub mems: NodesRequirement,
    pub countables: Countables,
    pub properties: Properties,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResourcesProvider {
    pub cpus: NodeSet,
    pub mems: NodeSet,
    pub countables: Countables,
    pub properties: Properties,
}

impl ResourcesProvider {
    pub fn acceptable(&self, requirement: &ResourcesRequirement) -> bool {
        self.cpus_acceptable(&requirement.cpus)
            && self.countables_acceptable(&requirement.countables)
            && self.properties_acceptable(&requirement.properties)
    }

    pub fn execlusive_mem_acceptable(&self, requirement: &ResourcesRequirement) -> bool {
        self.mems_acceptable(&requirement.mems) && self.acceptable(requirement)
    }

    fn cpus_acceptable(&self, requirement: &NodesRequirement) -> bool {
        requirement <= &NodesRequirement::Select(self.cpus.clone())
    }

    fn mems_acceptable(&self, requirement: &NodesRequirement) -> bool {
        requirement <= &NodesRequirement::Select(self.mems.clone())
    }

    fn countables_acceptable(&self, requirement: &Countables) -> bool {
        requirement <= &self.countables
    }

    fn properties_acceptable(&self, requirement: &Properties) -> bool {
        requirement <= &self.properties
    }
}
