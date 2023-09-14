use std::collections::{hash_set, HashMap, HashSet};

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct Countables(HashMap<String, usize>);

impl PartialOrd for Countables {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        for (k, v) in self.get_all() {
            if v > &other.get(k) {
                return Some(std::cmp::Ordering::Greater);
            }
        }
        Some(std::cmp::Ordering::Less)
    }
}

impl Countables {
    pub fn get_all(&self) -> &HashMap<String, usize> {
        &self.0
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
}

impl PartialOrd for NodesRequirement {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
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

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct ResourcesRequirement {
    pub cpus: NodesRequirement,
    pub mems: NodesRequirement,
    pub countables: Countables,
    pub properties: Properties,
}

impl PartialOrd for ResourcesRequirement {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.cpus > other.cpus
            || self.mems > other.mems
            || self.countables > other.countables
            || self.properties > other.properties
        {
            Some(std::cmp::Ordering::Greater)
        } else {
            Some(std::cmp::Ordering::Less)
        }
    }
}

pub struct ResourcesProvider {
    cpus: NodeSet,
    mems: NodeSet,
    countables: Countables,
    properties: Properties,
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
        match requirement {
            NodesRequirement::Auto => self.cpus.len() > 0,
            NodesRequirement::Use(size) => self.cpus.len() >= *size,
            NodesRequirement::Select(required) => required.is_subset(&self.cpus),
        }
    }

    fn mems_acceptable(&self, requirement: &NodesRequirement) -> bool {
        match requirement {
            NodesRequirement::Auto => self.mems.len() > 0,
            NodesRequirement::Use(size) => self.mems.len() >= *size,
            NodesRequirement::Select(required) => required.is_subset(&self.mems),
        }
    }

    fn countables_acceptable(&self, requirement: &Countables) -> bool {
        let requirement = requirement.get_all();
        requirement
            .keys()
            .all(|k| self.countables.enough(k, requirement[k]))
    }

    fn properties_acceptable(&self, requirement: &Properties) -> bool {
        let requirement = requirement.get_all();
        requirement
            .keys()
            .all(|k| self.properties.matches(k, &requirement[k]))
    }
}
