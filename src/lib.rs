pub mod jobs;
pub mod resources;
pub mod queue;
pub mod http_utils;
pub mod request_util;

pub mod config {
    use std::fs;
    use serde_yaml;
    use serde::Deserialize;
    pub fn load_config<T>(paths: Vec<&str>) -> Option<T> 
    where T: for<'de> Deserialize<'de>{
        for path in paths {
            if let Ok(config_content) = fs::File::open(path) {
                println!("loading {}", path);
                match serde_yaml::from_reader::<_, T>(&config_content) {
                    Ok(config) => {
                        println!("configuration loaded");
                        return Some(config)
                    },
                    Err(err) => println!("failed to parse config {}, detail: \n{:#?}", path, err)
                }
            }
        }
        None
    }
}

pub mod resouces_manage {
    use std::collections::{HashMap, HashSet};

    pub struct Countables(HashMap<String, usize>);

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

    pub struct  Properties(HashMap<String, String>);

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



    pub enum NodesRequirement {
        Select(HashSet<usize>),
        Use(usize),
        Auto
    }

    pub struct ResourcesRequirement {
        cpus: NodesRequirement,
        mems: NodesRequirement,
        countables: Countables,
        properties: Properties
    }

    pub struct ResourcesProvider {
        cpus: NodeSet,
        mems: NodeSet,
        countables: Countables,
        properties: Properties
    }

    impl ResourcesProvider {
        pub fn acceptable(&self, requirement: &ResourcesRequirement) -> bool {
            self.cpus_acceptable(&requirement.cpus) && self.countables_acceptable(&requirement.countables) && self.properties_acceptable(&requirement.properties)
        }

        pub fn execlusive_mem_acceptable(&self, requirement: &ResourcesRequirement) -> bool {
            self.mems_acceptable(&requirement.mems) && self.acceptable(requirement)
        }

        fn cpus_acceptable(&self, requirement: &NodesRequirement) -> bool {
            match requirement {
                NodesRequirement::Auto => self.cpus.len() > 0,
                NodesRequirement::Use(size) => self.cpus.len() >= *size,
                NodesRequirement::Select(required) => required.is_subset(&self.cpus)
            }
        }

        fn mems_acceptable(&self, requirement: &NodesRequirement) -> bool {
            match requirement {
                NodesRequirement::Auto => self.mems.len() > 0,
                NodesRequirement::Use(size) => self.mems.len() >= *size,
                NodesRequirement::Select(required) => required.is_subset(&self.mems)  
            }
        }

        fn countables_acceptable(&self, requirement: &Countables) -> bool {
            let requirement = requirement.get_all();
            requirement.keys().all(|k| self.countables.enough(k, requirement[k]))
        }

        fn properties_acceptable(&self, requirement: &Properties) -> bool {
            let requirement = requirement.get_all();
            requirement.keys().all(|k| self.properties.matches(k, &requirement[k]))
        }
    }
}