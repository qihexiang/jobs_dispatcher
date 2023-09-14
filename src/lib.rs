pub mod http_utils;
pub mod jobs;
pub mod queue;
pub mod request_util;
pub mod resources;

pub mod config {
    use serde::Deserialize;
    use serde_yaml;
    use std::fs;
    pub fn load_config<T>(paths: Vec<&str>) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        for path in paths {
            if let Ok(config_content) = fs::File::open(path) {
                println!("loading {}", path);
                match serde_yaml::from_reader::<_, T>(&config_content) {
                    Ok(config) => {
                        println!("configuration loaded");
                        return Some(config);
                    }
                    Err(err) => println!("failed to parse config {}, detail: \n{:#?}", path, err),
                }
            }
        }
        None
    }
}

pub mod resources_manage {
    use std::collections::{HashMap, HashSet};

    #[derive(PartialEq)]
    pub struct Countables(HashMap<String, usize>);

    impl PartialOrd for Countables {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            for (k, v) in self.get_all() {
                if v > &other.get(k) {
                    return Some(std::cmp::Ordering::Greater)
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

    #[derive(PartialEq)]
    pub struct Properties(HashMap<String, String>);

    impl PartialOrd for Properties {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            for (k, v) in self.get_all() {
                if let Some(other_value) = other.get(k) {
                    if v != other_value {
                        return Some(std::cmp::Ordering::Greater)
                    }
                } else {
                    return Some(std::cmp::Ordering::Greater)
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

    #[derive(PartialEq)]
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
                Self::Auto => false 
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
                },
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
                },
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

    #[derive(PartialEq)]
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
}

pub mod jobs_manage {
    use std::{collections::HashMap, env, process::Command};

    use crate::{jobs::Phase, resources_manage::ResourcesRequirement};

    pub enum ExecutePhase {
        Sh(String),
        Run(Vec<String>),
        WorkDir(String),
        Env(HashMap<String, String>),
    }

    impl ExecutePhase {
        pub fn execute_all(phases: &Vec<Self>) -> Result<(), std::io::Error> {
            for phase in phases {
                phase.execute()?
            }
            Ok(())
        }

        pub fn execute(&self) -> Result<(), std::io::Error> {
            match self {
                Self::Sh(script) => Command::new("sh")
                    .arg("-c")
                    .arg(script)
                    .spawn()
                    .map(|mut child| child.wait())
                    .map(|_| ()),
                Self::Run(commands) => {
                    let program = &commands[0];
                    let arguments = commands.iter().skip(1).collect::<Vec<_>>();
                    Command::new(program)
                        .args(arguments)
                        .spawn()
                        .map(|mut child| child.wait())
                        .map(|_| ())
                }
                Self::WorkDir(workdir) => env::set_current_dir(workdir).map(|_| ()),
                Self::Env(envs) => {
                    for (k, v) in envs.iter() {
                        env::set_var(k, v);
                    }
                    Ok(())
                }
            }
        }
    }

    pub struct JobConfiguration {
        pub name: String,
        pub uid: u32,
        pub gid: u32,
        pub stdout_file: String,
        pub stderr_file: String,
        pub requirement: ResourcesRequirement,
        phases: Vec<ExecutePhase>,
    }
}

pub mod queue_management {
    use std::collections::HashSet;

    use crate::{resources_manage::{NodesRequirement, ResourcesRequirement}, jobs_manage::JobConfiguration};

    pub struct QueueConfiguration {
        priority_rule: Vec<PriorityRule>,
        users: IdControl,
        groups: IdControl,
        resources_limit: ResourcesRequirement,
        global_limit: Option<AmountLimit>,
        user_limit: Option<AmountLimit>,
        group_limit: Option<AmountLimit>,
    }

    impl QueueConfiguration {
        pub fn can_be_added(&self, job: &JobConfiguration) -> bool {
            let JobConfiguration { uid, gid, requirement, .. } = job;
            self.users.allow(uid) && self.groups.allow(gid) && requirement <= &self.resources_limit
        }
        
        pub fn priority(&self, requirement: &ResourcesRequirement, waited: u64) -> f64 {
            let mut priority = 0.;
            for rule in &self.priority_rule {
                match rule {
                    PriorityRule::PropertyRule(k, v, offset) => {
                        if requirement.properties.matches(k, v) {
                            priority += offset
                        }
                    }
                    PriorityRule::CountableRule(k, offset, ratio) => {
                        priority += offset + requirement.countables.get(k) as f64 * ratio;
                    }
                    PriorityRule::CpusetRule(select_factor, use_factor, auto_offset) => {
                        match &requirement.cpus {
                            NodesRequirement::Select(set) => {
                                priority += set.len() as f64 * select_factor;
                            }
                            NodesRequirement::Use(size) => {
                                priority += (*size as f64) * use_factor;
                            }
                            NodesRequirement::Auto => {
                                priority += *auto_offset;
                            }
                        }
                    },
                    PriorityRule::WaitingRule(factor) => {
                        priority += waited as f64 * factor
                    }
                }
            }
            priority
        }
    }

    pub enum IdControl {
        Allow(HashSet<u32>),
        Deny(HashSet<u32>),
    }

    impl IdControl {
        fn allow(&self, id: &u32) -> bool {
            match self {
                Self::Allow(allowed) => allowed.contains(id),
                Self::Deny(denied) => !denied.contains(id)
            }
        }
    }

    pub struct AmountLimit {
        max_running: usize,
        max_queue: usize,
    }

    pub enum PriorityRule {
        CpusetRule(f64, f64, f64),
        CountableRule(String, f64, f64),
        PropertyRule(String, String, f64),
        WaitingRule(f64),
    }
}
