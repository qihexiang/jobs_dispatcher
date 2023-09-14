use std::collections::HashSet;
use serde::{Serialize, Deserialize};

use crate::{resources_management::{NodesRequirement, ResourcesRequirement}, jobs_management::JobConfiguration};

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Clone, Debug)]
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AmountLimit {
    max_running: usize,
    max_queue: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum PriorityRule {
    CpusetRule(f64, f64, f64),
    CountableRule(String, f64, f64),
    PropertyRule(String, String, f64),
    WaitingRule(f64),
}
