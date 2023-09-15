use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::{HashMap, HashSet};

use crate::{
    jobs_management::JobConfiguration,
    resources_management::{NodesRequirement, ResourcesRequirement},
    utils::now_to_secs,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Queue {
    configuration: QueueConfiguration,
    jobs: Vec<(String, JobConfiguration, Option<u64>)>,
    running: HashMap<String, JobConfiguration>,
}

impl Queue {
    pub fn jobs_submitable(&mut self) -> Vec<(&String, &JobConfiguration, &u64)> {
        if self.running_full() {
            Vec::new()
        } else {
            self.jobs_in_queue().into_iter().filter(|(_, JobConfiguration {uid, gid, ..}, _)| {
                !self.running_full_user(*uid) && !self.running_full_group(*gid)
            }).collect::<Vec<_>>()
        }
    }

    pub fn jobs_in_queue(&self) -> Vec<(&String, &JobConfiguration, &u64)> {
        self.jobs.iter().filter_map(|(id, job, waited)| {
            if let Some(waited) = waited {
                Some((id, job, waited))
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
    }

    pub fn add_to_queue(&mut self, job: &JobConfiguration) -> Result<String, ()> {
        if self.configuration.can_be_added(job) {
            let task_id = Uuid::new_v4();
            self.jobs.push((task_id.to_string(), job.clone(), None));
            Ok(task_id.to_string())
        } else {
            Err(())
        }
    }

    pub fn queue_to_running(&mut self, task_id: &str) -> Option<()> {
        let in_queue = self.jobs.iter().position(|(id, _, _)| id == task_id)?;
        let (_, job_conf, _) = &self.jobs[in_queue];
        self.running.insert(task_id.to_string(), job_conf.clone());
        self.jobs.remove(in_queue);
        Some(())
    }

    pub fn refresh_running(&mut self, running_ids: &HashSet<String>) {
        self.running = self
            .running
            .clone()
            .into_iter()
            .filter(|(id, _)| running_ids.contains(id))
            .collect::<HashMap<_, _>>()
    }

    pub fn refresh_jobs(&mut self) {
        while let Some(idx) =
            self.jobs
                .iter()
                .position(|(_, JobConfiguration { uid, gid, .. }, in_queue)| {
                    in_queue.is_none() && self.queueable(*uid, *gid)
                })
        {
            self.jobs[idx].2 = Some(now_to_secs())
        }
    }

    pub fn queueable(&self, uid: u32, gid: u32) -> bool {
        !self.queue_full() && !self.queue_full_user(uid) && !self.queue_full_group(gid)
    }

    fn queue_full(&self) -> bool {
        Some(self.jobs_in_queue().len()) >= self.configuration.global_limit.as_ref().map(|limit| limit.max_queue)
    }
    fn queue_full_user(&self, uid: u32) -> bool {
        Some(self.jobs_in_queue().iter().filter(|(_, job, _)| job.uid == uid).collect::<Vec<_>>().len()) >= self.configuration.user_limit.as_ref().map(|limit| limit.max_queue)
    }
    fn queue_full_group(&self, gid: u32) -> bool {
        Some(self.jobs_in_queue().iter().filter(|(_, job, _)| job.gid == gid).collect::<Vec<_>>().len()) >= self.configuration.group_limit.as_ref().map(|limit| limit.max_queue)
    }

    fn running_full(&self) -> bool {
        Some(self.jobs_in_queue().len()) >= self.configuration.global_limit.as_ref().map(|limit| limit.max_running)
    }
    fn running_full_user(&self, uid: u32) -> bool {
        Some(self.jobs_in_queue().iter().filter(|(_, job, _)| job.uid == uid).collect::<Vec<_>>().len()) >= self.configuration.user_limit.as_ref().map(|limit| limit.max_running)
    }
    fn running_full_group(&self, gid: u32) -> bool {
        Some(self.jobs_in_queue().iter().filter(|(_, job, _)| job.gid == gid).collect::<Vec<_>>().len()) >= self.configuration.group_limit.as_ref().map(|limit| limit.max_running)
    }
}

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
        let JobConfiguration {
            uid,
            gid,
            requirement,
            ..
        } = job;
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
                }
                PriorityRule::WaitingRule(factor) => priority += waited as f64 * factor,
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
            Self::Deny(denied) => !denied.contains(id),
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
