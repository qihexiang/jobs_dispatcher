use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    jobs_management::JobConfiguration,
    resources_management::{NodesRequirement, Properties, ResourcesProvider, ResourcesRequirement},
    utils::now_to_secs,
};

pub struct QueueGroup(HashMap<String, Queue>);

impl QueueGroup {
    pub fn new(queues: HashMap<String, Queue>) -> Self {
        Self(queues)
    }

    pub fn add_to_queue(&mut self, queue: &str, job: &JobConfiguration) -> Result<String, ()> {
        if let Some(queue) = self.0.get_mut(queue) {
            queue.add_to_queue(job)
        } else {
            Err(())
        }
    }

    pub fn remove_job(&mut self, task_id: &str, uid: u32) -> Option<Result<(), ()>> {
        for (_, queue) in self.0.iter_mut() {
            if let Some(index) = queue.jobs.iter().position(|(id, _, _)| id == task_id) {
                return Some(if queue.jobs[index].1.uid == uid || uid == 0 {
                    queue.jobs.remove(index);
                    Ok(())
                } else {
                    Err(())
                });
            }
        }
        None
    }

    pub fn try_take_job(
        &self,
        provider: &ResourcesProvider,
        exlusive_mem: bool,
    ) -> Option<(String, JobConfiguration, String)> {
        let Self(queues) = &self;
        let mut submitables = queues
            .iter()
            .map(|(name, queue)| (name, queue.jobs_submitable()))
            .map(|(name, submitables)| {
                submitables
                    .into_iter()
                    .map(|(task_id, job_conf, _, priority)| {
                        (task_id, job_conf, priority, name.clone())
                    })
            })
            .flatten()
            .collect::<Vec<_>>();
        submitables.sort_by(|(_, _, a, _), (_, _, b, _)| b.partial_cmp(a).unwrap());
        let available_job = submitables.into_iter().find(|(_, job, _, _)| {
            if exlusive_mem {
                provider.execlusive_mem_acceptable(&job.requirement)
            } else {
                provider.acceptable(&job.requirement)
            }
        });
        if let Some((id, job, _, queue)) = available_job {
            let id = id.clone();
            let job = job.clone();
            Some((id.clone(), job.clone(), queue))
        } else {
            None
        }
    }

    pub fn truly_take_job(
        &mut self,
        queue: &str,
        send_id: &str,
        received_id: &str,
        job: &JobConfiguration,
    ) -> Option<()> {
        if let Some(queue) = self.0.get_mut(queue) {
            if let Some(_) = queue.remove_from_queue(send_id) {
                queue.add_to_running(received_id, job);
                queue.refresh_jobs();
                Some(())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn refresh_running(&mut self, running_ids: &HashSet<String>) {
        for (_, v) in self.0.iter_mut() {
            v.refresh_running(running_ids)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Queue {
    configuration: QueueConfiguration,
    jobs: Vec<(String, JobConfiguration, Option<u64>)>,
    running: HashMap<String, JobConfiguration>,
}

impl Queue {
    pub fn new(configuration: &QueueConfiguration) -> Self {
        Self {
            configuration: configuration.clone(),
            jobs: Vec::new(),
            running: HashMap::new(),
        }
    }

    pub fn jobs_submitable(&self) -> Vec<(&String, &JobConfiguration, &u64, f64)> {
        if self.running_full() {
            Vec::new()
        } else {
            self.jobs_in_queue()
                .into_iter()
                .filter(|(_, JobConfiguration { uid, gid, .. }, _, _)| {
                    !self.running_full_user(*uid) && !self.running_full_group(*gid)
                })
                .collect::<Vec<_>>()
        }
    }

    pub fn jobs_in_queue(&self) -> Vec<(&String, &JobConfiguration, &u64, f64)> {
        self.jobs
            .iter()
            .filter_map(|(id, job, waited)| {
                if let Some(waited) = waited {
                    Some((
                        id,
                        job,
                        waited,
                        self.configuration.priority(&job.requirement, *waited),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn add_to_queue(&mut self, job: &JobConfiguration) -> Result<String, ()> {
        if self.configuration.can_be_added(job) {
            let task_id = Uuid::new_v4();
            let mut job_configuration = job.clone();
            job_configuration
                .requirement
                .properties
                .extend(&self.configuration.properties);
            self.jobs.push((task_id.to_string(), job.clone(), None));
            Ok(task_id.to_string())
        } else {
            Err(())
        }
    }

    pub fn remove_from_queue(&mut self, task_id: &str) -> Option<()> {
        let index = self.jobs.iter().position(|(id, _, _)| id == task_id);
        if let Some(index) = index {
            self.jobs.remove(index);
            Some(())
        } else {
            None
        }
    }

    pub fn add_to_running(&mut self, task_id: &str, job: &JobConfiguration) {
        self.running.insert(task_id.to_string(), job.clone());
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
        Some(self.jobs_in_queue().len())
            >= self
                .configuration
                .global_limit
                .as_ref()
                .map(|limit| limit.max_queue)
    }
    fn queue_full_user(&self, uid: u32) -> bool {
        Some(
            self.jobs_in_queue()
                .iter()
                .filter(|(_, job, _, _)| job.uid == uid)
                .collect::<Vec<_>>()
                .len(),
        ) >= self
            .configuration
            .user_limit
            .as_ref()
            .map(|limit| limit.max_queue)
    }
    fn queue_full_group(&self, gid: u32) -> bool {
        Some(
            self.jobs_in_queue()
                .iter()
                .filter(|(_, job, _, _)| job.gid == gid)
                .collect::<Vec<_>>()
                .len(),
        ) >= self
            .configuration
            .group_limit
            .as_ref()
            .map(|limit| limit.max_queue)
    }

    fn running_full(&self) -> bool {
        Some(self.jobs_in_queue().len())
            >= self
                .configuration
                .global_limit
                .as_ref()
                .map(|limit| limit.max_running)
    }
    fn running_full_user(&self, uid: u32) -> bool {
        Some(
            self.jobs_in_queue()
                .iter()
                .filter(|(_, job, _, _)| job.uid == uid)
                .collect::<Vec<_>>()
                .len(),
        ) >= self
            .configuration
            .user_limit
            .as_ref()
            .map(|limit| limit.max_running)
    }
    fn running_full_group(&self, gid: u32) -> bool {
        Some(
            self.jobs_in_queue()
                .iter()
                .filter(|(_, job, _, _)| job.gid == gid)
                .collect::<Vec<_>>()
                .len(),
        ) >= self
            .configuration
            .group_limit
            .as_ref()
            .map(|limit| limit.max_running)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueueConfiguration {
    priority_rule: Vec<PriorityRule>,
    users: IdControl,
    groups: IdControl,
    properties: Properties,
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
        self.users.allow(uid)
            && self.groups.allow(gid)
            && !self.properties.conflict(&requirement.properties)
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
