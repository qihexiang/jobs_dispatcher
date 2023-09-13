use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};

use crate::jobs::JobConfiguration;

pub struct Queue {
    name: String,
    configuration: QueueConfiguration,
    jobs: Vec<(Instant, JobConfiguration)>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueConfiguration {
    priority: CountableWeight,
    policy: QueueJobPolicy
}

// impl QueueConfiguration {
//     pub fn calc_job_weight(&self, job: &JobConfiguration, waiting: Duration) -> f64 {
//         let policy = &self.policy;
//         let mut weight = 0.;
//         let waiting_seconds = waiting.as_secs_f64();
//         weight += waiting_seconds * policy.wait_weight;
//         weight += (job.time_limit as f64) * policy.time_limit_weight;
//         for (k, cw) in &policy.resources_weight {
//             weight += (job.resources.get_countable(k) as f64) * cw;
//         }
//         for (k, v, cw) in &policy.property_weight {
//             if job.resources.property_is(k, v) {
//                 weight = cw.offset + weight * cw.factor
//             }
//         }
//         if let Some(job_gid) = job.gid {
//             if let Some((_, cw)) = policy.group_weight.iter().find(|(gid, _)| gid == &job_gid) {
//                 weight = cw.offset + weight * cw.factor
//             }
//         }
//         if let Some((_, cw)) = policy.user_weight.iter().find(|(uid, _)| uid == &job.uid) {
//             weight = cw.offset + weight * cw.factor
//         }   
//         self.priority.offset + self.priority.factor * weight
//     }
// }

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueJobPolicy {
    #[serde(default)]
    resources_weight: Vec<(String, f64)>,
    #[serde(default)]
    user_weight: Vec<(u32, CountableWeight)>,
    #[serde(default)]
    group_weight: Vec<(u32, CountableWeight)>,
    #[serde(default = "default_zero")]
    time_limit_weight: f64,
    #[serde(default = "default_one")]
    wait_weight: f64,
    #[serde(default)]
    property_weight: Vec<(String, String, CountableWeight)>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CountableWeight {
    #[serde(default = "default_zero")]
    offset: f64,
    #[serde(default = "default_one")]
    factor: f64
}

fn default_one() -> f64 {
    1.0
}

fn default_zero() -> f64 {
    0.0
}

#[test]
fn load_queue_configuration() {
    let file_content = std::fs::read_to_string("./example/data/queue.yml").unwrap();
    let configuration: QueueConfiguration = serde_yaml::from_str(&file_content).unwrap();
    println!("{:#?}", configuration)
}
