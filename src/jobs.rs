use chrono::Local;
use reqwest::Body;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    time::{SystemTime, UNIX_EPOCH}
};
use tokio::{
    process::Command,
    time::{timeout, Duration},
};

use crate::resources::{Nodes, Resources};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobConfiguration {
    pub uid: u32,
    pub gid: u32,
    pub name: String,
    pub cpus: Nodes,
    #[serde(default = "default_log_path")]
    pub log: String,
    #[serde(default = "default_cpu_mems")]
    pub mems: Nodes,
    #[serde(default)]
    pub time_limit: u64,
    #[serde(default)]
    pub resources: Resources,
    pub phases: Vec<Phase>,
}

impl Into<Body> for JobConfiguration {
    fn into(self) -> Body {
        Body::from(serde_json::to_string(&self).unwrap_or("null".to_string()))
    }
}

impl JobConfiguration {
    pub fn set_cpuset(self, cpus: Nodes, mems: Nodes) -> Self {
        Self { cpus, mems, ..self }
    }

    pub async fn execute(&self) -> Result<(), ()> {
        println!("Job starting. {}", self.name);

        let execution = Phase::execute_all(&self.phases, self.uid, self.gid);

        let time_limit = timeout(Duration::from_secs(self.time_limit), execution).await;

        if let Ok(execution) = time_limit {
            execution
        } else {
            Err(())
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Phase {
    WORKDIR(String),
    SH(String),
    RUN(Vec<String>),
    ENV(HashMap<String, String>),
}

impl Phase {
    pub async fn execute_all(
        phases: &Vec<Phase>,
        uid: u32,
        gid: u32,
    ) -> Result<(), ()> {
        for (i, phase) in phases.iter().enumerate() {
            println!("\nPhase {}: {:?}\n", { i + 1 }, phase);
            phase.execute(uid, gid).await?
        }
        Ok(())
    }

    pub async fn execute(&self, uid: u32, gid: u32) -> Result<(), ()> {
        match self {
            Self::WORKDIR(directory) => {
                println!("change directory to {}", directory);
                env::set_current_dir(directory).map_or(Err(()), |()| Ok(()))
            }
            Self::ENV(environment) => {
                for (k, v) in environment.iter() {
                    env::set_var(k, v);
                    println!("set env {} to {}\n", k, v);
                }
                println!(
                    "\n{} environment variables set\n",
                    environment.len()
                );
                Ok(())
            }
            Self::SH(commands) => Command::new("bash")
                .uid(uid)
                .gid(gid)
                .arg("-c")
                .arg(commands)
                .spawn()
                .unwrap()
                .wait()
                .await
                .map_or(Err(()), |_| Ok(())),
            Self::RUN(commands) => {
                let program = commands.get(0).expect("At least on argument.");
                let args = commands.iter().skip(1).collect::<Vec<_>>();
                Command::new(program)
                    .uid(uid)
                    .gid(gid)
                    .args(args)
                    .spawn()
                    .unwrap()
                    .wait()
                    .await
                    .map_or(Err(()), |_| Ok(()))
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ProcessStatus {
    RUNNING(u128, HashSet<usize>),
    FINISHED(u128, String),
}

impl ProcessStatus {
    pub fn finish(output: &str) -> Self {
        Self::FINISHED(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            output.to_string(),
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobStatus {
    pub task_id: String,
    pub basic_user: String,
    pub configuration: JobConfiguration,
    pub process: ProcessStatus,
}

impl JobStatus {
    pub fn is_running(&self) -> bool {
        if let ProcessStatus::RUNNING(_, _) = self.process {
            true
        } else {
            false
        }
    }
}

fn default_log_path() -> String {
    let mut cwd = env::current_dir().unwrap();
    cwd.push(format!("jobs_dispatcher_{}.log", Local::now().format("%Y%m%d%H%M%S").to_string()));
    cwd.to_str().unwrap().to_string()
}

fn default_cpu_mems() -> Nodes {
    Nodes::Select(HashSet::from([0]))
}
