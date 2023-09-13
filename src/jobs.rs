use reqwest::Body;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env};
use tokio::{
    process::Command,
    time::{timeout, Duration},
};

use crate::resources::Resources;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobConfiguration {
    pub uid: u32,
    pub gid: u32,
    pub name: String,
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
    pub async fn execute(&self) -> Result<String, (u64, String)> {
        let execution = Phase::execute_all(&self.phases, self.uid, self.gid);

        let time_limit = timeout(Duration::from_secs(self.time_limit), execution).await;

        if let Ok(execution) = time_limit {
            execution
        } else {
            Err((1, "Timeout".to_string()))
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
    pub async fn execute_all(phases: &Vec<Phase>, uid: u32, gid: u32) -> Result<String, (u64, String)> {
        let mut log = String::new();
        for phase in phases {
            log.push_str(&phase.execute(uid, gid).await?);
        }
        Ok(log)
    }

    pub async fn execute(&self, uid: u32, gid: u32) -> Result<String, (u64, String)> {
        println!("current uid: {}, gid: {}", uid, gid);
        match self {
            Self::WORKDIR(directory) => env::set_current_dir(directory).map_or_else(
                |e| Err((1, e.to_string())),
                |()| Ok(format!("cd to {}", directory)),
            ),
            Self::ENV(environment) => {
                for (k, v) in environment.iter() {
                    env::set_var(k, v);
                }
                Ok(format!("{} environment variables set", environment.len()))
            }
            Self::SH(commands) => Command::new("bash")
                .uid(uid)
                .gid(gid)
                .arg("-c")
                .arg(commands)
                .output()
                .await
                .map_or_else(
                    |e| Err((1, e.to_string())),
                    |output| {
                        let stdout = String::from_utf8(output.stdout)
                            .unwrap_or("Failed to parse stdout".to_string());
                        let stderr = String::from_utf8(output.stderr)
                            .unwrap_or("Failed to parse stderr".to_string());
                        Ok(format!(
                            "\nstdout:\n=====\n{}\n<<<<<\nstderr:\n=====\n{}\n<<<<<",
                            stdout, stderr
                        ))
                    },
                ),
            Self::RUN(commands) => {
                let program = commands.get(0).expect("At least on argument.");
                let args = commands.iter().skip(1).collect::<Vec<_>>();
                Command::new(program)
                    .uid(uid)
                    .gid(gid)
                    .args(args).output().await.map_or_else(
                    |e| Err((1, e.to_string())),
                    |output| {
                        let stdout = String::from_utf8(output.stdout)
                            .unwrap_or("Failed to parse stdout".to_string());
                        let stderr = String::from_utf8(output.stderr)
                            .unwrap_or("Failed to parse stderr".to_string());
                        Ok(format!(
                            "stdout:\n=====\n{}\n<<<<<\nstderr:\n=====\n{}\n<<<<<",
                            stdout, stderr
                        ))
                    },
                )
            }
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum ProcessStatus {
    RUNNING(u128),
    PAUSE(u128, u128),
    FINISHED(u128),
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
        if let ProcessStatus::RUNNING(_) = self.process {
            true
        } else {
            false
        }
    }
}
