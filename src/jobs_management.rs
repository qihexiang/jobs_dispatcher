use std::{collections::HashMap, env, process::Command};

use reqwest::Body;
use serde::{Deserialize, Serialize};

use crate::resources_management::ResourcesRequirement;

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum ExecutePhase {
    Sh(String),
    Run(Vec<String>),
    WorkDir(String),
    Env(HashMap<String, String>),
}

impl ExecutePhase {
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

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct JobConfiguration {
    pub name: String,
    pub uid: u32,
    pub gid: u32,
    pub stdout_file: String,
    pub stderr_file: String,
    pub requirement: ResourcesRequirement,
    phases: Vec<ExecutePhase>,
}

impl Into<Body> for JobConfiguration {
    fn into(self) -> Body {
        Body::from(
            serde_json::to_string(&self).unwrap()
        )
    }
}

impl JobConfiguration {
    pub fn execute(&self) -> Result<(), std::io::Error> {
        for phase in &self.phases {
            phase.execute()?
        }
        Ok(())
    }
}