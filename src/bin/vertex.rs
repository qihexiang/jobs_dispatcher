use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH}, process,
};

use axum::{
    extract::State,
    headers::{authorization::Basic, Authorization},
    routing::{get, post},
    Json, Router, TypedHeader,
};
use cgroups_rs::CgroupPid;
use job_dispatcher::jobs::JobStatus;
use job_dispatcher::{
    config::load_config,
    http_utils::basic_check,
    jobs::{JobConfiguration, ProcessStatus},
    resources::Resources,
};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct VertexConfiguration {
    name: String,
    resources: Resources,
    #[serde(default = "listen_all")]
    ip: [u8; 4],
    #[serde(default = "default_port")]
    port: u16,
    user_table: HashMap<String, String>,
}

fn listen_all() -> [u8; 4] {
    [0, 0, 0, 0]
}

fn default_port() -> u16 {
    9500
}

#[derive(Clone)]
struct VertexState {
    resources: Resources,
    jobs: Arc<RwLock<Vec<JobStatus>>>,
}

impl VertexState {
    fn new(resources: &Resources) -> Self {
        Self {
            resources: resources.clone(),
            jobs: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[tokio::main]
async fn main() {
    if let Some(executor_data) = env::args().collect::<Vec<_>>().get(1) {
        let job_configuration: JobConfiguration = serde_json::from_str(&executor_data).unwrap();
        let task_id = Uuid::new_v4();
        let hier = cgroups_rs::hierarchies::auto();
        let cg = cgroups_rs::cgroup_builder::CgroupBuilder::new(&task_id.to_string())
            .cpu()
            .shares(job_configuration.resources.get_countable("cpu"))
            .done()
            .memory()
            .memory_hard_limit(job_configuration.resources.get_countable("memory") as i64)
            .done()
            .build(hier)
            .map_err(|e| e.to_string())
            .unwrap();
        cg.add_task_by_tgid(CgroupPid::from(process::id() as u64)).unwrap();
        job_configuration.execute().await.unwrap();
        cg.move_task_to_parent_by_tgid(CgroupPid::from(process::id() as u64)).unwrap();
        cg.delete().unwrap();
    } else {
        let configuration: VertexConfiguration =
            if let Ok(config_path) = env::var("VERTEX_CONFIG_PATH") {
                load_config(vec![&config_path])
            } else {
                load_config(vec![
                    "./vertex.yml",
                    "/etc/vertex.yml",
                    "/usr/local/etc/vertex.yml",
                ])
            }
            .expect("No validate config file found.");
        let state = VertexState::new(&configuration.resources);
        let app = Router::new()
            .route("/free", get(get_free_resouces))
            .route("/jobs", get(get_jobs))
            .route("/job", post(execute_job))
            .layer(axum::middleware::from_fn_with_state(
                configuration.user_table.clone(),
                basic_check,
            ))
            .with_state(state);

        let addr = SocketAddr::from((configuration.ip, configuration.port));
        axum::Server::bind(&addr)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .unwrap();
    }
}

async fn get_jobs(
    state: State<VertexState>,
    TypedHeader(Authorization(basic)): TypedHeader<Authorization<Basic>>,
) -> axum::Json<Vec<JobStatus>> {
    let jobs = state
        .jobs
        .read()
        .unwrap()
        .iter()
        .filter(|job| job.basic_user == basic.username())
        .map(|job| job.clone())
        .collect::<Vec<_>>();
    Json(jobs)
}

async fn execute_job(
    State(state): State<VertexState>,
    TypedHeader(Authorization(basic)): TypedHeader<Authorization<Basic>>,
    Json(job_configuration): Json<JobConfiguration>,
) -> Result<axum::Json<JobStatus>, String> {
    let task_id = Uuid::new_v4().to_string();
    let vertex = env::current_exe().unwrap();
    let executor_data = serde_json::to_string(&job_configuration).map_err(|e| e.to_string())?;
    let username = basic.username();
    let new_job = JobStatus {
        basic_user: username.to_string(),
        task_id: task_id.clone(),
        configuration: job_configuration.clone(),
        process: ProcessStatus::RUNNING(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        ),
    };
    let mut jobs = state.jobs.write().unwrap();
    jobs.push(new_job.clone());
    let jobs = state.jobs.clone();
    tokio::spawn(async move {
        // let hier = cgroups_rs::hierarchies::auto();
        // let cg = cgroups_rs::cgroup_builder::CgroupBuilder::new(&task_id.to_string())
        //     .cpu()
        //     .shares(job_configuration.resources.get_countable("cpu"))
        //     .done()
        //     .memory()
        //     .memory_hard_limit(job_configuration.resources.get_countable("memroy") as i64)
        //     .done()
        //     .build(hier)
        //     .map_err(|e| e.to_string())?;
        if let Ok(mut sub_process) = Command::new(vertex).arg(executor_data).spawn() {
            println!("subprocess created");
            sub_process.wait().await.map_err(|e| e.to_string())?;
            let mut jobs = jobs.write().unwrap();
            let id = jobs
                .iter()
                .position(|item| item.task_id == task_id)
                .unwrap();
            jobs.remove(id);
            // }
        }
        // cg.delete().map_err(|e| e.to_string())?;
        Ok::<String, String>("Finished".to_string())
    });

    Ok(Json(new_job))
}

async fn get_free_resouces(state: State<VertexState>) -> axum::Json<Resources> {
    let jobs = state.jobs.read().unwrap();
    let runnings = jobs
        .iter()
        .filter(|job| job.is_running())
        .collect::<Vec<_>>();
    let mut current_free = state.resources.clone();
    for running in runnings {
        let usage = running.configuration.resources.countables();
        for (k, v) in usage {
            let current = current_free.get_countable(k);
            current_free = current_free.set_countable(
                k,
                if let Some(result) = current.checked_sub(*v) {
                    result
                } else {
                    0
                },
            );
        }
    }
    Json(current_free)
}
