use std::{
    env,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{ConnectInfo, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{Response, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use cgroups_rs::CgroupPid;
use dns_lookup::lookup_addr;
use job_dispatcher::{jobs::JobConfiguration, resources::Resources};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct VertexConfiguration {
    servers: Vec<String>,
    name: String,
    resources: Resources,
    ip: [u8; 4],
    port: u16,
    db: String,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
enum ProcessStatus {
    RUNNING(u128),
    PAUSE(u128, u128),
    FINISHED(u128),
}

#[derive(Clone, Serialize, Deserialize)]
struct JobStatus {
    task_id: String,
    configuration: JobConfiguration,
    process: ProcessStatus,
}

impl JobStatus {
    fn is_running(&self) -> bool {
        if let ProcessStatus::RUNNING(_) = self.process {
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
struct VertexState {
    servers: Vec<String>,
    resources: Resources,
    jobs: Arc<RwLock<Vec<JobStatus>>>,
}

impl VertexState {
    fn new(resources: &Resources, servers: &Vec<String>) -> Self {
        Self {
            servers: servers.clone(),
            resources: resources.clone(),
            jobs: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[tokio::main]
async fn main() {
    if let Some(executor_data) = env::args().collect::<Vec<_>>().get(0) {
        let job_configuration: JobConfiguration = serde_json::from_str(&executor_data).unwrap();
        job_configuration.execute().await.unwrap();
    } else {
        let configuration: VertexConfiguration = load_config().await;
        let state = VertexState::new(&configuration.resources, &configuration.servers);
        let app = Router::new()
            .route("/free", get(get_free_resouces))
            .route("/jobs", get(get_jobs))
            .route("/jobs", post(execute_job))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                request_source_check,
            ))
            .with_state(state);

        let addr = SocketAddr::from((configuration.ip, configuration.port));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    }
}

async fn request_source_check<B>(
    state: State<VertexState>,
    connect: ConnectInfo<SocketAddr>,
    req: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
    let ip_addr = connect.ip();
    if state.servers.contains(&ip_addr.to_string()) {
        Ok(next.run(req).await)
    } else if let Ok(hostname) = lookup_addr(&ip_addr) {
        if state.servers.contains(&hostname) {
            Ok(next.run(req).await)
        } else {
            Err(StatusCode::FORBIDDEN)
        }
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

async fn get_jobs(state: State<VertexState>) -> axum::Json<Vec<JobStatus>> {
    let jobs = state.jobs.read().unwrap().clone();
    Json(jobs)
}

async fn execute_job(
    state: State<VertexState>,
    Json(job_configuration): Json<JobConfiguration>,
) -> Result<axum::Json<JobStatus>, String> {
    let task_id = Uuid::new_v4().to_string();
    let vertex = env::current_exe().unwrap();
    let executor_data = serde_json::to_string(&job_configuration).map_err(|e| e.to_string())?;
    let new_job = JobStatus {
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
        let hier = cgroups_rs::hierarchies::auto();
        let cg = cgroups_rs::cgroup_builder::CgroupBuilder::new(&task_id.to_string())
            .cpu()
            .shares(job_configuration.resources.get_countable("cpu"))
            .done()
            .memory()
            .memory_hard_limit(job_configuration.resources.get_countable("memroy") as i64)
            .done()
            .build(hier)
            .map_err(|e| e.to_string())?;
        if let Ok(mut sub_process) = Command::new(vertex).arg(executor_data).spawn() {
            if let Ok(_) = cg.add_task(CgroupPid::from(sub_process.id().unwrap() as u64)) {
                sub_process.wait().await.map_err(|e| e.to_string())?;
                let mut jobs = jobs.write().unwrap();
                let id = jobs
                    .iter()
                    .position(|item| item.task_id == task_id)
                    .unwrap();
                jobs.remove(id);
            }
        }
        cg.delete().map_err(|e| e.to_string())?;
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

async fn load_config() -> VertexConfiguration {
    for target_path in [
        "/usr/local/etc/vertex.yml",
        "/etc/local/vertex.yml",
        "/root/.config/vertex.yml",
        "./vertex.yml",
    ] {
        if let Ok(data) = tokio::fs::read_to_string(target_path).await {
            println!("File {} loaded", target_path);
            return serde_yaml::from_str(&data).unwrap();
        }
    }
    panic!("Failed to load configuration file")
}
