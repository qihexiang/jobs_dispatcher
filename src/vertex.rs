use std::{
    collections::{HashMap, HashSet},
    fs,
    net::SocketAddr,
    sync::{Arc, RwLock}, thread::spawn, process::Command, env,
};

use crate::{
    jobs_management::JobConfiguration,
    resources_management::{ResourcesProvider, ResourcesRequirement, NodesRequirement},
    server::{basic_check, HttpServerConfig}, utils::now_to_secs,
};
use axum::{
    http::StatusCode,
    extract::State,
    headers::{authorization::Basic, Authorization},
    middleware,
    response::{Response, IntoResponse},
    routing::{get, post},
    Json, Router, TypedHeader,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct VertexConfig {
    #[serde(default)]
    http: HttpServerConfig,
    basic: HashMap<String, String>,
    resources: ResourcesProvider,
    history: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum VertexJobStatus {
    Running(JobConfiguration, u64),
    Error {
        configuration: JobConfiguration,
        status_code: i32,
        error_message: String,
        exit_at: u64
    },
    Finished(JobConfiguration, u64),
}

#[derive(Debug, Clone)]
struct VertexState {
    configuration: VertexConfig,
    jobs: Arc<RwLock<HashMap<(String, String), VertexJobStatus>>>,
}

pub async fn vertex(config_path: &str) {
    let configuration: VertexConfig = serde_yaml::from_str(&fs::read_to_string(config_path).unwrap()).unwrap();
    let history: HashMap<(String, String), VertexJobStatus> =
        serde_json::from_str(&fs::read_to_string(&configuration.history).unwrap()).unwrap();
    let state = VertexState {
        configuration,
        jobs: Arc::new(RwLock::new(history)),
    };
    let app = Router::new()
        .route("/", get(get_free))
        .route("/jobs", get(get_jobs))
        .route("/job", post(submit_job))
        .layer(middleware::from_fn_with_state(
            state.configuration.basic.clone(),
            basic_check,
        ))
        .with_state(state.clone());
    let addr = SocketAddr::from((state.configuration.http.ip, state.configuration.http.port));
    axum::Server::bind(&addr)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();
}

async fn get_free(State(state): State<VertexState>) -> Json<ResourcesProvider> {
    let available_resources = current_free(&state);
    Json(available_resources)
}

async fn get_jobs(
    State(state): State<VertexState>,
    TypedHeader(Authorization(basic)): TypedHeader<Authorization<Basic>>,
) -> Json<HashMap<String, VertexJobStatus>> {
    let username = basic.username();
    let jobs = state.jobs.read().unwrap();
    let filtered = jobs
        .iter()
        .filter(|((user, _), _)| user == username)
        .map(|((_, task_id), job_status)| (task_id.clone(), job_status.clone()))
        .collect::<HashMap<String, VertexJobStatus>>();
    Json(filtered)
}

async fn submit_job(
    State(state): State<VertexState>,
    TypedHeader(Authorization(basic)): TypedHeader<Authorization<Basic>>,
    Json(job_configuration): Json<JobConfiguration>,
) -> Response {
    let mut available_resources = current_free(&state);
    if available_resources.mems.len() == 0 {
        available_resources.mems = HashSet::from([0]);
    }
    if available_resources.acceptable(&job_configuration.requirement) {
        let mut job_configuration = job_configuration;
        if let NodesRequirement::Use(size) = job_configuration.requirement.cpus {
            job_configuration.requirement.cpus = NodesRequirement::Select(
                available_resources.cpus.into_iter().take(size).collect::<HashSet<_>>()
            );
        } else if let NodesRequirement::Auto = job_configuration.requirement.cpus {
            job_configuration.requirement.cpus = NodesRequirement::Select(
                available_resources.cpus
            )
        };
        if let NodesRequirement::Use(size) = job_configuration.requirement.mems {
            job_configuration.requirement.cpus = NodesRequirement::Select(
                available_resources.mems.into_iter().take(size).collect::<HashSet<_>>()
            );
        } else if let NodesRequirement::Auto = job_configuration.requirement.cpus {
            job_configuration.requirement.cpus = NodesRequirement::Select(
                available_resources.mems
            )
        };
        let task_id = Uuid::new_v4().to_string();
        let username = basic.username().to_string();
        state.jobs.write().unwrap().insert(
            (username.to_string(), task_id.clone()), VertexJobStatus::Running(job_configuration.clone(), now_to_secs())
        );
        let jobs = state.jobs.clone();
        let task_id_supervisor = task_id.clone();
        spawn(move || {
            let program = env::current_exe().unwrap();
            let mut command = Command::new(program)
                .arg("supervisor")
                .arg(serde_json::to_string(&job_configuration).unwrap())
                .spawn()
                .unwrap();
            let exit_status = command.wait().unwrap();
            let mut jobs = jobs.write().unwrap();
            if exit_status.success() {
                jobs.insert((username, task_id_supervisor), VertexJobStatus::Finished(job_configuration, now_to_secs()));
            } else {
                jobs.insert((username, task_id_supervisor), VertexJobStatus::Error { configuration: job_configuration, status_code: exit_status.code().unwrap_or(1), error_message: exit_status.to_string(), exit_at: now_to_secs() });
            }
        });
        (StatusCode::OK, task_id).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Resources not enough").into_response()
    }
}

fn current_free(state: &VertexState) -> ResourcesProvider {
    let mut available_resources = state.configuration.resources.clone();
    for (_, job_status) in state.jobs.read().unwrap().iter() {
        if let VertexJobStatus::Running(JobConfiguration { requirement, .. }, _) = job_status {
            let ResourcesRequirement {
                cpus,
                mems,
                countables,
                ..
            } = requirement;
            available_resources.cpus = available_resources
                .cpus
                .difference(cpus.take_set())
                .cloned()
                .collect::<HashSet<_>>();
            available_resources.mems = available_resources
                .mems
                .difference(mems.take_set())
                .cloned()
                .collect::<HashSet<_>>();
            for (k, v) in countables.get_all() {
                let current = available_resources.countables.get(k);
                available_resources
                    .countables
                    .set(k, current.checked_sub(*v).unwrap_or(0))
            }
        }
    }
    available_resources
}
