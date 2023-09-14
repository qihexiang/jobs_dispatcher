use std::{
    collections::{HashMap, HashSet},
    env,
    fs::File,
    net::SocketAddr,
    process::{self, Stdio},
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::State,
    headers::{authorization::Basic, Authorization},
    http::StatusCode,
    routing::{get, post},
    Json, Router, TypedHeader,
};
use cgroups_rs::CgroupPid;
use job_dispatcher::{
    config::load_config,
    http_utils::basic_check,
    jobs::{JobConfiguration, ProcessStatus},
    resources::{Nodes, Resources},
};
use job_dispatcher::{jobs::JobStatus, request_util::VertexFreeApi};
use libc::chown;
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct VertexConfiguration {
    name: String,
    resources: Resources,
    cpus: Nodes,
    mems: Nodes,
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
    cpus: HashSet<usize>,
    resources: Resources,
    jobs: Arc<RwLock<Vec<JobStatus>>>,
}

impl VertexState {
    fn new(config: &VertexConfiguration) -> Self {
        Self {
            cpus: config.cpus.to_hashset(),
            resources: config.resources.clone(),
            jobs: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn free_nodes(&self) -> HashSet<usize> {
        let mut used = HashSet::new();
        for job in self.jobs.read().unwrap().iter() {
            if let ProcessStatus::RUNNING(_, nodes) = &job.process {
                used = &used | &nodes
            }
        }
        let free = self.cpus.difference(&used);
        free.cloned().collect::<HashSet<_>>()
    }

    fn free_countables(&self) -> HashMap<String, usize> {
        let mut resources = self.resources.clone();
        let jobs = self.jobs.read().unwrap();
        for job in jobs.iter().filter(|job| job.is_running()) {
            let usage = job.configuration.resources.countables();
            for (k, v) in usage {
                let current = resources.get_countable(k);
                resources = resources.set_countable(
                    k,
                    if let Some(result) = current.checked_sub(*v) {
                        result
                    } else {
                        0
                    },
                );
            }
        }
        resources.countables().clone()
    }
}

#[tokio::main]
async fn main() {
    if let Some(executor_data) = env::args().collect::<Vec<_>>().get(2) {
        let job_configuration: JobConfiguration = serde_json::from_str(executor_data).unwrap();
        match job_configuration.execute().await {
            Ok(_) => {
                println!("Job finished")
            }
            Err(_) => {
                println!("Job terminated with error")
            }
        };
    } else if let Some(executor_data) = env::args().collect::<Vec<_>>().get(1) {
        let job_configuration: JobConfiguration = serde_json::from_str(&executor_data).unwrap();
        let cgroup_id = Uuid::new_v4();
        println!("Perparing for job in cgroup {}", cgroup_id);
        let hier = cgroups_rs::hierarchies::auto();
        let cpus = job_configuration.cpus.to_string();
        let mems = job_configuration.mems.to_string();
        let cg = cgroups_rs::cgroup_builder::CgroupBuilder::new(&cgroup_id.to_string())
            .cpu()
            .cpus(cpus)
            .mems(mems)
            .done()
            .memory()
            .memory_hard_limit(job_configuration.resources.get_countable("memory") as i64)
            .done()
            .build(hier)
            .unwrap();
        cg.add_task_by_tgid(CgroupPid::from(process::id() as u64))
            .unwrap();

        let log_path = job_configuration.log;
        let log_file = File::options()
            .create_new(true)
            .write(true)
            .append(true)
            .open(log_path.clone())
            .unwrap();

        unsafe {
            let cpath = std::ffi::CString::new(log_path.as_str()).unwrap();
            chown(cpath.as_ptr(), job_configuration.uid, job_configuration.gid);
        }

        println!("log file {} created: ", log_path);

        let vertex = env::current_exe().unwrap();

        println!("Starting executor");

        let mut sub_process = Command::new(vertex)
            .arg("--")
            .arg(executor_data)
            .uid(job_configuration.uid)
            .gid(job_configuration.gid)
            .stdout(Stdio::from(log_file))
            .spawn()
            .unwrap();

        println!("Executor started");
            
        sub_process.wait()
            .await
            .unwrap();

        println!("Job in cgroup {} finished", cgroup_id);

        cg.remove_task_by_tgid(CgroupPid::from(process::id() as u64))
            .unwrap();
        cg.kill().unwrap();
        cg.delete().unwrap();

        println!("Cgroup {} destroyed", cgroup_id);
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
        let state = VertexState::new(&configuration);
        let app = Router::new()
            .route("/free", get(get_free))
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
        .cloned()
        .collect::<Vec<_>>();
    Json(jobs)
}

async fn execute_job(
    State(state): State<VertexState>,
    TypedHeader(Authorization(basic)): TypedHeader<Authorization<Basic>>,
    Json(job_configuration): Json<JobConfiguration>,
) -> Result<axum::Json<JobStatus>, (StatusCode, String)> {
    let mut job_configuration = job_configuration;
    let free_nodes = state.free_nodes();
    let occupied_nodes = match &job_configuration.cpus {
        Nodes::Select(nodes) => {
            let intersection = free_nodes.intersection(&nodes).collect::<HashSet<_>>();
            if intersection.len() == nodes.len() {
                Some(nodes.clone())
            } else {
                None
            }
        }
        Nodes::Use(amount) => {
            if free_nodes.len() >= *amount {
                Some(
                    free_nodes
                        .iter()
                        .take(*amount)
                        .cloned()
                        .collect::<HashSet<_>>(),
                )
            } else {
                None
            }
        },
        Nodes::Auto => {
            if free_nodes.len() > 0 {
                Some(free_nodes)
            } else {
                None
            }
        }
    }
    .map_or(
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "CPU nodes not enough".to_string(),
        )),
        |node| Ok(node),
    )?;
    job_configuration.cpus = Nodes::Select(occupied_nodes.clone());
    let task_id = Uuid::new_v4().to_string();
    let vertex = env::current_exe().unwrap();
    let executor_data = serde_json::to_string(&job_configuration)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
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
            occupied_nodes.clone(),
        ),
    };
    state.jobs.write().unwrap().push(new_job.clone());
    let jobs = state.jobs.clone();
    tokio::spawn(async move {
        if let Ok(sub_process) = Command::new(vertex).arg(executor_data).spawn() {
            let output = sub_process
                .wait_with_output()
                .await
                .map_err(|e| e.to_string())?;
            let mut jobs = jobs.write().unwrap();
            if let Some(id) = jobs.iter().position(|item| item.task_id == task_id) {
                if let Some(job) = jobs.get_mut(id) {
                    job.process = ProcessStatus::finish(
                        &String::from_utf8(output.stderr)
                            .unwrap_or("Unable to encode bad to string".to_string()),
                    )
                }
            } else {
                println!("Failed to find that job {}", task_id);
            }
        }
        Ok::<String, String>("Finished".to_string())
    });

    Ok(Json(new_job))
}

async fn get_free(state: State<VertexState>) -> axum::Json<VertexFreeApi> {
    Json(VertexFreeApi::new(
        &state.free_nodes(),
        &state.free_countables(),
    ))
}
