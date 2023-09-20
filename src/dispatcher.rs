use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Result,
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::{
    jobs_management::JobConfiguration,
    queue_management::{Queue, QueueConfiguration, QueueGroup},
    utils::now_to_micros,
    vertex_client::{VertexClient, VertexConnect},
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{unix::UCred, UnixListener, UnixStream},
    time::timeout,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DispatcherConfig {
    listen: String,
    vertexes: HashMap<String, VertexConnect>,
    max_timeout: u64,
    loop_interval: u64,
    queues: HashMap<String, QueueConfiguration>,
    persistent: String,
}

#[derive(Clone)]
struct DispatcherCachedState {
    configuration: DispatcherConfig,
    vertex_status: Arc<RwLock<HashMap<String, (VertexClient, u128)>>>,
    queues: Arc<RwLock<QueueGroup>>,
}

pub async fn dispatcher(config_path: &str) {
    let configuration: DispatcherConfig =
        serde_yaml::from_str(&fs::read_to_string(config_path).unwrap()).unwrap();
    let mut queue_in_conf = configuration
        .queues
        .iter()
        .map(|(task_id, configuration)| (task_id.to_string(), Queue::new(configuration)))
        .collect::<HashMap<_, _>>();
    let persistent: HashMap<String, Queue> = serde_json::from_str(
        &fs::read_to_string(&configuration.persistent).unwrap_or("".to_string()),
    )
    .unwrap_or(HashMap::new());
    queue_in_conf.extend(persistent);
    let vertex_status = configuration
        .vertexes
        .iter()
        .map(|(name, config)| (name.to_string(), (config.create(), now_to_micros())))
        .collect::<HashMap<_, _>>();
    let cached_state = DispatcherCachedState {
        configuration,
        vertex_status: Arc::new(RwLock::new(vertex_status)),
        queues: Arc::new(RwLock::new(QueueGroup::new(queue_in_conf))),
    };

    let server_state = cached_state.clone();
    tokio::spawn(async move {
        let socket = UnixListener::bind(&server_state.configuration.listen).unwrap();
        loop {
            match socket.accept().await {
                Ok((mut stream, _)) => {
                    if let Ok(request) = get_request(&mut stream).await {
                        if let Ok(ucred) = stream.peer_cred() {
                            let mut status = server_state.clone();
                            let response = request.handle(&mut status, &ucred).await;
                            let _ = stream
                                .write_all(serde_json::to_string(&response).unwrap().as_bytes())
                                .await;
                            let _ = stream.shutdown().await;
                        } else {
                            let _ = stream
                                .write_all(
                                    serde_json::to_string(&DispatcherResponse::InvalidRequest)
                                        .unwrap()
                                        .as_bytes(),
                                )
                                .await;
                            let _ = stream.shutdown().await;
                        }
                    } else {
                        let _ = stream
                            .write_all(
                                serde_json::to_string(&DispatcherResponse::InvalidRequest)
                                    .unwrap()
                                    .as_bytes(),
                            )
                            .await;
                        let _ = stream.shutdown().await;
                    }
                }
                Err(err) => {
                    println!("Error: {:#?}", err);
                }
            }
        }
    });

    loop {
        for (_, (client, last_connected)) in cached_state.vertex_status.write().unwrap().iter_mut()
        {
            let request_free = client.free();
            let request_free = timeout(
                Duration::from_micros(cached_state.configuration.max_timeout),
                request_free,
            );
            if let Ok(Ok(request_free)) = request_free.await {
                *last_connected = now_to_micros();
                let mut queues = cached_state.queues.write().unwrap();
                while let Some((task_id, job, queue)) = queues.try_take_job(&request_free, false) {
                    let resp = client.submit_job(&task_id, &job).await;
                    if let Ok(resp) = resp {
                        if let Some(_) = queues.truly_take_job(&queue, &task_id, &resp, &job) {
                            println!("Submitted")
                        } else {
                            println!("Failed to submit job")
                        }
                    }
                }
            }

            let running_jobs = client.jobs();
            let running_jobs = timeout(
                Duration::from_micros(cached_state.configuration.max_timeout),
                running_jobs,
            );

            if let Ok(Ok(runnings)) = running_jobs.await {
                let running_ids = runnings.keys().cloned().collect::<HashSet<_>>();
                cached_state
                    .queues
                    .write()
                    .unwrap()
                    .refresh_running(&running_ids);
            }
        }
        tokio::time::sleep(Duration::from_micros(
            cached_state.configuration.loop_interval,
        ))
        .await;
    }
}

async fn get_request(stream: &mut UnixStream) -> Result<ClientRequest> {
    let mut content = String::new();
    let _size = stream.read_to_string(&mut content).await?;
    let request: ClientRequest = serde_json::from_str(&content)?;
    Ok(request)
}

#[derive(Serialize, Deserialize)]
enum ClientRequest {
    SubmitJob(String, JobConfiguration),
    DeleteJob(String),
    Status,
}

impl ClientRequest {
    async fn handle(self, status: &mut DispatcherCachedState, ucred: &UCred) -> DispatcherResponse {
        match self {
            Self::SubmitJob(queue, mut job) => {
                if ucred.uid() != 0 {
                    job.uid = ucred.uid();
                    job.gid = ucred.gid();
                }
                let submit = status.queues.write().unwrap().add_to_queue(&queue, &job);
                if let Ok(task_id) = submit {
                    DispatcherResponse::SubmitSuccess(task_id)
                } else {
                    DispatcherResponse::SubmitFailed
                }
            }
            Self::DeleteJob(task_id) => {
                let uid = ucred.uid();
                if let Some(result) = status.queues.write().unwrap().remove_job(&task_id, uid) {
                    if let Ok(_) = result {
                        DispatcherResponse::DeleteSuccess
                    } else {
                        DispatcherResponse::DeleteFailed(DispatcherFailReasons::PermissionDenied)
                    }
                } else {
                    DispatcherResponse::DeleteFailed(DispatcherFailReasons::NotFound)
                }
            }
            Self::Status => {
                // DispatcherResponse::Status(())
                todo!()
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
enum DispatcherResponse {
    InvalidRequest,
    SubmitSuccess(String),
    SubmitFailed,
    DeleteSuccess,
    DeleteFailed(DispatcherFailReasons),
    Status(),
}

#[derive(Serialize, Deserialize)]
enum DispatcherFailReasons {
    PermissionDenied,
    NotFound,
}
