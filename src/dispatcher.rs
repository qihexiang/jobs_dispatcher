use std::{
    collections::{HashMap, HashSet},
    fs,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::{
    queue_management::{Queue, QueueConfiguration, QueueGroup},
    server::HttpServerConfig,
    utils::now_to_micros,
    vertex_client::{VertexClient, VertexConnect},
};

use axum::{
    middleware::MapRequest,
    routing::{get, post},
    Router,
};

use serde::{Deserialize, Serialize};
use tokio::time::timeout;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DispatcherConfig {
    http: HttpServerConfig,
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

    let app = Router::new()
        .route("/", get(root))
        .with_state(cached_state.clone());

    let addr = SocketAddr::from((
        cached_state.configuration.http.ip,
        cached_state.configuration.http.port,
    ));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    loop {
        for (_, (client, last_connected)) in
            cached_state.vertex_status.write().unwrap().iter_mut()
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
                cached_state.queues.write().unwrap().refresh_running(&running_ids);
            }
        }
    }
}

async fn root() -> &'static str {
    "Hello, World!"
}
