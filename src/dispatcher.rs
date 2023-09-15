use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use crate::{
    queue_management::{Queue, QueueConfiguration},
    server::HttpServerConfig,
    utils::now_to_micros,
};

use axum::{
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct VertexConnect {
    url: String,
    username: String,
    password: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DispatcherConfig {
    http: HttpServerConfig,
    vertexes: HashMap<String, VertexConnect>,
    vertex_lost: u128,
    loop_interval: u128,
    queues: HashMap<String, QueueConfiguration>,
    persistent: String,
}

#[derive(Clone)]
struct DispatcherCachedState {
    configuration: DispatcherConfig,
    vertex_status: Arc<RwLock<HashMap<String, u128>>>,
    queues: Arc<RwLock<HashMap<String, Queue>>>,
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
        .keys()
        .map(|key| (key.clone(), now_to_micros()))
        .collect::<HashMap<_, _>>();
    let cached_state = DispatcherCachedState {
        configuration,
        vertex_status: Arc::new(RwLock::new(vertex_status)),
        queues: Arc::new(RwLock::new(queue_in_conf)),
    };

    let app = Router::new()
        .route("/", get(root))
        .with_state(cached_state.clone());

    let addr = SocketAddr::from((cached_state.configuration.http.ip, cached_state.configuration.http.port));

    // A thread for maintain all queues and vertexes connection
    tokio::spawn(async move {
        for (k, VertexConnect {url, username, password}) in &cached_state.configuration.vertexes {

        }
    });

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn root() -> &'static str {
    "Hello, World!"
}
