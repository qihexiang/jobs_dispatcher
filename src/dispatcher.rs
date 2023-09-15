use std::{collections::HashMap, fs, sync::{Arc, RwLock}};

use crate::{server::HttpServerConfig, queue_management::{Queue, QueueConfiguration}};

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct VertexConnect {
    url: String,
    username: String,
    password: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DispatcherConfig {
    http: HttpServerConfig,
    vertexes: HashMap<String, VertexConnect>,
    queues: HashMap<String, QueueConfiguration>,
    persistent: String
}

struct DispatcherCachedState {
    configuration: DispatcherConfig,
    queues: Arc<RwLock<HashMap<String, Queue>>>
}

pub async fn dispatcher(config_path: &str) {
    let configuration: DispatcherConfig = serde_yaml::from_str(&fs::read_to_string(config_path).unwrap()).unwrap();
    let queue_in_conf = configuration.queues.iter().map(|(task_id, configuration)| (task_id.to_string(), Queue::new(configuration))).collect::<HashMap<_, _>>();
    let persistent: HashMap<String, Queue> = serde_json::from_str(&fs::read_to_string(configuration.persistent).unwrap_or("".to_string())).unwrap_or(HashMap::new());
}