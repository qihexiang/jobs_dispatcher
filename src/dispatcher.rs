use std::{env, collections::HashMap};
use job_dispatcher::{queue::QueueConfiguration, jobs::JobConfiguration};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DispatcherConfiguration {
    vertexes: Vec<String>,
    queues: HashMap<String, JobConfiguration>,
    persistent: String,
    ip: [u8; 4],
    port: u16,
}

#[tokio::main]
async fn main() {
    let configuration = load_config().await;
}

async fn load_config() -> DispatcherConfiguration {
    if let Ok(target_path) = env::var("DISPATCHER_CONFIG_PATH") {
        if let Ok(data) = tokio::fs::read_to_string(&target_path).await {
            println!("File {} loaded", target_path);
            return serde_yaml::from_str(&data).unwrap();
        } else {
            panic!("Failed to parse file: {}", target_path)
        }
    } else {
        for target_path in [
            "/usr/local/etc/dispatcher.yml",
            "/etc/local/dispatcher.yml",
            "/root/.config/dispatcher.yml",
            "./dispatcher.yml",
        ] {
            if let Ok(data) = tokio::fs::read_to_string(target_path).await {
                println!("File {} loaded", target_path);
                return serde_yaml::from_str(&data).unwrap();
            } else {
                panic!("Failed to parse file: {}", target_path)
            }
        }
        panic!("Failed to load configuration file")
    }
}

async fn load_queue(name: &str, persistent: &str) -> Vec<JobConfiguration> {
    let target_file_path = format!("{}/{}.json", persistent, name);
    if let Ok(content) = tokio::fs::read_to_string(target_file_path).await {
        serde_json::from_str(&content).unwrap()
    } else {
        Vec::new()
    }
}
