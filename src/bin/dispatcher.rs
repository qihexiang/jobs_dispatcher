use std::env;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DispatcherConfiguration {
    vertexes: Vec<String>,
    ip: [u8; 4],
    port: u16,
}

#[tokio::main]
async fn main() {

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
