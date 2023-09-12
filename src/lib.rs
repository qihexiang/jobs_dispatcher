pub mod jobs;
pub mod resources;
pub mod queue;
pub mod http_utils;
pub mod request_util;

pub mod config {
    use std::fs;
    use serde_yaml;
    use serde::Deserialize;
    pub fn load_config<T>(paths: Vec<&str>) -> Option<T> 
    where T: for<'de> Deserialize<'de>{
        for path in paths {
            if let Ok(config_content) = fs::File::open(path) {
                println!("loading {}", path);
                match serde_yaml::from_reader::<_, T>(&config_content) {
                    Ok(config) => {
                        println!("configuration loaded");
                        return Some(config)
                    },
                    Err(err) => println!("failed to parse config {}, detail: \n{:#?}", path, err)
                }
            }
        }
        None
    }
}
