use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_to_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}