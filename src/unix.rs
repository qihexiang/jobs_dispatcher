use serde::{Serialize, Deserialize};
use crate::jobs_management::JobConfiguration;

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientRequest {
    SubmitJob(String, JobConfiguration),
    DeleteJob(String),
    Status,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DispatcherResponse {
    InvalidRequest,
    SubmitSuccess(String),
    SubmitFailed,
    DeleteSuccess,
    DeleteFailed(DispatcherFailReasons),
    Status(),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DispatcherFailReasons {
    PermissionDenied,
    NotFound,
}