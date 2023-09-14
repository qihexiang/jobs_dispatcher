use crate::jobs_management::JobConfiguration;

pub fn executor(input: &str) {
    let job_configuration: JobConfiguration = serde_json::from_str(input).unwrap();
    job_configuration.execute().unwrap();
}