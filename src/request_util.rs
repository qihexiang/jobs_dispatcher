use std::collections::{HashSet, HashMap};

use crate::{
    jobs::{JobConfiguration, JobStatus},
    resources::Resources,
};

use reqwest::{Body, Client, RequestBuilder};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VertexFreeApi {
    cpus: HashSet<usize>,
    countables: HashMap<String, usize>
}

impl VertexFreeApi {
    pub fn new(cpus: &HashSet<usize>, countables: &HashMap<String, usize>) -> Self {
        Self {
            cpus: cpus.clone(), countables: countables.clone()
        }
    }
}

pub struct VertexClient {
    server: String,
    username: String,
    password: String,
    client: Client,
}

impl VertexClient {
    pub fn new(server: &str, username: &str, password: &str) -> Self {
        Self {
            server: server.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            client: Client::new(),
        }
    }

    fn username(&self) -> String {
        self.username.clone()
    }

    fn password(&self) -> String {
        self.password.clone()
    }

    fn get(&self, pathname: &str) -> RequestBuilder {
        let url = format!("{}{}", self.server, pathname);
        println!("{}", url);
        self.client
            .get(url)
            .basic_auth(self.username(), Some(self.password()))
    }

    fn post<T: Into<Body>>(&self, pathname: &str, body: T) -> RequestBuilder {
        let url = format!("{}{}", self.server, pathname);
        self.client
            .post(url)
            .header("Content-Type", "application/json")
            .basic_auth(self.username(), Some(self.password()))
            .body(body)
    }

    pub async fn free(&self) -> Result<VertexFreeApi, String> {
        self.get("/free")
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn jobs(&self) -> Result<Vec<JobStatus>, String> {
        self.get("/jobs")
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn submit_job(&self, job: JobConfiguration) -> Result<JobStatus, String> {
        let resp = self.post("/job", job)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        println!("{}", resp.status());
        resp.json()
            .await
            .map_err(|e| e.to_string())
    }
}

#[tokio::test]
async fn send_request() {
    let vc = VertexClient::new(
        "http://localhost:9500",
        "jack1", "203JKFKDdfa"
    );
    let res = vc.free().await.unwrap();
    println!("{:#?}", res);
    let job_configuration: JobConfiguration = serde_yaml::from_reader(std::fs::File::open("./example/data/helloWorld.yml").unwrap()).unwrap();
    let res = vc.submit_job(job_configuration).await;
    println!("{:#?}", res);
}
