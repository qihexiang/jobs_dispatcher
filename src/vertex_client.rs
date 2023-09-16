use std::collections::HashMap;

use crate::{
    jobs_management::JobConfiguration,
    resources_management::ResourcesProvider, vertex::VertexJobStatus
};

use reqwest::{Body, Client, RequestBuilder};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VertexConnect {
    url: String,
    username: String,
    password: String,
}

impl VertexConnect {
    pub fn new(url: &str, username: &str, password: &str) -> Self {
        Self {
            url: url.to_string(), username: username.to_string(), password: password.to_string()
        }
    }

    pub fn create(&self) -> VertexClient {
        VertexClient { url: self.url.clone(), username: self.username.clone(), password: self.password.clone(), client: Client::new() }
    }
}

pub struct VertexClient {
    url: String,
    username: String, 
    password: String,
    client: Client
}

impl VertexClient {
    fn username(&self) -> String {
        self.username.clone()
    }

    fn password(&self) -> String {
        self.password.clone()
    }

    fn get(&self, pathname: &str) -> RequestBuilder {
        let url = format!("{}{}", self.url, pathname);
        println!("{}", url);
        self.client
            .get(url)
            .basic_auth(self.username(), Some(self.password()))
    }

    fn post<T: Into<Body>>(&self, pathname: &str, body: T) -> RequestBuilder {
        let url = format!("{}{}", self.url, pathname);
        self.client
            .post(url)
            .header("Content-Type", "application/json")
            .basic_auth(self.username(), Some(self.password()))
            .body(body)
    }

    pub async fn free(&self) -> Result<ResourcesProvider, String> {
        self.get("/free")
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn jobs(&self) -> Result<HashMap<String, VertexJobStatus>, String> {
        self.get("/jobs")
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn submit_job(&self, task_id: &str, job: &JobConfiguration) -> Result<String, String> {
        let resp = self.post(&format!("/job/{}", task_id), job.clone())
            .send()
            .await
            .map_err(|e| e.to_string())?;
        println!("{}", resp.status());
        resp.text()
            .await
            .map_err(|e| e.to_string())
    }
}
