use std::{env, time::Duration};

use clap::Subcommand;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::timeout,
};

use crate::{
    jobs_management::JobConfiguration,
    unix::{ClientRequest, DispatcherResponse},
};

#[derive(Subcommand, Debug)]
pub enum ClientCommands {
    Submit { queue: String, filepath: String },
    Delete { id: String },
    Status,
}

pub async fn client(command: ClientCommands) {
    let mut server = UnixStream::connect(
        env::var("JOB_DISPATCHER_SOCKET").unwrap_or("/tmp/job_dispatcher.socket".to_string()),
    )
    .await
    .unwrap();
    let request = match command {
        ClientCommands::Submit { queue, filepath } => {
            let content = fs::read_to_string(filepath).await.unwrap();
            let job: JobConfiguration = serde_yaml::from_str(&content).unwrap();
            ClientRequest::SubmitJob(queue, job)
        }
        ClientCommands::Delete { id } => ClientRequest::DeleteJob(id),
        ClientCommands::Status => ClientRequest::Status,
    };
    let data = serde_json::to_string(&request).unwrap();
    let data = data.as_bytes();
    server.write_all(data).await.unwrap();
    server.shutdown().await.unwrap();
    let mut response = String::new();
    let time_limit = timeout(Duration::from_secs(5), server.read_to_string(&mut response)).await;
    if let Ok(Ok(_)) = time_limit {
        let response: DispatcherResponse = serde_json::from_str(&response).unwrap();
        println!("{:#?}", response);
    } else if let Ok(Err(err)) = time_limit {
        panic!("{:#?}", err)
    } else {
        panic!("Timeout! Is server running correctly?")
    }
}
