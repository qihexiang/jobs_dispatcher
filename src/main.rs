use clap::{Parser, Subcommand};
use client::ClientCommands;

pub mod client;
pub mod http;
pub mod unix;
pub mod utils;
pub mod auth;
pub mod vertex_client;
mod executor;
mod supervisor;
mod vertex;
mod dispatcher;
pub mod jobs_management;
pub mod queue_management;
pub mod resources_management;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: SubCommands,
}

#[derive(Subcommand, Debug)]
enum SubCommands {
    Dispatcher {
        config_path: String,
    },
    Vertex {
        config_path: String,
    },
    Supervisor {
        task_id: String,
        data: String,
    },
    Executor {
        data: String,
    },
    Client {
        #[command(subcommand)]
        operation: ClientCommands
    }
}

#[tokio::main]
async fn main() {
    let Cli { command } = Cli::parse();
    match command {
        SubCommands::Executor { data } => {
            executor::executor(&data);
        }
        SubCommands::Supervisor { task_id, data } => {
            supervisor::supervisor(&task_id, &data).await;
        }
        SubCommands::Vertex { config_path } => {
            vertex::vertex(&config_path).await;
        }
        SubCommands::Dispatcher { config_path } => {
            dispatcher::dispatcher(&config_path).await;
        }
        SubCommands::Client { operation } => {
            client::client(operation).await;
        }
    }
}

