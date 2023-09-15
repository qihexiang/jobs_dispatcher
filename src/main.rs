use clap::{Parser, Subcommand};

pub mod server;
pub mod utils;
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
        _ => {
            todo!()
        }
    }
}

