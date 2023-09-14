use clap::{Parser, Subcommand};

mod executor;
mod supervisor;

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
        #[arg(short, long)]
        config: String,
    },
    Vertex {
        config: String,
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
            supervisor::supervisor(&task_id, &data);
        }
        _ => {
            todo!()
        }
    }
}
