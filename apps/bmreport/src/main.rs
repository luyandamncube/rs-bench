// apps\bmreport\src\main.rs
use anyhow::Result;
use bm_report::{
    compare_runs, print_terminal_summary, summarize_run,
};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "bmreport")]
#[command(about = "Benchmark reporting tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Summarize {
        #[arg(long)]
        input: String,
    },
    Compare {
        #[arg(long, num_args = 1..)]
        inputs: Vec<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Summarize { input } => {
            let (summaries, output_path) = summarize_run(&input)?;
            print_terminal_summary(&summaries);
            println!();
            println!("Summary written to {}", output_path.display());
        }
        Commands::Compare { inputs } => {
            let (summaries, output_path) = compare_runs(&inputs)?;
            print_terminal_summary(&summaries);
            println!();
            println!("Comparison written to {}", output_path.display());
        }
    }

    Ok(())
}