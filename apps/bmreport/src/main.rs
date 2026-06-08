// apps\bmreport\src\main.rs
use anyhow::Result;
use bm_report::{
    compare_runs, compare_streaming_runs, print_terminal_streaming_summary, print_terminal_summary,
    summarize_run, summarize_streaming_run,
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
    SummarizeStreaming {
        #[arg(long)]
        input: String,
    },
    Compare {
        #[arg(long, num_args = 1..)]
        inputs: Vec<String>,
    },
    CompareStreaming {
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
        Commands::SummarizeStreaming { input } => {
            let (summaries, output_path) = summarize_streaming_run(&input)?;
            print_terminal_streaming_summary(&summaries);
            println!();
            println!("Streaming summary written to {}", output_path.display());
        }
        Commands::Compare { inputs } => {
            let (summaries, output_path) = compare_runs(&inputs)?;
            print_terminal_summary(&summaries);
            println!();
            println!("Comparison written to {}", output_path.display());
        }
        Commands::CompareStreaming { inputs } => {
            let (summaries, output_path) = compare_streaming_runs(&inputs)?;
            print_terminal_streaming_summary(&summaries);
            println!();
            println!("Streaming comparison written to {}", output_path.display());
        }
    }

    Ok(())
}
