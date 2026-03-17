// apps\bmgen\src\main.rs
use anyhow::{Context, Result};
use bm_generator::config::ClickstreamGeneratorConfig;
use bm_generator::generate_clickstream_dataset;
use clap::{Parser, Subcommand};
use std::fs;

#[derive(Parser)]
#[command(name = "bmgen")]
#[command(about = "Benchmark dataset generator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Generate {
        #[arg(long)]
        config: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Generate { config } => run_generate(&config)?,
    }

    Ok(())
}

fn run_generate(config_path: &str) -> Result<()> {
    let raw = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read config file: {config_path}"))?;

    let config: ClickstreamGeneratorConfig =
        toml::from_str(&raw).with_context(|| "failed to parse TOML config")?;

    let output_dir = generate_clickstream_dataset(config.clone())?;

    println!("Generated dataset {}", config.name);
    println!("Rows: {}", config.rows);
    println!("Formats: {}", config.formats.join(", "));
    println!("Output: {}", output_dir.display());
    println!("CSV manifest: {}", output_dir.join("csv/manifest.json").display());
    println!("JSONL manifest: {}", output_dir.join("jsonl/manifest.json").display());
    println!("Parquet manifest: {}", output_dir.join("parquet/manifest.json").display());
    Ok(())
}