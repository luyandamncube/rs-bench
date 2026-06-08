use anyhow::{Context, Result};
use bm_engine_mini_flink::LiveInputEvent;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::fs::File;
use std::io::{BufRead, BufReader};
use tokio::io::{self, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::{self, Duration, MissedTickBehavior};

#[derive(Parser)]
#[command(name = "bmproduce")]
#[command(about = "Live streaming source toolbox")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Synthetic {
        #[command(subcommand)]
        command: SyntheticCommands,
    },
    Manual {
        #[command(subcommand)]
        command: ManualCommands,
    },
    Replay {
        #[command(subcommand)]
        command: ReplayCommands,
    },
    Serve {
        #[command(subcommand)]
        command: ServeCommands,
    },
}

#[derive(Subcommand)]
enum SyntheticCommands {
    Clickstream(SyntheticClickstreamArgs),
}

#[derive(Subcommand)]
enum ManualCommands {
    Stdin(TcpConnectArgs),
}

#[derive(Subcommand)]
enum ReplayCommands {
    File(ReplayFileArgs),
}

#[derive(Subcommand)]
enum ServeCommands {
    TcpClickstream(TcpServerClickstreamArgs),
}

#[derive(Debug, Clone, Args)]
struct TcpConnectArgs {
    #[arg(long, default_value = "127.0.0.1:7001")]
    connect: String,
}

#[derive(Debug, Clone, Args)]
struct SyntheticClickstreamArgs {
    #[command(flatten)]
    target: TcpConnectArgs,
    #[arg(long, default_value_t = 2)]
    rate: u64,
    #[arg(long)]
    count: Option<u64>,
    #[arg(long, value_enum, default_value_t = ProducerPattern::RoundRobin)]
    pattern: ProducerPattern,
    #[arg(long, default_value_t = 4)]
    burst_size: u64,
    #[arg(long)]
    sticky_device: Option<String>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "mobile,desktop,tablet,tv"
    )]
    devices: Vec<String>,
    #[arg(long, default_value = "page_view")]
    event_type: String,
}

#[derive(Debug, Clone, Args)]
struct ReplayFileArgs {
    #[command(flatten)]
    target: TcpConnectArgs,
    #[arg(long)]
    input: String,
    #[arg(long, default_value_t = 0)]
    rate: u64,
    #[arg(long)]
    count: Option<u64>,
}

#[derive(Debug, Clone, Args)]
struct TcpServerClickstreamArgs {
    #[arg(long, default_value = "127.0.0.1:7001")]
    listen: String,
    #[arg(long, default_value_t = 2)]
    rate: u64,
    #[arg(long)]
    count: Option<u64>,
    #[arg(long, value_enum, default_value_t = ProducerPattern::RoundRobin)]
    pattern: ProducerPattern,
    #[arg(long, default_value_t = 4)]
    burst_size: u64,
    #[arg(long)]
    sticky_device: Option<String>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "mobile,desktop,tablet,tv"
    )]
    devices: Vec<String>,
    #[arg(long, default_value = "page_view")]
    event_type: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProducerPattern {
    RoundRobin,
    SingleKey,
    Burst,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Synthetic { command } => match command {
            SyntheticCommands::Clickstream(args) => run_synthetic_clickstream(args).await?,
        },
        Commands::Manual { command } => match command {
            ManualCommands::Stdin(args) => run_manual_stdin(args).await?,
        },
        Commands::Replay { command } => match command {
            ReplayCommands::File(args) => run_replay_file(args).await?,
        },
        Commands::Serve { command } => match command {
            ServeCommands::TcpClickstream(args) => run_tcp_clickstream_server(args).await?,
        },
    }

    Ok(())
}

async fn run_synthetic_clickstream(args: SyntheticClickstreamArgs) -> Result<()> {
    validate_devices(&args.devices, args.sticky_device.as_deref())?;
    let mut stream = TcpStream::connect(&args.target.connect)
        .await
        .with_context(|| {
            format!(
                "failed to connect to live processor at {}",
                args.target.connect
            )
        })?;

    println!("Connected producer to {}", args.target.connect);
    println!("Press Ctrl-C to stop the producer.");
    println!("Pattern: {}", pattern_name(args.pattern));

    stream_clickstream_events(
        &mut stream,
        args.rate,
        args.count,
        args.pattern,
        args.burst_size,
        args.sticky_device.as_deref(),
        &args.devices,
        &args.event_type,
        "produced",
    )
    .await
}

async fn run_manual_stdin(args: TcpConnectArgs) -> Result<()> {
    let mut stream = TcpStream::connect(&args.connect)
        .await
        .with_context(|| format!("failed to connect to live processor at {}", args.connect))?;
    let stdin = io::stdin();
    let mut lines = io::BufReader::new(stdin).lines();

    println!("Connected manual producer to {}", args.connect);
    println!("Paste one JSON event per line. Press Ctrl-C to stop.");

    loop {
        tokio::select! {
            maybe_line = lines.next_line() => {
                let Some(line) = maybe_line.context("failed to read stdin line")? else {
                    break;
                };
                if line.trim().is_empty() {
                    continue;
                }
                let event: LiveInputEvent = serde_json::from_str(&line)
                    .with_context(|| format!("invalid LiveInputEvent JSON: {line}"))?;
                send_event(&mut stream, &event).await?;
                println!("produced: {}", serde_json::to_string(&event)?);
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Manual producer received Ctrl-C, stopping");
                break;
            }
        }
    }

    Ok(())
}

async fn run_replay_file(args: ReplayFileArgs) -> Result<()> {
    let file = File::open(&args.input)
        .with_context(|| format!("failed to open replay input file {}", args.input))?;
    let reader = BufReader::new(file);
    let mut stream = TcpStream::connect(&args.target.connect)
        .await
        .with_context(|| {
            format!(
                "failed to connect to live processor at {}",
                args.target.connect
            )
        })?;

    println!("Connected replay producer to {}", args.target.connect);
    println!("Replaying from {}", args.input);

    let mut emitted = 0u64;
    let delay = if args.rate == 0 {
        None
    } else {
        Some(Duration::from_secs_f64(1.0 / args.rate as f64))
    };

    for line in reader.lines() {
        if args.count.is_some_and(|limit| emitted >= limit) {
            break;
        }

        let line =
            line.with_context(|| format!("failed to read replay line from {}", args.input))?;
        if line.trim().is_empty() {
            continue;
        }
        let event: LiveInputEvent = serde_json::from_str(&line)
            .with_context(|| format!("invalid LiveInputEvent JSON in {}: {line}", args.input))?;
        send_event(&mut stream, &event).await?;
        println!("replayed: {}", serde_json::to_string(&event)?);
        emitted += 1;

        if let Some(delay) = delay {
            tokio::select! {
                _ = time::sleep(delay) => {}
                _ = tokio::signal::ctrl_c() => {
                    println!("Replay producer received Ctrl-C, stopping");
                    break;
                }
            }
        }
    }

    println!("Replay producer stopped after emitting {emitted} events");
    Ok(())
}

async fn run_tcp_clickstream_server(args: TcpServerClickstreamArgs) -> Result<()> {
    validate_devices(&args.devices, args.sticky_device.as_deref())?;
    let listener = TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("failed to bind TCP feed server at {}", args.listen))?;
    let (tx, _) = broadcast::channel::<String>(1024);

    println!("TCP clickstream feed listening on {}", args.listen);
    println!("Clients can attach at any time.");
    println!("Pattern: {}", pattern_name(args.pattern));

    let accept_tx = tx.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    println!("Client attached from {}", addr);
                    let rx = accept_tx.subscribe();
                    tokio::spawn(async move {
                        if let Err(error) = serve_broadcast_client(socket, rx).await {
                            eprintln!("client stream error for {}: {}", addr, error);
                        }
                    });
                }
                Err(error) => {
                    eprintln!("failed to accept TCP feed client: {}", error);
                    break;
                }
            }
        }
    });

    stream_clickstream_feed(
        &tx,
        args.rate,
        args.count,
        args.pattern,
        args.burst_size,
        args.sticky_device.as_deref(),
        &args.devices,
        &args.event_type,
    )
    .await
}

async fn stream_clickstream_events<W: AsyncWrite + Unpin>(
    writer: &mut W,
    rate: u64,
    count: Option<u64>,
    pattern: ProducerPattern,
    burst_size: u64,
    sticky_device: Option<&str>,
    devices: &[String],
    event_type: &str,
    log_prefix: &str,
) -> Result<()> {
    let interval_duration = if rate == 0 {
        Duration::from_secs(1)
    } else {
        Duration::from_secs_f64(1.0 / rate as f64)
    };
    let mut interval = time::interval(interval_duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut emitted = 0u64;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if count.is_some_and(|limit| emitted >= limit) {
                    break;
                }

                let event = build_clickstream_event(
                    emitted,
                    pattern,
                    burst_size,
                    sticky_device,
                    devices,
                    event_type,
                )?;
                send_event(writer, &event).await?;
                println!("{log_prefix}: {}", serde_json::to_string(&event)?);
                emitted += 1;
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Producer received Ctrl-C, stopping");
                break;
            }
        }
    }

    println!("Producer stopped after emitting {emitted} events");
    Ok(())
}

async fn stream_clickstream_feed(
    tx: &broadcast::Sender<String>,
    rate: u64,
    count: Option<u64>,
    pattern: ProducerPattern,
    burst_size: u64,
    sticky_device: Option<&str>,
    devices: &[String],
    event_type: &str,
) -> Result<()> {
    let interval_duration = if rate == 0 {
        Duration::from_secs(1)
    } else {
        Duration::from_secs_f64(1.0 / rate as f64)
    };
    let mut interval = time::interval(interval_duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut emitted = 0u64;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if count.is_some_and(|limit| emitted >= limit) {
                    break;
                }

                let event = build_clickstream_event(
                    emitted,
                    pattern,
                    burst_size,
                    sticky_device,
                    devices,
                    event_type,
                )?;
                let line = serde_json::to_string(&event)?;
                let _ = tx.send(line.clone());
                println!("served: {line}");
                emitted += 1;
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Feed server received Ctrl-C, stopping");
                break;
            }
        }
    }

    println!("Feed server stopped after emitting {emitted} events");
    Ok(())
}

async fn serve_broadcast_client(
    mut socket: TcpStream,
    mut rx: broadcast::Receiver<String>,
) -> Result<()> {
    loop {
        match rx.recv().await {
            Ok(line) => {
                socket
                    .write_all(line.as_bytes())
                    .await
                    .context("failed to write broadcast event")?;
                socket
                    .write_all(b"\n")
                    .await
                    .context("failed to write broadcast newline")?;
                socket
                    .flush()
                    .await
                    .context("failed to flush broadcast event")?;
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                eprintln!("client lagged behind live feed, skipped {} events", skipped);
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    Ok(())
}

async fn send_event<W: AsyncWrite + Unpin>(writer: &mut W, event: &LiveInputEvent) -> Result<()> {
    let line = serde_json::to_string(event).context("failed to serialize LiveInputEvent")?;
    writer
        .write_all(line.as_bytes())
        .await
        .context("failed to write live event")?;
    writer
        .write_all(b"\n")
        .await
        .context("failed to write live event newline")?;
    writer.flush().await.context("failed to flush live event")?;
    Ok(())
}

fn build_clickstream_event(
    sequence: u64,
    pattern: ProducerPattern,
    burst_size: u64,
    sticky_device: Option<&str>,
    devices: &[String],
    event_type: &str,
) -> Result<LiveInputEvent> {
    let device_type = select_device(sequence, pattern, burst_size.max(1), sticky_device, devices)?;
    Ok(LiveInputEvent {
        event_time_ms: Some(sequence.saturating_mul(1000)),
        user_id: Some((sequence % 16) + 1),
        session_id: Some((sequence / 4) + 1),
        device_type: Some(device_type),
        event_type: event_type.to_string(),
        value: (sequence % 5) + 1,
        key: None,
    })
}

fn select_device(
    sequence: u64,
    pattern: ProducerPattern,
    burst_size: u64,
    sticky_device: Option<&str>,
    devices: &[String],
) -> Result<String> {
    let device = match pattern {
        ProducerPattern::RoundRobin => devices[(sequence as usize) % devices.len()].clone(),
        ProducerPattern::SingleKey => sticky_device
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| devices[0].clone()),
        ProducerPattern::Burst => {
            let burst_index = (sequence / burst_size) as usize;
            devices[burst_index % devices.len()].clone()
        }
    };
    Ok(device)
}

fn validate_devices(devices: &[String], sticky_device: Option<&str>) -> Result<()> {
    if devices.is_empty() {
        anyhow::bail!("devices list cannot be empty");
    }
    if let Some(sticky) = sticky_device {
        if !devices.iter().any(|device| device == sticky) {
            anyhow::bail!("sticky device {sticky} is not in --devices");
        }
    }
    Ok(())
}

fn pattern_name(pattern: ProducerPattern) -> &'static str {
    match pattern {
        ProducerPattern::RoundRobin => "round_robin",
        ProducerPattern::SingleKey => "single_key",
        ProducerPattern::Burst => "burst",
    }
}

#[cfg(test)]
mod tests {
    use super::{select_device, validate_devices, ProducerPattern};

    #[test]
    fn round_robin_pattern_cycles_devices() {
        let devices = vec!["mobile".to_string(), "desktop".to_string()];
        assert_eq!(
            select_device(0, ProducerPattern::RoundRobin, 4, None, &devices).unwrap(),
            "mobile"
        );
        assert_eq!(
            select_device(1, ProducerPattern::RoundRobin, 4, None, &devices).unwrap(),
            "desktop"
        );
    }

    #[test]
    fn single_key_pattern_sticks_to_requested_device() {
        let devices = vec!["mobile".to_string(), "desktop".to_string()];
        assert_eq!(
            select_device(5, ProducerPattern::SingleKey, 4, Some("desktop"), &devices).unwrap(),
            "desktop"
        );
    }

    #[test]
    fn burst_pattern_holds_device_for_burst_size() {
        let devices = vec!["mobile".to_string(), "desktop".to_string()];
        assert_eq!(
            select_device(0, ProducerPattern::Burst, 3, None, &devices).unwrap(),
            "mobile"
        );
        assert_eq!(
            select_device(2, ProducerPattern::Burst, 3, None, &devices).unwrap(),
            "mobile"
        );
        assert_eq!(
            select_device(3, ProducerPattern::Burst, 3, None, &devices).unwrap(),
            "desktop"
        );
    }

    #[test]
    fn sticky_device_must_exist_in_devices() {
        let devices = vec!["mobile".to_string(), "desktop".to_string()];
        assert!(validate_devices(&devices, Some("tablet")).is_err());
    }
}
