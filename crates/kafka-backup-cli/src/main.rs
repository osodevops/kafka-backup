use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod commands;

#[derive(Parser)]
#[command(name = "kafka-backup")]
#[command(about = "Kafka backup and restore tool", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable verbose logging (-v for debug, -vv for trace)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a backup operation
    Backup {
        /// Path to the configuration file
        #[arg(short, long)]
        config: String,
    },

    /// Run a restore operation
    Restore {
        /// Path to the configuration file
        #[arg(short, long)]
        config: String,
    },

    /// List available backups
    List {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Specific backup ID to show details for
        #[arg(short, long)]
        backup_id: Option<String>,
    },

    /// Show status of a backup job
    Status {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Backup ID to show status for
        #[arg(short, long)]
        backup_id: String,

        /// Path to the offset database
        #[arg(long)]
        db_path: Option<String>,
    },

    /// Validate a backup's integrity
    Validate {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Backup ID to validate
        #[arg(short, long)]
        backup_id: String,

        /// Perform deep validation (read and verify each segment)
        #[arg(long, default_value = "false")]
        deep: bool,
    },

    /// Show detailed backup manifest information
    Describe {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Backup ID to describe
        #[arg(short, long)]
        backup_id: String,

        /// Output format (text, json, yaml)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Validate a restore configuration (dry-run)
    ValidateRestore {
        /// Path to the restore configuration file
        #[arg(short, long)]
        config: String,

        /// Output format (text, json, yaml)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Show offset mapping for a backup
    ShowOffsetMapping {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Backup ID to show offset mapping for
        #[arg(short, long)]
        backup_id: String,

        /// Output format (text, json, yaml, csv)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Generate or execute consumer group offset reset plan (Phase 3)
    OffsetReset {
        #[command(subcommand)]
        action: OffsetResetAction,
    },

    /// Run three-phase restore (restore + offset reset)
    ThreePhaseRestore {
        /// Path to the configuration file
        #[arg(short, long)]
        config: String,
    },
}

#[derive(Subcommand)]
enum OffsetResetAction {
    /// Generate an offset reset plan from a restore's offset mapping
    Plan {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Backup ID to generate plan for
        #[arg(short, long)]
        backup_id: String,

        /// Consumer groups to reset (comma-separated)
        #[arg(short, long, value_delimiter = ',')]
        groups: Vec<String>,

        /// Kafka bootstrap servers (comma-separated)
        #[arg(long, value_delimiter = ',')]
        bootstrap_servers: Vec<String>,

        /// Output format (text, json, csv, shell-script)
        #[arg(short, long, default_value = "text")]
        format: String,

        /// Dry run mode (preview only, no changes)
        #[arg(long, default_value = "true")]
        dry_run: bool,
    },

    /// Execute an offset reset plan
    Execute {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Backup ID with offset mapping
        #[arg(short, long)]
        backup_id: String,

        /// Consumer groups to reset (comma-separated)
        #[arg(short, long, value_delimiter = ',')]
        groups: Vec<String>,

        /// Kafka bootstrap servers (comma-separated)
        #[arg(long, value_delimiter = ',')]
        bootstrap_servers: Vec<String>,

        /// Security protocol (PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT)
        #[arg(long)]
        security_protocol: Option<String>,
    },

    /// Generate a shell script for manual offset reset
    Script {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Backup ID with offset mapping
        #[arg(short, long)]
        backup_id: String,

        /// Consumer groups to reset (comma-separated)
        #[arg(short, long, value_delimiter = ',')]
        groups: Vec<String>,

        /// Kafka bootstrap servers (comma-separated)
        #[arg(long, value_delimiter = ',')]
        bootstrap_servers: Vec<String>,

        /// Output file path (prints to stdout if not specified)
        #[arg(short, long)]
        output: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    // Priority: RUST_LOG env var > verbose flag > default (info)
    let filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::from_default_env()
    } else {
        match cli.verbose {
            0 => EnvFilter::new("info"),
            1 => EnvFilter::new("debug"),
            _ => EnvFilter::new("trace"),
        }
    };

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();

    match cli.command {
        Commands::Backup { config } => {
            commands::backup::run(&config).await?;
        }
        Commands::Restore { config } => {
            commands::restore::run(&config).await?;
        }
        Commands::List { path, backup_id } => {
            commands::list::run(&path, backup_id.as_deref()).await?;
        }
        Commands::Status {
            path,
            backup_id,
            db_path,
        } => {
            commands::status::run(&path, &backup_id, db_path.as_deref()).await?;
        }
        Commands::Validate {
            path,
            backup_id,
            deep,
        } => {
            commands::validate::run(&path, &backup_id, deep).await?;
        }
        Commands::Describe {
            path,
            backup_id,
            format,
        } => {
            commands::describe::run(&path, &backup_id, &format).await?;
        }
        Commands::ValidateRestore { config, format } => {
            commands::validate_restore::run(&config, &format).await?;
        }
        Commands::ShowOffsetMapping {
            path,
            backup_id,
            format,
        } => {
            commands::offset_mapping::run(&path, &backup_id, &format).await?;
        }
        Commands::OffsetReset { action } => match action {
            OffsetResetAction::Plan {
                path,
                backup_id,
                groups,
                bootstrap_servers,
                format,
                dry_run,
            } => {
                commands::offset_reset::generate_plan(
                    &path,
                    &backup_id,
                    &groups,
                    &bootstrap_servers,
                    dry_run,
                    commands::offset_reset::OutputFormat::from(format.as_str()),
                )
                .await?;
            }
            OffsetResetAction::Execute {
                path,
                backup_id,
                groups,
                bootstrap_servers,
                security_protocol,
            } => {
                commands::offset_reset::execute_plan(
                    &path,
                    &backup_id,
                    &groups,
                    &bootstrap_servers,
                    security_protocol.as_deref(),
                )
                .await?;
            }
            OffsetResetAction::Script {
                path,
                backup_id,
                groups,
                bootstrap_servers,
                output,
            } => {
                commands::offset_reset::generate_script(
                    &path,
                    &backup_id,
                    &groups,
                    &bootstrap_servers,
                    output.as_deref(),
                )
                .await?;
            }
        },
        Commands::ThreePhaseRestore { config } => {
            commands::three_phase::run(&config).await?;
        }
    }

    Ok(())
}
