use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod commands;

#[derive(Parser)]
#[command(name = "kafka-backup")]
#[command(about = "High-performance Kafka backup and restore with point-in-time recovery")]
#[command(
    long_about = "High-performance Kafka backup and restore with point-in-time recovery.\n\n\
    Back up Kafka topics to S3, Azure Blob, GCS, or local filesystem.\n\
    Restore with millisecond-precision PITR, topic remapping, and\n\
    automatic consumer group offset recovery.\n\n\
    Documentation: https://osodevops.github.io/kafka-backup-docs/"
)]
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
    /// Back up Kafka topics to cloud storage or local filesystem
    #[command(
        after_help = "Examples:\n  kafka-backup backup --config backup.yaml\n  kafka-backup backup -v --config backup.yaml   # with debug logging"
    )]
    Backup {
        /// Path to the YAML configuration file
        #[arg(short, long)]
        config: String,
    },

    /// Restore Kafka topics from a backup with optional PITR filtering
    #[command(
        after_help = "Examples:\n  kafka-backup restore --config restore.yaml\n  kafka-backup validate-restore --config restore.yaml   # dry-run first"
    )]
    Restore {
        /// Path to the YAML configuration file
        #[arg(short, long)]
        config: String,
    },

    /// List available backups in a storage location
    List {
        /// Storage path (local path or s3://bucket/prefix, azure://..., gcs://...)
        #[arg(short, long)]
        path: String,

        /// Specific backup ID to show details for
        #[arg(short, long)]
        backup_id: Option<String>,
    },

    /// Show status of a running or completed backup job
    #[command(long_about = "Show status of a backup job.\n\n\
        Two modes:\n  \
        Static:  --path and --backup-id to inspect a completed backup\n  \
        Live:    --config to monitor a running backup (add --watch for continuous refresh)")]
    Status {
        /// Path to the storage location (for inspecting a completed backup)
        #[arg(short, long, conflicts_with = "config")]
        path: Option<String>,

        /// Backup ID to show status for (for inspecting a completed backup)
        #[arg(short, long, conflicts_with = "config")]
        backup_id: Option<String>,

        /// Path to the offset database
        #[arg(long)]
        db_path: Option<String>,

        /// Path to config file (for live monitoring of a running backup)
        #[arg(short, long, conflicts_with_all = ["path", "backup_id"])]
        config: Option<String>,

        /// Enable watch mode to continuously poll metrics (requires --config)
        #[arg(long, requires = "config")]
        watch: bool,

        /// Refresh interval in seconds for watch mode
        #[arg(long, default_value = "2")]
        interval: u64,
    },

    /// Validate a backup's integrity (checksums, segment counts, manifests)
    #[command(
        after_help = "Examples:\n  kafka-backup validate --path s3://bucket --backup-id my-backup\n  kafka-backup validate --path s3://bucket --backup-id my-backup --deep"
    )]
    Validate {
        /// Storage path (local path or s3://bucket/prefix, azure://..., gcs://...)
        #[arg(short, long)]
        path: String,

        /// Backup ID to validate
        #[arg(short, long)]
        backup_id: String,

        /// Perform deep validation (read and verify each segment)
        #[arg(long, default_value = "false")]
        deep: bool,
    },

    /// Show detailed backup manifest (topics, partitions, time ranges, record counts)
    Describe {
        /// Storage path (local path or s3://bucket/prefix, azure://..., gcs://...)
        #[arg(short, long)]
        path: String,

        /// Backup ID to describe
        #[arg(short, long)]
        backup_id: String,

        /// Output format (text, json, yaml)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Validate a restore configuration without writing any data (dry-run)
    ValidateRestore {
        /// Path to the restore configuration file
        #[arg(short, long)]
        config: String,

        /// Output format (text, json, yaml)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Show source-to-target offset mapping from a completed restore
    ShowOffsetMapping {
        /// Storage path (local path or s3://bucket/prefix, azure://..., gcs://...)
        #[arg(short, long)]
        path: String,

        /// Backup ID to show offset mapping for
        #[arg(short, long)]
        backup_id: String,

        /// Output format (text, json, yaml, csv)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Plan, execute, or script consumer group offset resets after a restore
    OffsetReset {
        #[command(subcommand)]
        action: OffsetResetAction,
    },

    /// Run a complete restore with automatic consumer group offset recovery
    #[command(
        long_about = "Run a complete restore with automatic consumer group offset recovery.\n\n\
        Orchestrates three phases:\n  \
        1. Restore records to target cluster\n  \
        2. Collect source-to-target offset mapping\n  \
        3. Reset consumer group offsets using the mapping"
    )]
    ThreePhaseRestore {
        /// Path to the YAML configuration file
        #[arg(short, long)]
        config: String,
    },

    /// Reset consumer group offsets in parallel after a restore (~50x faster than sequential)
    OffsetResetBulk {
        /// Storage path containing the offset mapping from a completed restore
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

        /// Maximum concurrent reset operations [default: 50]
        #[arg(long, default_value = "50")]
        max_concurrent: usize,

        /// Maximum retry attempts for failed partitions [default: 3]
        #[arg(long, default_value = "3")]
        max_retries: u32,

        /// Security protocol (PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT)
        #[arg(long)]
        security_protocol: Option<String>,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Snapshot and rollback consumer group offsets (safety net for offset changes)
    OffsetRollback {
        #[command(subcommand)]
        action: OffsetRollbackAction,
    },

    /// Run backup validation checks and generate compliance evidence reports
    Validation {
        #[command(subcommand)]
        action: ValidationAction,
    },
}

#[derive(Subcommand)]
enum OffsetRollbackAction {
    /// Create a snapshot of current consumer group offsets
    Snapshot {
        /// Path to the storage location for snapshots
        #[arg(short, long)]
        path: String,

        /// Consumer groups to snapshot (comma-separated)
        #[arg(short, long, value_delimiter = ',')]
        groups: Vec<String>,

        /// Kafka bootstrap servers (comma-separated)
        #[arg(long, value_delimiter = ',')]
        bootstrap_servers: Vec<String>,

        /// Description for the snapshot
        #[arg(short, long)]
        description: Option<String>,

        /// Security protocol (PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT)
        #[arg(long)]
        security_protocol: Option<String>,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// List available offset snapshots
    List {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Show details of a specific snapshot
    Show {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Snapshot ID to show
        #[arg(short, long)]
        snapshot_id: String,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Rollback offsets to a previous snapshot
    Rollback {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Snapshot ID to rollback to
        #[arg(short, long)]
        snapshot_id: String,

        /// Kafka bootstrap servers (comma-separated)
        #[arg(long, value_delimiter = ',')]
        bootstrap_servers: Vec<String>,

        /// Security protocol (PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT)
        #[arg(long)]
        security_protocol: Option<String>,

        /// Verify offsets after rollback
        #[arg(long, default_value = "true")]
        verify: bool,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Verify current offsets match a snapshot
    Verify {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Snapshot ID to verify against
        #[arg(short, long)]
        snapshot_id: String,

        /// Kafka bootstrap servers (comma-separated)
        #[arg(long, value_delimiter = ',')]
        bootstrap_servers: Vec<String>,

        /// Security protocol (PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT)
        #[arg(long)]
        security_protocol: Option<String>,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Delete a snapshot
    Delete {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Snapshot ID to delete
        #[arg(short, long)]
        snapshot_id: String,
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

#[derive(Subcommand)]
enum ValidationAction {
    /// Run validation checks against a restored cluster and generate evidence
    Run {
        /// Path to the validation configuration file
        #[arg(short, long)]
        config: String,

        /// PITR timestamp override (epoch milliseconds)
        #[arg(long)]
        pitr: Option<i64>,

        /// Record who/what triggered this run (e.g. "KPMG Q1 2026 audit")
        #[arg(long)]
        triggered_by: Option<String>,
    },

    /// List evidence reports in storage
    EvidenceList {
        /// Path to the storage location (e.g. s3://bucket/prefix)
        #[arg(short, long)]
        path: String,

        /// Maximum number of reports to show
        #[arg(short, long, default_value = "50")]
        limit: usize,
    },

    /// Download an evidence report
    EvidenceGet {
        /// Path to the storage location
        #[arg(short, long)]
        path: String,

        /// Report ID to download
        #[arg(short, long)]
        report_id: String,

        /// Format to download (json, pdf)
        #[arg(short, long, default_value = "json")]
        format: String,

        /// Output file path
        #[arg(short, long)]
        output: String,
    },

    /// Verify an evidence report's cryptographic signature
    EvidenceVerify {
        /// Path to the JSON evidence report file
        #[arg(short, long)]
        report: String,

        /// Path to the detached signature (.sig) file
        #[arg(short, long)]
        signature: String,

        /// Path to the PEM-encoded public key (optional)
        #[arg(long)]
        public_key: Option<String>,
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
            config,
            watch,
            interval,
        } => {
            commands::status::run(
                path.as_deref(),
                backup_id.as_deref(),
                db_path.as_deref(),
                config.as_deref(),
                watch,
                interval,
            )
            .await?;
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
        Commands::OffsetResetBulk {
            path,
            backup_id,
            groups,
            bootstrap_servers,
            max_concurrent,
            max_retries,
            security_protocol,
            format,
        } => {
            commands::offset_reset_bulk::execute_bulk(
                &path,
                &backup_id,
                &groups,
                &bootstrap_servers,
                max_concurrent,
                max_retries,
                security_protocol.as_deref(),
                commands::offset_reset_bulk::OutputFormat::from(format.as_str()),
            )
            .await?;
        }
        Commands::OffsetRollback { action } => match action {
            OffsetRollbackAction::Snapshot {
                path,
                groups,
                bootstrap_servers,
                description,
                security_protocol,
                format,
            } => {
                commands::offset_rollback::create_snapshot(
                    &path,
                    &groups,
                    &bootstrap_servers,
                    description.as_deref(),
                    security_protocol.as_deref(),
                    commands::offset_rollback::OutputFormat::from(format.as_str()),
                )
                .await?;
            }
            OffsetRollbackAction::List { path, format } => {
                commands::offset_rollback::list_snapshots(
                    &path,
                    commands::offset_rollback::OutputFormat::from(format.as_str()),
                )
                .await?;
            }
            OffsetRollbackAction::Show {
                path,
                snapshot_id,
                format,
            } => {
                commands::offset_rollback::show_snapshot(
                    &path,
                    &snapshot_id,
                    commands::offset_rollback::OutputFormat::from(format.as_str()),
                )
                .await?;
            }
            OffsetRollbackAction::Rollback {
                path,
                snapshot_id,
                bootstrap_servers,
                security_protocol,
                verify,
                format,
            } => {
                commands::offset_rollback::execute_rollback(
                    &path,
                    &snapshot_id,
                    &bootstrap_servers,
                    security_protocol.as_deref(),
                    verify,
                    commands::offset_rollback::OutputFormat::from(format.as_str()),
                )
                .await?;
            }
            OffsetRollbackAction::Verify {
                path,
                snapshot_id,
                bootstrap_servers,
                security_protocol,
                format,
            } => {
                commands::offset_rollback::verify_snapshot(
                    &path,
                    &snapshot_id,
                    &bootstrap_servers,
                    security_protocol.as_deref(),
                    commands::offset_rollback::OutputFormat::from(format.as_str()),
                )
                .await?;
            }
            OffsetRollbackAction::Delete { path, snapshot_id } => {
                commands::offset_rollback::delete_snapshot(&path, &snapshot_id).await?;
            }
        },
        Commands::Validation { action } => match action {
            ValidationAction::Run {
                config,
                pitr,
                triggered_by,
            } => {
                commands::validation::run(&config, pitr, triggered_by.as_deref()).await?;
            }
            ValidationAction::EvidenceList { path, limit } => {
                commands::validation::evidence_list(&path, limit).await?;
            }
            ValidationAction::EvidenceGet {
                path,
                report_id,
                format,
                output,
            } => {
                commands::validation::evidence_get(&path, &report_id, &format, &output).await?;
            }
            ValidationAction::EvidenceVerify {
                report,
                signature,
                public_key,
            } => {
                commands::validation::evidence_verify(&report, &signature, public_key.as_deref())
                    .await?;
            }
        },
    }

    Ok(())
}
