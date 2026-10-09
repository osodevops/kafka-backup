use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod commands;

use commands::security_args::SecurityCliArgs;

/// `--path` help for every subcommand that resolves it with
/// `commands::storage_path::backend_from_path`.
const STORAGE_PATH_HELP: &str = "Storage location: a local directory, file:///abs/path, \
s3://bucket/prefix (S3-compatible stores: append ?endpoint=http://host:9000&region=...), \
azure://account.blob.core.windows.net/container or gcs://bucket. \
Credentials come from AWS_*, AZURE_* or GOOGLE_* environment variables";

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

    /// Restore Kafka topics from a backup with optional PITR filtering and,
    /// when `reset_consumer_offsets` / `auto_consumer_groups` is set, consumer
    /// group offset reset on the target after the data restore
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
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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
        #[arg(short, long, help = STORAGE_PATH_HELP, conflicts_with = "config")]
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
        after_help = "Examples:\n  kafka-backup validate --config backup.yaml\n  kafka-backup validate --path s3://bucket --backup-id my-backup\n  kafka-backup validate --path s3://bucket --backup-id my-backup --deep"
    )]
    Validate {
        /// Path to the backup configuration file (storage + backup_id come from it)
        #[arg(short, long, conflicts_with_all = ["path", "backup_id"])]
        config: Option<String>,

        #[arg(short, long, help = STORAGE_PATH_HELP, requires = "backup_id")]
        path: Option<String>,

        /// Backup ID to validate
        #[arg(short, long, requires = "path")]
        backup_id: Option<String>,

        /// Perform deep validation (read and verify each segment)
        #[arg(long, default_value = "false")]
        deep: bool,
    },

    /// Delete aged/oversized segments from a backup set, recording the
    /// pruned ranges in the manifest. Plan-only unless --execute is passed.
    /// Do NOT use bucket lifecycle rules on incremental backup sets.
    #[command(
        after_help = "Examples:\n  kafka-backup prune --config backup.yaml --older-than 30d\n  kafka-backup prune --path s3://bucket/prefix --backup-id daily --before 2026-08-01T00:00:00Z --execute"
    )]
    Prune {
        /// Path to the backup configuration file
        #[arg(short, long, conflicts_with_all = ["path", "backup_id"])]
        config: Option<String>,

        #[arg(short, long, help = STORAGE_PATH_HELP, requires = "backup_id")]
        path: Option<String>,

        /// Backup ID to prune
        #[arg(short, long, requires = "path")]
        backup_id: Option<String>,

        /// Prune segments older than this duration (30d, 12h, 1d12h, ...)
        #[arg(long, conflicts_with = "before")]
        older_than: Option<String>,

        /// Prune segments older than this instant (RFC 3339 or epoch ms)
        #[arg(long)]
        before: Option<String>,

        /// Never prune a partition below this many newest segments
        #[arg(long, default_value = "1")]
        keep_segments: usize,

        /// Keep pruning oldest-first until the set fits under this many compressed bytes
        #[arg(long)]
        max_total_bytes: Option<u64>,

        /// Actually delete (default is a dry-run plan)
        #[arg(long)]
        execute: bool,

        /// Proceed even when a backup run looks live
        #[arg(long)]
        force: bool,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Show detailed backup manifest (topics, partitions, time ranges, record counts)
    #[command(
        after_help = "Examples:\n  kafka-backup describe --config backup.yaml\n  kafka-backup describe --path s3://bucket --backup-id my-backup --format json"
    )]
    Describe {
        /// Path to the backup configuration file (storage + backup_id come from it)
        #[arg(short, long, conflicts_with_all = ["path", "backup_id"])]
        config: Option<String>,

        #[arg(short, long, help = STORAGE_PATH_HELP, requires = "backup_id")]
        path: Option<String>,

        /// Backup ID to describe
        #[arg(short, long, requires = "path")]
        backup_id: Option<String>,

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
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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

        #[command(flatten)]
        security: SecurityCliArgs,

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

    /// Snapshot consumer group offsets for backed-up topics
    ///
    /// Queries every broker for consumer groups (KRaft-safe), fetches their committed
    /// offsets, filters to groups that have offsets on backed-up topics, and saves the
    /// result to {backup_id}/consumer-groups-snapshot.json in the configured storage
    /// backend.
    ///
    /// The snapshot is loaded automatically at restore time when
    /// `auto_consumer_groups: true` is set in the restore configuration.
    #[command(name = "snapshot-groups")]
    SnapshotGroups {
        /// Path to the backup configuration file (mode must be 'backup')
        #[arg(short, long)]
        config: String,
    },
}

#[derive(Subcommand)]
enum OffsetRollbackAction {
    /// Create a snapshot of current consumer group offsets
    Snapshot {
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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

        #[command(flatten)]
        security: SecurityCliArgs,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// List available offset snapshots
    List {
        #[arg(short, long, help = STORAGE_PATH_HELP)]
        path: String,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Show details of a specific snapshot
    Show {
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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
        #[arg(short, long, help = STORAGE_PATH_HELP)]
        path: String,

        /// Snapshot ID to rollback to
        #[arg(short, long)]
        snapshot_id: String,

        /// Kafka bootstrap servers (comma-separated)
        #[arg(long, value_delimiter = ',')]
        bootstrap_servers: Vec<String>,

        #[command(flatten)]
        security: SecurityCliArgs,

        /// Verify offsets after rollback (`--verify false` to skip)
        #[arg(
            long,
            default_value_t = true,
            action = ArgAction::Set,
            num_args = 0..=1,
            default_missing_value = "true"
        )]
        verify: bool,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Verify current offsets match a snapshot
    Verify {
        #[arg(short, long, help = STORAGE_PATH_HELP)]
        path: String,

        /// Snapshot ID to verify against
        #[arg(short, long)]
        snapshot_id: String,

        /// Kafka bootstrap servers (comma-separated)
        #[arg(long, value_delimiter = ',')]
        bootstrap_servers: Vec<String>,

        #[command(flatten)]
        security: SecurityCliArgs,

        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Delete a snapshot
    Delete {
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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

        /// Label the plan a dry run (`--dry-run false` labels it manual). `plan`
        /// never changes offsets either way; apply a plan with `offset-reset execute`
        #[arg(
            long,
            default_value_t = true,
            action = ArgAction::Set,
            num_args = 0..=1,
            default_missing_value = "true"
        )]
        dry_run: bool,
    },

    /// Execute an offset reset plan
    Execute {
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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

        #[command(flatten)]
        security: SecurityCliArgs,
    },

    /// Generate a shell script for manual offset reset
    Script {
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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
        #[arg(short, long, help = STORAGE_PATH_HELP)]
        path: String,

        /// Maximum number of reports to show
        #[arg(short, long, default_value = "50")]
        limit: usize,
    },

    /// Download an evidence report
    EvidenceGet {
        #[arg(short, long, help = STORAGE_PATH_HELP)]
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
            config,
            path,
            backup_id,
            deep,
        } => {
            commands::validate::run(
                config.as_deref(),
                path.as_deref(),
                backup_id.as_deref(),
                deep,
            )
            .await?;
        }
        Commands::Prune {
            config,
            path,
            backup_id,
            older_than,
            before,
            keep_segments,
            max_total_bytes,
            execute,
            force,
            format,
        } => {
            commands::prune::run(
                config.as_deref(),
                path.as_deref(),
                backup_id.as_deref(),
                older_than.as_deref(),
                before.as_deref(),
                keep_segments,
                max_total_bytes,
                execute,
                force,
                &format,
            )
            .await?;
        }
        Commands::Describe {
            config,
            path,
            backup_id,
            format,
        } => {
            commands::describe::run(
                config.as_deref(),
                path.as_deref(),
                backup_id.as_deref(),
                &format,
            )
            .await?;
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
                security,
            } => {
                let security = security.into_security_config()?;
                commands::offset_reset::execute_plan(
                    &path,
                    &backup_id,
                    &groups,
                    &bootstrap_servers,
                    security,
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
            security,
            format,
        } => {
            let security = security.into_security_config()?;
            commands::offset_reset_bulk::execute_bulk(
                &path,
                &backup_id,
                &groups,
                &bootstrap_servers,
                max_concurrent,
                max_retries,
                security,
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
                security,
                format,
            } => {
                let security = security.into_security_config()?;
                commands::offset_rollback::create_snapshot(
                    &path,
                    &groups,
                    &bootstrap_servers,
                    description.as_deref(),
                    security,
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
                security,
                verify,
                format,
            } => {
                let security = security.into_security_config()?;
                commands::offset_rollback::execute_rollback(
                    &path,
                    &snapshot_id,
                    &bootstrap_servers,
                    security,
                    verify,
                    commands::offset_rollback::OutputFormat::from(format.as_str()),
                )
                .await?;
            }
            OffsetRollbackAction::Verify {
                path,
                snapshot_id,
                bootstrap_servers,
                security,
                format,
            } => {
                let security = security.into_security_config()?;
                commands::offset_rollback::verify_snapshot(
                    &path,
                    &snapshot_id,
                    &bootstrap_servers,
                    security,
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
        Commands::SnapshotGroups { config } => {
            commands::snapshot_groups::run(&config).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{error::ErrorKind, ArgAction, CommandFactory};

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// Issue #220: clap 4 derive makes a `bool` field a `SetTrue` flag, so a
    /// `default_value = "true"` on it is a flag that can never be turned off
    /// (and `--flag false` is rejected). Same for `SetFalse` defaulting to
    /// false. Declare such options with `ArgAction::Set` instead.
    #[test]
    fn no_flag_is_stuck_at_its_default() {
        fn walk(cmd: &clap::Command, path: &str, stuck: &mut Vec<String>) {
            for arg in cmd.get_arguments() {
                let stuck_value = match arg.get_action() {
                    ArgAction::SetTrue => "true",
                    ArgAction::SetFalse => "false",
                    _ => continue,
                };
                if arg.get_default_values().iter().any(|v| v == stuck_value) {
                    stuck.push(format!("{path} --{}", arg.get_long().unwrap_or("?")));
                }
            }
            for sub in cmd.get_subcommands() {
                walk(sub, &format!("{path} {}", sub.get_name()), stuck);
            }
        }

        let mut stuck = Vec::new();
        walk(&Cli::command(), "kafka-backup", &mut stuck);
        assert!(
            stuck.is_empty(),
            "flags that can never be turned off: {stuck:?}"
        );
    }

    fn rollback_verify(extra: &[&str]) -> std::result::Result<bool, clap::Error> {
        let args = ["kafka-backup", "offset-rollback", "rollback"]
            .into_iter()
            .chain(["--path", "/tmp/p", "--snapshot-id", "s"])
            .chain(extra.iter().copied());
        match Cli::try_parse_from(args)?.command {
            Commands::OffsetRollback {
                action: OffsetRollbackAction::Rollback { verify, format, .. },
            } => {
                // A value-taking `--verify` must not swallow the next option.
                if extra.contains(&"-f") || extra.contains(&"--format") {
                    assert_eq!(format, "json", "{extra:?} lost --format");
                }
                Ok(verify)
            }
            _ => unreachable!(),
        }
    }

    fn plan_dry_run(extra: &[&str]) -> std::result::Result<bool, clap::Error> {
        let args = ["kafka-backup", "offset-reset", "plan"]
            .into_iter()
            .chain(["--path", "/tmp/p", "--backup-id", "b"])
            .chain(extra.iter().copied());
        match Cli::try_parse_from(args)?.command {
            Commands::OffsetReset {
                action:
                    OffsetResetAction::Plan {
                        dry_run, format, ..
                    },
            } => {
                if extra.contains(&"-f") || extra.contains(&"--format") {
                    assert_eq!(format, "json", "{extra:?} lost --format");
                }
                Ok(dry_run)
            }
            _ => unreachable!(),
        }
    }

    /// Shared table for both flags: (extra args with `FLAG` as placeholder,
    /// expected value).
    const ACCEPTED: &[(&[&str], bool)] = &[
        (&[], true),
        (&["FLAG"], true),
        (&["FLAG", "true"], true),
        (&["FLAG=true"], true),
        (&["FLAG", "false"], false),
        (&["FLAG=false"], false),
        // Bare flag followed by another option keeps the default-missing
        // value and leaves the option alone.
        (&["FLAG", "-f", "json"], true),
        (&["FLAG", "--format", "json"], true),
        (&["FLAG", "false", "-f", "json"], false),
        (&["-f", "json", "FLAG"], true),
        (&["-f", "json", "FLAG", "false"], false),
    ];

    const REJECTED: &[(&[&str], ErrorKind)] = &[
        (&["FLAG", "maybe"], ErrorKind::InvalidValue),
        (&["FLAG=maybe"], ErrorKind::InvalidValue),
        (&["FLAG="], ErrorKind::InvalidValue),
        // Repeating the flag was an error before #220 and still is: no
        // silent last-one-wins.
        (&["FLAG", "FLAG=false"], ErrorKind::ArgumentConflict),
        (&["FLAG=false", "FLAG"], ErrorKind::ArgumentConflict),
    ];

    fn check(flag: &'static str, parse: fn(&[&str]) -> std::result::Result<bool, clap::Error>) {
        for (args, expected) in ACCEPTED {
            let args: Vec<String> = args.iter().map(|a| a.replace("FLAG", flag)).collect();
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            match parse(&args) {
                Ok(v) => assert_eq!(v, *expected, "{args:?}"),
                Err(e) => panic!("{args:?} should parse, got: {e}"),
            }
        }
        for (args, kind) in REJECTED {
            let args: Vec<String> = args.iter().map(|a| a.replace("FLAG", flag)).collect();
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            match parse(&args) {
                Ok(v) => panic!("{args:?} should be rejected, parsed as {v}"),
                Err(e) => assert_eq!(e.kind(), *kind, "{args:?}: {e}"),
            }
        }
    }

    #[test]
    fn rollback_verify_accepts_an_optional_bool() {
        check("--verify", rollback_verify);
    }

    #[test]
    fn plan_dry_run_accepts_an_optional_bool() {
        check("--dry-run", plan_dry_run);
    }

    /// Every `--path` is resolved by `storage_path::backend_from_path`
    /// (clippy.toml enforces that), so every `--path` help must list the
    /// forms it accepts. A hand-maintained list of subcommands missed
    /// `validation evidence-list` / `evidence-get` (#219).
    #[test]
    fn every_path_arg_documents_the_storage_forms() {
        fn walk(cmd: &clap::Command, name: &str, missing: &mut Vec<String>) {
            for arg in cmd.get_arguments() {
                if arg.get_long() == Some("path")
                    && arg.get_help().map(|h| h.to_string()).as_deref() != Some(STORAGE_PATH_HELP)
                {
                    missing.push(name.to_string());
                }
            }
            for sub in cmd.get_subcommands() {
                walk(sub, &format!("{name} {}", sub.get_name()), missing);
            }
        }
        let mut missing = Vec::new();
        walk(&Cli::command(), "kafka-backup", &mut missing);
        assert!(
            missing.is_empty(),
            "--path without STORAGE_PATH_HELP: {missing:?}"
        );
    }
}
