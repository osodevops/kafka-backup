use anyhow::Result;
use kafka_backup_core::{backup::BackupEngine, Config};
use tracing::info;

pub async fn run(config_path: &str) -> Result<()> {
    info!("Loading configuration from: {}", config_path);

    let config_content = tokio::fs::read_to_string(config_path).await?;
    let config: Config = serde_yaml::from_str(&config_content)?;

    info!("Starting backup: {}", config.backup_id);

    let engine = BackupEngine::new(config).await?;
    engine.run().await?;

    info!("Backup completed successfully");
    Ok(())
}
