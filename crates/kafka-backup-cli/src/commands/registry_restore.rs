//! Registry restore through the provider-neutral engine
//! (`kafka_backup_core::registry_restore`): Apicurio Registry targets, and
//! restores between Confluent Schema Registry and Apicurio.
//!
//! A Confluent-to-Confluent restore does not come through here: it uses
//! `schema_registry.restore` (see `commands::schema_registry`).

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use kafka_backup_core::registry_restore::{
    engine::{RegistryPlan, RegistryRestore, Target},
    snapshot, Provider,
};
use kafka_backup_core::schema_registry::run as confluent;
use kafka_backup_core::{
    restore::transform::RecordTransformHandle, storage::create_backend, Config,
};

pub struct PreparedRegistry {
    pub engine: RegistryRestore,
    pub plan: RegistryPlan,
}

/// Which engine restores which registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// The Confluent-only restore (`schema_registry.restore`).
    pub confluent: bool,
    /// Destinations of the provider-neutral engine.
    pub engine: Vec<Provider>,
}

fn apicurio_enabled(config: &Config) -> bool {
    config
        .apicurio_registry
        .as_ref()
        .is_some_and(|ap| ap.enabled)
}

/// Decide what a restore-shaped command restores.
pub fn route(config: &Config, schema_only: bool) -> Result<Route> {
    let sr = confluent::enabled(config).is_some();
    let ap = apicurio_enabled(config);
    let common = config.registry_restore.as_ref();

    let route = match common.and_then(|c| c.destination) {
        Some(Provider::Apicurio) => {
            if !ap {
                bail!(
                    "registry_restore.destination is apicurio, but the configuration has no \
                     enabled apicurio_registry section (its url is the target registry)"
                );
            }
            Route {
                confluent: false,
                engine: vec![Provider::Apicurio],
            }
        }
        Some(Provider::Confluent) => {
            if !sr {
                bail!(
                    "registry_restore.destination is confluent, but the configuration has no \
                     enabled schema_registry section (its url is the target registry)"
                );
            }
            if common.and_then(|c| c.source).unwrap_or(Provider::Confluent) == Provider::Confluent {
                bail!(
                    "registry_restore is for Apicurio Registry and for restoring between \
                     Confluent Schema Registry and Apicurio. For a Confluent-to-Confluent \
                     restore remove registry_restore and use schema_registry.restore"
                );
            }
            Route {
                confluent: false,
                engine: vec![Provider::Confluent],
            }
        }
        None => {
            if common.is_some() && !ap {
                bail!(
                    "registry_restore is set, but the configuration has no enabled \
                     apicurio_registry section. For a Confluent-to-Confluent restore remove \
                     registry_restore and use schema_registry.restore; to restore an Apicurio \
                     snapshot into Confluent Schema Registry set registry_restore.source: \
                     apicurio and destination: confluent"
                );
            }
            if common.is_some_and(|c| c.source.is_some()) && sr && ap {
                bail!(
                    "registry_restore.source is set and both schema_registry and \
                     apicurio_registry are enabled; set registry_restore.destination"
                );
            }
            Route {
                confluent: sr,
                engine: if ap { vec![Provider::Apicurio] } else { vec![] },
            }
        }
    };
    if schema_only && !route.confluent && route.engine.is_empty() {
        bail!(
            "--schema-only restores a registry, but the configuration has no enabled \
             schema_registry or apicurio_registry section (its url is the target registry)"
        );
    }
    Ok(route)
}

/// A dry-run flag anywhere applies to the entire invocation.
pub fn normalize_dry_run(config: &mut Config, force: bool) -> bool {
    let engine_dry = config.registry_restore.as_ref().is_some_and(|r| r.dry_run);
    let dry = confluent::normalize_dry_run(config, force || engine_dry);
    if dry {
        if let Some(common) = config.registry_restore.as_mut() {
            common.dry_run = true;
        }
    }
    dry
}

/// Load the snapshots, connect to the targets and plan. Read-only.
pub async fn prepare(
    config: &Config,
    destinations: &[Provider],
    schema_only: bool,
) -> Result<Vec<PreparedRegistry>> {
    if destinations.is_empty() {
        return Ok(vec![]);
    }
    if config
        .restore
        .as_ref()
        .is_some_and(|r| r.rewrite_schema_ids)
    {
        bail!(
            "restore.rewrite_schema_ids cannot be combined with registry_restore; select \
             registry_restore.id_policy: rewrite instead"
        );
    }
    let storage = create_backend(&config.storage)?;
    let dry = config.restore.as_ref().is_some_and(|r| r.dry_run);
    let common = config.registry_restore.clone();
    let mut prepared = vec![];
    for destination in destinations {
        let mut options = common.clone().unwrap_or_default();
        options.dry_run = dry;
        let source = options.source.unwrap_or(*destination);
        let (target, url) = match destination {
            Provider::Confluent => {
                let sr =
                    confluent::enabled(config).context("Confluent destination is not enabled")?;
                if sr.restore.is_some() {
                    bail!(
                        "schema_registry.restore does not apply when the registry is restored \
                         through registry_restore; set the options there"
                    );
                }
                (Target::confluent(sr)?, sr.url.clone())
            }
            Provider::Apicurio => {
                let ap = config
                    .apicurio_registry
                    .as_ref()
                    .filter(|ap| ap.enabled)
                    .context("Apicurio destination is not enabled")?;
                (Target::apicurio(ap)?, ap.url.clone())
            }
        };
        let snapshot = snapshot::load(storage.as_ref(), &config.backup_id, source).await?;
        let mut identity = reqwest::Url::parse(&url)?;
        let _ = identity.set_username("");
        let _ = identity.set_password(None);
        identity.set_query(None);
        identity.set_fragment(None);
        let target_identity = target.identity(identity.as_str()).await?;
        let engine = RegistryRestore::new(
            snapshot,
            options,
            target,
            storage.clone(),
            target_identity,
            config
                .restore
                .as_ref()
                .and_then(|r| r.checkpoint_state.as_deref()),
        )?;
        let mut plan = engine.plan().await?;
        if !schema_only && plan.conflicts.is_empty() {
            let options = config.restore.clone().unwrap_or_default();
            let target = config.target.as_ref().context(
                "Kafka restore requires target configuration; use --schema-only for registry-only backups",
            )?;
            if let Err(e) = engine
                .preflight_records(&config.backup_id, &options, &target.topics)
                .await
            {
                plan.conflicts
                    .push(format!("Record identifier preflight: {e:#}"));
            }
        }
        prepared.push(PreparedRegistry { engine, plan });
    }
    Ok(prepared)
}

/// Execute the prepared restores. For a full restore the record transform
/// (ID and framing rewrite) is installed on `config.restore`.
pub async fn execute(
    config: &mut Config,
    prepared: Vec<PreparedRegistry>,
    schema_only: bool,
) -> Result<()> {
    if prepared.is_empty() {
        return Ok(());
    }
    let conflicts: Vec<_> = prepared
        .iter()
        .flat_map(|p| p.plan.conflicts.clone())
        .collect();
    if !conflicts.is_empty() {
        println!(
            "{}",
            serde_json::to_string_pretty(&prepared.iter().map(|p| &p.plan).collect::<Vec<_>>())?
        );
        bail!(
            "Registry preflight failed: {}.{}",
            conflicts.join("; "),
            if schema_only {
                ""
            } else {
                " The Kafka records were not restored."
            }
        );
    }
    let dry = config.restore.as_ref().is_some_and(|r| r.dry_run);
    if dry {
        println!(
            "{}",
            serde_json::to_string_pretty(&prepared.iter().map(|p| &p.plan).collect::<Vec<_>>())?
        );
        return Ok(());
    }
    if !schema_only {
        // The Kafka dry-run preflight runs before the registry is changed.
        let mut preflight = config.clone();
        preflight
            .restore
            .get_or_insert_with(Default::default)
            .dry_run = true;
        let report = kafka_backup_core::RestoreEngine::new(preflight)?
            .dry_run()
            .await?;
        if !report.valid || !report.errors.is_empty() {
            bail!(
                "Kafka preflight failed before the registry was changed: {}",
                report.errors.join("; ")
            );
        }
    }
    for prepared in prepared {
        let cancel = tokio_util::sync::CancellationToken::new();
        let shutdown = cancel.clone();
        let signals = tokio::spawn(async move {
            #[cfg(unix)]
            {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            shutdown.cancel();
        });
        let result = prepared
            .engine
            .execute_with_cancel(&prepared.plan, cancel)
            .await;
        signals.abort();
        let (mut report, transform) = result.map_err(|e| {
            if schema_only {
                e
            } else {
                e.context("Registry restore failed. The Kafka records were not restored")
            }
        })?;
        prepared.engine.verify(&report).await?;
        if !schema_only {
            let options = config.restore.clone().unwrap_or_default();
            let topics = &config
                .target
                .as_ref()
                .context("Kafka target is required")?
                .topics;
            if let Err(e) = prepared
                .engine
                .preflight_records_with_transform(&config.backup_id, &options, topics, &transform)
                .await
            {
                report
                    .failures
                    .push(format!("Destination record preflight failed: {e:#}"));
                prepared.engine.write_report(&report).await?;
                bail!("Destination record preflight failed; registry report saved: {e:#}. The Kafka records were not restored");
            }
            config
                .restore
                .get_or_insert_with(Default::default)
                .record_transform = Some(RecordTransformHandle::new(Arc::new(transform)));
        }
        println!("{}", serde_json::to_string_pretty(&report)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(extra: &str) -> Config {
        serde_yaml::from_str(&format!(
            "mode: restore\nbackup_id: test\nstorage:\n  backend: memory\n{extra}"
        ))
        .unwrap()
    }
    const SR: &str = "schema_registry:\n  url: http://sr\n";
    const AP: &str = "apicurio_registry:\n  url: http://ap\n";

    #[test]
    fn each_registry_goes_to_its_own_engine() {
        let both = Route {
            confluent: true,
            engine: vec![Provider::Apicurio],
        };
        assert_eq!(route(&config(&format!("{SR}{AP}")), false).unwrap(), both);
        assert_eq!(
            route(&config(SR), false).unwrap(),
            Route {
                confluent: true,
                engine: vec![]
            }
        );
        assert_eq!(
            route(&config(AP), true).unwrap(),
            Route {
                confluent: false,
                engine: vec![Provider::Apicurio]
            }
        );
        // No registry: a full restore restores records only.
        assert_eq!(
            route(&config(""), false).unwrap(),
            Route {
                confluent: false,
                engine: vec![]
            }
        );
        assert!(route(&config(""), true).is_err());
        assert!(route(
            &config("apicurio_registry:\n  url: http://ap\n  enabled: false\n"),
            true
        )
        .is_err());
    }

    #[test]
    fn cross_provider_needs_an_explicit_destination_with_its_section() {
        let cross =
            format!("{SR}{AP}registry_restore:\n  source: apicurio\n  destination: confluent\n");
        assert_eq!(
            route(&config(&cross), false).unwrap(),
            Route {
                confluent: false,
                engine: vec![Provider::Confluent]
            }
        );
        let into_apicurio =
            format!("{AP}registry_restore:\n  source: confluent\n  destination: apicurio\n");
        assert_eq!(
            route(&config(&into_apicurio), false).unwrap().engine,
            vec![Provider::Apicurio]
        );
        for bad in [
            format!("{SR}registry_restore:\n  destination: apicurio\n"),
            format!("{AP}registry_restore:\n  destination: confluent\n"),
            format!("{SR}{AP}registry_restore:\n  source: confluent\n"),
        ] {
            assert!(route(&config(&bad), false).is_err(), "{bad}");
        }
    }

    #[test]
    fn confluent_to_confluent_is_sent_to_schema_registry_restore() {
        for yaml in [
            format!("{SR}registry_restore:\n  id_policy: rewrite\n"),
            format!("{SR}registry_restore:\n  destination: confluent\n"),
            format!("{SR}registry_restore:\n  source: confluent\n  destination: confluent\n"),
        ] {
            let err = route(&config(&yaml), false).unwrap_err().to_string();
            assert!(err.contains("schema_registry.restore"), "{yaml}: {err}");
        }
    }

    #[test]
    fn a_dry_run_flag_in_any_section_applies_to_everything() {
        let mut cfg = config(&format!("{SR}{AP}registry_restore:\n  dry_run: true\n"));
        assert!(normalize_dry_run(&mut cfg, false));
        assert!(cfg.restore.as_ref().unwrap().dry_run);
        assert!(
            cfg.schema_registry
                .as_ref()
                .unwrap()
                .restore_options()
                .dry_run
        );

        let mut cfg = config(&format!(
            "{AP}registry_restore: {{}}\nrestore:\n  dry_run: true\n"
        ));
        assert!(normalize_dry_run(&mut cfg, false));
        assert!(cfg.registry_restore.as_ref().unwrap().dry_run);

        let mut cfg = config(AP);
        assert!(!normalize_dry_run(&mut cfg, false));
        assert!(normalize_dry_run(&mut cfg, true));
    }
}
