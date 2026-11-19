//! Failure contracts of the Schema Registry restore and client: what is
//! guaranteed when a restore is cancelled, when someone else changes a mode
//! while it runs, when the target cannot clean up mode overrides, and which
//! requests are retried.

use std::sync::Arc;

use kafka_backup_core::schema_registry::client::SchemaRegistryClient;
use kafka_backup_core::schema_registry::config::{SchemaConnectionConfig, SchemaRestoreOptions};
use kafka_backup_core::schema_registry::models::{RegisterSchemaRequest, SchemaRestoreReport};
use kafka_backup_core::schema_registry::restore::{report_key, SchemaRegistryRestore};
use kafka_backup_core::schema_registry::{SchemaRegistryConfig, SchemaRegistryError};
use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::fake_schema_registry::{FakeSchemaRegistry, Schema};
use super::restore::{
    backup, into_existing, preserve_ids, restore, source, sr_config, ADDRESS, BACKUP,
};

async fn restore_with_cancel(
    target: &MockServer,
    storage: &Arc<MemoryBackend>,
    options: SchemaRestoreOptions,
    cancel: CancellationToken,
) -> (SchemaRestoreReport, Option<SchemaRegistryError>) {
    let cfg = sr_config(&target.uri(), None, Some(options));
    SchemaRegistryRestore::new(
        SchemaRegistryClient::new(&cfg).unwrap(),
        storage.clone(),
        BACKUP.to_string(),
        cfg,
    )
    .run_with_cancel(cancel)
    .await
}

fn record(name: &str) -> String {
    format!(
        r#"{{"type":"record","name":"{name}","namespace":"com.example","fields":[{{"name":"id","type":"string"}}]}}"#
    )
}

/// `a-value` v1 (ID 1), v2 (ID 2) and `b-value` v1 (ID 3), default context,
/// no modes or compatibility overrides.
async fn small_source() -> (MockServer, FakeSchemaRegistry) {
    let (server, sr) = FakeSchemaRegistry::start().await;
    sr.seed("a-value", 1, 1, Schema::avro(&record("A1")));
    sr.seed("a-value", 2, 2, Schema::avro(&record("A2")));
    sr.seed("b-value", 1, 3, Schema::avro(&record("B1")));
    (server, sr)
}

// ===========================================================================
// Cancellation
// ===========================================================================

#[tokio::test]
async fn cancellation_after_a_committed_write_puts_the_modes_back() {
    let (src, source_sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    // The signal arrives while a registration is in flight: the registry
    // has applied it, the client has not seen the answer yet.
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    target.after_once("POST", "/subjects/orders-value/versions", move |_| {
        trigger.cancel()
    });

    let (report, err) = restore_with_cancel(&tgt, &storage, preserve_ids(), cancel).await;
    assert!(
        matches!(err, Some(SchemaRegistryError::Cancelled(_))),
        "{err:?}"
    );
    assert!(report.cancelled && !report.success);
    assert!(report.modes_rolled_back, "{report:?}");
    assert!(report.mode_restore_errors.is_empty());

    // The write that was in flight is in the target, and nothing was
    // registered after the cancellation.
    assert!(target.snapshot().contains_key("orders-value"));
    let mutations = target.mutations();
    let cancelled_at = mutations
        .iter()
        .position(|m| m.starts_with("POST /subjects/orders-value/versions"))
        .unwrap();
    assert!(
        mutations[cancelled_at + 1..]
            .iter()
            .all(|m| m.contains("/mode")),
        "{mutations:?}"
    );

    // Global, context and subject modes are as before the restore.
    assert_eq!(target.global_mode(), "READWRITE");
    assert!(target.modes().is_empty(), "{:?}", target.modes());

    // The report is written for a cancelled restore too.
    let stored: SchemaRestoreReport =
        serde_json::from_slice(&storage.get(&report_key(BACKUP, false)).await.unwrap()).unwrap();
    assert!(stored.cancelled && !stored.success && stored.modes_rolled_back);

    // Rerunning with into_existing completes: nothing is registered twice.
    let (report, err) = restore(&tgt, &storage, into_existing()).await;
    assert!(err.is_none(), "{err:?}");
    assert!(report.versions_skipped > 0);
    assert_eq!(source_sr.snapshot(), target.snapshot());
    assert_eq!(target.global_mode(), "READWRITE");
    assert!(
        !target.modes().values().any(|m| m == "IMPORT"),
        "{:?}",
        target.modes()
    );
}

#[tokio::test]
async fn cancellation_of_an_into_existing_restore_removes_its_subject_overrides() {
    let (src, _source) = small_source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("other-value", 1, 50, Schema::avro(ADDRESS));
    target.set_mode("b-value", "READONLY");
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    // Cancelled while the IMPORT override of the second subject is being set.
    target.after_once("PUT", "/mode/b-value", move |_| trigger.cancel());

    let (report, err) = restore_with_cancel(&tgt, &storage, into_existing(), cancel).await;
    assert!(matches!(err, Some(SchemaRegistryError::Cancelled(_))));
    assert!(report.modes_rolled_back, "{report:?}");
    // a-value's temporary override is gone; b-value has its own mode back
    // although the request that changed it never returned to the client.
    assert_eq!(
        target.modes(),
        [("b-value".to_string(), "READONLY".to_string())].into()
    );
    assert_eq!(target.global_mode(), "READWRITE");
}

#[tokio::test]
async fn a_cancellation_before_the_restore_starts_changes_nothing() {
    let (src, _source) = small_source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (report, err) = restore_with_cancel(&tgt, &storage, preserve_ids(), cancel).await;
    assert!(matches!(err, Some(SchemaRegistryError::Cancelled(_))));
    assert!(!report.success);
    assert!(target.mutations().is_empty(), "{:?}", target.mutations());
}

// ===========================================================================
// A mode changed by someone else during the restore
// ===========================================================================

#[tokio::test]
async fn a_concurrent_subject_mode_change_is_not_overwritten_on_success() {
    let (src, _source) = small_source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("other-value", 1, 50, Schema::avro(ADDRESS));
    // After b-value's only version is registered, an operator gives the
    // subject its own mode.
    target.after_once("POST", "/subjects/b-value/versions", |st| {
        st.modes
            .insert("b-value".to_string(), "READWRITE".to_string());
    });

    let (report, err) = restore(&tgt, &storage, into_existing()).await;
    assert!(err.is_none(), "{err:?}");
    assert!(report.success);
    // The restore's own override on a-value is removed; the operator's
    // setting on b-value stays instead of being deleted.
    assert_eq!(
        target.modes(),
        [("b-value".to_string(), "READWRITE".to_string())].into()
    );
    assert_eq!(report.modes_changed_by_others.len(), 1, "{report:?}");
    assert!(report.modes_changed_by_others[0].contains("b-value"));
    assert!(report.modes_changed_by_others[0].contains("READWRITE"));
    assert!(report
        .warnings
        .iter()
        .any(|w| w.contains("changed to READWRITE by someone else")));
}

#[tokio::test]
async fn a_concurrent_subject_mode_change_is_not_overwritten_on_failure() {
    let (src, _source) = small_source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("other-value", 1, 50, Schema::avro(ADDRESS));
    // Between a-value's two versions an operator makes the subject
    // read-only, which also makes the second registration fail.
    target.after_once("POST", "/subjects/a-value/versions", |st| {
        st.modes
            .insert("a-value".to_string(), "READONLY".to_string());
    });

    let (report, err) = restore(&tgt, &storage, into_existing()).await;
    assert!(err.is_some());
    assert_eq!(report.versions_registered, 1);
    assert_eq!(
        target.modes(),
        [("a-value".to_string(), "READONLY".to_string())].into(),
        "the operator's READONLY must survive the rollback"
    );
    assert_eq!(report.modes_changed_by_others.len(), 1, "{report:?}");
    assert!(report.modes_changed_by_others[0].contains("a-value"));
    assert!(report.mode_restore_errors.is_empty());
    assert!(report.modes_rolled_back);
}

#[tokio::test]
async fn a_concurrent_global_mode_change_is_not_overwritten() {
    let (src, _source) = small_source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.after_once("POST", "/subjects/a-value/versions", |st| {
        st.global_mode = "READONLY".to_string();
    });

    let (report, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(
        err.is_some(),
        "registrations fail once the registry is read-only"
    );
    assert_eq!(target.global_mode(), "READONLY");
    assert_eq!(report.modes_changed_by_others.len(), 1, "{report:?}");
    assert!(report.modes_changed_by_others[0].contains("global mode"));
    assert!(report.mode_restore_errors.is_empty());
}

// ===========================================================================
// A target that cannot delete mode overrides (Confluent Platform 6.2)
// ===========================================================================

#[tokio::test]
async fn a_target_that_cannot_delete_mode_overrides_refuses_subject_level_import() {
    let (src, _source) = small_source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    for dry_run in [false, true] {
        let (tgt, target) = FakeSchemaRegistry::start().await;
        target.with(|st| st.mode_delete_supported = false);
        target.seed("other-value", 1, 50, Schema::avro(ADDRESS));
        let (report, err) = restore(
            &tgt,
            &storage,
            SchemaRestoreOptions {
                dry_run,
                ..into_existing()
            },
        )
        .await;
        let err = err.expect("refused");
        assert!(
            matches!(err, SchemaRegistryError::RestoreConflict(_)),
            "{err}"
        );
        assert!(
            err.to_string().contains("cannot delete mode overrides"),
            "{err}"
        );
        assert!(err.to_string().contains("a-value"), "{err}");
        assert!(!report.success);
        assert!(target.mutations().is_empty(), "{:?}", target.mutations());
    }
}

#[tokio::test]
async fn a_target_that_cannot_delete_mode_overrides_refuses_a_context_import() {
    // The shared fixture has a non-default context, which preserve_ids
    // opens with a context-level IMPORT override.
    let (src, _source) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.with(|st| st.mode_delete_supported = false);
    let (_, err) = restore(&tgt, &storage, preserve_ids()).await;
    let err = err.expect("refused");
    assert!(
        err.to_string().contains("cannot delete mode overrides"),
        "{err}"
    );
    assert!(target.mutations().is_empty(), "{:?}", target.mutations());
}

#[tokio::test]
async fn preserve_ids_without_contexts_needs_no_mode_override_to_delete() {
    let (src, source_sr) = small_source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.with(|st| st.mode_delete_supported = false);
    let (report, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(report.versions_registered, 3);
    assert_eq!(source_sr.snapshot(), target.snapshot());
    assert_eq!(target.global_mode(), "READWRITE");
    assert!(target.modes().is_empty());
    assert!(!target.log().iter().any(|l| l.starts_with("DELETE /mode")));
}

// ===========================================================================
// Retries
// ===========================================================================

fn client(server: &MockServer, max_retries: u32) -> SchemaRegistryClient {
    let mut cfg = SchemaRegistryConfig::new(server.uri());
    cfg.connection = Some(SchemaConnectionConfig {
        timeout_ms: 5_000,
        max_retries,
        retry_backoff_ms: 1,
        rate_limit_rps: 10_000,
    });
    SchemaRegistryClient::new(&cfg).unwrap()
}

async fn requests(server: &MockServer, verb: &str) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.method.as_str() == verb)
        .count()
}

#[tokio::test]
async fn reads_are_retried_after_429_and_5xx() {
    for status in [429u16, 500, 502, 503, 504] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_json(json!({"error_code": status, "message": "try later"})),
            )
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!(["orders-value"])))
            .mount(&server)
            .await;

        let subjects = client(&server, 3).list_subjects(false).await.unwrap();
        assert_eq!(subjects, ["orders-value"], "status {status}");
        assert_eq!(requests(&server, "GET").await, 3, "status {status}");
    }
}

#[tokio::test]
async fn a_read_that_keeps_failing_gives_up_after_max_retries_with_the_last_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(
            ResponseTemplate::new(503)
                .set_body_json(json!({"error_code": 50301, "message": "leader not available"})),
        )
        .mount(&server)
        .await;
    let err = client(&server, 2).list_subjects(false).await.unwrap_err();
    assert!(
        matches!(err, SchemaRegistryError::Api { status: 503, .. }),
        "{err}"
    );
    assert!(err.to_string().contains("leader not available"));
    assert_eq!(requests(&server, "GET").await, 3);
}

#[tokio::test]
async fn writes_are_sent_once_because_a_failed_write_may_have_been_applied() {
    for status in [429u16, 503] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_json(json!({"error_code": status, "message": "try later"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let client = client(&server, 3);
        let request = RegisterSchemaRequest {
            schema: record("A"),
            schema_type: Some("AVRO".into()),
            references: vec![],
            id: Some(1),
            version: Some(1),
            metadata: None,
            rule_set: None,
        };
        assert!(client.register_schema("a-value", &request).await.is_err());
        assert!(client.set_global_mode("IMPORT").await.is_err());
        assert_eq!(requests(&server, "POST").await, 1, "status {status}");
        assert_eq!(requests(&server, "PUT").await, 1, "status {status}");
    }
}

#[tokio::test]
async fn auth_failures_and_client_errors_are_not_retried() {
    for (status, auth) in [(401u16, true), (403, true), (422, false)] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_json(json!({"error_code": status, "message": "no"})),
            )
            .mount(&server)
            .await;
        let err = client(&server, 3).list_subjects(false).await.unwrap_err();
        assert_eq!(
            matches!(err, SchemaRegistryError::Auth(_)),
            auth,
            "{status}: {err}"
        );
        assert_eq!(requests(&server, "GET").await, 1, "status {status}");
    }
}
