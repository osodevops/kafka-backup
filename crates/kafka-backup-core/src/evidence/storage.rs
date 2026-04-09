//! Evidence report upload, download, and listing via the storage backend.

use bytes::Bytes;
use chrono::Utc;

use crate::storage::StorageBackend;
use crate::Result;

/// Upload a JSON evidence report to object storage.
pub async fn upload_evidence_json(
    storage: &dyn StorageBackend,
    prefix: &str,
    report_id: &str,
    json_bytes: &[u8],
) -> Result<String> {
    let now = Utc::now();
    let key = format!(
        "{}{}/{:04}/{:02}/{}.json",
        prefix,
        report_id,
        now.format("%Y"),
        now.format("%m"),
        report_id
    );
    storage.put(&key, Bytes::from(json_bytes.to_vec())).await?;
    Ok(key)
}

/// Upload a PDF evidence report to object storage.
pub async fn upload_evidence_pdf(
    storage: &dyn StorageBackend,
    prefix: &str,
    report_id: &str,
    pdf_bytes: &[u8],
) -> Result<String> {
    let now = Utc::now();
    let key = format!(
        "{}{}/{:04}/{:02}/{}.pdf",
        prefix,
        report_id,
        now.format("%Y"),
        now.format("%m"),
        report_id
    );
    storage.put(&key, Bytes::from(pdf_bytes.to_vec())).await?;
    Ok(key)
}

/// Upload a detached signature file to object storage.
pub async fn upload_evidence_signature(
    storage: &dyn StorageBackend,
    prefix: &str,
    report_id: &str,
    sig_content: &str,
) -> Result<String> {
    let now = Utc::now();
    let key = format!(
        "{}{}/{:04}/{:02}/{}.sig",
        prefix,
        report_id,
        now.format("%Y"),
        now.format("%m"),
        report_id
    );
    storage
        .put(&key, Bytes::from(sig_content.as_bytes().to_vec()))
        .await?;
    Ok(key)
}

/// List evidence report keys under the given prefix.
pub async fn list_evidence_reports(
    storage: &dyn StorageBackend,
    prefix: &str,
) -> Result<Vec<String>> {
    let keys = storage.list(prefix).await?;
    // Filter to only .json files (the canonical reports)
    Ok(keys.into_iter().filter(|k| k.ends_with(".json")).collect())
}

/// Download an evidence report (JSON) from object storage.
pub async fn download_evidence_report(storage: &dyn StorageBackend, key: &str) -> Result<Vec<u8>> {
    let data = storage.get(key).await?;
    Ok(data.to_vec())
}
