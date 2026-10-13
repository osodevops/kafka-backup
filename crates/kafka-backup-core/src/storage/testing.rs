//! Test-only storage wrapper that injects failures and records writes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use super::{MemoryBackend, ObjectMetadata, StorageBackend};
use crate::error::StorageError;
use crate::{Error, Result};

/// Hook run after a matching `put` has been stored: `(key, bytes)`.
type PutHook = Arc<dyn Fn(&str, &Bytes) + Send + Sync>;

struct PutFault {
    suffix: String,
    error: StorageError,
    /// Matching puts allowed through before the fault starts.
    after: usize,
    seen: usize,
}

struct PutHookEntry {
    suffix: String,
    /// 1-based matching put to run on; `None` runs on every matching put.
    nth: Option<usize>,
    seen: usize,
    hook: PutHook,
}

/// A [`MemoryBackend`] that can fail chosen reads and writes, slow writes
/// down, run a hook after chosen writes, and records every stored key.
///
/// Reads of keys ending in a configured suffix fail, e.g. with an S3 403
/// (`Backend`) rather than `NotFound`. Puts can fail after the first `n`
/// matching ones, sleep before storing, and trigger a hook once stored (to
/// observe state at that instant, or to simulate a crash right after).
#[derive(Default)]
pub struct FaultyStorage {
    inner: MemoryBackend,
    /// (key suffix, error) for get / exists / head / size.
    failing_reads: Mutex<Vec<(String, StorageError)>>,
    failing_puts: Mutex<Vec<PutFault>>,
    put_delays: Mutex<Vec<(String, Duration)>>,
    put_hooks: Mutex<Vec<PutHookEntry>>,
    /// Keys successfully stored, in order.
    puts: Mutex<Vec<String>>,
    /// Last bytes stored per key, readable synchronously from hooks.
    objects: Mutex<HashMap<String, Bytes>>,
}

fn clone_error(e: &StorageError, key: &str) -> StorageError {
    match e {
        StorageError::NotFound(_) => StorageError::NotFound(key.to_string()),
        StorageError::PermissionDenied(m) => StorageError::PermissionDenied(m.clone()),
        StorageError::Backend(m) => StorageError::Backend(m.clone()),
        StorageError::InvalidPath(m) => StorageError::InvalidPath(m.clone()),
    }
}

impl FaultyStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail reads of keys ending in `suffix` with `error`.
    pub fn fail_reads(&self, suffix: &str, error: StorageError) {
        self.failing_reads
            .lock()
            .unwrap()
            .push((suffix.to_string(), error));
    }

    /// Fail reads of keys ending in `suffix` like S3 403 AccessDenied.
    pub fn deny_reads(&self, suffix: &str) {
        self.fail_reads(
            suffix,
            StorageError::Backend("S3 GET failed: 403 AccessDenied".to_string()),
        );
    }

    /// Stop failing reads.
    pub fn heal(&self) {
        self.failing_reads.lock().unwrap().clear();
    }

    /// Let the first `after_n` puts of keys ending in `suffix` through, then
    /// fail every later one with `error` (nothing is stored) until
    /// [`heal_puts`](Self::heal_puts).
    pub fn fail_puts(&self, suffix: &str, error: StorageError, after_n: usize) {
        self.failing_puts.lock().unwrap().push(PutFault {
            suffix: suffix.to_string(),
            error,
            after: after_n,
            seen: 0,
        });
    }

    /// Stop failing puts.
    pub fn heal_puts(&self) {
        self.failing_puts.lock().unwrap().clear();
    }

    /// Sleep `delay` before storing any key ending in `suffix` (latency
    /// injection: keeps an upload in flight).
    pub fn delay_puts(&self, suffix: &str, delay: Duration) {
        self.put_delays
            .lock()
            .unwrap()
            .push((suffix.to_string(), delay));
    }

    /// Run `hook` right after the `nth` (1-based) put of a key ending in
    /// `suffix` has been stored, before the put returns to its caller.
    pub fn on_put(
        &self,
        suffix: &str,
        nth: usize,
        hook: impl Fn(&str, &Bytes) + Send + Sync + 'static,
    ) {
        self.add_hook(suffix, Some(nth), Arc::new(hook));
    }

    /// Run `hook` after every stored put of a key ending in `suffix`.
    pub fn on_every_put(&self, suffix: &str, hook: impl Fn(&str, &Bytes) + Send + Sync + 'static) {
        self.add_hook(suffix, None, Arc::new(hook));
    }

    fn add_hook(&self, suffix: &str, nth: Option<usize>, hook: PutHook) {
        self.put_hooks.lock().unwrap().push(PutHookEntry {
            suffix: suffix.to_string(),
            nth,
            seen: 0,
            hook,
        });
    }

    /// Keys stored so far, in order (failed puts are not recorded).
    pub fn puts(&self) -> Vec<String> {
        self.puts.lock().unwrap().clone()
    }

    /// The bytes currently stored under `key`, read synchronously (usable
    /// from a put hook).
    pub fn stored(&self, key: &str) -> Option<Bytes> {
        self.objects.lock().unwrap().get(key).cloned()
    }

    fn check(&self, key: &str) -> Result<()> {
        let failing = self.failing_reads.lock().unwrap();
        match failing
            .iter()
            .find(|(suffix, _)| key.ends_with(suffix.as_str()))
        {
            Some((_, e)) => Err(Error::Storage(clone_error(e, key))),
            None => Ok(()),
        }
    }

    fn put_delay(&self, key: &str) -> Option<Duration> {
        self.put_delays
            .lock()
            .unwrap()
            .iter()
            .filter(|(suffix, _)| key.ends_with(suffix.as_str()))
            .map(|(_, delay)| *delay)
            .max()
    }

    fn check_put(&self, key: &str) -> Result<()> {
        let mut faults = self.failing_puts.lock().unwrap();
        for fault in faults
            .iter_mut()
            .filter(|f| key.ends_with(f.suffix.as_str()))
        {
            fault.seen += 1;
            if fault.seen > fault.after {
                return Err(Error::Storage(clone_error(&fault.error, key)));
            }
        }
        Ok(())
    }

    fn due_hooks(&self, key: &str) -> Vec<PutHook> {
        let mut hooks = self.put_hooks.lock().unwrap();
        hooks
            .iter_mut()
            .filter(|h| key.ends_with(h.suffix.as_str()))
            .filter_map(|h| {
                h.seen += 1;
                match h.nth {
                    Some(n) if n != h.seen => None,
                    _ => Some(h.hook.clone()),
                }
            })
            .collect()
    }
}

#[async_trait]
impl StorageBackend for FaultyStorage {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        if let Some(delay) = self.put_delay(key) {
            tokio::time::sleep(delay).await;
        }
        self.check_put(key)?;
        self.inner.put(key, data.clone()).await?;
        self.puts.lock().unwrap().push(key.to_string());
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), data.clone());
        // Hooks run outside every lock, so they may call back into `self`.
        for hook in self.due_hooks(key) {
            hook(key, &data);
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        self.check(key)?;
        self.inner.get(key).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.check(key)?;
        self.inner.exists(key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await?;
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    async fn size(&self, key: &str) -> Result<u64> {
        self.check(key)?;
        self.inner.size(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        self.check(key)?;
        self.inner.head(key).await
    }

    async fn copy(&self, src: &str, dest: &str) -> Result<()> {
        self.check(src)?;
        self.inner.copy(src, dest).await?;
        let copied = self.objects.lock().unwrap().get(src).cloned();
        if let Some(bytes) = copied {
            self.objects.lock().unwrap().insert(dest.to_string(), bytes);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn data() -> Bytes {
        Bytes::from_static(b"x")
    }

    #[tokio::test]
    async fn fail_puts_lets_the_first_n_through_then_fails() {
        let storage = FaultyStorage::new();
        storage.fail_puts(
            "manifest.json",
            StorageError::Backend("503 SlowDown".to_string()),
            2,
        );

        storage.put("b/manifest.json", data()).await.unwrap();
        storage.put("b/manifest.json", data()).await.unwrap();
        let err = storage.put("b/manifest.json", data()).await.unwrap_err();
        assert!(err.to_string().contains("503 SlowDown"), "{err}");
        // Other keys are unaffected.
        storage.put("b/offsets.db", data()).await.unwrap();
        // A failed put stores nothing and is not recorded.
        assert_eq!(
            storage.puts(),
            vec!["b/manifest.json", "b/manifest.json", "b/offsets.db"]
        );

        storage.heal_puts();
        storage.put("b/manifest.json", data()).await.unwrap();
    }

    #[tokio::test]
    async fn delay_puts_slows_matching_keys_only() {
        let storage = FaultyStorage::new();
        storage.delay_puts(".bin.zst", Duration::from_millis(60));

        let start = Instant::now();
        storage.put("b/manifest.json", data()).await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(50));

        let start = Instant::now();
        storage.put("b/seg.bin.zst", data()).await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(60));
    }

    #[tokio::test]
    async fn on_put_runs_after_the_nth_matching_put_is_stored() {
        let storage = Arc::new(FaultyStorage::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let inner = storage.clone();
        storage.on_put("manifest.json", 2, move |key, bytes| {
            assert_eq!(key, "b/manifest.json");
            assert_eq!(bytes.as_ref(), b"second");
            // The object is already stored when the hook runs.
            assert!(inner.stored(key).is_some());
            seen.fetch_add(1, Ordering::SeqCst);
        });

        storage
            .put("b/manifest.json", Bytes::from_static(b"first"))
            .await
            .unwrap();
        storage.put("b/other", data()).await.unwrap();
        storage
            .put("b/manifest.json", Bytes::from_static(b"second"))
            .await
            .unwrap();
        storage
            .put("b/manifest.json", Bytes::from_static(b"third"))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let every = Arc::new(AtomicUsize::new(0));
        let count = every.clone();
        storage.on_every_put("offsets.db", move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
        });
        for _ in 0..3 {
            storage.put("b/offsets.db", data()).await.unwrap();
        }
        assert_eq!(every.load(Ordering::SeqCst), 3);
    }
}
