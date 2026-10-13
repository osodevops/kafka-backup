//! Fallible replay-time transformations for embedding applications.
//! Called after time/record filtering and before partition selection. The
//! fingerprint is part of checkpoint compatibility; archived bytes are immutable.
use crate::{manifest::BackupRecord, Result};
use std::{fmt, sync::Arc};

pub trait RecordTransform: Send + Sync {
    fn name(&self) -> &str;
    /// Stable digest of all configuration and external mappings affecting output.
    fn fingerprint(&self) -> &str;
    fn transform(&self, source_topic: &str, record: &mut BackupRecord) -> Result<()>;
}
#[derive(Clone)]
pub struct RecordTransformHandle(Arc<dyn RecordTransform>);
impl RecordTransformHandle {
    pub fn new(transform: Arc<dyn RecordTransform>) -> Self {
        Self(transform)
    }
    pub fn fingerprint(&self) -> &str {
        self.0.fingerprint()
    }
    pub fn transform(&self, topic: &str, record: &mut BackupRecord) -> Result<()> {
        self.0.transform(topic, record)
    }
}
impl fmt::Debug for RecordTransformHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordTransformHandle({})", self.0.name())
    }
}
pub(crate) fn apply(
    topic: &str,
    mut records: Vec<BackupRecord>,
    transform: Option<&RecordTransformHandle>,
) -> Result<Vec<BackupRecord>> {
    if let Some(transform) = transform {
        for record in &mut records {
            transform.transform(topic, record)?;
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Reject;
    impl RecordTransform for Reject {
        fn name(&self) -> &str {
            "reject"
        }
        fn fingerprint(&self) -> &str {
            "reject-v1"
        }
        fn transform(&self, _: &str, _: &mut BackupRecord) -> Result<()> {
            Err(crate::Error::Config("unknown record".into()))
        }
    }
    #[test]
    fn transform_errors_propagate_without_returning_a_replay_batch() {
        let r = BackupRecord {
            key: None,
            value: None,
            headers: vec![],
            timestamp: 123,
            offset: 42,
        };
        let handle = RecordTransformHandle::new(Arc::new(Reject));
        assert!(apply("topic", vec![r], Some(&handle)).is_err());
    }
}
