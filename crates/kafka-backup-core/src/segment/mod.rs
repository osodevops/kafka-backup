//! Binary segment format for efficient backup storage.
//!
//! This module provides a binary wire format for backup segments that is more
//! efficient than JSON, supporting zero-copy reads and better compression.

pub mod format;
pub mod reader;
pub mod writer;

pub use format::{BinaryRecord, SegmentHeader, MAGIC_BYTES, VERSION};
pub use reader::SegmentReader;
pub use writer::{SegmentWriter, SegmentWriterConfig};
