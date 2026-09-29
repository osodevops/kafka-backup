//! Connection-loss classification (issue #146).
//!
//! Single source of truth for "is this error a lost or unusable broker
//! connection that warrants reconnecting and retrying?" — used by
//! `KafkaClient::send_request` (reconnect + retry once) and by the
//! `PartitionLeaderRouter` produce / delete-records loops (bounded retries
//! with backoff).
//!
//! Classification is **structural**: it reads the [`std::io::ErrorKind`] and
//! raw OS error code preserved in [`KafkaError::ConnectionIo`]. It never
//! decides based on the OS message text, because that text is localized
//! (`FormatMessageW` on Windows follows the UI language; `strerror_r` on Unix
//! follows `LC_MESSAGES`) and worded differently on every platform — e.g.
//! `WSAECONNABORTED` reads *"An established connection was aborted by the
//! software in your host machine. (os error 10053)"* on an English Windows,
//! which is what the old substring matcher missed.
//!
//! A message-based fallback is kept for the legacy [`KafkaError::Protocol`]
//! form so errors constructed by older code paths (and tests) still classify.

use std::io;

use crate::error::KafkaError;

/// Windows Winsock / Win32 codes that mean the connection is gone but that
/// `std` maps to `ErrorKind::Uncategorized` (so `kind()` alone would miss
/// them). Winsock codes are ≥ 10000 and never collide with Unix errno values,
/// so matching them unconditionally is safe on every platform.
const WINSOCK_CONNECTION_LOST: &[i32] = &[
    10052, // WSAENETRESET   — network dropped the connection on reset
    10053, // WSAECONNABORTED — aborted by the local stack (also mapped by kind)
    10054, // WSAECONNRESET  — forcibly closed by the remote host (also mapped by kind)
    10057, // WSAENOTCONN    — socket is not connected (also mapped by kind)
    10058, // WSAESHUTDOWN   — socket already shut down in that direction
    10060, // WSAETIMEDOUT   — timed out (also mapped by kind)
    10061, // WSAECONNREFUSED (also mapped by kind)
];

/// Small Win32 codes that share their numeric range with Unix errno, so they
/// are only consulted when running on Windows.
#[cfg(windows)]
const WIN32_CONNECTION_LOST: &[i32] = &[
    64,  // ERROR_NETNAME_DELETED — "The specified network name is no longer available"
    121, // ERROR_SEM_TIMEOUT      — TCP send/receive timeout surfaced as a semaphore timeout
];

/// True if an I/O failure with this kind / OS code means the broker
/// connection is lost or unusable, so the caller should reconnect and retry
/// rather than fail.
pub fn is_connection_io_kind(kind: io::ErrorKind, raw_os_error: Option<i32>) -> bool {
    use io::ErrorKind::*;
    match kind {
        ConnectionAborted | ConnectionReset | ConnectionRefused | BrokenPipe | NotConnected
        | TimedOut | UnexpectedEof | NetworkDown | NetworkUnreachable | HostUnreachable => true,
        _ => match raw_os_error {
            Some(code) => {
                #[cfg(windows)]
                if WIN32_CONNECTION_LOST.contains(&code) {
                    return true;
                }
                WINSOCK_CONNECTION_LOST.contains(&code)
            }
            None => false,
        },
    }
}

/// True if `error` is a connection-level failure that warrants reconnecting
/// and retrying the request.
pub fn is_connection_error(error: &crate::Error) -> bool {
    match error {
        crate::Error::Kafka(KafkaError::ConnectionIo {
            kind, raw_os_error, ..
        }) => is_connection_io_kind(*kind, *raw_os_error),
        crate::Error::Kafka(KafkaError::Protocol(msg)) => is_legacy_connection_message(msg),
        // A broker that refuses, resets or never completes a *new* connection
        // (it is restarting, or its address is briefly unreachable) is the
        // same class of failure as a connection it dropped: retry with
        // back-off instead of failing the partition (issue #201).
        crate::Error::Kafka(KafkaError::ConnectionFailed { .. })
        | crate::Error::Kafka(KafkaError::NoBrokersAvailable) => true,
        _ => false,
    }
}

/// Message-based fallback for the legacy `Protocol(String)` form.
///
/// Matches the client's own wording ("timed out after", "Not connected",
/// tokio's "early eof"), the English Unix `strerror` texts, and the
/// non-localized `(os error N)` suffix Rust appends for the Winsock codes.
/// Deliberately does **not** match a bare "aborted": that word appears in
/// unrelated protocol text (aborted transactions) and would misclassify a
/// decode failure as a connection loss.
fn is_legacy_connection_message(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("broken pipe")
        || m.contains("early eof")
        || m.contains("connection reset")
        || m.contains("not connected")
        || m.contains("connection abort")
        || m.contains("connection was aborted")
        || m.contains("forcibly closed")
        || m.contains("timed out")
        || WINSOCK_CONNECTION_LOST
            .iter()
            .any(|code| m.contains(&format!("(os error {code})")))
}

/// Build the error for an I/O failure during `operation` on the broker
/// connection, preserving the kind and OS code for classification.
pub(crate) fn connection_io_error(operation: &str, e: &io::Error) -> KafkaError {
    KafkaError::ConnectionIo {
        operation: operation.to_string(),
        kind: e.kind(),
        raw_os_error: e.raw_os_error(),
        message: e.to_string(),
    }
}

/// Build the error for one of the client's own request timeouts.
pub(crate) fn request_timeout_error(operation: &str, message: String) -> KafkaError {
    KafkaError::ConnectionIo {
        operation: operation.to_string(),
        kind: io::ErrorKind::TimedOut,
        raw_os_error: None,
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use io::ErrorKind;

    fn conn_io(kind: ErrorKind, raw: Option<i32>) -> Error {
        Error::Kafka(KafkaError::ConnectionIo {
            operation: "send request".into(),
            kind,
            raw_os_error: raw,
            message: "irrelevant — classification must not read this".into(),
        })
    }

    fn protocol(msg: &str) -> Error {
        Error::Kafka(KafkaError::Protocol(msg.to_string()))
    }

    #[test]
    fn structured_kinds_that_mean_connection_lost() {
        for kind in [
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionRefused,
            ErrorKind::BrokenPipe,
            ErrorKind::NotConnected,
            ErrorKind::TimedOut,
            ErrorKind::UnexpectedEof,
            ErrorKind::NetworkDown,
            ErrorKind::NetworkUnreachable,
            ErrorKind::HostUnreachable,
        ] {
            assert!(is_connection_error(&conn_io(kind, None)), "{kind:?}");
        }
    }

    #[test]
    fn structured_kinds_that_are_not_connection_errors() {
        for kind in [
            ErrorKind::InvalidData,
            ErrorKind::InvalidInput,
            ErrorKind::PermissionDenied,
            ErrorKind::NotFound,
            ErrorKind::Other,
            ErrorKind::Interrupted,
            ErrorKind::WouldBlock,
        ] {
            assert!(!is_connection_error(&conn_io(kind, None)), "{kind:?}");
        }
    }

    #[test]
    fn winsock_codes_classify_even_when_kind_is_uncategorized() {
        // WSAENETRESET / WSAESHUTDOWN are not mapped to a specific ErrorKind by
        // std, so only the raw code identifies them.
        for code in [10052, 10053, 10054, 10057, 10058, 10060, 10061] {
            assert!(
                is_connection_error(&conn_io(ErrorKind::Other, Some(code))),
                "winsock {code}"
            );
        }
        // Unrelated Winsock codes stay unclassified.
        assert!(!is_connection_error(&conn_io(
            ErrorKind::Other,
            Some(10013)
        ))); // WSAEACCES
        assert!(!is_connection_error(&conn_io(
            ErrorKind::Other,
            Some(10048)
        ))); // WSAEADDRINUSE
    }

    #[cfg(windows)]
    #[test]
    fn win32_small_codes_classify_on_windows_only() {
        assert!(is_connection_error(&conn_io(ErrorKind::Other, Some(64))));
        assert!(is_connection_error(&conn_io(ErrorKind::Other, Some(121))));
    }

    #[cfg(not(windows))]
    #[test]
    fn win32_small_codes_are_ignored_off_windows() {
        // 64 is ENONET on Linux / EHOSTDOWN on macOS; without a mapped kind we
        // do not guess.
        assert!(!is_connection_error(&conn_io(ErrorKind::Other, Some(64))));
        assert!(!is_connection_error(&conn_io(ErrorKind::Other, Some(121))));
    }

    #[test]
    fn real_io_errors_round_trip_through_constructor() {
        let e = io::Error::new(ErrorKind::ConnectionAborted, "boom");
        let err = Error::Kafka(connection_io_error("send request", &e));
        assert!(is_connection_error(&err));
        let text = err.to_string();
        assert!(text.contains("send request"), "{text}");
        assert!(text.contains("ConnectionAborted"), "{text}");
        assert!(text.contains("boom"), "{text}");

        // A raw OS error keeps its code. ECONNRESET differs per platform, so
        // just check the plumbing.
        let e = io::Error::from_raw_os_error(10054);
        let KafkaError::ConnectionIo { raw_os_error, .. } = connection_io_error("read", &e) else {
            panic!("expected ConnectionIo");
        };
        assert_eq!(raw_os_error, Some(10054));
    }

    #[test]
    fn request_timeouts_are_connection_errors() {
        let err = Error::Kafka(request_timeout_error(
            "read response length",
            "Request timed out after 60s waiting for broker response".into(),
        ));
        assert!(is_connection_error(&err));
    }

    /// The exact texts `format!("Failed to …: {}", io_err)` produced before
    /// the structured variant existed, on each platform. All must classify
    /// via the legacy fallback — including the Windows texts that motivated
    /// issue #146 and the localized (German) variant, which only the
    /// `(os error N)` suffix identifies.
    #[test]
    fn legacy_protocol_messages_still_classify() {
        let positives = [
            // Windows (English UI)
            "Failed to send request: An established connection was aborted by the software in your host machine. (os error 10053)",
            "Failed to read response length: An existing connection was forcibly closed by the remote host. (os error 10054)",
            "Failed to read response body: A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond. (os error 10060)",
            "Failed to send request: A request to send or receive data was disallowed because the socket is not connected and (when sending on a datagram socket using a sendto call) no address was supplied. (os error 10057)",
            "Failed to send request: A request to send or receive data was disallowed because the socket had already been shut down in that direction with a previous shutdown call. (os error 10058)",
            // Windows (German UI) — only the numeric suffix is stable
            "Failed to send request: Eine bestehende Verbindung wurde softwaregesteuert durch den Hostcomputer abgebrochen. (os error 10053)",
            // macOS
            "Failed to read response length: Connection reset by peer (os error 54)",
            "Failed to send request: Software caused connection abort (os error 53)",
            "Failed to send request: Broken pipe (os error 32)",
            "Failed to send request: Socket is not connected (os error 57)",
            "Failed to read response length: Operation timed out (os error 60)",
            // Linux
            "Failed to read response length: Connection reset by peer (os error 104)",
            "Failed to read response length: Connection timed out (os error 110)",
            "Failed to send request: Transport endpoint is not connected (os error 107)",
            // tokio / client-internal
            "Failed to read response length: early eof",
            "Request timed out after 60s waiting for broker response",
            "Response body read timed out after 60s",
            "Not connected",
        ];
        for msg in positives {
            assert!(
                is_connection_error(&protocol(msg)),
                "should classify: {msg}"
            );
        }

        let negatives = [
            "Failed to decode response: DecodeError",
            "Failed to encode request: EncodeError",
            // Contains "aborted" but is not a connection error — a bare
            // "aborted" substring match would get this wrong.
            "Failed to decode response: aborted transaction marker missing",
            "Unsupported API version",
        ];
        for msg in negatives {
            assert!(
                !is_connection_error(&protocol(msg)),
                "must not classify: {msg}"
            );
        }
    }

    #[test]
    fn other_error_variants_are_never_connection_errors() {
        assert!(!is_connection_error(&Error::Kafka(
            KafkaError::BrokerError {
                code: 6,
                message: "not leader".into(),
            }
        )));
        assert!(!is_connection_error(&Error::Compression("zstd".into())));
    }

    /// A refused / timed-out *new* connection is retried like a dropped one
    /// (issue #201): a broker that is restarting refuses connections for a
    /// few seconds, and a hard-killed one has no listener at all.
    #[test]
    fn refused_or_failed_connect_is_a_connection_error() {
        assert!(is_connection_error(&Error::Kafka(
            KafkaError::ConnectionFailed {
                broker: "b:9092".into(),
                message: "Connection refused (os error 61)".into(),
            }
        )));
        assert!(is_connection_error(&Error::Kafka(
            KafkaError::NoBrokersAvailable
        )));
    }
}
