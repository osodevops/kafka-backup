//! Schema Registry contexts.
//!
//! A subject in a non-default context is written in its qualified form
//! `:.ctx-a:orders-value` (`:` + context name + `:` + subject). The default
//! context is `.`, so `:.:orders-value` and `orders-value` are the same
//! subject. Each context has its own schema ID space and its own
//! context-level mode and compatibility (`/mode/:.ctx-a:`).
//!
//! A schema reference whose subject is unqualified resolves in the context
//! of the schema that holds it.

/// The default context.
pub const DEFAULT_CONTEXT: &str = ".";

/// Split a subject into `(context, unqualified subject)`. Unqualified
/// subjects are in [`DEFAULT_CONTEXT`].
pub fn split(subject: &str) -> (&str, &str) {
    if let Some(rest) = subject.strip_prefix(":.") {
        if let Some(end) = rest.find(':') {
            // `rest` starts after ":."; the context name keeps its leading dot.
            let context = &subject[1..end + 2];
            let name = &rest[end + 1..];
            return (context, name);
        }
    }
    (DEFAULT_CONTEXT, subject)
}

/// The context a subject belongs to (`.` for the default context).
pub fn context_of(subject: &str) -> &str {
    split(subject).0
}

/// The canonical form of a subject: unqualified in the default context,
/// `:.ctx:name` otherwise.
pub fn normalize(subject: &str) -> String {
    let (context, name) = split(subject);
    qualify(context, name)
}

/// Build the subject `name` in `context`.
pub fn qualify(context: &str, name: &str) -> String {
    if context == DEFAULT_CONTEXT {
        name.to_string()
    } else {
        format!(":{context}:{name}")
    }
}

/// The subject a reference resolves to: a qualified reference subject is used
/// as is, an unqualified one resolves in the context of `referrer`.
pub fn resolve_reference(referrer: &str, reference_subject: &str) -> String {
    if reference_subject.starts_with(":.") {
        normalize(reference_subject)
    } else {
        qualify(context_of(referrer), reference_subject)
    }
}

/// The "subject" that addresses a context-level resource (`:.ctx-a:`), as
/// used by `/mode/{subject}`, `/config/{subject}` and the `subject` query
/// parameter of `/schemas/ids/{id}`. `None` for the default context.
pub fn context_subject(context: &str) -> Option<String> {
    (context != DEFAULT_CONTEXT).then(|| format!(":{context}:"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_qualified_and_plain_subjects() {
        assert_eq!(split(":.ctx-a:orders-value"), (".ctx-a", "orders-value"));
        assert_eq!(split("orders-value"), (".", "orders-value"));
        assert_eq!(split(":.:orders-value"), (".", "orders-value"));
        // A subject may itself contain colons.
        assert_eq!(split(":.ctx:a:b"), (".ctx", "a:b"));
        // Not a context prefix.
        assert_eq!(split(":orders"), (".", ":orders"));
    }

    #[test]
    fn normalizes_the_default_context() {
        assert_eq!(normalize(":.:orders-value"), "orders-value");
        assert_eq!(normalize(":.ctx-a:orders-value"), ":.ctx-a:orders-value");
        assert_eq!(normalize("orders-value"), "orders-value");
    }

    #[test]
    fn unqualified_references_resolve_in_the_referrers_context() {
        assert_eq!(
            resolve_reference(":.ctx-a:orders-value", "address-value"),
            ":.ctx-a:address-value"
        );
        assert_eq!(
            resolve_reference("orders-value", "address-value"),
            "address-value"
        );
        assert_eq!(
            resolve_reference(":.ctx-a:orders-value", ":.:address-value"),
            "address-value"
        );
        assert_eq!(
            resolve_reference("orders-value", ":.ctx-b:address-value"),
            ":.ctx-b:address-value"
        );
    }

    #[test]
    fn context_subjects() {
        assert_eq!(context_subject(".ctx-a").as_deref(), Some(":.ctx-a:"));
        assert_eq!(context_subject("."), None);
    }
}
