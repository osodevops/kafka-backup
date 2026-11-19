//! Registration order for schema versions.
//!
//! A schema can only be registered once every version it references exists,
//! so versions are ordered topologically on their references. The order is
//! computed per (subject, version), not per subject: subject-level ordering
//! can form a cycle (`a` v2 references `b` v1 while `b` v2 references `a` v1)
//! even though the versions themselves never do. Ties are broken by the
//! original schema ID, which approximates the source's registration order,
//! then by subject and version, so the order is deterministic.

use std::collections::{BTreeSet, HashMap};

use super::error::SchemaRegistryError;

use super::context;

/// One schema version to order.
#[derive(Debug, Clone)]
pub struct VersionNode {
    /// Subject (any form; normalised for matching).
    pub subject: String,
    pub version: i32,
    pub id: i32,
    /// `(subject, version)` of every reference, already resolved against the
    /// referrer's context (see [`context::resolve_reference`]).
    pub references: Vec<(String, i32)>,
}

/// Indices into `nodes` in registration order. References to versions that
/// are not in `nodes` are ignored (they must already exist in the target).
pub fn registration_order(nodes: &[VersionNode]) -> Result<Vec<usize>, SchemaRegistryError> {
    let index: HashMap<(String, i32), usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| ((context::normalize(&n.subject), n.version), i))
        .collect();

    let mut in_degree = vec![0usize; nodes.len()];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    for (i, node) in nodes.iter().enumerate() {
        let mut seen = BTreeSet::new();
        for (subject, version) in &node.references {
            let key = (context::normalize(subject), *version);
            if let Some(&dep) = index.get(&key) {
                if dep != i && seen.insert(dep) {
                    dependents[dep].push(i);
                    in_degree[i] += 1;
                }
            }
        }
    }

    let key = |i: usize| {
        let n = &nodes[i];
        (n.id, context::normalize(&n.subject), n.version, i)
    };
    let mut ready: BTreeSet<(i32, String, i32, usize)> = (0..nodes.len())
        .filter(|&i| in_degree[i] == 0)
        .map(key)
        .collect();

    let mut order = Vec::with_capacity(nodes.len());
    while let Some(next) = ready.pop_first() {
        let i = next.3;
        order.push(i);
        for &d in &dependents[i] {
            in_degree[d] -= 1;
            if in_degree[d] == 0 {
                ready.insert(key(d));
            }
        }
    }

    if order.len() != nodes.len() {
        let mut stuck: Vec<String> = (0..nodes.len())
            .filter(|&i| in_degree[i] > 0)
            .map(|i| format!("{} v{}", nodes[i].subject, nodes[i].version))
            .collect();
        stuck.sort();
        return Err(SchemaRegistryError::ReferenceCycle(format!(
            "cycle among schema versions: {}",
            stuck.join(", ")
        )));
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(subject: &str, version: i32, id: i32, refs: &[(&str, i32)]) -> VersionNode {
        VersionNode {
            subject: subject.to_string(),
            version,
            id,
            references: refs.iter().map(|(s, v)| (s.to_string(), *v)).collect(),
        }
    }

    fn labels(nodes: &[VersionNode], order: &[usize]) -> Vec<String> {
        order
            .iter()
            .map(|&i| format!("{}@{}", nodes[i].subject, nodes[i].version))
            .collect()
    }

    #[test]
    fn ties_are_broken_by_original_id() {
        let nodes = vec![
            node("c", 1, 30, &[]),
            node("a", 1, 10, &[]),
            node("b", 1, 20, &[]),
        ];
        let order = registration_order(&nodes).unwrap();
        assert_eq!(labels(&nodes, &order), ["a@1", "b@1", "c@1"]);
    }

    #[test]
    fn references_come_first_even_with_a_higher_id() {
        // orders v1 (id 1) references address v1 (id 5).
        let nodes = vec![
            node("orders", 1, 1, &[("address", 1)]),
            node("address", 1, 5, &[]),
        ];
        let order = registration_order(&nodes).unwrap();
        assert_eq!(labels(&nodes, &order), ["address@1", "orders@1"]);
    }

    #[test]
    fn crossing_subject_references_do_not_form_a_cycle() {
        // Subject level: a -> b and b -> a (a cycle). Version level:
        // a@2 -> b@1 and b@2 -> a@1, which orders fine.
        let nodes = vec![
            node("a", 1, 1, &[]),
            node("b", 1, 2, &[]),
            node("a", 2, 3, &[("b", 1)]),
            node("b", 2, 4, &[("a", 1)]),
        ];
        let order = registration_order(&nodes).unwrap();
        let l = labels(&nodes, &order);
        let pos = |s: &str| l.iter().position(|x| x == s).unwrap();
        assert!(pos("b@1") < pos("a@2"));
        assert!(pos("a@1") < pos("b@2"));
        assert_eq!(l, ["a@1", "b@1", "a@2", "b@2"]);
    }

    #[test]
    fn a_referenced_version_with_a_higher_id_is_pulled_forward() {
        // IMPORT-mode sources can have IDs that do not follow registration
        // order: x@1 (id 2) references y@3 (id 9); z@1 (id 5) is independent.
        let nodes = vec![
            node("x", 1, 2, &[("y", 3)]),
            node("y", 3, 9, &[]),
            node("z", 1, 5, &[]),
        ];
        let order = registration_order(&nodes).unwrap();
        assert_eq!(labels(&nodes, &order), ["z@1", "y@3", "x@1"]);
    }

    #[test]
    fn references_outside_the_set_are_ignored() {
        let nodes = vec![node("a", 1, 1, &[("external", 4)])];
        assert_eq!(registration_order(&nodes).unwrap(), vec![0]);
    }

    #[test]
    fn context_references_match_qualified_subjects() {
        let nodes = vec![
            node(":.ctx:orders", 1, 1, &[(":.ctx:address", 1)]),
            node(":.ctx:address", 1, 2, &[]),
            // Same unqualified name in the default context: not a dependency.
            node("address", 1, 0, &[]),
        ];
        let order = registration_order(&nodes).unwrap();
        assert_eq!(
            labels(&nodes, &order),
            ["address@1", ":.ctx:address@1", ":.ctx:orders@1"]
        );
    }

    #[test]
    fn a_real_cycle_is_an_error() {
        let nodes = vec![node("a", 1, 1, &[("b", 1)]), node("b", 1, 2, &[("a", 1)])];
        let err = registration_order(&nodes).unwrap_err();
        assert!(matches!(err, SchemaRegistryError::ReferenceCycle(_)));
        assert!(err.to_string().contains("a v1"), "{err}");
    }
}
