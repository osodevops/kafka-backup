//! A stateful fake Confluent Schema Registry served by wiremock.
//!
//! It models the behaviour the restore depends on, as observed on
//! cp-schema-registry 8.3.1:
//! - IMPORT mode requires an explicit ID; READWRITE refuses one.
//! - IMPORT with an explicit version silently overwrites an existing version.
//! - An ID already held by a different schema (soft-deleted included) is
//!   refused with 42205 "Overwrite new schema with id … is not permitted".
//! - Mode precedence: subject, then context (`:.ctx:`), then — default
//!   context only — global. Global IMPORT does not reach other contexts.
//! - Switching to IMPORT where live subjects exist needs `force=true`.
//! - READONLY refuses registration, deletes and config changes.
//! - References must resolve to live versions at registration time.
//! - READWRITE registration of a schema already in the subject returns the
//!   existing version; otherwise the ID is reused for an identical schema in
//!   the context or is max ID + 1.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

pub const DEFAULT_CTX: &str = ".";

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub schema: String,
    pub schema_type: String,
    /// `(name, resolved subject, version)`.
    pub references: Vec<(String, String, i32)>,
    pub metadata: Option<Value>,
    pub rule_set: Option<Value>,
}

impl Schema {
    pub fn avro(schema: &str) -> Self {
        Schema {
            schema: schema.to_string(),
            schema_type: "AVRO".to_string(),
            references: Vec::new(),
            metadata: None,
            rule_set: None,
        }
    }

    pub fn typed(schema: &str, schema_type: &str) -> Self {
        Schema {
            schema_type: schema_type.to_string(),
            ..Self::avro(schema)
        }
    }

    pub fn with_ref(mut self, name: &str, subject: &str, version: i32) -> Self {
        self.references
            .push((name.to_string(), subject.to_string(), version));
        self
    }

    /// References resolved in `ctx` (references are stored as sent).
    fn canonical_refs(&self, ctx: &str) -> Vec<(String, String, i32)> {
        let anchor = if ctx == DEFAULT_CTX {
            "x".to_string()
        } else {
            format!(":{ctx}:x")
        };
        self.references
            .iter()
            .map(|(n, s, v)| (n.clone(), resolve(&anchor, s), *v))
            .collect()
    }

    /// Same schema in context `ctx`: text, type and resolved references.
    fn same(&self, other: &Schema, ctx: &str) -> bool {
        self.schema == other.schema
            && self.schema_type == other.schema_type
            && self.canonical_refs(ctx) == other.canonical_refs(ctx)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub id: i32,
    pub deleted: bool,
}

#[derive(Debug)]
pub struct FailRule {
    pub method: String,
    /// Substring of "METHOD decoded-path?query".
    pub contains: String,
    pub status: u16,
    pub error_code: i32,
    pub message: String,
    pub remaining: usize,
}

#[derive(Debug)]
pub struct State {
    pub global_mode: String,
    pub global_compat: String,
    /// Subject or context subject (`:.ctx:`) -> mode override.
    pub modes: BTreeMap<String, String>,
    pub configs: BTreeMap<String, String>,
    /// Qualified subject -> version -> entry.
    pub subjects: BTreeMap<String, BTreeMap<i32, Entry>>,
    /// (context, id) -> schema.
    pub schemas: BTreeMap<(String, i32), Schema>,
    pub fail: Vec<FailRule>,
    /// Schema version limit (soft-deleted included), like Confluent Cloud.
    pub max_versions: Option<usize>,
    /// `false`: register responses carry only the ID (Schema Registry 7.x).
    pub register_returns_version: bool,
    /// `false`: GET /mode answers 404 (very old registries).
    pub global_mode_available: bool,
    /// Drop `ruleSet` on registration (a registry without the rule-set
    /// extension, e.g. 7.6.0 or a community 8.x without it configured).
    pub drops_rule_set: bool,
    /// `false`: mode overrides cannot be deleted (Confluent Platform 6.2):
    /// `OPTIONS /mode/{subject}` lists no DELETE and DELETE answers 405.
    pub mode_delete_supported: bool,
    /// Every request: "METHOD /decoded/path?query".
    pub log: Vec<String>,
}

impl Default for State {
    fn default() -> Self {
        State {
            global_mode: "READWRITE".to_string(),
            global_compat: "BACKWARD".to_string(),
            modes: BTreeMap::new(),
            configs: BTreeMap::new(),
            subjects: BTreeMap::new(),
            schemas: BTreeMap::new(),
            fail: Vec::new(),
            max_versions: None,
            register_returns_version: true,
            global_mode_available: true,
            drops_rule_set: false,
            mode_delete_supported: true,
            log: Vec::new(),
        }
    }
}

/// Runs once after a matching request was handled.
pub struct Hook {
    pub method: String,
    /// Substring of "METHOD decoded-path?query".
    pub contains: String,
    pub remaining: usize,
    pub action: Box<dyn FnMut(&mut State) + Send>,
}

#[derive(Clone, Default)]
pub struct FakeSchemaRegistry {
    pub state: Arc<Mutex<State>>,
    pub hooks: Arc<Mutex<Vec<Hook>>>,
}

// ---------------------------------------------------------------------------
// Context helpers (mirror the production rules, kept independent on purpose)
// ---------------------------------------------------------------------------

pub fn split(subject: &str) -> (String, String) {
    if let Some(rest) = subject.strip_prefix(":.") {
        if let Some(end) = rest.find(':') {
            return (subject[1..end + 2].to_string(), rest[end + 1..].to_string());
        }
    }
    (DEFAULT_CTX.to_string(), subject.to_string())
}

pub fn ctx_of(subject: &str) -> String {
    split(subject).0
}

pub fn normalize(subject: &str) -> String {
    let (ctx, name) = split(subject);
    if ctx == DEFAULT_CTX {
        name
    } else {
        format!(":{ctx}:{name}")
    }
}

fn resolve(referrer: &str, reference: &str) -> String {
    if reference.starts_with(":.") {
        normalize(reference)
    } else {
        let ctx = ctx_of(referrer);
        if ctx == DEFAULT_CTX {
            reference.to_string()
        } else {
            format!(":{ctx}:{reference}")
        }
    }
}

fn error(status: u16, code: i32, message: impl Into<String>) -> ResponseTemplate {
    ResponseTemplate::new(status)
        .set_body_json(json!({"error_code": code, "message": message.into()}))
}

fn ok(body: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(body)
}

impl State {
    fn live_subjects(&self) -> Vec<String> {
        self.subjects
            .iter()
            .filter(|(_, v)| v.values().any(|e| !e.deleted))
            .map(|(s, _)| s.clone())
            .collect()
    }

    fn version_count(&self) -> usize {
        self.subjects.values().map(|v| v.len()).sum()
    }

    pub fn effective_mode(&self, subject: &str) -> String {
        if let Some(m) = self.modes.get(subject) {
            return m.clone();
        }
        let ctx = ctx_of(subject);
        if ctx == DEFAULT_CTX {
            return self.global_mode.clone();
        }
        self.modes
            .get(&format!(":{ctx}:"))
            .cloned()
            .unwrap_or_else(|| "READWRITE".to_string())
    }

    fn version_json(&self, subject: &str, version: i32, entry: &Entry) -> Value {
        let schema = &self.schemas[&(ctx_of(subject), entry.id)];
        let mut v = json!({
            "subject": subject,
            "version": version,
            "id": entry.id,
            "schemaType": schema.schema_type,
            "schema": schema.schema,
            "references": schema.references.iter().map(|(n, s, v)| json!({"name": n, "subject": s, "version": v})).collect::<Vec<_>>(),
            "deleted": entry.deleted,
        });
        if let Some(m) = &schema.metadata {
            v["metadata"] = m.clone();
        }
        if let Some(r) = &schema.rule_set {
            v["ruleSet"] = r.clone();
        }
        v
    }

    fn parse_schema(&self, _subject: &str, body: &Value) -> Schema {
        Schema {
            schema: body["schema"].as_str().unwrap_or_default().to_string(),
            schema_type: body["schemaType"].as_str().unwrap_or("AVRO").to_string(),
            references: body["references"]
                .as_array()
                .map(|refs| {
                    refs.iter()
                        .map(|r| {
                            (
                                r["name"].as_str().unwrap_or_default().to_string(),
                                // Stored as sent, like the real registry.
                                r["subject"].as_str().unwrap_or_default().to_string(),
                                r["version"].as_i64().unwrap_or_default() as i32,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            metadata: body.get("metadata").filter(|v| !v.is_null()).cloned(),
            rule_set: body.get("ruleSet").filter(|v| !v.is_null()).cloned(),
        }
    }

    fn register(&mut self, subject: &str, body: &Value) -> ResponseTemplate {
        let mode = self.effective_mode(subject);
        let ctx = ctx_of(subject);
        if mode.starts_with("READONLY") {
            return error(
                422,
                42205,
                format!("Subject {subject} in context  is in read-only mode"),
            );
        }
        let mut schema = self.parse_schema(subject, body);
        if self.drops_rule_set {
            schema.rule_set = None;
        }
        for (_, ref_subject, ref_version) in &schema.references {
            let target = resolve(subject, ref_subject);
            let live = self
                .subjects
                .get(&target)
                .and_then(|v| v.get(ref_version))
                .is_some_and(|e| !e.deleted);
            if !live {
                return error(
                    422,
                    42201,
                    format!("Invalid schema: reference {target} v{ref_version} not found"),
                );
            }
        }
        let given_id = body["id"].as_i64().map(|v| v as i32);
        let given_version = body["version"].as_i64().map(|v| v as i32);

        let (id, version) = if mode == "IMPORT" {
            let Some(id) = given_id else {
                return error(
                    422,
                    42205,
                    format!("Subject {subject} in context  is not in read-write mode"),
                );
            };
            if let Some(existing) = self.schemas.get(&(ctx.clone(), id)) {
                if !existing.same(&schema, &ctx) {
                    return error(
                        422,
                        42205,
                        format!("Overwrite new schema with id {id} in context  is not permitted."),
                    );
                }
            }
            let versions = self.subjects.entry(subject.to_string()).or_default();
            let version =
                given_version.unwrap_or_else(|| versions.keys().max().copied().unwrap_or(0) + 1);
            (id, version)
        } else {
            if given_id.is_some() {
                return error(
                    422,
                    42205,
                    format!("Subject {subject} in context  is not in import mode"),
                );
            }
            let existing = self.subjects.get(subject).and_then(|versions| {
                versions
                    .iter()
                    .find(|(_, e)| {
                        !e.deleted && self.schemas[&(ctx.clone(), e.id)].same(&schema, &ctx)
                    })
                    .map(|(v, e)| (e.id, *v))
            });
            if let Some((id, version)) = existing {
                return ok(json!({"id": id, "version": version}));
            }
            let id = self
                .schemas
                .iter()
                .find(|((c, _), s)| *c == ctx && s.same(&schema, &ctx))
                .map(|((_, id), _)| *id)
                .unwrap_or_else(|| {
                    self.schemas
                        .keys()
                        .filter(|(c, _)| *c == ctx)
                        .map(|(_, id)| *id)
                        .max()
                        .unwrap_or(0)
                        + 1
                });
            let version = self
                .subjects
                .get(subject)
                .and_then(|v| v.keys().max().copied())
                .unwrap_or(0)
                + 1;
            (id, version)
        };

        let creates = !self
            .subjects
            .get(subject)
            .is_some_and(|v| v.contains_key(&version));
        if creates {
            if let Some(max) = self.max_versions {
                if self.version_count() >= max {
                    return error(422, 403, "Schema Limit Exceeded");
                }
            }
        }
        self.schemas
            .entry((ctx, id))
            .or_insert_with(|| schema.clone());
        self.subjects
            .entry(subject.to_string())
            .or_default()
            .insert(version, Entry { id, deleted: false });
        if self.register_returns_version {
            ok(json!({"id": id, "version": version}))
        } else {
            ok(json!({"id": id}))
        }
    }

    fn set_mode(&mut self, scope: Option<&str>, body: &Value, force: bool) -> ResponseTemplate {
        let mode = body["mode"].as_str().unwrap_or_default().to_string();
        if mode == "IMPORT" && !force {
            let live = self.live_subjects();
            let blocked = match scope {
                None => !live.is_empty(),
                Some(s) if s.ends_with(':') && s.starts_with(":.") => {
                    let ctx = ctx_of(&format!("{s}x"));
                    live.iter().any(|l| ctx_of(l) == ctx)
                }
                Some(s) => live.iter().any(|l| l == s),
            };
            if blocked {
                return error(422, 42205, "Cannot import since found existing subjects");
            }
        }
        match scope {
            None => self.global_mode = mode.clone(),
            Some(s) => {
                self.modes.insert(s.to_string(), mode.clone());
            }
        }
        ok(json!({"mode": mode}))
    }
}

impl Respond for FakeSchemaRegistry {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let response = self.handle(request);
        // Hooks run after the request was applied, before the client sees
        // the response: "someone else" acting at a known point.
        let mut st = self.state.lock().unwrap();
        let line = st.log.last().cloned().unwrap_or_default();
        let mut hooks = self.hooks.lock().unwrap();
        for hook in hooks.iter_mut() {
            if hook.remaining > 0 && line.starts_with(&hook.method) && line.contains(&hook.contains)
            {
                hook.remaining -= 1;
                (hook.action)(&mut st);
            }
        }
        response
    }
}

impl FakeSchemaRegistry {
    fn handle(&self, request: &Request) -> ResponseTemplate {
        let mut st = self.state.lock().unwrap();
        let method = request.method.as_str().to_string();
        let segments: Vec<String> = request
            .url
            .path()
            .trim_start_matches('/')
            .split('/')
            .map(|s| urlencoding::decode(s).unwrap().into_owned())
            .collect();
        let query: Vec<(String, String)> = request
            .url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let q = |name: &str| {
            query
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        let flag = |name: &str| q(name).as_deref() == Some("true");
        let line = format!(
            "{method} /{}{}",
            segments.join("/"),
            request
                .url
                .query()
                .map(|q| format!("?{}", urlencoding::decode(q).unwrap()))
                .unwrap_or_default()
        );
        st.log.push(line.clone());

        if let Some(rule) = st
            .fail
            .iter_mut()
            .find(|r| r.remaining > 0 && r.method == method && line.contains(&r.contains))
        {
            rule.remaining -= 1;
            return error(rule.status, rule.error_code, rule.message.clone());
        }

        let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
        let seg: Vec<&str> = segments.iter().map(String::as_str).collect();
        match (method.as_str(), seg.as_slice()) {
            ("GET", ["subjects"]) => {
                let list: Vec<String> = if flag("deleted") {
                    st.subjects.keys().cloned().collect()
                } else {
                    st.live_subjects()
                };
                ok(json!(list))
            }
            ("GET", ["contexts"]) => {
                let mut ctxs: BTreeSet<String> = st.subjects.keys().map(|s| ctx_of(s)).collect();
                ctxs.insert(DEFAULT_CTX.to_string());
                ok(json!(ctxs))
            }
            ("GET", ["subjects", s, "versions"]) => {
                let s = normalize(s);
                let deleted = flag("deleted");
                let versions: Vec<i32> = st
                    .subjects
                    .get(&s)
                    .map(|v| {
                        v.iter()
                            .filter(|(_, e)| deleted || !e.deleted)
                            .map(|(v, _)| *v)
                            .collect()
                    })
                    .unwrap_or_default();
                if versions.is_empty() {
                    error(404, 40401, format!("Subject '{s}' not found."))
                } else {
                    ok(json!(versions))
                }
            }
            ("GET", ["subjects", s, "versions", v]) => {
                let s = normalize(s);
                let deleted = flag("deleted");
                let version: i32 = match *v {
                    "latest" => st
                        .subjects
                        .get(&s)
                        .and_then(|vs| vs.iter().filter(|(_, e)| !e.deleted).map(|(v, _)| *v).max())
                        .unwrap_or(-1),
                    other => other.parse().unwrap_or(-1),
                };
                match st.subjects.get(&s).and_then(|vs| vs.get(&version)) {
                    Some(e) if deleted || !e.deleted => {
                        let e = e.clone();
                        ok(st.version_json(&s, version, &e))
                    }
                    _ if !st.subjects.contains_key(&s) => {
                        error(404, 40401, format!("Subject '{s}' not found."))
                    }
                    _ => error(404, 40402, format!("Version {version} not found.")),
                }
            }
            ("POST", ["subjects", s, "versions"]) => {
                let s = normalize(s);
                st.register(&s, &body)
            }
            ("POST", ["subjects", s]) => {
                let s = normalize(s);
                let schema = st.parse_schema(&s, &body);
                let ctx = ctx_of(&s);
                let found = st.subjects.get(&s).and_then(|vs| {
                    vs.iter()
                        .find(|(_, e)| {
                            !e.deleted && st.schemas[&(ctx.clone(), e.id)].same(&schema, &ctx)
                        })
                        .map(|(v, e)| (*v, e.clone()))
                });
                match found {
                    Some((v, e)) => ok(st.version_json(&s, v, &e)),
                    None => error(404, 40403, "Schema not found"),
                }
            }
            ("DELETE", ["subjects", s, "versions", v]) => {
                let s = normalize(s);
                let version: i32 = v.parse().unwrap_or(-1);
                if st.effective_mode(&s).starts_with("READONLY") {
                    return error(
                        422,
                        42205,
                        format!("Subject {s} in context  is in read-only mode"),
                    );
                }
                let permanent = flag("permanent");
                let Some(entry) = st.subjects.get_mut(&s).and_then(|vs| vs.get_mut(&version))
                else {
                    return error(404, 40402, format!("Version {version} not found."));
                };
                if permanent {
                    if !entry.deleted {
                        return error(404, 40407, "Version not soft-deleted first");
                    }
                    st.subjects.get_mut(&s).unwrap().remove(&version);
                } else {
                    if entry.deleted {
                        return error(404, 40406, "Version was soft deleted");
                    }
                    entry.deleted = true;
                }
                ok(json!(version))
            }
            ("DELETE", ["subjects", s]) => {
                let s = normalize(s);
                let permanent = flag("permanent");
                let Some(vs) = st.subjects.get_mut(&s) else {
                    return error(404, 40401, format!("Subject '{s}' not found."));
                };
                let versions: Vec<i32> = vs.keys().copied().collect();
                if permanent {
                    st.subjects.remove(&s);
                } else {
                    vs.values_mut().for_each(|e| e.deleted = true);
                }
                ok(json!(versions))
            }
            ("GET", ["schemas"]) => {
                let deleted = flag("deleted");
                let all: Vec<Value> = st
                    .subjects
                    .iter()
                    .flat_map(|(s, vs)| {
                        vs.iter()
                            .filter(|(_, e)| deleted || !e.deleted)
                            .map(|(v, e)| st.version_json(s, *v, e))
                            .collect::<Vec<_>>()
                    })
                    .collect();
                ok(json!(all))
            }
            ("GET", ["schemas", "ids", id]) => {
                let ctx = q("subject")
                    .map(|s| ctx_of(&format!("{s}x")))
                    .unwrap_or_else(|| DEFAULT_CTX.to_string());
                let id: i32 = id.parse().unwrap_or(-1);
                match st.schemas.get(&(ctx, id)) {
                    Some(schema) => ok(json!({
                        "schema": schema.schema,
                        "schemaType": schema.schema_type,
                        "references": schema.references.iter().map(|(n, s, v)| json!({"name": n, "subject": s, "version": v})).collect::<Vec<_>>(),
                    })),
                    None => error(404, 40403, "Schema not found"),
                }
            }
            ("GET", ["mode"]) => {
                if st.global_mode_available {
                    ok(json!({"mode": st.global_mode}))
                } else {
                    error(404, 404, "HTTP 404 Not Found")
                }
            }
            ("PUT", ["mode"]) => {
                let force = flag("force");
                st.set_mode(None, &body, force)
            }
            ("GET", ["mode", s]) => match st.modes.get(*s) {
                Some(m) => ok(json!({"mode": m})),
                None => error(
                    404,
                    40409,
                    format!("Subject '{s}' does not have subject-level mode configured"),
                ),
            },
            ("PUT", ["mode", s]) => {
                let force = flag("force");
                let s = s.to_string();
                st.set_mode(Some(&s), &body, force)
            }
            ("OPTIONS", ["mode", _]) => ResponseTemplate::new(200).insert_header(
                "Allow",
                if st.mode_delete_supported {
                    "DELETE,GET,OPTIONS,PUT"
                } else {
                    "GET,OPTIONS,PUT"
                },
            ),
            ("DELETE", ["mode", _]) if !st.mode_delete_supported => {
                error(405, 405, "HTTP 405 Method Not Allowed")
            }
            ("DELETE", ["mode", s]) => match st.modes.remove(*s) {
                Some(m) => ok(json!({"mode": m})),
                None => error(
                    404,
                    40409,
                    format!("Subject '{s}' does not have subject-level mode configured"),
                ),
            },
            ("GET", ["config"]) => ok(json!({"compatibilityLevel": st.global_compat})),
            ("PUT", ["config"]) => {
                st.global_compat = body["compatibility"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                ok(json!({"compatibility": st.global_compat}))
            }
            ("GET", ["config", s]) => match st.configs.get(*s) {
                Some(c) => ok(json!({"compatibilityLevel": c})),
                None => error(
                    404,
                    40408,
                    format!("Subject '{s}' does not have subject-level compatibility configured"),
                ),
            },
            ("PUT", ["config", s]) => {
                if !s.ends_with(':') && st.effective_mode(s).starts_with("READONLY") {
                    return error(
                        422,
                        42205,
                        format!("Subject {s} in context  is in read-only mode"),
                    );
                }
                let c = body["compatibility"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                st.configs.insert(s.to_string(), c.clone());
                ok(json!({"compatibility": c}))
            }
            ("DELETE", ["config", s]) => match st.configs.remove(*s) {
                Some(c) => ok(json!({"compatibilityLevel": c})),
                None => error(404, 40408, "not configured"),
            },
            _ => error(404, 404, format!("no route for {line}")),
        }
    }
}

impl FakeSchemaRegistry {
    pub async fn start() -> (MockServer, FakeSchemaRegistry) {
        let server = MockServer::start().await;
        let fake = FakeSchemaRegistry::default();
        Mock::given(any())
            .respond_with(fake.clone())
            .mount(&server)
            .await;
        (server, fake)
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        f(&mut self.state.lock().unwrap())
    }

    /// Seed a version directly (no mode checks).
    pub fn seed(&self, subject: &str, version: i32, id: i32, schema: Schema) {
        self.with(|st| {
            let subject = normalize(subject);
            st.schemas.entry((ctx_of(&subject), id)).or_insert(schema);
            st.subjects
                .entry(subject)
                .or_default()
                .insert(version, Entry { id, deleted: false });
        });
    }

    pub fn soft_delete(&self, subject: &str, version: i32) {
        self.with(|st| {
            st.subjects
                .get_mut(&normalize(subject))
                .unwrap()
                .get_mut(&version)
                .unwrap()
                .deleted = true;
        });
    }

    pub fn set_mode(&self, scope: &str, mode: &str) {
        self.with(|st| st.modes.insert(scope.to_string(), mode.to_string()));
    }

    pub fn set_config(&self, scope: &str, compat: &str) {
        self.with(|st| st.configs.insert(scope.to_string(), compat.to_string()));
    }

    pub fn fail_once(&self, method: &str, contains: &str, status: u16, error_code: i32) {
        self.with(|st| {
            st.fail.push(FailRule {
                method: method.to_string(),
                contains: contains.to_string(),
                status,
                error_code,
                message: format!("injected failure for {contains}"),
                remaining: 1,
            })
        });
    }

    /// Run `action` on the registry state right after the next request
    /// whose line starts with `method` and contains `contains`.
    pub fn after_once(
        &self,
        method: &str,
        contains: &str,
        action: impl FnMut(&mut State) + Send + 'static,
    ) {
        self.hooks.lock().unwrap().push(Hook {
            method: method.to_string(),
            contains: contains.to_string(),
            remaining: 1,
            action: Box::new(action),
        });
    }

    pub fn log(&self) -> Vec<String> {
        self.with(|st| st.log.clone())
    }

    pub fn clear_log(&self) {
        self.with(|st| st.log.clear());
    }

    /// Requests that change state.
    pub fn mutations(&self) -> Vec<String> {
        self.log()
            .into_iter()
            .filter(|l| {
                (l.starts_with("POST") && l.contains("/versions"))
                    || l.starts_with("PUT")
                    || l.starts_with("DELETE")
            })
            .collect()
    }

    /// subject -> version -> (id, deleted, schema text)
    pub fn snapshot(&self) -> BTreeMap<String, BTreeMap<i32, (i32, bool, String)>> {
        self.with(|st| {
            st.subjects
                .iter()
                .map(|(s, vs)| {
                    let versions = vs
                        .iter()
                        .map(|(v, e)| {
                            let schema = &st.schemas[&(ctx_of(s), e.id)];
                            (*v, (e.id, e.deleted, schema.schema.clone()))
                        })
                        .collect();
                    (s.clone(), versions)
                })
                .collect()
        })
    }

    pub fn schema_of(&self, subject: &str, version: i32) -> Schema {
        self.with(|st| {
            let s = normalize(subject);
            let e = &st.subjects[&s][&version];
            st.schemas[&(ctx_of(&s), e.id)].clone()
        })
    }

    pub fn modes(&self) -> BTreeMap<String, String> {
        self.with(|st| st.modes.clone())
    }

    pub fn configs(&self) -> BTreeMap<String, String> {
        self.with(|st| st.configs.clone())
    }

    pub fn global_mode(&self) -> String {
        self.with(|st| st.global_mode.clone())
    }

    pub fn global_compat(&self) -> String {
        self.with(|st| st.global_compat.clone())
    }
}
