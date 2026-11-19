//! Explicit serializer profiles; no magic-byte autodetection on raw topics.
use super::{
    snapshot::{hash, Schema},
    Provider,
};
use crate::{
    manifest::{BackupRecord, RecordHeader},
    restore::transform::RecordTransform,
};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdKind {
    Numeric,
    Global,
    Content,
    Guid,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IdKey {
    pub provider: String,
    pub namespace: String,
    pub kind: IdKind,
    pub id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Serializer {
    Raw,
    Confluent {
        #[serde(default)]
        namespace: String,
    },
    /// Confluent header version byte 1 followed by a 16-byte GUID.
    ConfluentGuidHeader {
        namespace: String,
        header: String,
    },
    /// Confluent-compatible serdes backed by Apicurio native identifiers.
    ApicurioCompatible {
        namespace: String,
        id_kind: IdKind,
    },
    Apicurio {
        namespace: String,
        id_kind: IdKind,
        width: u8,
        #[serde(default)]
        header: Option<String>,
    },
}
impl Serializer {
    pub fn id_key(&self, id: String) -> Result<IdKey> {
        let (provider, namespace, kind) = match self {
            Self::Confluent { namespace } => ("confluent", namespace, IdKind::Numeric),
            Self::ConfluentGuidHeader { namespace, .. } => ("confluent", namespace, IdKind::Guid),
            Self::ApicurioCompatible { namespace, id_kind } => ("apicurio", namespace, *id_kind),
            Self::Apicurio {
                namespace, id_kind, ..
            } => ("apicurio", namespace, *id_kind),
            Self::Raw => bail!("Raw serializers have no schema identifiers"),
        };
        Ok(IdKey {
            provider: provider.into(),
            namespace: namespace.clone(),
            kind,
            id,
        })
    }
    fn confluent(&self) -> bool {
        matches!(
            self,
            Self::Confluent { .. }
                | Self::ConfluentGuidHeader { .. }
                | Self::ApicurioCompatible { .. }
        )
    }
    fn header(&self) -> Option<&str> {
        match self {
            Self::ConfluentGuidHeader { header, .. } => Some(header),
            Self::Apicurio { header, .. } => header.as_deref(),
            _ => None,
        }
    }
    fn width(&self) -> usize {
        match self {
            Self::Confluent { .. } | Self::ApicurioCompatible { .. } => 4,
            Self::ConfluentGuidHeader { .. } => 16,
            Self::Apicurio { width, .. } => *width as usize,
            _ => 0,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtobufFrame {
    #[default]
    Indexes,
    TypeReference,
    IndexesAndTypeReference,
    Bare,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadProfile {
    pub source: Serializer,
    pub target: Serializer,
    #[serde(default)]
    pub protobuf_source: ProtobufFrame,
    #[serde(default)]
    pub protobuf_target: ProtobufFrame,
    /// Fully qualified descriptor message name, required for frame conversion.
    #[serde(default)]
    pub message: Option<String>,
    /// Apicurio v2 native Protobuf headers carry the simple message name while
    /// the record payload is bare. Names are explicit, including for keys.
    #[serde(default)]
    pub protobuf_source_type_header: Option<String>,
    #[serde(default)]
    pub protobuf_target_type_header: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicProfile {
    pub key: PayloadProfile,
    pub value: PayloadProfile,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMapping {
    pub source: IdKey,
    pub target: IdKey,
    pub schema_type: String,
    pub top_level_bytes: bool,
    pub requires_rule_executor: bool,
    #[serde(default)]
    pub blocked_reason: Option<String>,
    pub message_indexes: BTreeMap<String, Vec<u32>>,
    pub target_message_indexes: BTreeMap<String, Vec<u32>>,
}

#[derive(Debug, Clone)]
pub struct RegistryTransform {
    profiles: BTreeMap<String, TopicProfile>,
    // A numeric source schema can be used by both a prefix profile and a
    // GUID/content-ID destination profile. Ambiguity is scoped to the chosen
    // destination representation, not just the source ID.
    mappings: BTreeMap<(IdKey, IdKey), WireMapping>,
    fingerprint: String,
}
impl RegistryTransform {
    pub fn new(
        profiles: BTreeMap<String, TopicProfile>,
        mappings: Vec<WireMapping>,
    ) -> Result<Self> {
        let mut map = BTreeMap::new();
        for m in mappings {
            let mut representation = m.target.clone();
            representation.id.clear();
            if let Some(previous) = map.insert((m.source.clone(), representation), m.clone()) {
                if serde_json::to_value(&previous)? != serde_json::to_value(&m)? {
                    bail!("Ambiguous identifier mapping: {:?}", m.source);
                }
            }
        }
        for p in profiles.values() {
            if p.key.source.header().is_some() && p.key.source.header() == p.value.source.header() {
                bail!("Key/value source serializers cannot share one schema-ID header");
            }
            if p.key.target.header().is_some() && p.key.target.header() == p.value.target.header() {
                bail!("Key/value destination serializers cannot share one schema-ID header");
            }
            for p in [&p.key, &p.value] {
                for (name, serializer) in [
                    (p.protobuf_source_type_header.as_deref(), &p.source),
                    (p.protobuf_target_type_header.as_deref(), &p.target),
                ] {
                    if name.is_some_and(|n| n.is_empty() || serializer.header() == Some(n)) {
                        bail!("Protobuf type header must have a separate nonempty name");
                    }
                }
                if (p.protobuf_source_type_header.is_some()
                    && p.protobuf_source != ProtobufFrame::Bare)
                    || (p.protobuf_target_type_header.is_some()
                        && p.protobuf_target != ProtobufFrame::Bare)
                {
                    bail!("A separate Protobuf type header requires bare payload framing");
                }
                match (&p.source, &p.target) {
                    (Serializer::Raw, Serializer::Raw) => {}
                    (Serializer::Raw, _) | (_, Serializer::Raw) => {
                        bail!("Raw/schema conversion requires a supported serializer")
                    }
                    _ => {}
                }
                for s in [&p.source, &p.target] {
                    if let Serializer::ApicurioCompatible { id_kind, .. } = s {
                        if !matches!(id_kind, IdKind::Global | IdKind::Content) {
                            bail!("Apicurio compatible framing requires explicit global/content ID kind");
                        }
                    }
                    if let Serializer::Apicurio {
                        id_kind,
                        width,
                        header,
                        ..
                    } = s
                    {
                        if !matches!(id_kind, IdKind::Global | IdKind::Content)
                            || !matches!(width, 4 | 8)
                            || (header.is_some() && *width != 8)
                        {
                            bail!("Apicurio supports 4/8-byte prefixes and 8-byte native headers");
                        }
                    }
                    if s.header().is_some_and(str::is_empty) {
                        bail!("Schema header name cannot be empty");
                    }
                }
                if (matches!(p.source, Serializer::ConfluentGuidHeader { .. })
                    && p.protobuf_source != ProtobufFrame::Indexes)
                    || (matches!(p.target, Serializer::ConfluentGuidHeader { .. })
                        && p.protobuf_target != ProtobufFrame::Indexes)
                {
                    bail!("Confluent GUID headers require Protobuf message indexes in the header");
                }
                if p.protobuf_source != p.protobuf_target && p.message.is_none() {
                    bail!("Protobuf conversion requires explicit message identity");
                }
            }
        }
        let fingerprint = hash(&serde_json::to_vec(&(
            profiles.clone(),
            map.values().collect::<Vec<_>>(),
        ))?);
        Ok(Self {
            profiles,
            mappings: map,
            fingerprint,
        })
    }
    pub fn check_record(&self, topic: &str, record: &BackupRecord) -> Result<()> {
        let mut copy = record.clone();
        self.apply(topic, &mut copy)
    }
    fn apply(&self, topic: &str, record: &mut BackupRecord) -> Result<()> {
        let p = self.profiles.get(topic).context(format!(
            "Missing explicit key/value serializer profiles for topic {topic}"
        ))?;
        self.payload(&p.key, &mut record.key, &mut record.headers)?;
        self.payload(&p.value, &mut record.value, &mut record.headers)?;
        Ok(())
    }
    fn payload(
        &self,
        p: &PayloadProfile,
        bytes: &mut Option<Vec<u8>>,
        headers: &mut Vec<RecordHeader>,
    ) -> Result<()> {
        let Some(data) = bytes.as_ref() else {
            return Ok(());
        };
        if matches!(p.source, Serializer::Raw) {
            return Ok(());
        }
        let (id, mut payload, header_indexes) = read_frame(&p.source, data, headers)?;
        let key = p.source.id_key(id)?;
        let representation = p.target.id_key(String::new())?;
        let m = self
            .mappings
            .get(&(key.clone(), representation))
            .context(format!("Unmapped registry identifier {key:?}"))?;
        if m.requires_rule_executor {
            bail!("Kafka replay with data contract rules requires a verified rule executor/key restore integration; registry-only exact reuse remains available");
        }
        if let Some(reason) = &m.blocked_reason {
            bail!("{reason}");
        }
        if !matches!(m.schema_type.as_str(), "AVRO" | "JSON" | "PROTOBUF") {
            bail!(
                "Schema type {} has no supported Kafka serializer conversion",
                m.schema_type
            );
        }
        if m.schema_type == "JSON" {
            serde_json::from_slice::<serde_json::Value>(&payload)
                .context("Malformed configured JSON payload")?;
        }
        if matches!(p.source, Serializer::ConfluentGuidHeader { .. }) {
            if m.schema_type == "PROTOBUF" {
                let mut cursor = 0;
                read_indexes(&header_indexes, &mut cursor)?;
                if cursor != header_indexes.len() {
                    bail!("Unexpected trailing bytes in Protobuf GUID header");
                }
                payload.splice(0..0, header_indexes);
            } else if !header_indexes.is_empty() {
                bail!("Unexpected indexes in non-Protobuf GUID header");
            }
        }
        let expected = p.target.id_key(m.target.id.clone())?;
        if expected != m.target {
            bail!("Destination serializer namespace/ID kind does not match mapping for {key:?}");
        }
        if m.schema_type == "AVRO"
            && m.top_level_bytes
            && p.source.confluent() != p.target.confluent()
        {
            if p.source.confluent() {
                let mut out = varint((payload.len() as u64) << 1);
                out.extend(payload);
                payload = out;
            } else {
                let mut cursor = 0;
                let length = read_varint(&payload, &mut cursor)?;
                if length & 1 != 0 || length >> 1 != ((payload.len() - cursor) as u64) {
                    bail!("Malformed Avro top-level bytes encoding");
                }
                payload = payload[cursor..].to_vec();
            }
        }
        if m.schema_type == "PROTOBUF" {
            for (serializer, frame, type_header) in [
                (&p.source, p.protobuf_source, &p.protobuf_source_type_header),
                (&p.target, p.protobuf_target, &p.protobuf_target_type_header),
            ] {
                if serializer.confluent() && frame != ProtobufFrame::Indexes {
                    bail!(
                        "Confluent/compatible Protobuf serializers require message-index framing"
                    );
                }
                if matches!(serializer, Serializer::Apicurio { .. })
                    && !(frame == ProtobufFrame::TypeReference
                        || (frame == ProtobufFrame::Bare && type_header.is_some()))
                {
                    bail!("Native Apicurio Protobuf indexes-only/combined/custom framing is not qualified for this release; use type_reference or a native bare type header");
                }
            }
            let mut cursor = 0;
            let type_name = if let Some(header) = &p.protobuf_source_type_header {
                let value = headers
                    .iter()
                    .rfind(|h| h.key == *header)
                    .context("Missing configured Protobuf type header")?
                    .value
                    .as_deref()
                    .context("Null Protobuf type header")?;
                Some(std::str::from_utf8(value)?.to_owned())
            } else if matches!(
                p.protobuf_source,
                ProtobufFrame::TypeReference | ProtobufFrame::IndexesAndTypeReference
            ) {
                Some(read_type_ref(&payload, &mut cursor)?)
            } else {
                None
            };
            let indexes = if matches!(
                p.protobuf_source,
                ProtobufFrame::Indexes | ProtobufFrame::IndexesAndTypeReference
            ) {
                Some(read_indexes(&payload, &mut cursor)?)
            } else {
                None
            };
            let identity = p
                .message
                .as_deref()
                .or_else(|| {
                    indexes.as_ref().and_then(|idx| {
                        m.message_indexes
                            .iter()
                            .find(|(_, v)| *v == idx)
                            .map(|(k, _)| k.as_str())
                    })
                })
                .context("Ambiguous Protobuf message identity; configure message")?;
            let expected = m
                .message_indexes
                .get(identity)
                .context("Protobuf message missing from schema descriptors")?;
            if indexes.as_ref().is_some_and(|idx| idx != expected)
                || type_name
                    .as_deref()
                    .is_some_and(|n| n != identity.rsplit('.').next().unwrap_or(identity))
            {
                bail!("Protobuf frame message does not match explicit descriptor identity");
            }
            let target_indexes = m
                .target_message_indexes
                .get(identity)
                .context("Protobuf destination message missing from descriptors")?;
            if (matches!(p.source, Serializer::Apicurio { .. }) && expected.len() > 1)
                || (matches!(p.target, Serializer::Apicurio { .. }) && target_indexes.len() > 1)
            {
                bail!("Native Apicurio Protobuf serializers cannot faithfully resolve nested messages; use Confluent-compatible framing or registry-only restore");
            }
            if let Some(name) = &p.protobuf_source_type_header {
                if p.protobuf_target_type_header.as_ref() != Some(name) {
                    headers.retain(|h| h.key != *name);
                }
            }
            if let Some(name) = &p.protobuf_target_type_header {
                let value = Some(
                    identity
                        .rsplit('.')
                        .next()
                        .unwrap_or(identity)
                        .as_bytes()
                        .to_vec(),
                );
                if let Some(header) = headers.iter_mut().rfind(|h| h.key == *name) {
                    header.value = value;
                } else {
                    headers.push(RecordHeader {
                        key: name.clone(),
                        value,
                    });
                }
            }
            let mut out = Vec::new();
            if matches!(
                p.protobuf_target,
                ProtobufFrame::TypeReference | ProtobufFrame::IndexesAndTypeReference
            ) {
                let name = identity.rsplit('.').next().unwrap_or(identity).as_bytes();
                let mut r = vec![10];
                r.extend(varint(name.len() as u64));
                r.extend(name);
                out.extend(varint(r.len() as u64));
                out.extend(r);
            }
            if matches!(
                p.protobuf_target,
                ProtobufFrame::Indexes | ProtobufFrame::IndexesAndTypeReference
            ) {
                out.extend(write_indexes(target_indexes));
            }
            out.extend_from_slice(&payload[cursor..]);
            payload = out;
        }
        *bytes = Some(write_frame(
            &p.source,
            &p.target,
            &m.target.id,
            payload,
            headers,
            m.schema_type == "PROTOBUF",
        )?);
        Ok(())
    }
}
impl RecordTransform for RegistryTransform {
    fn name(&self) -> &str {
        "registry-restore"
    }
    fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
    fn transform(&self, topic: &str, record: &mut BackupRecord) -> crate::Result<()> {
        self.apply(topic, record).map_err(|e| {
            crate::Error::Config(format!(
                "Registry transform failed at {topic}/offset {}: {e:#}",
                record.offset
            ))
        })
    }
}
fn read_frame(
    s: &Serializer,
    data: &[u8],
    headers: &[RecordHeader],
) -> Result<(String, Vec<u8>, Vec<u8>)> {
    let width = s.width();
    let mut header_indexes = vec![];
    let (id, payload) = if let Some(header) = s.header() {
        let matches: Vec<_> = headers.iter().filter(|h| h.key == header).collect();
        // Kafka serializers read the last duplicate header. Preserve earlier headers.
        let value = matches
            .last()
            .context(format!("Missing schema header {header}"))?
            .value
            .as_ref()
            .context("Null schema ID header")?;
        if matches!(s, Serializer::ConfluentGuidHeader { .. }) {
            if value.len() < 17 || value[0] != 1 {
                bail!("Malformed Confluent GUID header");
            }
            header_indexes.extend_from_slice(&value[17..]);
            (value[1..17].to_vec(), data.to_vec())
        } else {
            if value.len() != width {
                bail!("Malformed native schema ID header");
            }
            (value.clone(), data.to_vec())
        }
    } else {
        if data.len() < width + 1 || data[0] != 0 {
            bail!("Malformed configured schema prefix");
        }
        (data[1..=width].to_vec(), data[width + 1..].to_vec())
    };
    let id = match width {
        4 => i32::from_be_bytes(id.try_into().unwrap()) as i64,
        8 => i64::from_be_bytes(id.try_into().unwrap()),
        16 => {
            return Ok((
                id.iter().map(|b| format!("{b:02x}")).collect(),
                payload,
                header_indexes,
            ))
        }
        _ => bail!("Unsupported ID width"),
    };
    if id <= 0 {
        bail!("Invalid source registry ID {id}");
    }
    Ok((id.to_string(), payload, header_indexes))
}
fn write_frame(
    source: &Serializer,
    target: &Serializer,
    id: &str,
    mut payload: Vec<u8>,
    headers: &mut Vec<RecordHeader>,
    protobuf: bool,
) -> Result<Vec<u8>> {
    let width = target.width();
    let id_bytes = if width == 16 {
        if id.len() != 32 {
            bail!("Invalid destination GUID");
        }
        let mut out = vec![1];
        for chunk in id.as_bytes().chunks(2) {
            out.push(u8::from_str_radix(std::str::from_utf8(chunk)?, 16)?);
        }
        if protobuf {
            let mut cursor = 0;
            read_indexes(&payload, &mut cursor)?;
            out.extend(payload.drain(..cursor));
        }
        out
    } else {
        let id = id.parse::<i64>()?;
        if id <= 0 {
            bail!("Invalid destination registry ID");
        }
        if width == 4 {
            i32::try_from(id)
                .context("Destination ID overflows 32-bit serializer")?
                .to_be_bytes()
                .to_vec()
        } else if width == 8 {
            id.to_be_bytes().to_vec()
        } else {
            bail!("Unsupported destination serializer");
        }
    };
    if let Some(name) = source.header() {
        if target.header() != Some(name) {
            headers.retain(|h| h.key != name);
        }
    }
    if let Some(name) = target.header() {
        if source.header() == Some(name) {
            let h = headers
                .iter_mut()
                .rfind(|h| h.key == name)
                .context("Missing source header")?;
            h.value = Some(id_bytes);
        } else {
            headers.push(RecordHeader {
                key: name.into(),
                value: Some(id_bytes),
            });
        }
        Ok(payload)
    } else {
        let mut out = vec![0];
        out.extend(id_bytes);
        out.extend(payload);
        Ok(out)
    }
}
fn varint(mut value: u64) -> Vec<u8> {
    let mut out = vec![];
    while value >= 128 {
        out.push((value as u8) | 128);
        value >>= 7;
    }
    out.push(value as u8);
    out
}
fn read_varint(data: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut result = 0u64;
    for shift in (0..70).step_by(7) {
        let b = *data.get(*cursor).context("Truncated varint")?;
        *cursor += 1;
        if shift == 63 && b > 1 {
            bail!("Varint overflow");
        }
        result |= u64::from(b & 127) << shift;
        if b & 128 == 0 {
            return Ok(result);
        }
    }
    bail!("Varint overflow")
}
fn read_indexes(data: &[u8], cursor: &mut usize) -> Result<Vec<u32>> {
    let length = read_varint(data, cursor)?;
    if length == 0 {
        return Ok(vec![0]);
    }
    if length & 1 != 0 || length >> 1 > 128 {
        bail!("Invalid Protobuf message index count");
    }
    let mut indexes = vec![];
    for _ in 0..length >> 1 {
        let v = read_varint(data, cursor)?;
        if v & 1 != 0 {
            bail!("Negative Protobuf message index");
        }
        indexes.push(u32::try_from(v >> 1)?);
    }
    Ok(indexes)
}
fn write_indexes(indexes: &[u32]) -> Vec<u8> {
    if indexes == [0] {
        return vec![0];
    }
    let mut out = varint((indexes.len() as u64) << 1);
    for i in indexes {
        out.extend(varint(u64::from(*i) << 1));
    }
    out
}
fn read_type_ref(data: &[u8], cursor: &mut usize) -> Result<String> {
    let length = usize::try_from(read_varint(data, cursor)?)?;
    let end = cursor
        .checked_add(length)
        .context("Protobuf reference overflow")?;
    if data.get(*cursor) != Some(&10) || end > data.len() {
        bail!("Malformed Apicurio Protobuf type reference");
    }
    *cursor += 1;
    let n = usize::try_from(read_varint(data, cursor)?)?;
    if cursor.checked_add(n) != Some(end) {
        bail!("Malformed Protobuf reference name");
    }
    let name = std::str::from_utf8(&data[*cursor..end])?.to_owned();
    *cursor = end;
    Ok(name)
}

pub fn key(provider: Provider, namespace: &str, kind: IdKind, id: i64) -> IdKey {
    IdKey {
        provider: match provider {
            Provider::Confluent => "confluent",
            Provider::Apicurio => "apicurio",
        }
        .into(),
        namespace: namespace.into(),
        kind,
        id: id.to_string(),
    }
}
pub fn namespace(provider: Provider, s: &Schema) -> String {
    match provider {
        Provider::Apicurio => s.coordinate.group.clone(),
        Provider::Confluent => {
            let name = &s.coordinate.name;
            if name.starts_with(':') {
                name.split(':').nth(1).unwrap_or("").into()
            } else {
                String::new()
            }
        }
    }
}
pub fn top_level_bytes(s: &Schema) -> bool {
    if s.schema_type != "AVRO" {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(&s.content)
        .is_ok_and(|v| v == "bytes" || v["type"] == "bytes")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn mapping() -> WireMapping {
        WireMapping {
            source: key(Provider::Confluent, "", IdKind::Numeric, 7),
            target: key(Provider::Apicurio, "g", IdKind::Global, 42),
            schema_type: "AVRO".into(),
            top_level_bytes: false,
            requires_rule_executor: false,
            blocked_reason: None,
            message_indexes: BTreeMap::new(),
            target_message_indexes: BTreeMap::new(),
        }
    }
    fn profile() -> PayloadProfile {
        PayloadProfile {
            source: Serializer::Confluent {
                namespace: "".into(),
            },
            target: Serializer::Apicurio {
                namespace: "g".into(),
                id_kind: IdKind::Global,
                width: 8,
                header: None,
            },
            protobuf_source: ProtobufFrame::Indexes,
            protobuf_target: ProtobufFrame::Indexes,
            message: None,
            protobuf_source_type_header: None,
            protobuf_target_type_header: None,
        }
    }
    fn transform(profile: PayloadProfile, map: WireMapping) -> RegistryTransform {
        RegistryTransform::new(
            BTreeMap::from([(
                "t".into(),
                TopicProfile {
                    key: PayloadProfile {
                        source: Serializer::Raw,
                        target: Serializer::Raw,
                        ..profile.clone()
                    },
                    value: profile,
                },
            )]),
            vec![map],
        )
        .unwrap()
    }
    fn record(value: Option<Vec<u8>>) -> BackupRecord {
        BackupRecord {
            key: None,
            value,
            headers: vec![RecordHeader {
                key: "unrelated".into(),
                value: None,
            }],
            timestamp: 123,
            offset: 456,
        }
    }
    #[test]
    fn translates_prefix_without_changing_data_or_record_metadata() {
        let mut r = record(Some(vec![0, 0, 0, 0, 7, 2, 65]));
        transform(profile(), mapping()).apply("t", &mut r).unwrap();
        assert_eq!(r.value, Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 42, 2, 65]));
        assert_eq!(r.key, None);
        assert_eq!(r.timestamp, 123);
        assert_eq!(r.offset, 456);
        assert_eq!(r.headers[0].value, None);
    }
    #[test]
    fn translates_top_level_bytes_length_and_round_trips() {
        let mut m = mapping();
        m.top_level_bytes = true;
        let mut r = record(Some(vec![0, 0, 0, 0, 7, 0, 1, 255]));
        transform(profile(), m.clone()).apply("t", &mut r).unwrap();
        assert_eq!(r.value.as_ref().unwrap()[9..], [6, 0, 1, 255]);
        let p = profile();
        let p = PayloadProfile {
            source: p.target,
            target: p.source,
            ..p
        };
        let m = WireMapping {
            source: m.target,
            target: m.source,
            ..m
        };
        transform(p, m).apply("t", &mut r).unwrap();
        assert_eq!(r.value, Some(vec![0, 0, 0, 0, 7, 0, 1, 255]));
    }
    #[test]
    fn tombstones_and_null_keys_are_preserved() {
        let mut r = record(None);
        transform(profile(), mapping()).apply("t", &mut r).unwrap();
        assert!(r.key.is_none() && r.value.is_none());
    }
    #[test]
    fn raw_topics_are_explicit_and_keep_magic_zero() {
        let p = PayloadProfile {
            source: Serializer::Raw,
            target: Serializer::Raw,
            ..profile()
        };
        let mut r = record(Some(vec![0, 1]));
        transform(p, mapping()).apply("t", &mut r).unwrap();
        assert_eq!(r.value, Some(vec![0, 1]));
    }
    #[test]
    fn unknown_topic_unknown_id_and_truncated_frames_are_errors() {
        let t = transform(profile(), mapping());
        for payload in [vec![0, 0, 0, 0, 8], vec![0, 1], vec![1, 0, 0, 0, 7]] {
            assert!(t.check_record("t", &record(Some(payload))).is_err());
        }
        assert!(t.check_record("other", &record(None)).is_err());
    }
    #[test]
    fn integer_overflow_is_rejected() {
        let mut m = mapping();
        m.target.id = (i64::from(i32::MAX) + 1).to_string();
        let mut p = profile();
        if let Serializer::Apicurio { width, .. } = &mut p.target {
            *width = 4;
        }
        assert!(transform(p, m)
            .check_record("t", &record(Some(vec![0, 0, 0, 0, 7])))
            .unwrap_err()
            .to_string()
            .contains("overflows"));
    }
    #[test]
    fn duplicate_native_headers_use_last_and_preserve_earlier_headers() {
        let mut p = profile();
        p.source = Serializer::Apicurio {
            namespace: "g".into(),
            id_kind: IdKind::Global,
            width: 8,
            header: Some("id".into()),
        };
        p.target = p.source.clone();
        let mut m = mapping();
        m.source = key(Provider::Apicurio, "g", IdKind::Global, 7);
        let mut r = record(Some(vec![99]));
        r.headers.extend([
            RecordHeader {
                key: "id".into(),
                value: Some(6i64.to_be_bytes().to_vec()),
            },
            RecordHeader {
                key: "id".into(),
                value: Some(7i64.to_be_bytes().to_vec()),
            },
        ]);
        transform(p, m).apply("t", &mut r).unwrap();
        assert_eq!(r.headers[1].value, Some(6i64.to_be_bytes().to_vec()));
        assert_eq!(r.headers[2].value, Some(42i64.to_be_bytes().to_vec()));
        assert_eq!(r.value, Some(vec![99]));
    }
    #[test]
    fn content_and_global_id_namespaces_cannot_collide() {
        let mut m = mapping();
        m.source = key(Provider::Apicurio, "g", IdKind::Content, 7);
        let mut p = profile();
        p.source = Serializer::Apicurio {
            namespace: "g".into(),
            id_kind: IdKind::Global,
            width: 8,
            header: None,
        };
        assert!(transform(p, m)
            .check_record("t", &record(Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 7])))
            .is_err());
    }
    #[test]
    fn conflicting_mappings_are_rejected() {
        let m = mapping();
        let mut other = m.clone();
        other.target.id = "43".into();
        assert!(RegistryTransform::new(BTreeMap::new(), vec![m, other]).is_err());
    }
    #[test]
    fn translates_protobuf_message_indexes_using_explicit_descriptor_identity() {
        let mut m = mapping();
        m.schema_type = "PROTOBUF".into();
        m.message_indexes.insert("example.Second".into(), vec![1]);
        m.target_message_indexes = m.message_indexes.clone();
        let mut p = profile();
        p.protobuf_target = ProtobufFrame::TypeReference;
        p.message = Some("example.Second".into());
        let mut r = record(Some(vec![0, 0, 0, 0, 7, 2, 2, 10, 1, 65]));
        transform(p, m).apply("t", &mut r).unwrap();
        assert_eq!(
            &r.value.unwrap()[9..],
            &[8, 10, 6, b'S', b'e', b'c', b'o', b'n', b'd', 10, 1, 65]
        );
    }
    #[test]
    fn incorrect_protobuf_message_identity_is_rejected() {
        let mut m = mapping();
        m.schema_type = "PROTOBUF".into();
        m.message_indexes.insert("example.Second".into(), vec![1]);
        m.target_message_indexes = m.message_indexes.clone();
        let mut p = profile();
        p.protobuf_target = ProtobufFrame::TypeReference;
        p.message = Some("example.Second".into());
        assert!(transform(p, m)
            .check_record("t", &record(Some(vec![0, 0, 0, 0, 7, 0, 10, 1, 65])))
            .is_err());
    }
    #[test]
    fn protobuf_indexes_use_destination_descriptor_order() {
        let mut m = mapping();
        m.schema_type = "PROTOBUF".into();
        m.message_indexes.insert("example.Second".into(), vec![1]);
        m.target_message_indexes
            .insert("example.Second".into(), vec![2]);
        let mut p = profile();
        p.target = Serializer::ApicurioCompatible {
            namespace: "g".into(),
            id_kind: IdKind::Global,
        };
        p.message = Some("example.Second".into());
        let mut r = record(Some(vec![0, 0, 0, 0, 7, 2, 2, 10, 1, 65]));
        transform(p, m).apply("t", &mut r).unwrap();
        assert_eq!(&r.value.unwrap()[5..], &[2, 4, 10, 1, 65]);
    }
    #[test]
    fn native_apicurio_nested_messages_and_unqualified_frames_reject() {
        let mut m = mapping();
        m.schema_type = "PROTOBUF".into();
        m.message_indexes
            .insert("example.Parent.Child".into(), vec![1, 0]);
        m.target_message_indexes = m.message_indexes.clone();
        let mut p = profile();
        p.message = Some("example.Parent.Child".into());
        p.protobuf_target = ProtobufFrame::TypeReference;
        let r = record(Some(vec![0, 0, 0, 0, 7, 4, 2, 0, 10, 1, 65]));
        assert!(transform(p.clone(), m.clone())
            .check_record("t", &r)
            .unwrap_err()
            .to_string()
            .contains("nested messages"));
        p.protobuf_target = ProtobufFrame::IndexesAndTypeReference;
        assert!(transform(p, m)
            .check_record("t", &r)
            .unwrap_err()
            .to_string()
            .contains("not qualified"));
    }
    #[test]
    fn protobuf_guid_headers_carry_indexes_and_payload_stays_bare() {
        let mut m = mapping();
        m.schema_type = "PROTOBUF".into();
        m.message_indexes.insert("example.Second".into(), vec![1]);
        m.target_message_indexes
            .insert("example.Second".into(), vec![2]);
        m.target = IdKey {
            provider: "confluent".into(),
            namespace: "".into(),
            kind: IdKind::Guid,
            id: "00112233445566778899aabbccddeeff".into(),
        };
        let mut p = profile();
        p.target = Serializer::ConfluentGuidHeader {
            namespace: "".into(),
            header: "guid".into(),
        };
        p.message = Some("example.Second".into());
        let mut r = record(Some(vec![0, 0, 0, 0, 7, 2, 2, 10, 1, 65]));
        transform(p.clone(), m.clone()).apply("t", &mut r).unwrap();
        assert_eq!(r.value, Some(vec![10, 1, 65]));
        let h = r.headers.last().unwrap().value.as_ref().unwrap();
        assert_eq!(&h[17..], &[2, 4]);
        let inverse_profile = PayloadProfile {
            source: p.target,
            target: p.source,
            ..p
        };
        let inverse_mapping = WireMapping {
            source: m.target,
            target: m.source,
            message_indexes: m.target_message_indexes,
            target_message_indexes: m.message_indexes,
            ..m
        };
        transform(inverse_profile, inverse_mapping)
            .apply("t", &mut r)
            .unwrap();
        assert_eq!(r.value, Some(vec![0, 0, 0, 0, 7, 2, 2, 10, 1, 65]));
        assert_eq!(r.headers.len(), 1);
    }
    #[test]
    fn fingerprints_change_with_profiles_or_mappings() {
        let a = transform(profile(), mapping());
        let mut m = mapping();
        m.target.id = "43".into();
        assert_ne!(a.fingerprint(), transform(profile(), m).fingerprint());
    }
}
