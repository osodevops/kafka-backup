//! Ordered Kafka record headers around kafka-protocol's map-based Record API.
//! Batch framing, compression, timestamps and CRC validation stay with the
//! upstream codec; only the v2 record header list is decoded/replaced here.
use crate::manifest::RecordHeader;
use anyhow::{bail, Context, Result};

struct Layout {
    start: usize,
    headers_start: usize,
    headers: Vec<RecordHeader>,
}
fn signed(data: &[u8], cursor: &mut usize) -> Result<i64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let b = *data.get(*cursor).context("Truncated record varint")?;
        *cursor += 1;
        if shift == 63 && b > 1 {
            bail!("Record varint overflow");
        }
        value |= u64::from(b & 127) << shift;
        if b & 128 == 0 {
            return Ok(((value >> 1) as i64) ^ -((value & 1) as i64));
        }
    }
    bail!("Record varint overflow")
}
fn write_signed(value: i64, out: &mut Vec<u8>) {
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    while v >= 128 {
        out.push((v as u8) | 128);
        v >>= 7;
    }
    out.push(v as u8);
}
fn bytes<'a>(data: &'a [u8], cursor: &mut usize, nullable: bool) -> Result<Option<&'a [u8]>> {
    let n = signed(data, cursor)?;
    if n == -1 && nullable {
        return Ok(None);
    }
    let n = usize::try_from(n).context("Negative Kafka record field length")?;
    let end = cursor.checked_add(n).context("Record length overflow")?;
    let value = data
        .get(*cursor..end)
        .context("Truncated Kafka record field")?;
    *cursor = end;
    Ok(Some(value))
}
fn layouts(data: &[u8]) -> Result<Vec<Layout>> {
    let mut cursor = 0;
    let mut records = vec![];
    while cursor < data.len() {
        let n = usize::try_from(signed(data, &mut cursor)?).context("Negative record length")?;
        let start = cursor;
        let end = cursor.checked_add(n).context("Record length overflow")?;
        let record = data.get(start..end).context("Truncated Kafka record")?;
        let mut p = 1;
        if record.is_empty() {
            bail!("Empty Kafka record");
        }
        signed(record, &mut p)?;
        signed(record, &mut p)?;
        bytes(record, &mut p, true)?;
        bytes(record, &mut p, true)?;
        let headers_start = start + p;
        let count = usize::try_from(signed(record, &mut p)?).context("Negative header count")?;
        if count > record.len() {
            bail!("Impossible Kafka header count");
        }
        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            let key = std::str::from_utf8(bytes(record, &mut p, false)?.unwrap())?.to_owned();
            let value = bytes(record, &mut p, true)?.map(<[u8]>::to_vec);
            headers.push(RecordHeader { key, value });
        }
        if p != record.len() {
            bail!("Trailing bytes in Kafka record");
        }
        records.push(Layout {
            start,
            headers_start,
            headers,
        });
        cursor = end;
    }
    Ok(records)
}
pub(super) fn decode(data: &[u8]) -> Result<Vec<Vec<RecordHeader>>> {
    Ok(layouts(data)?.into_iter().map(|r| r.headers).collect())
}
pub(super) fn replace(data: &[u8], headers: &[Vec<RecordHeader>]) -> Result<Vec<u8>> {
    let records = layouts(data)?;
    if records.len() != headers.len() {
        bail!("Kafka record/header list count mismatch");
    }
    let mut out = vec![];
    for (r, headers) in records.into_iter().zip(headers) {
        let mut body = data[r.start..r.headers_start].to_vec();
        write_signed(i64::try_from(headers.len())?, &mut body);
        for header in headers {
            write_signed(i64::try_from(header.key.len())?, &mut body);
            body.extend_from_slice(header.key.as_bytes());
            match &header.value {
                Some(v) => {
                    write_signed(i64::try_from(v.len())?, &mut body);
                    body.extend(v);
                }
                None => write_signed(-1, &mut body),
            }
        }
        write_signed(i64::try_from(body.len())?, &mut out);
        out.extend(body);
    }
    Ok(out)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_duplicate_null_and_empty_headers_and_rejects_truncation() {
        // attrs=0, timestampDelta=0, offsetDelta=0, null key/value, no headers.
        let raw = [12, 0, 0, 0, 1, 1, 0];
        let headers = vec![vec![
            RecordHeader {
                key: "x".into(),
                value: None,
            },
            RecordHeader {
                key: "x".into(),
                value: Some(vec![]),
            },
            RecordHeader {
                key: "x".into(),
                value: Some(vec![42]),
            },
        ]];
        let encoded = replace(&raw, &headers).unwrap();
        assert_eq!(decode(&encoded).unwrap(), headers);
        assert!(decode(&encoded[..encoded.len() - 1]).is_err());
    }
}
