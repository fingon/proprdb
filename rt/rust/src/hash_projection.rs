use crate::{Error, Result};
use prost::Message;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

const FORMAT: &[u8] = b"PDBH\x01";
const MAX_DEPTH: usize = 100;

const KIND_DOUBLE: i32 = 1;
const KIND_FLOAT: i32 = 2;
const KIND_INT64: i32 = 3;
const KIND_UINT64: i32 = 4;
const KIND_INT32: i32 = 5;
const KIND_FIXED64: i32 = 6;
const KIND_FIXED32: i32 = 7;
const KIND_BOOL: i32 = 8;
const KIND_STRING: i32 = 9;
const KIND_GROUP: i32 = 10;
const KIND_MESSAGE: i32 = 11;
const KIND_BYTES: i32 = 12;
const KIND_UINT32: i32 = 13;
const KIND_ENUM: i32 = 14;
const KIND_SFIXED32: i32 = 15;
const KIND_SFIXED64: i32 = 16;
const KIND_SINT32: i32 = 17;
const KIND_SINT64: i32 = 18;
type Field = [i32; 4];
type Fields = HashMap<i32, Vec<WireValue>>;

#[derive(Deserialize)]
struct Selection {
    p: Vec<i32>,
    f: Field,
}
#[derive(Deserialize)]
struct Schema {
    p: Vec<Selection>,
    m: Vec<Vec<Field>>,
}
#[derive(Clone)]
struct WireValue {
    wire: u8,
    value: u64,
    bytes: Vec<u8>,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}
fn wire_type(kind: i32) -> u8 {
    match kind {
        KIND_DOUBLE | KIND_FIXED64 | KIND_SFIXED64 => 1,
        KIND_FLOAT | KIND_FIXED32 | KIND_SFIXED32 => 5,
        KIND_STRING | KIND_MESSAGE | KIND_BYTES => 2,
        _ => 0,
    }
}
fn u32_bytes(value: usize) -> Vec<u8> {
    (value as u32).to_le_bytes().to_vec()
}
fn frame(bytes: &[u8]) -> Vec<u8> {
    let mut result = (bytes.len() as u64).to_le_bytes().to_vec();
    result.extend(bytes);
    result
}

struct Reader<'a> {
    bytes: &'a [u8],
}
impl Reader<'_> {
    fn take(&mut self, count: usize) -> Result<&[u8]> {
        if count > self.bytes.len() {
            return Err(invalid("truncated hash input"));
        }
        let (head, tail) = self.bytes.split_at(count);
        self.bytes = tail;
        Ok(head)
    }
    fn varint(&mut self) -> Result<u64> {
        let mut value = 0;
        for shift in (0..70).step_by(7) {
            let byte = self.take(1)?[0];
            if shift == 63 && byte > 1 {
                return Err(invalid("overflow in hash varint"));
            }
            value |= u64::from(byte & 127) << shift;
            if byte < 128 {
                return Ok(value);
            }
        }
        Err(invalid("invalid hash varint"))
    }
    fn value(&mut self, wire: u8) -> Result<WireValue> {
        let mut value = WireValue {
            wire,
            value: 0,
            bytes: Vec::new(),
        };
        match wire {
            0 => value.value = self.varint()?,
            1 | 5 => {
                for (index, byte) in self.take(if wire == 1 { 8 } else { 4 })?.iter().enumerate() {
                    value.value |= u64::from(*byte) << (index * 8);
                }
            }
            2 => {
                let count = usize::try_from(self.varint()?)
                    .map_err(|_| invalid("hash input length overflow"))?;
                value.bytes = self.take(count)?.to_vec();
            }
            _ => return Err(invalid("invalid hash wire type")),
        }
        Ok(value)
    }
    fn skip_group(&mut self, number: u64, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(invalid("hash group nesting exceeds limit"));
        }
        while !self.bytes.is_empty() {
            let tag = self.varint()?;
            let field = tag >> 3;
            let wire = (tag & 7) as u8;
            if wire == 4 {
                if field != number {
                    return Err(invalid("mismatched hash group"));
                }
                return Ok(());
            }
            if wire == 3 {
                self.skip_group(field, depth + 1)?;
            } else {
                self.value(wire)?;
            }
        }
        Err(invalid("unterminated hash group"))
    }
}
fn parse_wire(bytes: &[u8]) -> Result<Fields> {
    let mut reader = Reader { bytes };
    let mut fields = Fields::new();
    while !reader.bytes.is_empty() {
        let tag = reader.varint()?;
        let number = tag >> 3;
        let wire = (tag & 7) as u8;
        if number == 0 || number >= 1 << 29 {
            return Err(invalid("invalid hash field number"));
        }
        if wire == 3 {
            reader.skip_group(number, 0)?;
        } else {
            fields
                .entry(number as i32)
                .or_default()
                .push(reader.value(wire)?);
        }
    }
    Ok(fields)
}

impl Schema {
    fn validate(&self) -> Result<()> {
        if self.p.is_empty() {
            return Err(invalid("empty hash selections"));
        }
        for selection in &self.p {
            if selection.p.is_empty() || selection.p.last() != Some(&selection.f[0]) {
                return Err(invalid("invalid hash path"));
            }
        }
        for field in self.p.iter().map(|p| &p.f).chain(self.m.iter().flatten()) {
            if field[0] <= 0
                || !(KIND_DOUBLE..=KIND_SINT64).contains(&field[1])
                || field[1] == KIND_GROUP
                || !(0..=3).contains(&field[2])
                || (field[1] == KIND_MESSAGE && (field[3] < 0 || field[3] as usize >= self.m.len()))
            {
                return Err(invalid("invalid hash field descriptor"));
            }
        }
        Ok(())
    }
    fn field(&self, field: &Field, raw: &[WireValue], depth: usize) -> Result<Vec<u8>> {
        if depth > MAX_DEPTH {
            return Err(invalid("hash message nesting exceeds limit"));
        }
        let kind = field[1];
        let mode = field[2];
        let mut raw = raw.to_vec();
        if mode == 2 && ![KIND_STRING, KIND_MESSAGE, KIND_BYTES].contains(&kind) {
            let mut expanded = Vec::new();
            for value in raw {
                if value.wire != 2 {
                    expanded.push(value);
                    continue;
                }
                let mut reader = Reader {
                    bytes: &value.bytes,
                };
                while !reader.bytes.is_empty() {
                    expanded.push(reader.value(wire_type(kind))?);
                }
            }
            raw = expanded;
        }
        if mode < 2 {
            if raw.is_empty() && mode == 0 {
                raw.push(WireValue {
                    wire: wire_type(kind),
                    value: 0,
                    bytes: Vec::new(),
                });
            }
            if raw.len() > 1 {
                raw = if kind == KIND_MESSAGE {
                    vec![WireValue {
                        wire: 2,
                        value: 0,
                        bytes: raw.iter().flat_map(|v| v.bytes.iter().copied()).collect(),
                    }]
                } else {
                    raw[raw.len() - 1..].to_vec()
                };
            }
        }
        let mut values = raw
            .iter()
            .map(|v| self.value(field, v, depth))
            .collect::<Result<Vec<_>>>()?;
        if mode == 3 {
            values.sort();
        }
        let mut result = u32_bytes(field[0] as usize);
        result.extend([kind as u8, mode as u8]);
        result.extend((values.len() as u64).to_le_bytes());
        for value in values {
            result.extend(frame(&value));
        }
        Ok(result)
    }
    fn value(&self, field: &Field, raw: &WireValue, depth: usize) -> Result<Vec<u8>> {
        let kind = field[1];
        if raw.wire != wire_type(kind) {
            return Err(invalid("invalid hash scalar wire type"));
        }
        let mut value = raw.value;
        match kind {
            KIND_MESSAGE => {
                let fields = parse_wire(&raw.bytes)?;
                let mut result = Vec::new();
                for child in &self.m[field[3] as usize] {
                    result.extend(self.field(
                        child,
                        fields.get(&child[0]).map(Vec::as_slice).unwrap_or_default(),
                        depth + 1,
                    )?);
                }
                return Ok(result);
            }
            KIND_STRING | KIND_BYTES => return Ok(raw.bytes.clone()),
            KIND_DOUBLE => {
                let number = f64::from_bits(value);
                if number.is_nan() {
                    value = 0x7ff8000000000000;
                } else if number == 0.0 {
                    value = 0;
                }
            }
            KIND_FLOAT => {
                let number = f32::from_bits(value as u32);
                if number.is_nan() {
                    value = 0x7fc00000;
                } else if number == 0.0 {
                    value = 0;
                }
            }
            KIND_BOOL => value = u64::from(value != 0),
            KIND_SINT32 | KIND_SINT64 => value = (value >> 1) ^ (0_u64.wrapping_sub(value & 1)),
            _ => {}
        }
        if [
            KIND_FLOAT,
            KIND_INT32,
            KIND_FIXED32,
            KIND_UINT32,
            KIND_ENUM,
            KIND_SFIXED32,
            KIND_SINT32,
        ]
        .contains(&kind)
        {
            return Ok((value as u32).to_le_bytes().to_vec());
        }
        if kind == KIND_BOOL {
            return Ok(vec![value as u8]);
        }
        if [
            KIND_DOUBLE,
            KIND_INT64,
            KIND_UINT64,
            KIND_FIXED64,
            KIND_SFIXED64,
            KIND_SINT64,
        ]
        .contains(&kind)
        {
            return Ok(value.to_le_bytes().to_vec());
        }
        Err(invalid("invalid hash scalar kind"))
    }
}

pub fn canonical_hash_bytes(data: &[u8], definition: &str) -> Result<Vec<u8>> {
    static SCHEMAS: OnceLock<Mutex<HashMap<String, Arc<Schema>>>> = OnceLock::new();
    let schema = {
        let mut schemas = SCHEMAS
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| invalid("hash schema cache poisoned"))?;
        if let Some(schema) = schemas.get(definition) {
            Arc::clone(schema)
        } else {
            let schema: Schema = serde_json::from_str(definition)?;
            schema.validate()?;
            let schema = Arc::new(schema);
            schemas.insert(definition.into(), Arc::clone(&schema));
            schema
        }
    };
    let mut result = FORMAT.to_vec();
    result.extend(u32_bytes(schema.p.len()));
    for selection in &schema.p {
        result.extend(u32_bytes(selection.p.len()));
        for number in &selection.p {
            result.extend(u32_bytes(*number as usize));
        }
        let mut current = data.to_vec();
        let mut depth = 0;
        for number in &selection.p[..selection.p.len() - 1] {
            let fields = parse_wire(&current)?;
            let Some(values) = fields.get(number).filter(|v| !v.is_empty()) else {
                break;
            };
            if values.iter().any(|v| v.wire != 2) {
                return Err(invalid("invalid hash message wire type"));
            }
            current = values
                .iter()
                .flat_map(|v| v.bytes.iter().copied())
                .collect();
            depth += 1;
        }
        result.extend(u32_bytes(depth));
        if depth == selection.p.len() - 1 {
            let fields = parse_wire(&current)?;
            result.extend(
                schema.field(
                    &selection.f,
                    fields
                        .get(&selection.f[0])
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                    0,
                )?,
            );
        }
    }
    Ok(result)
}

pub fn project_hash<M: Message>(message: &M, definition: &str) -> Result<Vec<u8>> {
    let canonical = canonical_hash_bytes(&message.encode_to_vec(), definition)?;
    Ok(xxhash_rust::xxh3::xxh3_64(&canonical)
        .to_be_bytes()
        .to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct Vector {
        name: String,
        schema: String,
        wire: String,
        canonical: String,
        hash: String,
    }
    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|bytes| u8::from_str_radix(std::str::from_utf8(bytes).unwrap(), 16).unwrap())
            .collect()
    }
    #[test]
    fn shared_vectors() -> Result<()> {
        let vectors: Vec<Vector> = serde_json::from_str(include_str!(
            "../../../test/testdata/hash-projection-vectors.json"
        ))?;
        for vector in vectors {
            let canonical = canonical_hash_bytes(&decode_hex(&vector.wire), &vector.schema)?;
            assert_eq!(canonical, decode_hex(&vector.canonical), "{}", vector.name);
            assert_eq!(
                format!("{:016x}", xxhash_rust::xxh3::xxh3_64(&canonical)),
                vector.hash,
                "{}",
                vector.name
            );
        }
        Ok(())
    }
}
