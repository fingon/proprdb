use std::io::{BufRead, Write};

use prost::Message;
use prost_reflect::DynamicMessage;
use rusqlite::{OptionalExtension, params, params_from_iter};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use uuid::Uuid;

use crate::{
    Change, Connection, Error, Model, Result, Row, Table, TableDescriptor, TableIntrospection,
    atomic, validate_id,
};

const TYPE_KEY: &str = "@type";
const TYPE_URL_PREFIX: &str = "type.googleapis.com/";
const UNKNOWN_PREFIX: &str = "@unknown:";
const DATABASE_ID_KEY: &str = "database_id";
const CHECKPOINT_VERSION: u32 = 1;
const SYNC_SQL: &str = "CREATE TABLE IF NOT EXISTS _sync (object_id TEXT NOT NULL, table_name TEXT NOT NULL, at_ns INTEGER NOT NULL, remote TEXT NOT NULL, PRIMARY KEY (object_id, table_name, remote));
CREATE TABLE IF NOT EXISTS _proprdb_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS _unknown_sync (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, remote TEXT NOT NULL, PRIMARY KEY (type_name, id, remote));
CREATE TABLE IF NOT EXISTS _export_batches (batch_id TEXT PRIMARY KEY, database_id TEXT NOT NULL, remote TEXT NOT NULL, complete INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS _export_batch_entries (batch_id TEXT NOT NULL, sequence INTEGER NOT NULL, table_name TEXT NOT NULL, object_id TEXT NOT NULL, at_ns INTEGER NOT NULL, record_json BLOB, PRIMARY KEY (batch_id, sequence), FOREIGN KEY (batch_id) REFERENCES _export_batches(batch_id) ON DELETE CASCADE);";
const UNKNOWN_SQL: &str = "CREATE TABLE IF NOT EXISTS _unknown_types (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY (type_name, id))";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsonlRecord {
    pub id: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub deleted: bool,
    #[serde(rename = "atNs", deserialize_with = "deserialize_timestamp")]
    pub at_ns: i64,
    pub data: Json,
}

fn is_false(value: &bool) -> bool {
    !value
}

fn deserialize_timestamp<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<i64, D::Error> {
    let value = Json::deserialize(deserializer)?;
    let text = match value {
        Json::String(text) => text,
        Json::Number(number) => number.to_string(),
        _ => {
            return Err(serde::de::Error::custom(
                "atNs must be a signed decimal integer",
            ));
        }
    };
    let digits = text.strip_prefix('-').unwrap_or(&text);
    if digits.is_empty()
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
        || (digits.len() > 1 && digits.starts_with('0'))
    {
        return Err(serde::de::Error::custom(
            "atNs must be a signed decimal integer",
        ));
    }
    text.parse().map_err(serde::de::Error::custom)
}

impl JsonlRecord {
    pub fn type_name(&self) -> Result<&str> {
        let type_url = self
            .data
            .as_object()
            .and_then(|object| object.get(TYPE_KEY))
            .and_then(Json::as_str)
            .ok_or_else(|| Error::Invalid("data must be an object with a string @type".into()))?;
        let name = type_url.rsplit('/').next().unwrap_or_default();
        if name.trim().is_empty() {
            return Err(Error::Invalid("empty @type".into()));
        }
        Ok(name)
    }

    fn validate(&self) -> Result<()> {
        validate_id(&self.id)?;
        self.type_name()?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonlCheckpoint {
    pub version: u32,
    pub database_id: String,
    pub batch_id: String,
}

pub trait SyncTable {
    fn connection(&self) -> &Connection;
    fn descriptor(&self) -> TableDescriptor;
    fn initialize(&self) -> Result<()>;
    /// Bulk initialization calls this after ensuring the core tables exist.
    fn initialize_without_core(&self) -> Result<()> {
        self.initialize()
    }
    fn apply_record(&self, record: &JsonlRecord) -> Result<()>;
    fn export_records(&self, remote: &str) -> Result<Vec<JsonlRecord>>;
    fn introspect_all(&self, tables: &[&dyn SyncTable]) -> Result<Vec<TableIntrospection>> {
        crate::introspection::introspect(
            self.connection(),
            tables.iter().map(|table| table.descriptor()).collect(),
        )
    }
}

fn special_json(type_name: &str) -> bool {
    matches!(
        type_name,
        "google.protobuf.Any"
            | "google.protobuf.Timestamp"
            | "google.protobuf.Duration"
            | "google.protobuf.FieldMask"
            | "google.protobuf.Struct"
            | "google.protobuf.Value"
            | "google.protobuf.ListValue"
            | "google.protobuf.DoubleValue"
            | "google.protobuf.FloatValue"
            | "google.protobuf.Int64Value"
            | "google.protobuf.UInt64Value"
            | "google.protobuf.Int32Value"
            | "google.protobuf.UInt32Value"
            | "google.protobuf.BoolValue"
            | "google.protobuf.StringValue"
            | "google.protobuf.BytesValue"
    )
}

fn message_json<M: Model>(bytes: &[u8]) -> Result<Json> {
    let mut data = serde_json::to_value(DynamicMessage::decode(M::descriptor()?, bytes)?)?;
    if special_json(M::TYPE_NAME) {
        data = json!({"value": data});
    }
    let object = data
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("protobuf JSON is not an object".into()))?;
    object.insert(
        TYPE_KEY.into(),
        format!("{TYPE_URL_PREFIX}{}", M::TYPE_NAME).into(),
    );
    Ok(data)
}

fn parse_message<M: Model>(data: &Json) -> Result<DynamicMessage> {
    let mut payload = data.clone();
    let object = payload
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("data is not an object".into()))?;
    object.remove(TYPE_KEY);
    if special_json(M::TYPE_NAME) {
        if object.len() != 1 {
            return Err(Error::Invalid("invalid well-known Any payload".into()));
        }
        payload = object
            .remove("value")
            .ok_or_else(|| Error::Invalid("missing Any value".into()))?;
    }
    Ok(DynamicMessage::deserialize(M::descriptor()?, payload)?)
}

impl<M: Model> SyncTable for Table<'_, M> {
    fn connection(&self) -> &Connection {
        self.connection
    }
    fn descriptor(&self) -> TableDescriptor {
        TableDescriptor::of::<M>()
    }
    fn initialize(&self) -> Result<()> {
        Table::initialize(self)
    }
    fn initialize_without_core(&self) -> Result<()> {
        Table::initialize_without_core(self)
    }

    fn apply_record(&self, record: &JsonlRecord) -> Result<()> {
        record.validate()?;
        if record.type_name()? != M::TYPE_NAME {
            return Err(Error::Invalid("record type does not match table".into()));
        }
        let message = if record.deleted {
            None
        } else {
            Some(parse_message::<M>(&record.data)?)
        };
        let latest: Option<(i64, bool)> = self.connection.query_row(&format!("SELECT at_ns, deleted FROM (SELECT at_ns, 0 AS deleted FROM \"{}\" WHERE id = ? UNION ALL SELECT at_ns, 1 FROM _deleted WHERE table_name = ? AND id = ?) ORDER BY at_ns DESC LIMIT 1", M::TABLE_NAME), params![record.id, M::TABLE_NAME, record.id], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;
        if let Some((at_ns, deleted)) = latest {
            if record.at_ns < at_ns {
                return Ok(());
            }
            if record.at_ns == at_ns {
                if deleted && record.deleted {
                    return Ok(());
                }
                if !deleted && !record.deleted {
                    let bytes: Vec<u8> = self.connection.query_row(
                        &format!("SELECT data FROM \"{}\" WHERE id = ?", M::TABLE_NAME),
                        [&record.id],
                        |row| row.get(0),
                    )?;
                    if Some(DynamicMessage::decode(M::descriptor()?, bytes.as_slice())?) == message
                    {
                        return Ok(());
                    }
                }
                return Err(conflict(M::TYPE_NAME, record));
            }
        } else if record.at_ns <= 0 {
            if record.at_ns < 0 {
                return Ok(());
            }
            return Err(conflict(M::TYPE_NAME, record));
        }
        if let Some(message) = message {
            let bytes = message.encode_to_vec();
            let data = M::Data::decode(bytes.as_slice())?;
            let mut values = self.values(&record.id, record.at_ns, &data);
            values[crate::DATA_COLUMN_INDEX] = crate::Value::Blob(bytes);
            self.connection
                .execute(M::UPSERT_SQL, params_from_iter(values))?;
            self.clear_tombstone(&record.id)?;
            self.notify(Change::Upsert(Row {
                id: record.id.clone(),
                at_ns: record.at_ns,
                data,
            }));
        } else {
            self.connection.execute(
                &format!("DELETE FROM \"{}\" WHERE id = ?", M::TABLE_NAME),
                [&record.id],
            )?;
            self.connection.execute("INSERT INTO _deleted (table_name, id, at_ns) VALUES (?, ?, ?) ON CONFLICT(table_name, id) DO UPDATE SET at_ns = excluded.at_ns", params![M::TABLE_NAME, record.id, record.at_ns])?;
            self.notify(Change::Delete {
                id: record.id.clone(),
                at_ns: record.at_ns,
            });
        }
        Ok(())
    }

    fn export_records(&self, remote: &str) -> Result<Vec<JsonlRecord>> {
        let mut records = Vec::new();
        let mut cursor = String::new();
        loop {
            let sql = format!(
                "SELECT id, at_ns, data FROM \"{}\" WHERE id > ? ORDER BY id LIMIT 256",
                M::TABLE_NAME
            );
            let rows = self
                .connection
                .prepare(&sql)?
                .query_map([&cursor], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if rows.is_empty() {
                break;
            }
            for (id, at_ns, bytes) in rows {
                cursor.clone_from(&id);
                if needs_send(self.connection, M::TABLE_NAME, &id, remote, at_ns)? {
                    records.push(JsonlRecord {
                        id,
                        at_ns,
                        deleted: false,
                        data: message_json::<M>(&bytes)?,
                    });
                }
            }
        }
        let deleted = self
            .connection
            .prepare("SELECT id, at_ns FROM _deleted WHERE table_name = ? ORDER BY id")?
            .query_map([M::TABLE_NAME], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for (id, at_ns) in deleted {
            if needs_send(self.connection, M::TABLE_NAME, &id, remote, at_ns)? {
                records.push(JsonlRecord {
                    id,
                    at_ns,
                    deleted: true,
                    data: json!({TYPE_KEY: format!("{TYPE_URL_PREFIX}{}", M::TYPE_NAME)}),
                });
            }
        }
        Ok(records)
    }
}

fn conflict(type_name: &str, record: &JsonlRecord) -> Error {
    Error::Conflict {
        type_name: type_name.into(),
        id: record.id.clone(),
        at_ns: record.at_ns,
    }
}

pub(crate) fn ensure_core_tables(connection: &Connection) -> Result<()> {
    connection.execute_batch(crate::CORE_SQL)?;
    connection.execute_batch(SYNC_SQL)?;
    let sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?",
            ["_unknown_types"],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(sql) = sql {
        let normalized = sql.to_lowercase().split_whitespace().collect::<String>();
        if !normalized.contains("primarykey(type_name,id)") {
            connection
                .execute_batch("ALTER TABLE _unknown_types RENAME TO _unknown_types_legacy")?;
            connection.execute_batch(UNKNOWN_SQL)?;
            connection.execute_batch("INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json) SELECT old.type_name, old.id, old.at_ns, old.deleted, old.data_json FROM _unknown_types_legacy old WHERE old.rowid = (SELECT candidate.rowid FROM _unknown_types_legacy candidate WHERE candidate.type_name = old.type_name AND candidate.id = old.id ORDER BY candidate.at_ns DESC, candidate.rowid DESC LIMIT 1); DROP TABLE _unknown_types_legacy")?;
        }
    } else {
        connection.execute_batch(UNKNOWN_SQL)?;
    }
    let database_id: Option<String> = connection
        .query_row(
            "SELECT value FROM _proprdb_metadata WHERE key = ?",
            [DATABASE_ID_KEY],
            |row| row.get(0),
        )
        .optional()?;
    if database_id.is_none() {
        connection.execute(
            "INSERT INTO _proprdb_metadata (key, value) VALUES (?, ?)",
            params![DATABASE_ID_KEY, Uuid::now_v7().to_string()],
        )?;
    }
    Ok(())
}

fn tables_connection<'a>(tables: &[&'a dyn SyncTable]) -> Result<&'a Connection> {
    let connection = tables
        .first()
        .ok_or_else(|| Error::Invalid("empty table bindings".into()))?
        .connection();
    let mut names = std::collections::HashSet::new();
    let mut types = std::collections::HashSet::new();
    for table in tables {
        let descriptor = table.descriptor();
        if !std::ptr::eq(connection, table.connection())
            || !names.insert(descriptor.table_name)
            || !types.insert(descriptor.type_name)
        {
            return Err(Error::Invalid(
                "table bindings must be unique and use the same connection".into(),
            ));
        }
    }
    Ok(connection)
}

pub fn initialize_tables(tables: &[&dyn SyncTable]) -> Result<()> {
    let connection = tables_connection(tables)?;
    atomic(connection, || {
        ensure_core_tables(connection)?;
        for table in tables {
            table.initialize_without_core()?;
        }
        Ok(())
    })
}

fn sync_upsert(
    connection: &Connection,
    table_name: &str,
    record: &JsonlRecord,
    remote: &str,
) -> Result<()> {
    if remote.is_empty() {
        return Ok(());
    }
    if let Some(type_name) = table_name.strip_prefix(UNKNOWN_PREFIX) {
        connection.execute("INSERT INTO _unknown_sync (type_name, id, at_ns, remote) VALUES (?, ?, ?, ?) ON CONFLICT(type_name, id, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)", params![type_name, record.id, record.at_ns, remote])?;
    } else {
        connection.execute("INSERT INTO _sync (object_id, table_name, at_ns, remote) VALUES (?, ?, ?, ?) ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)", params![record.id, table_name, record.at_ns, remote])?;
    }
    Ok(())
}

fn needs_send(
    connection: &Connection,
    table_name: &str,
    id: &str,
    remote: &str,
    at_ns: i64,
) -> Result<bool> {
    if remote.is_empty() {
        return Ok(true);
    }
    let watermark: Option<i64> = if let Some(type_name) = table_name.strip_prefix(UNKNOWN_PREFIX) {
        connection
            .query_row(
                "SELECT at_ns FROM _unknown_sync WHERE type_name = ? AND id = ? AND remote = ?",
                params![type_name, id, remote],
                |row| row.get(0),
            )
            .optional()?
    } else {
        connection
            .query_row(
                "SELECT at_ns FROM _sync WHERE table_name = ? AND object_id = ? AND remote = ?",
                params![table_name, id, remote],
                |row| row.get(0),
            )
            .optional()?
    };
    Ok(watermark.is_none_or(|watermark| watermark < at_ns))
}

fn park_unknown(connection: &Connection, record: &JsonlRecord) -> Result<()> {
    let type_name = record.type_name()?;
    let local: Option<(i64, bool, String)> = connection
        .query_row(
            "SELECT at_ns, deleted, data_json FROM _unknown_types WHERE type_name = ? AND id = ?",
            params![type_name, record.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if let Some((at_ns, deleted, data)) = local {
        if record.at_ns < at_ns {
            return Ok(());
        }
        if record.at_ns == at_ns {
            if deleted == record.deleted && serde_json::from_str::<Json>(&data)? == record.data {
                return Ok(());
            }
            return Err(conflict(type_name, record));
        }
    }
    connection.execute("INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?) ON CONFLICT(type_name, id) DO UPDATE SET at_ns = excluded.at_ns, deleted = excluded.deleted, data_json = excluded.data_json", params![type_name, record.id, record.at_ns, record.deleted, serde_json::to_string(&record.data)?])?;
    Ok(())
}

pub fn read_jsonl(tables: &[&dyn SyncTable], remote: &str, reader: impl BufRead) -> Result<()> {
    let connection = tables_connection(tables)?;
    for (line_index, line) in reader.lines().enumerate() {
        let process = || -> Result<()> {
            let line = line?;
            if line.trim().is_empty() {
                return Ok(());
            }
            let record: JsonlRecord = serde_json::from_str(&line)?;
            record.validate()?;
            let type_name = record.type_name()?;
            let table = tables
                .iter()
                .find(|table| table.descriptor().type_name == type_name);
            if table.is_some_and(|table| !table.descriptor().sync_enabled) {
                log::error!(
                    "ignoring unsynced JSONL record type={type_name} id={} remote={remote}",
                    record.id
                );
                return Ok(());
            }
            atomic(connection, || {
                if let Some(table) = table {
                    table.apply_record(&record)?;
                    sync_upsert(connection, table.descriptor().table_name, &record, remote)
                } else {
                    park_unknown(connection, &record)?;
                    sync_upsert(
                        connection,
                        &format!("{UNKNOWN_PREFIX}{type_name}"),
                        &record,
                        remote,
                    )
                }
            })
        };
        process().map_err(|source| Error::Line {
            line: line_index + 1,
            source: Box::new(source),
        })?;
    }
    Ok(())
}

pub(crate) fn drain_unknown(tables: &[&dyn SyncTable]) -> Result<()> {
    let connection = tables_connection(tables)?;
    for table in tables {
        let descriptor = table.descriptor();
        if !descriptor.sync_enabled {
            continue;
        }
        let records = connection.prepare("SELECT id, at_ns, deleted, data_json FROM _unknown_types WHERE type_name = ? ORDER BY id")?.query_map([descriptor.type_name], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, bool>(2)?, row.get::<_, String>(3)?)))?.collect::<std::result::Result<Vec<_>, _>>()?;
        for (id, at_ns, deleted, data) in records {
            atomic(connection, || {
                table.apply_record(&JsonlRecord {
                    id: id.clone(),
                    at_ns,
                    deleted,
                    data: serde_json::from_str(&data)?,
                })?;
                connection.execute("INSERT INTO _sync (object_id, table_name, at_ns, remote) SELECT id, ?, at_ns, remote FROM _unknown_sync WHERE type_name = ? AND id = ? ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)", params![descriptor.table_name, descriptor.type_name, id])?;
                connection.execute(
                    "DELETE FROM _unknown_sync WHERE type_name = ? AND id = ?",
                    params![descriptor.type_name, id],
                )?;
                connection.execute(
                    "DELETE FROM _unknown_types WHERE type_name = ? AND id = ?",
                    params![descriptor.type_name, id],
                )?;
                Ok(())
            })?;
        }
    }
    Ok(())
}

pub fn prepare_jsonl(
    tables: &[&dyn SyncTable],
    remote: &str,
    mut writer: impl Write,
) -> Result<JsonlCheckpoint> {
    let connection = tables_connection(tables)?;
    let checkpoint = atomic(connection, || {
        ensure_core_tables(connection)?;
        let database_id = database_id(connection)?;
        let checkpoint = JsonlCheckpoint {
            version: CHECKPOINT_VERSION,
            database_id,
            batch_id: Uuid::now_v7().to_string(),
        };
        connection.execute(
            "INSERT INTO _export_batches (batch_id, database_id, remote) VALUES (?, ?, ?)",
            params![checkpoint.batch_id, checkpoint.database_id, remote],
        )?;
        let mut sequence = 0_i64;
        for table in tables {
            if !table.descriptor().sync_enabled {
                continue;
            }
            for record in table.export_records(remote)? {
                stage(
                    connection,
                    &checkpoint,
                    sequence,
                    table.descriptor().table_name,
                    &record,
                )?;
                sequence += 1;
            }
        }
        let mut cursor = (String::new(), String::new());
        loop {
            let rows = connection.prepare("SELECT type_name, id, at_ns, deleted, data_json FROM _unknown_types WHERE (type_name, id) > (?, ?) ORDER BY type_name, id LIMIT 256")?.query_map(params![cursor.0, cursor.1], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?, row.get::<_, bool>(3)?, row.get::<_, String>(4)?)))?.collect::<std::result::Result<Vec<_>, _>>()?;
            if rows.is_empty() {
                break;
            }
            for (type_name, id, at_ns, deleted, data) in rows {
                cursor = (type_name.clone(), id.clone());
                if tables
                    .iter()
                    .any(|table| table.descriptor().type_name == type_name)
                {
                    continue;
                }
                let table_name = format!("{UNKNOWN_PREFIX}{type_name}");
                if needs_send(connection, &table_name, &id, remote, at_ns)? {
                    stage(
                        connection,
                        &checkpoint,
                        sequence,
                        &table_name,
                        &JsonlRecord {
                            id,
                            at_ns,
                            deleted,
                            data: serde_json::from_str(&data)?,
                        },
                    )?;
                    sequence += 1;
                }
            }
        }
        connection.execute(
            "UPDATE _export_batches SET complete = 1 WHERE batch_id = ?",
            [&checkpoint.batch_id],
        )?;
        Ok(checkpoint)
    })?;
    let mut write = || -> Result<()> {
        let mut statement = connection.prepare(
            "SELECT record_json FROM _export_batch_entries WHERE batch_id = ? ORDER BY sequence",
        )?;
        let mut rows = statement.query([&checkpoint.batch_id])?;
        while let Some(row) = rows.next()? {
            writer.write_all(&row.get::<_, Vec<u8>>(0)?)?;
        }
        Ok(())
    };
    if let Err(original) = write() {
        if let Err(cleanup) = discard_jsonl(connection, &checkpoint) {
            return Err(Error::Invalid(format!(
                "{original}; discard failed: {cleanup}"
            )));
        }
        return Err(original);
    }
    Ok(checkpoint)
}

fn stage(
    connection: &Connection,
    checkpoint: &JsonlCheckpoint,
    sequence: i64,
    table: &str,
    record: &JsonlRecord,
) -> Result<()> {
    record.validate()?;
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    connection.execute("INSERT INTO _export_batch_entries (batch_id, sequence, table_name, object_id, at_ns, record_json) VALUES (?, ?, ?, ?, ?, ?)", params![checkpoint.batch_id, sequence, table, record.id, record.at_ns, bytes])?;
    Ok(())
}

fn database_id(connection: &Connection) -> Result<String> {
    Ok(connection.query_row(
        "SELECT value FROM _proprdb_metadata WHERE key = ?",
        [DATABASE_ID_KEY],
        |row| row.get(0),
    )?)
}

fn validate_checkpoint(connection: &Connection, checkpoint: &JsonlCheckpoint) -> Result<()> {
    if checkpoint.version != CHECKPOINT_VERSION
        || checkpoint.database_id != database_id(connection)?
        || checkpoint.batch_id.is_empty()
    {
        return Err(Error::Invalid(
            "export checkpoint belongs to a different database or is invalid".into(),
        ));
    }
    Ok(())
}

pub fn discard_jsonl(connection: &Connection, checkpoint: &JsonlCheckpoint) -> Result<()> {
    atomic(connection, || {
        validate_checkpoint(connection, checkpoint)?;
        delete_batch(connection, &checkpoint.batch_id)
    })
}

fn delete_batch(connection: &Connection, batch_id: &str) -> Result<()> {
    connection.execute(
        "DELETE FROM _export_batch_entries WHERE batch_id = ?",
        [batch_id],
    )?;
    connection.execute("DELETE FROM _export_batches WHERE batch_id = ?", [batch_id])?;
    Ok(())
}

pub fn acknowledge_jsonl(connection: &Connection, checkpoint: &JsonlCheckpoint) -> Result<()> {
    atomic(connection, || {
        validate_checkpoint(connection, checkpoint)?;
        let batch: Option<(String, bool)> = connection.query_row("SELECT remote, complete FROM _export_batches WHERE batch_id = ? AND database_id = ?", params![checkpoint.batch_id, checkpoint.database_id], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;
        let Some((remote, complete)) = batch else {
            return Ok(());
        };
        if !complete {
            return Err(Error::Invalid(
                "cannot acknowledge incomplete export batch".into(),
            ));
        }
        let entries = connection.prepare("SELECT table_name, object_id, at_ns FROM _export_batch_entries WHERE batch_id = ? ORDER BY sequence")?.query_map([&checkpoint.batch_id], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?)))?.collect::<std::result::Result<Vec<_>, _>>()?;
        for (table, id, at_ns) in entries {
            sync_upsert(
                connection,
                &table,
                &JsonlRecord {
                    id,
                    at_ns,
                    deleted: false,
                    data: Json::Null,
                },
                &remote,
            )?;
        }
        delete_batch(connection, &checkpoint.batch_id)
    })
}

pub fn write_jsonl(tables: &[&dyn SyncTable], remote: &str, writer: impl Write) -> Result<()> {
    let checkpoint = prepare_jsonl(tables, remote, writer)?;
    acknowledge_jsonl(tables_connection(tables)?, &checkpoint)
}
