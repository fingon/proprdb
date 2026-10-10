use std::{
    collections::HashSet,
    marker::PhantomData,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use prost::Message;
pub use rusqlite::types::Value;

mod connection;
mod introspection;
mod sync;
pub use connection::{ChangeReceiver, Connection, Transaction};
pub use introspection::{
    QueryStatistic, TableDescriptor, TableIntrospection, clear_query_statistics, query_statistics,
};
pub use prost_reflect::{DescriptorPool, MessageDescriptor};
use rusqlite::{OptionalExtension, params, params_from_iter};
pub use sync::{
    JsonlCheckpoint, JsonlRecord, SyncTable, acknowledge_jsonl, discard_jsonl, initialize_tables,
    prepare_jsonl, read_jsonl, write_jsonl,
};
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("descriptor: {0}")]
    Descriptor(#[from] prost_reflect::DescriptorError),
    #[error("conflicting state type={type_name} id={id} at_ns={at_ns}")]
    Conflict {
        type_name: String,
        id: String,
        at_ns: i64,
    },
    #[error("JSONL line {line}: {source}")]
    Line { line: usize, source: Box<Error> },
    #[error("change stream: {0}")]
    Receive(#[from] std::sync::mpsc::RecvError),
    #[error("change stream: {0}")]
    TryReceive(#[from] std::sync::mpsc::TryRecvError),
    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("decode protobuf: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("clock: {0}")]
    Clock(#[from] std::time::SystemTimeError),
    #[error("integer overflow: {0}")]
    Overflow(#[from] std::num::TryFromIntError),
    #[error("{original}; rollback failed: {rollback}")]
    Rollback {
        original: Box<Error>,
        rollback: rusqlite::Error,
    },
}

pub struct Column {
    pub name: &'static str,
    pub definition: &'static str,
    pub legacy_oneof_presence_repair: bool,
}

pub trait Model {
    type Data: Message + Default + Clone;
    const TABLE_NAME: &str;
    const TYPE_NAME: &str;
    const PROJECTION_SCHEMA: &str;
    const CREATE_TABLE_SQL: &str;
    const INSERT_SQL: &str;
    const UPSERT_SQL: &str;
    const INDEX_PREFIX: &str;
    const SYNC_ENABLED: bool;
    const CHANGE_LISTENERS: bool;
    const QUERY_STATISTICS: bool;
    const COLUMNS: &[Column];
    const INDEXES: &[&str];
    fn descriptor() -> Result<MessageDescriptor>;
    fn projected_values(data: &Self::Data) -> Vec<Value>;
    fn validate(_data: &Self::Data) -> Result<()> {
        Ok(())
    }
}

pub trait CustomIdModel: Model {}

#[derive(Clone, Debug, PartialEq)]
pub struct Row<T> {
    pub id: String,
    pub at_ns: i64,
    pub data: T,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Change<T> {
    Upsert(Row<T>),
    Delete { id: String, at_ns: i64 },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryStatistics {
    pub calls: i64,
    pub duration_sum_ns: i64,
}

pub struct Table<'a, M: Model> {
    connection: &'a Connection,
    model: PhantomData<M>,
}

pub(crate) const CORE_SQL: &str = "CREATE TABLE IF NOT EXISTS _deleted (table_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, PRIMARY KEY (table_name, id));
CREATE TABLE IF NOT EXISTS _proprdb_schema (table_name TEXT PRIMARY KEY, schema_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS _querystat (table_name TEXT NOT NULL, query TEXT NOT NULL, calls INTEGER NOT NULL, duration_sum_ns INTEGER NOT NULL, PRIMARY KEY (table_name, query));";
const BASE_COLUMNS: [&str; 3] = ["id", "at_ns", "data"];
const SELECT_COLUMNS: &str = "SELECT id, at_ns, data FROM";
const DATA_COLUMN_INDEX: usize = 2;

pub fn atomic<T>(connection: &Connection, action: impl FnOnce() -> Result<T>) -> Result<T> {
    let name = format!("proprdb_{}", Uuid::now_v7().simple());
    connection.execute_batch(&format!("SAVEPOINT {name}"))?;
    let pending_count = connection.pending_count();
    let result = action().and_then(|value| {
        connection.execute_batch(&format!("RELEASE {name}"))?;
        Ok(value)
    });
    match result {
        Ok(value) => {
            connection.flush_changes();
            Ok(value)
        }
        Err(original) => {
            connection.truncate_changes(pending_count);
            match connection.execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}")) {
                Ok(()) => Err(original),
                Err(rollback) => Err(Error::Rollback {
                    original: Box::new(original),
                    rollback,
                }),
            }
        }
    }
}

fn now_ns() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
}

pub fn validate_id(id: &str) -> Result<()> {
    let uuid =
        Uuid::parse_str(id).map_err(|error| Error::Invalid(format!("invalid UUID: {error}")))?;
    if !(1..=8).contains(&uuid.get_version_num())
        || uuid.get_variant() != uuid::Variant::RFC4122
        || uuid.to_string() != id
    {
        return Err(Error::Invalid(
            "id must be a canonical lowercase RFC UUID".into(),
        ));
    }
    Ok(())
}

fn generated_index_name(sql: &str) -> Result<&str> {
    let quoted = sql
        .strip_prefix("CREATE INDEX IF NOT EXISTS ")
        .or_else(|| sql.strip_prefix("CREATE INDEX "))
        .and_then(|rest| rest.strip_prefix('"'))
        .ok_or_else(|| Error::Invalid(format!("invalid generated index SQL: {sql}")))?;
    let end = quoted
        .find("\" ON ")
        .ok_or_else(|| Error::Invalid(format!("invalid generated index SQL: {sql}")))?;
    Ok(&quoted[..end])
}

impl<'a, M: Model> Table<'a, M> {
    pub fn new(connection: &'a Connection) -> Self {
        Self {
            connection,
            model: PhantomData,
        }
    }

    pub fn initialize(&self) -> Result<()> {
        self.initialize_with_core(true)
    }

    fn initialize_without_core(&self) -> Result<()> {
        self.initialize_with_core(false)
    }

    fn initialize_with_core(&self, ensure_core: bool) -> Result<()> {
        log::debug!(
            "initialize generated table table={} ensure_core={ensure_core}",
            M::TABLE_NAME
        );
        atomic(self.connection, || {
            if ensure_core {
                sync::ensure_core_tables(self.connection)?;
            }
            self.connection.execute_batch(M::CREATE_TABLE_SQL)?;
            self.remove_stale_indexes()?;
            self.reconcile_columns()?;
            for sql in M::INDEXES {
                let name = generated_index_name(sql)?;
                let exists: bool = self.connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'index' AND tbl_name = ? AND name = ?)",
                    params![M::TABLE_NAME, name],
                    |row| row.get(0),
                )?;
                if !exists {
                    self.connection.execute_batch(sql)?;
                }
            }
            self.drain_unknown_rows()
        })
    }

    fn remove_stale_indexes(&self) -> Result<()> {
        let desired = M::INDEXES
            .iter()
            .map(|sql| generated_index_name(sql))
            .collect::<Result<HashSet<_>>>()?;
        let mut statement = self
            .connection
            .prepare("SELECT name FROM pragma_index_list(?)")?;
        let names = statement
            .query_map([M::TABLE_NAME], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);
        for name in names {
            if name.starts_with(M::INDEX_PREFIX) && !desired.contains(name.as_str()) {
                self.drop_generated_index(&name)?;
            }
        }
        Ok(())
    }

    fn drop_generated_index(&self, name: &str) -> Result<()> {
        self.connection
            .execute_batch(&format!("DROP INDEX \"{}\"", name.replace('"', "\"\"")))?;
        Ok(())
    }

    fn drop_indexes_for_column(&self, column_name: &str) -> Result<()> {
        let mut statement = self.connection.prepare("SELECT DISTINCT indexes.name FROM pragma_index_list(?) AS indexes JOIN pragma_index_info(indexes.name) AS columns ON columns.name = ?")?;
        let names = statement
            .query_map(params![M::TABLE_NAME, column_name], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);
        for name in names {
            if name.starts_with(M::INDEX_PREFIX) {
                self.drop_generated_index(&name)?;
            }
        }
        Ok(())
    }

    fn reconcile_columns(&self) -> Result<()> {
        let previous: Option<String> = self
            .connection
            .query_row(
                "SELECT schema_hash FROM _proprdb_schema WHERE table_name = ?",
                [M::TABLE_NAME],
                |row| row.get(0),
            )
            .optional()?;
        for old in previous
            .as_deref()
            .unwrap_or_default()
            .split(';')
            .filter(|entry| !entry.is_empty())
        {
            let Some((name, old_kind)) = old.split_once(':') else {
                return Err(Error::Invalid("invalid stored projection schema".into()));
            };
            for current in M::PROJECTION_SCHEMA.split(';') {
                if let Some((current_name, new_kind)) = current.split_once(':') {
                    if name != current_name || old_kind == new_kind {
                        continue;
                    }
                    let repair = M::COLUMNS
                        .iter()
                        .any(|column| column.name == name && column.legacy_oneof_presence_repair)
                        && new_kind.strip_suffix(":optional") == Some(old_kind);
                    if !repair {
                        return Err(Error::Invalid(format!(
                            "incompatible projection history {}.{name}",
                            M::TABLE_NAME
                        )));
                    }
                }
            }
        }
        let mut statement = self
            .connection
            .prepare(&format!("PRAGMA table_info(\"{}\")", M::TABLE_NAME))?;
        let columns = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut changed = previous.as_deref() != Some(M::PROJECTION_SCHEMA);
        for column in M::COLUMNS {
            if let Some((_, sql_type, not_null, default)) =
                columns.iter().find(|(name, ..)| name == column.name)
            {
                let existing = format!(
                    "\"{}\" {}{}",
                    column.name,
                    sql_type,
                    if *not_null {
                        format!(" NOT NULL DEFAULT {}", default.as_deref().unwrap_or("NULL"))
                    } else {
                        String::new()
                    }
                );
                if !existing.eq_ignore_ascii_case(column.definition) {
                    let legacy = column.legacy_oneof_presence_repair
                        && *not_null
                        && column
                            .definition
                            .eq_ignore_ascii_case(&format!("\"{}\" {}", column.name, sql_type));
                    if !legacy {
                        return Err(Error::Invalid(format!(
                            "incompatible projection {}.{}",
                            M::TABLE_NAME,
                            column.name
                        )));
                    }
                    self.drop_indexes_for_column(column.name)?;
                    self.connection.execute_batch(&format!(
                        "ALTER TABLE \"{}\" DROP COLUMN \"{}\"; ALTER TABLE \"{}\" ADD COLUMN {}",
                        M::TABLE_NAME,
                        column.name,
                        M::TABLE_NAME,
                        column.definition
                    ))?;
                    changed = true;
                }
            } else {
                self.connection.execute_batch(&format!(
                    "ALTER TABLE \"{}\" ADD COLUMN {}",
                    M::TABLE_NAME,
                    column.definition
                ))?;
                changed = true;
            }
        }
        for (name, ..) in columns {
            if !BASE_COLUMNS.contains(&name.as_str())
                && !M::COLUMNS.iter().any(|column| column.name == name)
            {
                self.drop_indexes_for_column(&name)?;
                self.connection.execute_batch(&format!(
                    "ALTER TABLE \"{}\" DROP COLUMN \"{}\"",
                    M::TABLE_NAME,
                    name.replace('"', "\"\"")
                ))?;
                changed = true;
            }
        }
        if changed {
            let assignments = M::COLUMNS
                .iter()
                .map(|column| format!("\"{}\" = ?", column.name))
                .collect::<Vec<_>>()
                .join(", ");
            let update_sql = format!(
                "UPDATE \"{}\" SET {} WHERE id = ?",
                M::TABLE_NAME,
                assignments
            );
            let mut cursor = String::new();
            loop {
                let mut statement = self.connection.prepare(&format!(
                    "{SELECT_COLUMNS} \"{}\" WHERE id > ? ORDER BY id LIMIT 1",
                    M::TABLE_NAME
                ))?;
                let row = statement
                    .query_row([&cursor], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Vec<u8>>(DATA_COLUMN_INDEX)?,
                        ))
                    })
                    .optional()?;
                let Some((id, _at_ns, bytes)) = row else {
                    break;
                };
                let data = M::Data::decode(bytes.as_slice())?;
                let mut values = M::projected_values(&data);
                if !M::COLUMNS.is_empty() {
                    values.push(Value::Text(id.clone()));
                    self.connection
                        .execute(&update_sql, params_from_iter(values))?;
                }
                cursor = id;
            }
        }
        if previous.as_deref() != Some(M::PROJECTION_SCHEMA) {
            self.connection.execute("INSERT INTO _proprdb_schema (table_name, schema_hash) VALUES (?, ?) ON CONFLICT(table_name) DO UPDATE SET schema_hash = excluded.schema_hash", [M::TABLE_NAME, M::PROJECTION_SCHEMA])?;
        }
        Ok(())
    }

    fn values(&self, id: &str, at_ns: i64, data: &M::Data) -> Vec<Value> {
        let mut values = vec![
            Value::Text(id.into()),
            Value::Integer(at_ns),
            Value::Blob(data.encode_to_vec()),
        ];
        values.extend(M::projected_values(data));
        values
    }

    pub fn insert(&self, data: &M::Data) -> Result<Row<M::Data>> {
        self.insert_id(&Uuid::now_v7().to_string(), data)
    }

    fn insert_id(&self, id: &str, data: &M::Data) -> Result<Row<M::Data>> {
        M::validate(data)?;
        let row = atomic(self.connection, || {
            let at_ns = self.next_at_ns(id)?;
            self.connection.execute(
                M::INSERT_SQL,
                params_from_iter(self.values(id, at_ns, data)),
            )?;
            self.clear_tombstone(id)?;
            Ok(Row {
                id: id.into(),
                at_ns,
                data: data.clone(),
            })
        })?;
        self.notify(Change::Upsert(row.clone()));
        Ok(row)
    }

    fn next_at_ns(&self, id: &str) -> Result<i64> {
        let latest: i64 = self.connection.query_row(&format!("SELECT MAX(at_ns) FROM (SELECT at_ns FROM \"{}\" WHERE id = ? UNION ALL SELECT at_ns FROM _deleted WHERE table_name = ? AND id = ?)", M::TABLE_NAME), params![id, M::TABLE_NAME, id], |row| Ok(row.get::<_, Option<i64>>(0)?.unwrap_or(0)))?;
        Ok(now_ns()?.max(
            latest
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("timestamp overflow".into()))?,
        ))
    }

    fn clear_tombstone(&self, id: &str) -> Result<()> {
        self.connection.execute(
            "DELETE FROM _deleted WHERE table_name = ? AND id = ?",
            params![M::TABLE_NAME, id],
        )?;
        Ok(())
    }

    pub fn update_by_id(&self, id: &str, data: &M::Data) -> Result<Option<Row<M::Data>>> {
        validate_id(id)?;
        M::validate(data)?;
        let row = atomic(self.connection, || {
            let at_ns = self.next_at_ns(id)?;
            self.connection.execute(
                M::UPSERT_SQL,
                params_from_iter(self.values(id, at_ns, data)),
            )?;
            self.clear_tombstone(id)?;
            Ok(Some(Row {
                id: id.into(),
                at_ns,
                data: data.clone(),
            }))
        })?;
        if let Some(row) = &row {
            self.notify(Change::Upsert(row.clone()));
        }
        Ok(row)
    }

    pub fn update_row(&self, row: &Row<M::Data>) -> Result<Option<Row<M::Data>>> {
        self.update_by_id(&row.id, &row.data)
    }

    pub fn select_by_id(&self, id: &str) -> Result<Option<Row<M::Data>>> {
        let rows = self.select("id = ?", &[Value::Text(id.into())])?;
        Ok(rows.into_iter().next())
    }

    pub fn select(&self, where_sql: &str, values: &[Value]) -> Result<Vec<Row<M::Data>>> {
        if where_sql.trim().is_empty() {
            return Err(Error::Invalid("empty query predicate".into()));
        }
        let started = Instant::now();
        let sql = format!("{SELECT_COLUMNS} \"{}\" WHERE {where_sql}", M::TABLE_NAME);
        let mut statement = self.connection.prepare(&sql)?;
        let mut rows = statement.query(params_from_iter(values))?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let bytes: Vec<u8> = row.get(DATA_COLUMN_INDEX)?;
            result.push(Row {
                id: row.get(0)?,
                at_ns: row.get(1)?,
                data: M::Data::decode(bytes.as_slice())?,
            });
        }
        if M::QUERY_STATISTICS {
            self.connection.execute("INSERT INTO _querystat (table_name, query, calls, duration_sum_ns) VALUES (?, ?, 1, ?) ON CONFLICT(table_name, query) DO UPDATE SET calls = calls + 1, duration_sum_ns = duration_sum_ns + excluded.duration_sum_ns", params![M::TABLE_NAME, sql, i64::try_from(started.elapsed().as_nanos())?])?;
        }
        Ok(result)
    }

    pub fn delete_by_id(&self, id: &str) -> Result<bool> {
        validate_id(id)?;
        let result = atomic(self.connection, || {
            let at_ns = self.next_at_ns(id)?;
            let count = self.connection.execute(
                &format!("DELETE FROM \"{}\" WHERE id = ?", M::TABLE_NAME),
                [id],
            )?;
            if M::SYNC_ENABLED {
                self.connection.execute("INSERT INTO _deleted (table_name, id, at_ns) VALUES (?, ?, ?) ON CONFLICT(table_name, id) DO UPDATE SET at_ns = excluded.at_ns", params![M::TABLE_NAME, id, at_ns])?;
            }
            Ok((count != 0, at_ns))
        })?;
        self.notify(Change::Delete {
            id: id.into(),
            at_ns: result.1,
        });
        Ok(result.0)
    }

    pub fn listen(&self) -> Result<ChangeReceiver<M::Data>> {
        if !M::CHANGE_LISTENERS {
            return Err(Error::Invalid("change listeners are disabled".into()));
        }
        Ok(self.connection.listen(M::TABLE_NAME))
    }

    fn notify(&self, change: Change<M::Data>) {
        if M::CHANGE_LISTENERS {
            self.connection.notify(M::TABLE_NAME, change);
        }
    }

    pub fn drain_unknown_rows(&self) -> Result<()> {
        sync::drain_unknown(&[self])
    }

    pub fn delete_row(&self, row: &Row<M::Data>) -> Result<bool> {
        self.delete_by_id(&row.id)
    }

    pub fn query_statistics(&self, where_sql: &str) -> Result<QueryStatistics> {
        if !M::QUERY_STATISTICS {
            return Err(Error::Invalid("query statistics are disabled".into()));
        }
        Ok(self
            .connection
            .query_row(
                "SELECT calls, duration_sum_ns FROM _querystat WHERE table_name = ? AND query = ?",
                params![
                    M::TABLE_NAME,
                    format!("{SELECT_COLUMNS} \"{}\" WHERE {where_sql}", M::TABLE_NAME)
                ],
                |row| {
                    Ok(QueryStatistics {
                        calls: row.get(0)?,
                        duration_sum_ns: row.get(1)?,
                    })
                },
            )
            .optional()?
            .unwrap_or_default())
    }
}

impl<M: CustomIdModel> Table<'_, M> {
    pub fn insert_with_id(&self, id: &str, data: &M::Data) -> Result<Row<M::Data>> {
        validate_id(id)?;
        self.insert_id(id, data)
    }
}
