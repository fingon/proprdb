use rusqlite::{OptionalExtension, params};
use uuid::Uuid;

use crate::{Connection, Error, Model, Result, atomic};

pub const CORE_TABLE_DIRTY_KEYS_DEFINITION: &str = "_proprdb_dirty_key_definitions";
pub const CORE_TABLE_DIRTY_KEYS_CONSUMER: &str = "_proprdb_dirty_key_consumers";
pub const CORE_TABLE_DIRTY_KEYS_ENTRY: &str = "_proprdb_dirty_keys";
const TRIGGER_PREFIX: &str = "_proprdb_dirty_keys_";
const OLD: &str = "OLD";
const NEW: &str = "NEW";
const INSERT: &str = "INSERT";
const UPDATE: &str = "UPDATE";
const DELETE: &str = "DELETE";
const EVENTS: [&str; 3] = [INSERT, UPDATE, DELETE];

pub struct DirtyKeysDescriptor {
    pub name: &'static str,
    pub watched_columns: &'static [&'static str],
}

#[derive(Clone, Debug)]
pub struct DirtyKeysToken {
    table: String,
    tracker: String,
    consumer: String,
    registration: String,
    revision: i64,
}

#[derive(Clone, Debug)]
pub struct DirtyKeysState {
    pub dirty: bool,
    pub full: bool,
    pub ids: Vec<String>,
    pub token: DirtyKeysToken,
}

fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn trigger_name(table: &str, event: &str) -> String {
    format!("{TRIGGER_PREFIX}{table}_{}", event.to_lowercase())
}

pub(crate) fn ensure_tables(connection: &Connection) -> Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {CORE_TABLE_DIRTY_KEYS_DEFINITION} (table_name TEXT NOT NULL, tracker_name TEXT NOT NULL, watched_columns TEXT NOT NULL, PRIMARY KEY(table_name, tracker_name));
        CREATE TABLE IF NOT EXISTS {CORE_TABLE_DIRTY_KEYS_CONSUMER} (table_name TEXT NOT NULL, tracker_name TEXT NOT NULL, consumer TEXT NOT NULL, registration TEXT NOT NULL, revision INTEGER NOT NULL CHECK(typeof(revision) = 'integer' AND revision >= 0), dirty INTEGER NOT NULL, full INTEGER NOT NULL, PRIMARY KEY(table_name, tracker_name, consumer));
        CREATE TABLE IF NOT EXISTS {CORE_TABLE_DIRTY_KEYS_ENTRY} (table_name TEXT NOT NULL, tracker_name TEXT NOT NULL, consumer TEXT NOT NULL, id TEXT NOT NULL, PRIMARY KEY(table_name, tracker_name, consumer, id));"
    ))?;
    Ok(())
}

pub(crate) fn signature<M: Model>() -> String {
    let mut trackers = M::DIRTY_KEYS.iter().collect::<Vec<_>>();
    trackers.sort_by_key(|tracker| tracker.name);
    let parts = trackers
        .iter()
        .map(|tracker| {
            let mut columns = tracker.watched_columns.to_vec();
            columns.sort_unstable();
            format!("keys:{}:{}", tracker.name, columns.join(","))
        })
        .collect::<Vec<_>>();
    if parts.is_empty() {
        String::new()
    } else {
        format!("|{}", parts.join("|"))
    }
}

pub(crate) fn drop_triggers(connection: &Connection, table: &str) -> Result<()> {
    for event in EVENTS {
        connection.execute_batch(&format!(
            "DROP TRIGGER IF EXISTS {}",
            identifier(&trigger_name(table, event))
        ))?;
    }
    Ok(())
}

pub(crate) fn reconcile<M: Model>(connection: &Connection, changed: bool) -> Result<()> {
    if changed {
        connection.execute(&format!("UPDATE {CORE_TABLE_DIRTY_KEYS_CONSUMER} SET revision = revision + 1, dirty = 1, full = 1 WHERE table_name = ?"), [M::TABLE_NAME])?;
        connection.execute(
            &format!("DELETE FROM {CORE_TABLE_DIRTY_KEYS_DEFINITION} WHERE table_name = ?"),
            [M::TABLE_NAME],
        )?;
        for tracker in M::DIRTY_KEYS {
            let mut columns = tracker.watched_columns.to_vec();
            columns.sort_unstable();
            connection.execute(&format!("INSERT INTO {CORE_TABLE_DIRTY_KEYS_DEFINITION} (table_name, tracker_name, watched_columns) VALUES (?, ?, ?)"), params![M::TABLE_NAME, tracker.name, columns.join(",")])?;
        }
        for target in [CORE_TABLE_DIRTY_KEYS_ENTRY, CORE_TABLE_DIRTY_KEYS_CONSUMER] {
            connection.execute(&format!("DELETE FROM {target} WHERE table_name = ? AND tracker_name NOT IN (SELECT tracker_name FROM {CORE_TABLE_DIRTY_KEYS_DEFINITION} WHERE table_name = ?)"), params![M::TABLE_NAME, M::TABLE_NAME])?;
        }
    }
    if !M::DIRTY_KEYS.is_empty() {
        for event in EVENTS {
            connection.execute_batch(&trigger_sql::<M>(event))?;
        }
    }
    Ok(())
}

fn trigger_sql<M: Model>(event: &str) -> String {
    let mut body = String::new();
    for tracker in M::DIRTY_KEYS {
        let mut predicate = format!(
            "table_name = {} AND tracker_name = {}",
            literal(M::TABLE_NAME),
            literal(tracker.name)
        );
        if event == UPDATE {
            let mut columns = vec!["id"];
            columns.extend(tracker.watched_columns);
            if tracker.watched_columns.is_empty() {
                columns.extend(["at_ns", "data"]);
                columns.extend(M::COLUMNS.iter().map(|column| column.name));
            }
            let comparisons = columns
                .iter()
                .map(|column| {
                    let column = identifier(column);
                    format!("OLD.{column} IS NOT NEW.{column}")
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            predicate.push_str(&format!(" AND ({comparisons})"));
        }
        body.push_str(&format!("UPDATE {CORE_TABLE_DIRTY_KEYS_CONSUMER} SET revision = revision + 1, dirty = 1 WHERE {predicate};"));
        let prefixes = match event {
            UPDATE => vec![OLD, NEW],
            DELETE => vec![OLD],
            _ => vec![NEW],
        };
        for prefix in prefixes {
            body.push_str(&format!("INSERT INTO {CORE_TABLE_DIRTY_KEYS_ENTRY} (table_name, tracker_name, consumer, id) SELECT table_name, tracker_name, consumer, {prefix}.id FROM {CORE_TABLE_DIRTY_KEYS_CONSUMER} WHERE {predicate} ON CONFLICT(table_name, tracker_name, consumer, id) DO NOTHING;"));
        }
    }
    format!(
        "CREATE TRIGGER IF NOT EXISTS {} AFTER {event} ON {} BEGIN {body} END",
        identifier(&trigger_name(M::TABLE_NAME, event)),
        identifier(M::TABLE_NAME)
    )
}

pub fn register_dirty_keys(
    connection: &Connection,
    table: &str,
    tracker: &str,
    consumer: &str,
) -> Result<()> {
    if table.is_empty() || tracker.is_empty() || consumer.is_empty() {
        return Err(Error::Invalid("empty dirty keys identity".into()));
    }
    atomic(connection, || {
        connection.query_row(&format!("SELECT 1 FROM {CORE_TABLE_DIRTY_KEYS_DEFINITION} WHERE table_name = ? AND tracker_name = ?"), params![table, tracker], |row| row.get::<_, i64>(0))?;
        connection.execute(&format!("INSERT INTO {CORE_TABLE_DIRTY_KEYS_CONSUMER} (table_name, tracker_name, consumer, registration, revision, dirty, full) VALUES (?, ?, ?, ?, 0, 1, 1) ON CONFLICT(table_name, tracker_name, consumer) DO NOTHING"), params![table, tracker, consumer, Uuid::now_v7().to_string()])?;
        Ok(())
    })
}

pub fn read_dirty_keys(
    connection: &Connection,
    table: &str,
    tracker: &str,
    consumer: &str,
) -> Result<DirtyKeysState> {
    log::debug!("read dirty keys table={table} tracker={tracker} consumer={consumer}");
    atomic(connection, || {
        let mut state = connection.query_row(&format!("SELECT registration, revision, dirty, full FROM {CORE_TABLE_DIRTY_KEYS_CONSUMER} WHERE table_name = ? AND tracker_name = ? AND consumer = ?"), params![table, tracker, consumer], |row| {
            Ok(DirtyKeysState { dirty: row.get(2)?, full: row.get(3)?, ids: Vec::new(), token: DirtyKeysToken { table: table.into(), tracker: tracker.into(), consumer: consumer.into(), registration: row.get(0)?, revision: row.get(1)? } })
        })?;
        let mut statement = connection.prepare(&format!("SELECT id FROM {CORE_TABLE_DIRTY_KEYS_ENTRY} WHERE table_name = ? AND tracker_name = ? AND consumer = ? ORDER BY id"))?;
        state.ids = statement
            .query_map(params![table, tracker, consumer], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        Ok(state)
    })
}

pub fn acknowledge_dirty_keys(connection: &Connection, token: &DirtyKeysToken) -> Result<()> {
    atomic(connection, || {
        let updated: Option<i64> = connection.query_row(&format!("UPDATE {CORE_TABLE_DIRTY_KEYS_CONSUMER} SET revision = revision + 1, dirty = 0, full = 0 WHERE table_name = ? AND tracker_name = ? AND consumer = ? AND registration = ? AND revision = ? RETURNING revision"), params![token.table, token.tracker, token.consumer, token.registration, token.revision], |row| row.get(0)).optional()?;
        if updated.is_none() {
            return Err(Error::StaleDirtyKeys);
        }
        connection.execute(&format!("DELETE FROM {CORE_TABLE_DIRTY_KEYS_ENTRY} WHERE table_name = ? AND tracker_name = ? AND consumer = ?"), params![token.table, token.tracker, token.consumer])?;
        Ok(())
    })
}

pub fn remove_dirty_keys_consumer(
    connection: &Connection,
    table: &str,
    tracker: &str,
    consumer: &str,
) -> Result<()> {
    atomic(connection, || {
        for target in [CORE_TABLE_DIRTY_KEYS_ENTRY, CORE_TABLE_DIRTY_KEYS_CONSUMER] {
            connection.execute(&format!("DELETE FROM {target} WHERE table_name = ? AND tracker_name = ? AND consumer = ?"), params![table, tracker, consumer])?;
        }
        Ok(())
    })
}
