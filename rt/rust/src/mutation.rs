use std::collections::BTreeMap;

use rusqlite::{OptionalExtension, params};
use uuid::Uuid;

use crate::{Connection, Error, Model, Result, atomic};

pub const CORE_TABLE_GENERATION: &str = "_proprdb_generations";
pub const CORE_TABLE_DIRTY_DEFINITION: &str = "_proprdb_dirty_definitions";
pub const CORE_TABLE_DIRTY_WINDOW: &str = "_proprdb_dirty_windows";
const TRIGGER_PREFIX: &str = "_proprdb_mutation_";
const FORMAT: &str = "v2|";
const BEFORE_INSERT: &str = "BEFORE_INSERT";
const BEFORE_UPDATE: &str = "BEFORE_UPDATE";
const EVENTS: [&str; 5] = ["INSERT", "UPDATE", "DELETE", BEFORE_INSERT, BEFORE_UPDATE];

pub struct DirtyWindowDescriptor {
    pub name: &'static str,
    pub start_column: &'static str,
    pub end_column: &'static str,
}

#[derive(Clone, Debug)]
pub struct DirtyWindowToken {
    table: String,
    window: String,
    consumer: String,
    registration: String,
    revision: i64,
}

#[derive(Clone, Debug)]
pub struct DirtyWindowState {
    pub dirty: bool,
    pub full: bool,
    pub lower: Option<i64>,
    pub upper: Option<i64>,
    pub token: DirtyWindowToken,
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
        "CREATE TABLE IF NOT EXISTS {CORE_TABLE_GENERATION} (table_name TEXT PRIMARY KEY, generation INTEGER NOT NULL CHECK(typeof(generation) = 'integer' AND generation >= 0), signature TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS {CORE_TABLE_DIRTY_DEFINITION} (table_name TEXT NOT NULL, window_name TEXT NOT NULL, start_column TEXT NOT NULL, end_column TEXT NOT NULL, PRIMARY KEY(table_name, window_name));
        CREATE TABLE IF NOT EXISTS {CORE_TABLE_DIRTY_WINDOW} (table_name TEXT NOT NULL, window_name TEXT NOT NULL, consumer TEXT NOT NULL, registration TEXT NOT NULL, revision INTEGER NOT NULL CHECK(typeof(revision) = 'integer' AND revision >= 0), dirty INTEGER NOT NULL, full INTEGER NOT NULL, lower_bound INTEGER, upper_bound INTEGER, PRIMARY KEY(table_name, window_name, consumer));"
    ))?;
    Ok(())
}

fn signature<M: Model>() -> String {
    let mut windows = M::DIRTY_WINDOWS.iter().collect::<Vec<_>>();
    windows.sort_by_key(|window| window.name);
    let mut parts = vec![format!("{FORMAT}{}", M::PROJECTION_SCHEMA)];
    parts.extend(windows.iter().map(|window| {
        format!(
            "{}:{}:{}",
            window.name, window.start_column, window.end_column
        )
    }));
    parts.join("|")
}

pub(crate) fn prepare<M: Model>(connection: &Connection) -> Result<bool> {
    let stored: Option<String> = connection
        .query_row(
            &format!("SELECT signature FROM {CORE_TABLE_GENERATION} WHERE table_name = ?"),
            [M::TABLE_NAME],
            |row| row.get(0),
        )
        .optional()?;
    let changed = stored.as_deref() != Some(&signature::<M>());
    if changed {
        for event in EVENTS {
            connection.execute_batch(&format!(
                "DROP TRIGGER IF EXISTS {}",
                identifier(&trigger_name(M::TABLE_NAME, event))
            ))?;
        }
    }
    Ok(changed)
}

pub(crate) fn finish<M: Model>(connection: &Connection, changed: bool) -> Result<()> {
    if changed {
        log::debug!(
            "reconcile mutation tracking table={} window_count={}",
            M::TABLE_NAME,
            M::DIRTY_WINDOWS.len()
        );
        connection.execute(&format!("INSERT INTO {CORE_TABLE_GENERATION} (table_name, generation, signature) VALUES (?, 0, ?) ON CONFLICT(table_name) DO UPDATE SET generation = generation + 1, signature = excluded.signature"), params![M::TABLE_NAME, signature::<M>()])?;
        connection.execute(&format!("UPDATE {CORE_TABLE_DIRTY_WINDOW} SET revision = revision + 1, dirty = 1, full = 1, lower_bound = NULL, upper_bound = NULL WHERE table_name = ?"), [M::TABLE_NAME])?;
        connection.execute(
            &format!("DELETE FROM {CORE_TABLE_DIRTY_DEFINITION} WHERE table_name = ?"),
            [M::TABLE_NAME],
        )?;
        for window in M::DIRTY_WINDOWS {
            connection.execute(&format!("INSERT INTO {CORE_TABLE_DIRTY_DEFINITION} (table_name, window_name, start_column, end_column) VALUES (?, ?, ?, ?)"), params![M::TABLE_NAME, window.name, window.start_column, window.end_column])?;
        }
        connection.execute(&format!("DELETE FROM {CORE_TABLE_DIRTY_WINDOW} WHERE table_name = ? AND window_name NOT IN (SELECT window_name FROM {CORE_TABLE_DIRTY_DEFINITION} WHERE table_name = ?)"), params![M::TABLE_NAME, M::TABLE_NAME])?;
    }
    for event in EVENTS {
        if (event == BEFORE_INSERT || event == BEFORE_UPDATE) && M::DIRTY_WINDOWS.is_empty() {
            continue;
        }
        connection.execute_batch(&trigger_sql::<M>(event))?;
    }
    Ok(())
}

fn trigger_sql<M: Model>(event: &str) -> String {
    let table = literal(M::TABLE_NAME);
    let captures_existing = event == BEFORE_INSERT || event == BEFORE_UPDATE;
    let mut condition = if event == "UPDATE" || captures_existing {
        let mut columns = vec!["id", "at_ns", "data"];
        columns.extend(M::COLUMNS.iter().map(|column| column.name));
        let comparisons = columns
            .iter()
            .map(|column| {
                let column = identifier(column);
                let prefix = if captures_existing { "existing" } else { "OLD" };
                format!("{prefix}.{column} IS NOT NEW.{column}")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        if captures_existing {
            format!(
                " WHEN EXISTS(SELECT 1 FROM {} AS existing WHERE existing.id = NEW.id AND ({comparisons}))",
                identifier(M::TABLE_NAME)
            )
        } else {
            format!(" WHEN {comparisons}")
        }
    } else {
        String::new()
    };
    if event == BEFORE_UPDATE {
        condition = condition.replacen(" WHEN ", " WHEN NEW.id IS NOT OLD.id AND ", 1);
    }
    let mut body = if captures_existing {
        String::new()
    } else {
        format!(
            "UPDATE {CORE_TABLE_GENERATION} SET generation = generation + 1 WHERE table_name = {table};"
        )
    };
    for window in M::DIRTY_WINDOWS {
        let prefixes = match event {
            BEFORE_INSERT | BEFORE_UPDATE => vec!["EXISTING"],
            "UPDATE" => vec!["OLD", "NEW"],
            "DELETE" => vec!["OLD"],
            _ => vec!["NEW"],
        };
        for prefix in prefixes {
            let start = if captures_existing {
                format!(
                    "(SELECT {} FROM {} WHERE id = NEW.id)",
                    identifier(window.start_column),
                    identifier(M::TABLE_NAME)
                )
            } else {
                format!("{prefix}.{}", identifier(window.start_column))
            };
            let end = if captures_existing {
                format!(
                    "(SELECT {} FROM {} WHERE id = NEW.id)",
                    identifier(window.end_column),
                    identifier(M::TABLE_NAME)
                )
            } else {
                format!("{prefix}.{}", identifier(window.end_column))
            };
            let invalid = format!(
                "typeof({start}) != 'integer' OR typeof({end}) != 'integer' OR {start} > {end}"
            );
            body.push_str(&format!("UPDATE {CORE_TABLE_DIRTY_WINDOW} SET revision = revision + 1, dirty = 1, full = full OR ({invalid}), lower_bound = CASE WHEN {invalid} THEN lower_bound WHEN lower_bound IS NULL THEN {start} ELSE min(lower_bound, {start}) END, upper_bound = CASE WHEN {invalid} THEN upper_bound WHEN upper_bound IS NULL THEN {end} ELSE max(upper_bound, {end}) END WHERE table_name = {table} AND window_name = {};", literal(window.name)));
        }
    }
    let timing = if event == BEFORE_INSERT {
        "BEFORE INSERT".into()
    } else if event == BEFORE_UPDATE {
        "BEFORE UPDATE".into()
    } else {
        format!("AFTER {event}")
    };
    format!(
        "CREATE TRIGGER IF NOT EXISTS {} {timing} ON {}{condition} BEGIN {body} END",
        identifier(&trigger_name(M::TABLE_NAME, event)),
        identifier(M::TABLE_NAME)
    )
}

pub fn table_generations(
    connection: &Connection,
    tables: &[&str],
) -> Result<BTreeMap<String, i64>> {
    let mut result = BTreeMap::new();
    for table in tables {
        let generation = connection.query_row(
            &format!("SELECT generation FROM {CORE_TABLE_GENERATION} WHERE table_name = ?"),
            [table],
            |row| row.get(0),
        )?;
        result.insert((*table).into(), generation);
    }
    Ok(result)
}

pub fn register_dirty_window(
    connection: &Connection,
    table: &str,
    window: &str,
    consumer: &str,
) -> Result<()> {
    if table.is_empty() || window.is_empty() || consumer.is_empty() {
        return Err(Error::Invalid("empty dirty window identity".into()));
    }
    atomic(connection, || {
        connection.query_row(&format!("SELECT 1 FROM {CORE_TABLE_DIRTY_DEFINITION} WHERE table_name = ? AND window_name = ?"), params![table, window], |row| row.get::<_, i64>(0))?;
        connection.execute(&format!("INSERT INTO {CORE_TABLE_DIRTY_WINDOW} (table_name, window_name, consumer, registration, revision, dirty, full) VALUES (?, ?, ?, ?, 0, 1, 1) ON CONFLICT(table_name, window_name, consumer) DO NOTHING"), params![table, window, consumer, Uuid::now_v7().to_string()])?;
        Ok(())
    })
}

pub fn read_dirty_window(
    connection: &Connection,
    table: &str,
    window: &str,
    consumer: &str,
) -> Result<DirtyWindowState> {
    Ok(connection.query_row(&format!("SELECT registration, revision, dirty, full, lower_bound, upper_bound FROM {CORE_TABLE_DIRTY_WINDOW} WHERE table_name = ? AND window_name = ? AND consumer = ?"), params![table, window, consumer], |row| {
        Ok(DirtyWindowState { dirty: row.get(2)?, full: row.get(3)?, lower: row.get(4)?, upper: row.get(5)?, token: DirtyWindowToken { table: table.into(), window: window.into(), consumer: consumer.into(), registration: row.get(0)?, revision: row.get(1)? } })
    })?)
}

pub fn acknowledge_dirty_window(connection: &Connection, token: &DirtyWindowToken) -> Result<()> {
    let updated: Option<i64> = connection.query_row(&format!("UPDATE {CORE_TABLE_DIRTY_WINDOW} SET revision = revision + 1, dirty = 0, full = 0, lower_bound = NULL, upper_bound = NULL WHERE table_name = ? AND window_name = ? AND consumer = ? AND registration = ? AND revision = ? RETURNING revision"), params![token.table, token.window, token.consumer, token.registration, token.revision], |row| row.get(0)).optional()?;
    if updated.is_none() {
        return Err(Error::StaleDirtyWindow);
    }
    Ok(())
}

pub fn remove_dirty_window_consumer(
    connection: &Connection,
    table: &str,
    window: &str,
    consumer: &str,
) -> Result<()> {
    connection.execute(&format!("DELETE FROM {CORE_TABLE_DIRTY_WINDOW} WHERE table_name = ? AND window_name = ? AND consumer = ?"), params![table, window, consumer])?;
    Ok(())
}
