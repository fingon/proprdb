use rusqlite::params;

use crate::{Connection, Model, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableDescriptor {
    pub table_name: &'static str,
    pub type_name: &'static str,
    pub is_core: bool,
    pub sync_enabled: bool,
    pub change_listeners_enabled: bool,
    pub query_statistics_enabled: bool,
}

impl TableDescriptor {
    pub fn of<M: Model>() -> Self {
        Self {
            table_name: M::TABLE_NAME,
            type_name: M::TYPE_NAME,
            is_core: false,
            sync_enabled: M::SYNC_ENABLED,
            change_listeners_enabled: M::CHANGE_LISTENERS,
            query_statistics_enabled: M::QUERY_STATISTICS,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableIntrospection {
    pub descriptor: TableDescriptor,
    pub object_count: i64,
    pub payload_bytes: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryStatistic {
    pub table_name: String,
    pub query: String,
    pub calls: i64,
    pub duration_sum_ns: i64,
}

pub fn query_statistics(connection: &Connection) -> Result<Vec<QueryStatistic>> {
    Ok(connection.prepare("SELECT table_name, query, calls, duration_sum_ns FROM _querystat ORDER BY table_name, query")?.query_map([], |row| Ok(QueryStatistic { table_name: row.get(0)?, query: row.get(1)?, calls: row.get(2)?, duration_sum_ns: row.get(3)? }))?.collect::<std::result::Result<Vec<_>, _>>()?)
}

pub fn clear_query_statistics(connection: &Connection) -> Result<()> {
    connection.execute("DELETE FROM _querystat", [])?;
    Ok(())
}

pub(crate) const CORE_TABLES: [&str; 9] = [
    "_deleted",
    "_sync",
    "_proprdb_schema",
    "_unknown_types",
    "_unknown_sync",
    "_proprdb_metadata",
    "_export_batches",
    "_export_batch_entries",
    "_querystat",
];

pub(crate) fn introspect(
    connection: &Connection,
    mut descriptors: Vec<TableDescriptor>,
) -> Result<Vec<TableIntrospection>> {
    descriptors.extend(CORE_TABLES.iter().map(|table_name| TableDescriptor {
        table_name,
        type_name: "",
        is_core: true,
        sync_enabled: false,
        change_listeners_enabled: false,
        query_statistics_enabled: false,
    }));
    descriptors
        .into_iter()
        .map(|descriptor| {
            let table = quote(descriptor.table_name);
            let object_count =
                connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            let columns = connection
                .prepare("SELECT name FROM pragma_table_info(?)")?
                .query_map(params![descriptor.table_name], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let expression = if columns.iter().any(|name| name == "data") {
                "LENGTH(data)".into()
            } else {
                columns
                    .iter()
                    .map(|column| format!("COALESCE(LENGTH(CAST({} AS BLOB)), 0)", quote(column)))
                    .collect::<Vec<_>>()
                    .join(" + ")
            };
            let payload_bytes = connection.query_row(
                &format!("SELECT COALESCE(SUM({expression}), 0) FROM {table}"),
                [],
                |row| row.get(0),
            )?;
            Ok(TableIntrospection {
                descriptor,
                object_count,
                payload_bytes,
            })
        })
        .collect()
}

fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
