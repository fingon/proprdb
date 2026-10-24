use proprdb_runtime::{
    Connection, Error, Model, Result, Value, acknowledge_dirty_keys, atomic, read_dirty_keys,
    register_dirty_keys,
};
use proprdb_rust_tests::{
    generatedtest::example::{HashRecord, hash_record},
    system,
};
use std::collections::HashMap;

const TRACKER: &str = "support";
const CONSUMER: &str = "refresh";
const TABLE: &str = system::HashRecordModel::TABLE_NAME;

fn acknowledge(connection: &Connection) -> Result<()> {
    let state = read_dirty_keys(connection, TABLE, TRACKER, CONSUMER)?;
    acknowledge_dirty_keys(connection, &state.token)
}

fn import(
    table: &dyn proprdb_runtime::SyncTable,
    id: &str,
    at_ns: i64,
    enabled: bool,
) -> Result<()> {
    let record = serde_json::json!({"id": id, "atNs": at_ns, "data": {"@type": "type.googleapis.com/generatedtest.example.HashRecord", "enabled": enabled}});
    proprdb_runtime::read_jsonl(&[table], "peer", std::io::Cursor::new(record.to_string()))
}

#[test]
fn tracked_writes_sync_rollback_and_deletion() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    register_dirty_keys(&connection, TABLE, TRACKER, CONSUMER)?;
    acknowledge(&connection)?;
    let mut data = HashRecord {
        detail: Some(hash_record::Detail {
            name: "first".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let row = crud.hash_record.insert(&data)?;
    let state = read_dirty_keys(&connection, TABLE, TRACKER, CONSUMER)?;
    assert_eq!(state.ids, std::slice::from_ref(&row.id));
    acknowledge(&connection)?;
    data.label = "metadata".into();
    let row = crud.hash_record.update_by_id(&row.id, &data)?.unwrap();
    assert!(!read_dirty_keys(&connection, TABLE, TRACKER, CONSUMER)?.dirty);
    data.enabled = true;
    let rollback: Result<()> = atomic(&connection, || {
        crud.hash_record.update_by_id(&row.id, &data)?;
        Err(Error::Invalid("rollback".into()))
    });
    assert!(rollback.is_err());
    assert!(!read_dirty_keys(&connection, TABLE, TRACKER, CONSUMER)?.dirty);
    import(&crud.hash_record, &row.id, row.at_ns + 1, data.enabled)?;
    let stale = read_dirty_keys(&connection, TABLE, TRACKER, CONSUMER)?;
    assert!(stale.dirty);
    data.enabled = false;
    import(&crud.hash_record, &row.id, row.at_ns + 2, data.enabled)?;
    assert!(matches!(
        acknowledge_dirty_keys(&connection, &stale.token),
        Err(Error::StaleDirtyKeys)
    ));
    acknowledge(&connection)?;
    import(&crud.hash_record, &row.id, row.at_ns + 1, data.enabled)?;
    assert!(!read_dirty_keys(&connection, TABLE, TRACKER, CONSUMER)?.dirty);
    crud.hash_record.delete_by_id(&row.id)?;
    assert_eq!(
        read_dirty_keys(&connection, TABLE, TRACKER, CONSUMER)?.ids,
        [row.id]
    );
    Ok(())
}

#[test]
fn nested_fields_presence_and_map_ordering() -> Result<()> {
    let mut data = HashRecord {
        counters: HashMap::from([("a".into(), 1), ("b".into(), 2)]),
        ..Default::default()
    };
    let original = system::HashRecordModel::projected_values(&data)?;
    data.counters = HashMap::from([("b".into(), 2), ("a".into(), 1)]);
    assert_eq!(system::HashRecordModel::projected_values(&data)?, original);
    data.optional_value = Some(0);
    assert_ne!(system::HashRecordModel::projected_values(&data)?, original);
    data.optional_value = None;
    data.detail = Some(hash_record::Detail::default());
    assert_ne!(system::HashRecordModel::projected_values(&data)?, original);
    Ok(())
}

struct ChangedHash;
impl Model for ChangedHash {
    type Data = HashRecord;
    const TABLE_NAME: &str = TABLE;
    const TYPE_NAME: &str = system::HashRecordModel::TYPE_NAME;
    const PROJECTION_SCHEMA: &str = "label:string;_proprdb_hash_name:bytes:hash=changed;_proprdb_hash_support:bytes:hash=changed";
    const CREATE_TABLE_SQL: &str = system::HashRecordModel::CREATE_TABLE_SQL;
    const INSERT_SQL: &str = system::HashRecordModel::INSERT_SQL;
    const UPSERT_SQL: &str = system::HashRecordModel::UPSERT_SQL;
    const INDEX_PREFIX: &str = system::HashRecordModel::INDEX_PREFIX;
    const SYNC_ENABLED: bool = true;
    const CHANGE_LISTENERS: bool = false;
    const QUERY_STATISTICS: bool = false;
    const COLUMNS: &[proprdb_runtime::Column] = system::HashRecordModel::COLUMNS;
    const INDEXES: &[&str] = system::HashRecordModel::INDEXES;
    const DIRTY_KEYS: &[proprdb_runtime::DirtyKeysDescriptor] = system::HashRecordModel::DIRTY_KEYS;
    fn descriptor() -> Result<proprdb_runtime::MessageDescriptor> {
        system::HashRecordModel::descriptor()
    }
    fn projected_values(data: &HashRecord) -> Result<Vec<Value>> {
        let mut values = system::HashRecordModel::projected_values(data)?;
        values[2] = Value::Blob(b"12345678".to_vec());
        Ok(values)
    }
}

#[test]
fn definition_change_backfills_and_invalidates_consumers() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let row = crud.hash_record.insert(&HashRecord::default())?;
    register_dirty_keys(&connection, TABLE, TRACKER, CONSUMER)?;
    acknowledge(&connection)?;
    let read = || -> Result<(Vec<u8>, i64, Vec<u8>)> {
        Ok(connection.query_row("SELECT data, at_ns, _proprdb_hash_support FROM generatedtest_example_hashrecord WHERE id = ?", [&row.id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?)
    };
    let before = read()?;
    let changed = proprdb_runtime::Table::<ChangedHash>::new(&connection);
    changed.initialize()?;
    let after = read()?;
    assert_eq!(before.0, after.0);
    assert_eq!(before.1, after.1);
    assert_eq!(after.2, b"12345678");
    assert!(read_dirty_keys(&connection, TABLE, TRACKER, CONSUMER)?.full);
    Ok(())
}
