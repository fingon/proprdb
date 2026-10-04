use std::io::{self, Cursor, Write};

use proprdb_runtime::{
    Change, Connection, Error, JsonlCheckpoint, JsonlRecord, Model, Result, SyncTable,
    TableDescriptor, Value, atomic, clear_query_statistics, query_statistics, read_jsonl,
};
use proprdb_rust_tests::{
    generatedtest::example::{Choice, Note, Person, choice},
    system,
};
use serde_json::{Value as Json, json};

const FIRST_ID: &str = "01951d6e-a000-7000-8000-000000000001";
const SECOND_ID: &str = "01951d6e-a000-7000-8000-000000000002";
const REMOTE: &str = "peer";
const PERSON_TYPE: &str = "type.googleapis.com/generatedtest.example.Person";
const NOTE_TYPE: &str = "type.googleapis.com/generatedtest.example.Note";
const UNKNOWN_TYPE: &str = "type.googleapis.com/future.Message";
const NAME: &str = "Ada";
const AT_NS: i64 = 1_761_736_535_123_456_789;
const GO_RECORDS: &str = include_str!("../testdata/go.jsonl");

fn person() -> Person {
    Person {
        name: NAME.into(),
        age: 35,
    }
}
fn record(id: &str, at_ns: i64, deleted: bool, type_url: &str) -> JsonlRecord {
    JsonlRecord {
        id: id.into(),
        at_ns,
        deleted,
        data: json!({"@type": type_url, "name": NAME, "age": "35"}),
    }
}
fn input(record: &JsonlRecord) -> Result<Cursor<Vec<u8>>> {
    Ok(Cursor::new(serde_json::to_vec(record)?))
}
fn exported(crud: &system::Crud<'_>, remote: &str) -> Result<Vec<JsonlRecord>> {
    let mut bytes = Vec::new();
    crud.write_jsonl(remote, &mut bytes)?;
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| Ok(serde_json::from_slice(line)?))
        .collect()
}

#[test]
fn go_jsonl_round_trip_and_remote_watermarks() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    crud.read_jsonl(REMOTE, Cursor::new(GO_RECORDS))?;
    let row = crud.person.select_by_id(FIRST_ID)?.unwrap();
    assert_eq!(row.data, person());
    assert_eq!(row.at_ns, AT_NS);
    assert_eq!(
        crud.choice.select_by_id(SECOND_ID)?.unwrap().data.selection,
        Some(choice::Selection::Count(9_007_199_254_740_993))
    );
    assert!(exported(&crud, REMOTE)?.is_empty());
    let first_export = exported(&crud, "other")?;
    assert_eq!(first_export.len(), 4);
    let full_export = exported(&crud, "")?;
    assert_eq!(full_export, first_export);
    assert_eq!(exported(&crud, " ")?.len(), 4);
    assert!(exported(&crud, " ")?.is_empty());
    assert_eq!(exported(&crud, "")?.len(), 4);
    let target = Connection::open_in_memory()?;
    let target_crud = system::Crud::new(&target);
    target_crud.initialize()?;
    for record in first_export {
        target_crud.read_jsonl(REMOTE, input(&record)?)?;
    }
    assert_eq!(target_crud.person.select_by_id(FIRST_ID)?, Some(row));
    Ok(())
}

#[test]
fn prepared_snapshot_is_stable_and_acknowledges_only_exported_versions() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let row = crud.person.insert(&person())?;
    let mut bytes = Vec::new();
    let checkpoint = crud.prepare_jsonl(REMOTE, &mut bytes)?;
    let decoded: JsonlCheckpoint = serde_json::from_slice(&serde_json::to_vec(&checkpoint)?)?;
    assert_eq!(decoded, checkpoint);
    let record: JsonlRecord = serde_json::from_slice(&bytes)?;
    assert_eq!(record.at_ns, row.at_ns);
    let updated = crud
        .person
        .update_by_id(
            &row.id,
            &Person {
                name: "Grace".into(),
                age: 40,
            },
        )?
        .unwrap();
    crud.acknowledge_jsonl(&checkpoint)?;
    crud.acknowledge_jsonl(&checkpoint)?;
    crud.discard_jsonl(&checkpoint)?;
    let after_ack = exported(&crud, REMOTE)?;
    assert_eq!(after_ack.len(), 1);
    assert_eq!(after_ack[0].at_ns, updated.at_ns);
    let foreign = Connection::open_in_memory()?;
    let foreign_crud = system::Crud::new(&foreign);
    foreign_crud.initialize()?;
    assert!(foreign_crud.acknowledge_jsonl(&checkpoint).is_err());
    assert!(foreign_crud.discard_jsonl(&checkpoint).is_err());
    let second_checkpoint = crud.prepare_jsonl("discard", Vec::new())?;
    crud.discard_jsonl(&second_checkpoint)?;
    assert_eq!(exported(&crud, "discard")?.len(), 1);
    Ok(())
}

struct FailingWriter;
impl Write for FailingWriter {
    fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("injected writer failure"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn failed_export_discards_checkpoint_without_advancing_sync() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    crud.person.insert(&person())?;
    assert!(crud.prepare_jsonl(REMOTE, FailingWriter).is_err());
    let count: i64 =
        connection.query_row("SELECT COUNT(*) FROM _export_batches", [], |row| row.get(0))?;
    assert_eq!(count, 0);
    assert_eq!(exported(&crud, REMOTE)?.len(), 1);
    Ok(())
}

#[test]
fn timestamp_ordering_and_semantic_conflicts() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let changes = crud.person.listen()?;
    let mut incoming = record(FIRST_ID, AT_NS, false, PERSON_TYPE);
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    assert!(matches!(changes.recv()?, Change::Upsert(_)));
    incoming.data["age"] = json!(35);
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    assert!(changes.try_recv().is_err());
    incoming.data["name"] = json!("different");
    assert!(
        matches!(crud.read_jsonl(REMOTE, input(&incoming)?), Err(Error::Line { source, .. }) if matches!(*source, Error::Conflict { .. }))
    );
    incoming.at_ns -= 1;
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    assert_eq!(crud.person.select_by_id(FIRST_ID)?.unwrap().data, person());
    incoming.at_ns += 1;
    incoming.deleted = true;
    assert!(crud.read_jsonl(REMOTE, input(&incoming)?).is_err());
    incoming.at_ns += 1;
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    assert!(matches!(changes.recv()?, Change::Delete { .. }));
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    assert!(changes.try_recv().is_err());
    assert!(crud.person.select_by_id(FIRST_ID)?.is_none());
    incoming.deleted = false;
    assert!(crud.read_jsonl(REMOTE, input(&incoming)?).is_err());
    incoming.at_ns += 1;
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    assert_eq!(
        crud.person.select_by_id(FIRST_ID)?.unwrap().data.name,
        "different"
    );
    Ok(())
}

#[test]
fn strict_jsonl_validation_and_physical_line_numbers() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let base = serde_json::to_value(record(FIRST_ID, AT_NS, false, PERSON_TYPE))?;
    for (field, value) in [
        ("id", json!("invalid")),
        ("id", json!(FIRST_ID.to_uppercase())),
        ("deleted", Json::Null),
        ("deleted", json!("true")),
        ("deleted", json!(1)),
        ("atNs", json!(1.5)),
        ("atNs", json!("01")),
        ("atNs", json!("+1")),
        ("atNs", json!("9223372036854775808")),
        ("atNs", Json::Null),
        ("data", Json::Null),
        ("data", json!([])),
        ("data", json!({"@type": ""})),
        ("data", json!({"@type": 1})),
    ] {
        let mut invalid = base.clone();
        invalid[field] = value;
        let text = format!("\n  \n{}", serde_json::to_string(&invalid)?);
        assert!(
            matches!(
                crud.read_jsonl(REMOTE, Cursor::new(text)),
                Err(Error::Line { line: 3, .. })
            ),
            "field={field}"
        );
    }
    let mut text = serde_json::to_string(&base)?;
    text.push_str("\n{}\n");
    assert!(crud.read_jsonl(REMOTE, Cursor::new(text)).is_err());
    assert!(crud.person.select_by_id(FIRST_ID)?.is_some());
    Ok(())
}

#[test]
fn sync_watermark_failure_rolls_back_state_and_notification() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let changes = crud.person.listen()?;
    connection.execute_batch("CREATE TRIGGER fail_sync BEFORE INSERT ON _sync BEGIN SELECT RAISE(ABORT, 'injected sync failure'); END")?;
    assert!(
        crud.read_jsonl(REMOTE, input(&record(FIRST_ID, AT_NS, false, PERSON_TYPE))?)
            .is_err()
    );
    assert!(crud.person.select_by_id(FIRST_ID)?.is_none());
    assert!(changes.try_recv().is_err());
    connection.execute_batch("DROP TRIGGER fail_sync")?;
    crud.read_jsonl(REMOTE, input(&record(FIRST_ID, AT_NS, false, PERSON_TYPE))?)?;
    assert!(matches!(changes.recv()?, Change::Upsert(_)));
    Ok(())
}

#[test]
fn listeners_share_connection_and_follow_transaction_outcomes() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let changes = crud.person.listen()?;
    let second = system::Crud::new(&connection);
    let transaction = connection.transaction()?;
    let row = second.person.insert(&person())?;
    assert!(changes.try_recv().is_err());
    transaction.commit()?;
    assert_eq!(changes.recv()?, Change::Upsert(row));
    let transaction = connection.transaction()?;
    second.person.insert(&person())?;
    transaction.rollback()?;
    assert!(changes.try_recv().is_err());
    {
        let _transaction = connection.transaction()?;
        second.person.insert(&person())?;
    }
    assert!(changes.try_recv().is_err());
    let result: Result<()> = atomic(&connection, || {
        second.person.insert(&person())?;
        Err(Error::Invalid("injected savepoint failure".into()))
    });
    assert!(result.is_err());
    assert!(changes.try_recv().is_err());
    Ok(())
}

#[test]
fn updates_create_rows_and_missing_deletes_create_tombstones() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    assert!(!crud.person.delete_by_id(FIRST_ID)?);
    let first_export = exported(&crud, REMOTE)?;
    assert_eq!(first_export.len(), 1);
    assert!(first_export[0].deleted);
    let row = crud.person.update_by_id(FIRST_ID, &person())?.unwrap();
    assert!(row.at_ns > first_export[0].at_ns);
    assert_eq!(crud.person.select_by_id(FIRST_ID)?, Some(row.clone()));
    assert!(crud.person.delete_row(&row)?);
    for id in ["", "invalid", "01951d6e-a000-0000-8000-000000000001"] {
        assert!(crud.person.update_by_id(id, &person()).is_err());
        assert!(crud.person.delete_by_id(id).is_err());
    }
    Ok(())
}

struct UnknownOnly<'a>(&'a Connection);
impl SyncTable for UnknownOnly<'_> {
    fn connection(&self) -> &Connection {
        self.0
    }
    fn descriptor(&self) -> TableDescriptor {
        TableDescriptor {
            table_name: "placeholder",
            type_name: "placeholder",
            is_core: false,
            sync_enabled: false,
            change_listeners_enabled: false,
            query_statistics_enabled: false,
        }
    }
    fn initialize(&self) -> Result<()> {
        Ok(())
    }
    fn apply_record(&self, _record: &JsonlRecord) -> Result<()> {
        panic!("not bound")
    }
    fn export_records(&self, _remote: &str) -> Result<Vec<JsonlRecord>> {
        panic!("not synced")
    }
}

#[test]
fn unknown_types_drain_and_transfer_remote_watermarks() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let changes = crud.person.listen()?;
    let unknown = UnknownOnly(&connection);
    let incoming = record(FIRST_ID, AT_NS, false, PERSON_TYPE);
    read_jsonl(&[&unknown], REMOTE, input(&incoming)?)?;
    assert!(crud.person.select_by_id(FIRST_ID)?.is_none());
    crud.initialize()?;
    assert_eq!(crud.person.select_by_id(FIRST_ID)?.unwrap().data, person());
    assert!(matches!(changes.recv()?, Change::Upsert(_)));
    assert!(exported(&crud, REMOTE)?.is_empty());
    assert_eq!(exported(&crud, "other")?.len(), 1);
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM _unknown_types WHERE type_name = ?",
        [system::PersonModel::TYPE_NAME],
        |row| row.get(0),
    )?;
    assert_eq!(count, 0);
    Ok(())
}

#[test]
fn unknown_latest_conflicts_and_omit_sync_records() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let mut incoming = record(FIRST_ID, AT_NS, false, UNKNOWN_TYPE);
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    incoming.data["name"] = json!("changed");
    assert!(crud.read_jsonl(REMOTE, input(&incoming)?).is_err());
    incoming.at_ns += 1;
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    let output = exported(&crud, "other")?;
    assert_eq!(output, vec![incoming.clone()]);
    let mut note = record(SECOND_ID, AT_NS, false, NOTE_TYPE);
    note.data = json!({"@type": NOTE_TYPE, "text": "local"});
    let unknown = UnknownOnly(&connection);
    read_jsonl(&[&unknown], REMOTE, input(&note)?)?;
    crud.initialize()?;
    assert!(crud.note.select_by_id(SECOND_ID)?.is_none());
    assert_eq!(exported(&crud, "")?, vec![incoming]);
    crud.read_jsonl(REMOTE, input(&note)?)?;
    assert!(crud.note.select_by_id(SECOND_ID)?.is_none());
    crud.note.insert(&Note {
        text: "local".into(),
    })?;
    assert_eq!(exported(&crud, "")?.len(), 1);
    Ok(())
}

#[test]
fn statistics_store_full_sql_persist_and_clear() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    crud.person.insert(&person())?;
    let predicate = "name = ?";
    for name in [NAME, "missing"] {
        crud.person.select(predicate, &[Value::Text(name.into())])?;
    }
    assert!(
        crud.person
            .select("invalid_column = ?", &[Value::Integer(1)])
            .is_err()
    );
    let statistics = query_statistics(&connection)?;
    assert_eq!(statistics.len(), 1);
    assert_eq!(
        statistics[0].query,
        format!(
            "SELECT id, at_ns, data FROM \"{}\" WHERE {predicate}",
            system::PersonModel::TABLE_NAME
        )
    );
    assert_eq!(statistics[0].calls, 2);
    assert!(statistics[0].duration_sum_ns > 0);
    crud.initialize()?;
    assert_eq!(query_statistics(&connection)?, statistics);
    clear_query_statistics(&connection)?;
    assert!(query_statistics(&connection)?.is_empty());
    Ok(())
}

#[test]
fn introspection_counts_payload_and_core_tables() -> Result<()> {
    use prost::Message;
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let row = crud.person.insert(&person())?;
    let tables = crud.introspect_tables()?;
    assert_eq!(tables.len(), 12);
    assert_eq!(
        tables
            .iter()
            .filter(|table| table.descriptor.is_core)
            .count(),
        9
    );
    let person_table = tables
        .iter()
        .find(|table| table.descriptor.table_name == system::PersonModel::TABLE_NAME)
        .unwrap();
    assert_eq!(person_table.object_count, 1);
    assert_eq!(
        person_table.payload_bytes,
        i64::try_from(row.data.encoded_len())?
    );
    assert!(person_table.descriptor.sync_enabled);
    assert!(person_table.descriptor.change_listeners_enabled);
    assert!(person_table.descriptor.query_statistics_enabled);
    Ok(())
}

#[test]
fn initialization_audits_ids_even_when_schema_is_unchanged() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let row = crud.person.insert(&person())?;
    connection.execute(
        "UPDATE generatedtest_example_person SET id = ? WHERE id = ?",
        ("invalid", row.id),
    )?;
    assert!(crud.initialize().is_err());
    Ok(())
}

#[test]
fn initialization_is_atomic_across_all_tables() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("CREATE TABLE generatedtest_example_note (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, text INTEGER NOT NULL DEFAULT 0)")?;
    let crud = system::Crud::new(&connection);
    assert!(crud.initialize().is_err());
    assert!(
        connection
            .prepare("SELECT id FROM generatedtest_example_person WHERE id = ?")
            .is_err()
    );
    Ok(())
}

#[test]
fn jsonl_import_bypasses_local_write_validation() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let mut incoming = record(FIRST_ID, AT_NS, false, PERSON_TYPE);
    incoming.data = json!({"@type": PERSON_TYPE});
    crud.read_jsonl(REMOTE, input(&incoming)?)?;
    assert_eq!(
        crud.person.select_by_id(FIRST_ID)?.unwrap().data,
        Person::default()
    );
    assert!(crud.person.insert(&Person::default()).is_err());
    Ok(())
}

#[test]
fn oneof_jsonl_round_trip_preserves_presence() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let row = crud.choice.insert(&Choice {
        selection: Some(choice::Selection::Label(String::new())),
    })?;
    let records = exported(&crud, "")?;
    assert_eq!(records[0].data["label"], json!(""));
    let target = Connection::open_in_memory()?;
    let target_crud = system::Crud::new(&target);
    target_crud.initialize()?;
    target_crud.read_jsonl(REMOTE, input(&records[0])?)?;
    assert_eq!(target_crud.choice.select_by_id(&row.id)?, Some(row));
    Ok(())
}

#[test]
fn go_and_rust_share_sqlite_jsonl_and_checkpoints() -> Result<()> {
    let directory = tempfile::tempdir()?;
    {
        let connection = Connection::open(directory.path().join("rust.db"))?;
        let crud = system::Crud::new(&connection);
        crud.initialize()?;
        crud.read_jsonl("", Cursor::new(GO_RECORDS))?;
        let file = std::fs::File::create(directory.path().join("rust.jsonl"))?;
        let checkpoint = crud.prepare_jsonl("go", file)?;
        std::fs::write(
            directory.path().join("rust.checkpoint"),
            serde_json::to_vec(&checkpoint)?,
        )?;
    }
    let output = std::process::Command::new("go")
        .args(["test", "-count=1", "-run", "^TestRustRuntimeInterop$", "."])
        .current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../system"))
        .env("PROPRDB_RUST_INTEROP_DIR", directory.path())
        .output()?;
    assert!(
        output.status.success(),
        "Go interoperability test failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let connection = Connection::open(directory.path().join("rust.db"))?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let data = crud.person.select_by_id(FIRST_ID)?.unwrap().data;
    assert_eq!(data.name, "Grace");
    assert_eq!(data.age, 41);
    let checkpoint: JsonlCheckpoint =
        serde_json::from_slice(&std::fs::read(directory.path().join("go.checkpoint"))?)?;
    crud.acknowledge_jsonl(&checkpoint)?;
    assert!(exported(&crud, "rust")?.is_empty());
    let target = Connection::open_in_memory()?;
    let target_crud = system::Crud::new(&target);
    target_crud.initialize()?;
    target_crud.read_jsonl(
        "go",
        std::io::BufReader::new(std::fs::File::open(directory.path().join("go.jsonl"))?),
    )?;
    assert_eq!(
        target_crud.person.select_by_id(FIRST_ID)?.unwrap().data,
        data
    );
    Ok(())
}

#[test]
fn table_initialization_drains_unknown_records_and_failure_preserves_them() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let unknown = UnknownOnly(&connection);
    let mut incoming = record(FIRST_ID, AT_NS, false, PERSON_TYPE);
    incoming.data["unknownField"] = json!(true);
    read_jsonl(&[&unknown], REMOTE, input(&incoming)?)?;
    assert!(crud.person.initialize().is_err());
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM _unknown_types WHERE id = ?",
        [FIRST_ID],
        |row| row.get(0),
    )?;
    assert_eq!(count, 1);
    assert!(crud.person.select_by_id(FIRST_ID)?.is_none());
    incoming.at_ns += 1;
    incoming
        .data
        .as_object_mut()
        .unwrap()
        .remove("unknownField");
    read_jsonl(&[&unknown], REMOTE, input(&incoming)?)?;
    crud.person.initialize()?;
    assert_eq!(crud.person.select_by_id(FIRST_ID)?.unwrap().data, person());
    assert!(exported(&crud, REMOTE)?.is_empty());
    Ok(())
}

#[test]
fn legacy_unknown_schema_keeps_latest_record() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("CREATE TABLE _unknown_types (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL)")?;
    for at_ns in [AT_NS, AT_NS + 1] {
        let incoming = record(FIRST_ID, at_ns, false, PERSON_TYPE);
        connection.execute("INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)", (system::PersonModel::TYPE_NAME, FIRST_ID, at_ns, false, serde_json::to_string(&incoming.data)?))?;
    }
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    assert_eq!(
        crud.person.select_by_id(FIRST_ID)?.unwrap().at_ns,
        AT_NS + 1
    );
    Ok(())
}

#[test]
fn nested_bytes_and_float_jsonl_round_trip() -> Result<()> {
    use proprdb_rust_tests::{extra, rusttest::outer};
    let connection = Connection::open_in_memory()?;
    let crud = extra::Crud::new(&connection);
    crud.initialize()?;
    let blob = crud.outer_inner.insert(&outer::Inner {
        selection: Some(outer::inner::Selection::Blob(vec![0, 1, 255])),
    })?;
    let number = crud.outer_inner.insert(&outer::Inner {
        selection: Some(outer::inner::Selection::Number(f64::INFINITY)),
    })?;
    let mut bytes = Vec::new();
    crud.write_jsonl("", &mut bytes)?;
    let target = Connection::open_in_memory()?;
    let target_crud = extra::Crud::new(&target);
    target_crud.initialize()?;
    target_crud.read_jsonl(REMOTE, Cursor::new(bytes))?;
    assert_eq!(target_crud.outer_inner.select_by_id(&blob.id)?, Some(blob));
    assert_eq!(
        target_crud.outer_inner.select_by_id(&number.id)?,
        Some(number)
    );
    Ok(())
}

#[test]
fn failed_local_write_preserves_tombstone_and_listener_state() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    crud.person.delete_by_id(FIRST_ID)?;
    let changes = crud.person.listen()?;
    connection.execute_batch("CREATE TRIGGER fail_person BEFORE INSERT ON generatedtest_example_person BEGIN SELECT RAISE(ABORT, 'injected person failure'); END")?;
    assert!(crud.person.update_by_id(FIRST_ID, &person()).is_err());
    let tombstone: i64 = connection.query_row(
        "SELECT at_ns FROM _deleted WHERE table_name = ? AND id = ?",
        (system::PersonModel::TABLE_NAME, FIRST_ID),
        |row| row.get(0),
    )?;
    assert!(tombstone > 0);
    assert!(changes.try_recv().is_err());
    assert!(crud.person.select_by_id(FIRST_ID)?.is_none());
    Ok(())
}

#[test]
fn protobuf_json_supports_maps_enums_bytes_and_well_known_types() -> Result<()> {
    use proprdb_rust_tests::{
        extra,
        rusttest::{SyncPayload, sync_payload},
    };
    use prost::Message;
    let connection = Connection::open_in_memory()?;
    let crud = extra::Crud::new(&connection);
    crud.initialize()?;
    let type_url = "type.googleapis.com/rusttest.SyncPayload.Detail";
    let data = SyncPayload {
        title: NAME.into(),
        tags: vec!["one".into(), "two".into()],
        counters: [("count".into(), 9_007_199_254_740_993)].into(),
        payload: vec![0, 1, 255],
        state: 1,
        created_at: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 123_456_789,
        }),
        detail: Some(prost_types::Any {
            type_url: type_url.into(),
            value: sync_payload::Detail {
                text: "nested".into(),
            }
            .encode_to_vec(),
        }),
    };
    let row = crud.sync_payload.insert(&data)?;
    let mut bytes = Vec::new();
    crud.write_jsonl("", &mut bytes)?;
    let record: JsonlRecord = serde_json::from_slice(&bytes)?;
    assert_eq!(record.data["counters"]["count"], json!("9007199254740993"));
    assert_eq!(record.data["payload"], json!("AAH/"));
    assert_eq!(record.data["state"], json!("STATE_READY"));
    assert_eq!(
        record.data["createdAt"],
        json!("2023-11-14T22:13:20.123456789Z")
    );
    assert_eq!(record.data["detail"]["@type"], json!(type_url));
    let target = Connection::open_in_memory()?;
    let target_crud = extra::Crud::new(&target);
    target_crud.initialize()?;
    target_crud.read_jsonl(REMOTE, Cursor::new(bytes))?;
    assert_eq!(target_crud.sync_payload.select_by_id(&row.id)?, Some(row));
    Ok(())
}
