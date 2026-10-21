use std::{
    io::Cursor,
    sync::{Arc, Barrier},
    time::Duration,
};

use proprdb_runtime::{
    Connection, DirtyKeysState, Error, Model, Result, acknowledge_dirty_keys, atomic,
    read_dirty_keys, register_dirty_keys, remove_dirty_keys_consumer,
};
use proprdb_rust_tests::{
    generatedtest::example::{Person, Photo},
    system,
};

const REMOTE: &str = "peer";
const TABLE: &str = system::PersonModel::TABLE_NAME;
const AGE: &str = "age";
const ALL: &str = "all";
const CONSUMER: &str = "refresh";
const OTHER: &str = "other";
const FIRST_ID: &str = "01951d6e-a000-7000-8000-000000000001";
const SECOND_ID: &str = "01951d6e-a000-7000-8000-000000000002";
const UPDATE_AGE: &str = "UPDATE generatedtest_example_person SET age = ? WHERE id = ?";
const INPUT: &str = include_str!("../../system/testdata/mutation-import.jsonl");

fn state(connection: &Connection, tracker: &str) -> Result<DirtyKeysState> {
    read_dirty_keys(connection, TABLE, tracker, CONSUMER)
}
fn register(connection: &Connection, tracker: &str) -> Result<()> {
    register_dirty_keys(connection, TABLE, tracker, CONSUMER)?;
    let initial = state(connection, tracker)?;
    assert!(initial.dirty && initial.full && initial.ids.is_empty());
    register_dirty_keys(connection, TABLE, tracker, CONSUMER)?;
    acknowledge_dirty_keys(connection, &initial.token)
}
fn person() -> Person {
    Person {
        name: "Ada".into(),
        age: 10,
    }
}

#[test]
fn consumers_repeated_mutations_deletion_and_reinsertion() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    register(&connection, AGE)?;
    register(&connection, ALL)?;
    register_dirty_keys(&connection, TABLE, AGE, OTHER)?;
    let row = crud.person.insert(&person())?;
    let first = state(&connection, AGE)?;
    assert_eq!(first.ids, std::slice::from_ref(&row.id));
    assert!(first.dirty && !first.full);
    for age in [20, 30, 10] {
        connection.execute(UPDATE_AGE, rusqlite::params![age, row.id])?;
    }
    assert!(matches!(
        acknowledge_dirty_keys(&connection, &first.token),
        Err(Error::StaleDirtyKeys)
    ));
    let current = state(&connection, AGE)?;
    assert_eq!(current.ids, std::slice::from_ref(&row.id));
    acknowledge_dirty_keys(&connection, &current.token)?;
    assert!(matches!(
        acknowledge_dirty_keys(&connection, &current.token),
        Err(Error::StaleDirtyKeys)
    ));
    assert!(read_dirty_keys(&connection, TABLE, AGE, OTHER)?.dirty);
    assert!(state(&connection, ALL)?.dirty);
    let clean = state(&connection, AGE)?;
    assert!(!clean.dirty && !clean.full && clean.ids.is_empty());
    connection.execute(
        "UPDATE generatedtest_example_person SET name = 'Grace' WHERE id = ?",
        [&row.id],
    )?;
    connection.execute(UPDATE_AGE, rusqlite::params![10, row.id])?;
    acknowledge_dirty_keys(&connection, &clean.token)?;
    crud.person.delete_by_id(&row.id)?;
    let deleted = state(&connection, AGE)?;
    assert_eq!(deleted.ids, std::slice::from_ref(&row.id));
    crud.person.insert_with_id(&row.id, &person())?;
    assert!(matches!(
        acknowledge_dirty_keys(&connection, &deleted.token),
        Err(Error::StaleDirtyKeys)
    ));
    assert_eq!(state(&connection, AGE)?.ids, [row.id]);
    remove_dirty_keys_consumer(&connection, TABLE, AGE, CONSUMER)?;
    register_dirty_keys(&connection, TABLE, AGE, CONSUMER)?;
    assert!(matches!(
        acknowledge_dirty_keys(&connection, &deleted.token),
        Err(Error::StaleDirtyKeys)
    ));
    assert!(state(&connection, AGE)?.full);
    Ok(())
}

#[test]
fn replacements_and_id_changes_with_and_without_recursive_triggers() -> Result<()> {
    for recursive in [false, true] {
        for (query, expected) in [
            (
                "INSERT OR REPLACE INTO generatedtest_example_person (id, at_ns, data, name, age) SELECT id, at_ns, data, name, age FROM generatedtest_example_person WHERE id = ?",
                vec![FIRST_ID],
            ),
            (
                "UPDATE OR REPLACE generatedtest_example_person SET id = '01951d6e-a000-7000-8000-000000000002' WHERE id = ?",
                vec![FIRST_ID, SECOND_ID],
            ),
        ] {
            let connection = Connection::open_in_memory()?;
            let crud = system::Crud::new(&connection);
            crud.initialize()?;
            connection.execute_batch(&format!("PRAGMA recursive_triggers = {recursive}"))?;
            for id in [FIRST_ID, SECOND_ID] {
                crud.person.insert_with_id(id, &person())?;
            }
            register(&connection, AGE)?;
            connection.execute(query, [FIRST_ID])?;
            assert_eq!(state(&connection, AGE)?.ids, expected);
        }
    }
    Ok(())
}

#[test]
fn rollback_refresh_and_savepoint_restore_tokens_and_pending_keys() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    register(&connection, AGE)?;
    let row = crud.person.insert(&person())?;
    let before = state(&connection, AGE)?;
    let result: Result<()> = atomic(&connection, || {
        register_dirty_keys(&connection, TABLE, AGE, OTHER)?;
        acknowledge_dirty_keys(&connection, &before.token)?;
        crud.note
            .insert(&proprdb_rust_tests::generatedtest::example::Note {
                text: "derived".into(),
            })?;
        Err(Error::Invalid("rollback refresh".into()))
    });
    assert!(result.is_err());
    assert!(read_dirty_keys(&connection, TABLE, AGE, OTHER).is_err());
    assert_eq!(state(&connection, AGE)?.ids, std::slice::from_ref(&row.id));
    let count: i64 = connection.query_row(
        "SELECT count(*) FROM generatedtest_example_note",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(count, 0);
    atomic(&connection, || {
        let result: Result<()> = atomic(&connection, || {
            connection.execute(UPDATE_AGE, rusqlite::params![20, row.id])?;
            Err(Error::Invalid("rollback mutation".into()))
        });
        assert!(result.is_err());
        acknowledge_dirty_keys(&connection, &before.token)
    })?;
    assert!(!state(&connection, AGE)?.dirty);
    connection.execute("UPDATE _proprdb_dirty_key_consumers SET revision = ? WHERE table_name = ? AND tracker_name = ? AND consumer = ?", rusqlite::params![i64::MAX, TABLE, AGE, CONSUMER])?;
    assert!(
        connection
            .execute(UPDATE_AGE, rusqlite::params![20, row.id])
            .is_err()
    );
    assert!(!state(&connection, AGE)?.dirty);
    Ok(())
}

#[test]
fn sync_accepted_and_ignored_records_and_unknown_drain() -> Result<()> {
    for drain in [false, true] {
        let connection = Connection::open_in_memory()?;
        let crud = system::Crud::new(&connection);
        crud.initialize()?;
        register(&connection, AGE)?;
        if drain {
            proprdb_runtime::read_jsonl(&[&crud.choice], REMOTE, Cursor::new(INPUT))?;
            assert!(!state(&connection, AGE)?.dirty);
            crud.initialize()?;
        } else {
            crud.read_jsonl("peer", Cursor::new(INPUT))?;
        }
        let current = state(&connection, AGE)?;
        assert_eq!(current.ids, [FIRST_ID, SECOND_ID]);
        acknowledge_dirty_keys(&connection, &current.token)?;
        crud.read_jsonl("peer", Cursor::new(INPUT))?;
        assert!(!state(&connection, AGE)?.dirty);
    }
    Ok(())
}

#[test]
fn nested_nullable_watched_fields_and_unsynced_tables() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let table = system::PhotoModel::TABLE_NAME;
    let tracker = "location";
    register_dirty_keys(&connection, table, tracker, CONSUMER)?;
    let photo = crud.photo.insert(&Photo::default())?;
    let initial = read_dirty_keys(&connection, table, tracker, CONSUMER)?;
    acknowledge_dirty_keys(&connection, &initial.token)?;
    for value in [Some(42), None] {
        connection.execute(
            "UPDATE generatedtest_example_photo SET location_altitude = ? WHERE id = ?",
            rusqlite::params![value, photo.id],
        )?;
        let current = read_dirty_keys(&connection, table, tracker, CONSUMER)?;
        assert_eq!(current.ids, std::slice::from_ref(&photo.id));
        acknowledge_dirty_keys(&connection, &current.token)?;
    }
    let table = system::NoteModel::TABLE_NAME;
    register_dirty_keys(&connection, table, ALL, CONSUMER)?;
    let initial = read_dirty_keys(&connection, table, ALL, CONSUMER)?;
    acknowledge_dirty_keys(&connection, &initial.token)?;
    let note = crud
        .note
        .insert(&proprdb_rust_tests::generatedtest::example::Note {
            text: "local".into(),
        })?;
    crud.note.delete_by_id(&note.id)?;
    assert_eq!(
        read_dirty_keys(&connection, table, ALL, CONSUMER)?.ids,
        [note.id]
    );
    Ok(())
}

#[test]
fn concurrent_acknowledgment_rejects_stale_and_reopen_preserves_work() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("keys.db");
    let first = Connection::open(&path)?;
    first.execute_batch("PRAGMA journal_mode = WAL")?;
    let crud = system::Crud::new(&first);
    crud.initialize()?;
    register(&first, AGE)?;
    let row = crud.person.insert(&person())?;
    let stale = state(&first, AGE)?;
    let second = Connection::open(&path)?;
    system::Crud::new(&second).initialize()?;
    second.execute(UPDATE_AGE, rusqlite::params![20, row.id])?;
    assert!(matches!(
        acknowledge_dirty_keys(&first, &stale.token),
        Err(Error::StaleDirtyKeys)
    ));
    let current = state(&first, AGE)?;
    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|_| {
            let path = path.clone();
            let token = current.token.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || -> Result<bool> {
                let connection = Connection::open(path)?;
                connection.busy_timeout(Duration::from_secs(10))?;
                barrier.wait();
                match acknowledge_dirty_keys(&connection, &token) {
                    Ok(()) => Ok(true),
                    Err(Error::StaleDirtyKeys) => Ok(false),
                    Err(error) => Err(error),
                }
            })
        })
        .collect::<Vec<_>>();
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().expect("acknowledgment thread panicked"))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(outcomes.iter().filter(|success| **success).count(), 1);
    second.execute(UPDATE_AGE, rusqlite::params![30, row.id])?;
    let reopened = Connection::open(path)?;
    system::Crud::new(&reopened).initialize()?;
    assert_eq!(state(&reopened, AGE)?.ids, [row.id]);
    Ok(())
}

#[test]
fn reconciliation_invalidates_tokens_and_preserves_pending_keys() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    register(&connection, AGE)?;
    let row = crud.person.insert(&person())?;
    let before = state(&connection, AGE)?;
    crud.initialize()?;
    assert_eq!(state(&connection, AGE)?.ids, std::slice::from_ref(&row.id));
    acknowledge_dirty_keys(&connection, &before.token)?;
    let clean = state(&connection, AGE)?;
    connection.execute(
        "UPDATE _proprdb_generations SET signature = 'v2|legacy' WHERE table_name = ?",
        [TABLE],
    )?;
    crud.initialize()?;
    assert!(state(&connection, AGE)?.full);
    assert!(matches!(
        acknowledge_dirty_keys(&connection, &clean.token),
        Err(Error::StaleDirtyKeys)
    ));
    let full = state(&connection, AGE)?;
    acknowledge_dirty_keys(&connection, &full.token)?;
    connection.execute(UPDATE_AGE, rusqlite::params![20, row.id])?;
    assert_eq!(state(&connection, AGE)?.ids, [row.id]);
    Ok(())
}
