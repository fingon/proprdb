use proprdb_runtime::{
    Connection, Error, Model, Result, acknowledge_dirty_window, atomic, read_dirty_window,
    register_dirty_window, remove_dirty_window_consumer, table_generations,
};
use proprdb_rust_tests::{
    generatedtest::example::{Person, Photo},
    system,
};

const CONSUMER: &str = "refresh";
const WINDOW: &str = "age";
const TABLE: &str = system::PersonModel::TABLE_NAME;
const UPDATE_AGE: &str = "UPDATE generatedtest_example_person SET age = ? WHERE id = ?";

fn register(connection: &Connection) -> Result<()> {
    register_dirty_window(connection, TABLE, WINDOW, CONSUMER)?;
    let state = read_dirty_window(connection, TABLE, WINDOW, CONSUMER)?;
    assert!(state.dirty && state.full);
    acknowledge_dirty_window(connection, &state.token)
}

#[test]
fn replacements_preserve_displaced_row_bounds() -> Result<()> {
    for recursive in [false, true] {
        let connection = Connection::open_in_memory()?;
        let crud = system::Crud::new(&connection);
        crud.initialize()?;
        connection.execute_batch(&format!("PRAGMA recursive_triggers = {recursive}"))?;
        register(&connection)?;
        let row = crud.person.insert(&Person {
            name: "Ada".into(),
            age: 10,
        })?;
        let state = read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
        acknowledge_dirty_window(&connection, &state.token)?;
        connection.execute("INSERT OR REPLACE INTO generatedtest_example_person (id, at_ns, data, name, age) VALUES (?, 2, X'', 'Ada', 100)", [&row.id])?;
        let state = read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
        assert_eq!((state.lower, state.upper), (Some(10), Some(100)));
        let target = crud.person.insert(&Person {
            name: "Grace".into(),
            age: 1000,
        })?;
        let state = read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
        acknowledge_dirty_window(&connection, &state.token)?;
        connection.execute(
            "UPDATE OR REPLACE generatedtest_example_person SET id = ?, age = 200 WHERE id = ?",
            rusqlite::params![target.id, row.id],
        )?;
        let state = read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
        assert_eq!((state.lower, state.upper), (Some(100), Some(1000)));
    }
    Ok(())
}

#[test]
fn generations_ranges_and_independent_consumers() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    register(&connection)?;
    register_dirty_window(&connection, TABLE, WINDOW, "other")?;
    assert_eq!(table_generations(&connection, &[TABLE])?[TABLE], 0);
    let row = crud.person.insert(&Person {
        name: "Ada".into(),
        age: 10,
    })?;
    let first = read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
    assert_eq!((first.lower, first.upper), (Some(10), Some(10)));
    for age in [30, -5, 20] {
        connection.execute(UPDATE_AGE, rusqlite::params![age, row.id])?;
    }
    let state = read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
    assert_eq!((state.lower, state.upper), (Some(-5), Some(30)));
    assert!(matches!(
        acknowledge_dirty_window(&connection, &first.token),
        Err(Error::StaleDirtyWindow)
    ));
    acknowledge_dirty_window(&connection, &state.token)?;
    assert!(!read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?.dirty);
    assert!(read_dirty_window(&connection, TABLE, WINDOW, "other")?.dirty);
    assert_eq!(table_generations(&connection, &[TABLE])?[TABLE], 4);
    connection.execute(UPDATE_AGE, rusqlite::params![20, row.id])?;
    assert_eq!(table_generations(&connection, &[TABLE])?[TABLE], 4);
    crud.person.delete_by_id(&row.id)?;
    let deleted = read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
    assert_eq!((deleted.lower, deleted.upper), (Some(20), Some(20)));
    assert_eq!(table_generations(&connection, &[TABLE])?[TABLE], 5);
    remove_dirty_window_consumer(&connection, TABLE, WINDOW, CONSUMER)?;
    register_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
    assert!(matches!(
        acknowledge_dirty_window(&connection, &deleted.token),
        Err(Error::StaleDirtyWindow)
    ));
    let count: i64 = connection.query_row(
        "SELECT count(*) FROM _proprdb_dirty_windows WHERE table_name = ?",
        [TABLE],
        |row| row.get(0),
    )?;
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn rollback_outputs_acknowledgments_and_overflow() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    register(&connection)?;
    let row = crud.person.insert(&Person {
        name: "Ada".into(),
        age: 10,
    })?;
    let state = read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
    let transaction = connection.transaction()?;
    connection.execute(UPDATE_AGE, rusqlite::params![100, row.id])?;
    transaction.rollback()?;
    assert_eq!(table_generations(&connection, &[TABLE])?[TABLE], 1);
    let transaction = connection.transaction()?;
    crud.note
        .insert(&proprdb_rust_tests::generatedtest::example::Note {
            text: "derived".into(),
        })?;
    acknowledge_dirty_window(&transaction, &state.token)?;
    transaction.rollback()?;
    assert!(read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?.dirty);
    assert_eq!(
        table_generations(&connection, &[system::NoteModel::TABLE_NAME])?
            [system::NoteModel::TABLE_NAME],
        0
    );
    let failure: Result<()> = atomic(&connection, || {
        connection.execute(UPDATE_AGE, rusqlite::params![200, row.id])?;
        Err(Error::Invalid("savepoint rollback".into()))
    });
    assert!(failure.is_err());
    acknowledge_dirty_window(&connection, &state.token)?;
    connection.execute(
        "UPDATE _proprdb_generations SET generation = ? WHERE table_name = ?",
        rusqlite::params![i64::MAX, TABLE],
    )?;
    assert!(
        connection
            .execute(UPDATE_AGE, rusqlite::params![100, row.id])
            .is_err()
    );
    let age: i64 = connection.query_row(
        "SELECT age FROM generatedtest_example_person WHERE id = ?",
        [&row.id],
        |row| row.get(0),
    )?;
    assert_eq!(age, 10);
    Ok(())
}

#[test]
fn missing_and_reversed_bounds_require_full_refresh() -> Result<()> {
    for (start, end, full) in [
        (Some(10), Some(20), false),
        (None, Some(20), true),
        (Some(30), Some(20), true),
    ] {
        let connection = Connection::open_in_memory()?;
        let crud = system::Crud::new(&connection);
        crud.initialize()?;
        let table = system::PhotoModel::TABLE_NAME;
        register_dirty_window(&connection, table, "capture", CONSUMER)?;
        let row = crud.photo.insert(&Photo::default())?;
        connection.execute("UPDATE generatedtest_example_photo SET exif_create_utc_time_seconds = ?, exif_modify_utc_time_seconds = ? WHERE id = ?", rusqlite::params![start, end, row.id])?;
        let state = read_dirty_window(&connection, table, "capture", CONSUMER)?;
        acknowledge_dirty_window(&connection, &state.token)?;
        connection.execute(
            "UPDATE generatedtest_example_photo SET at_ns = at_ns + 1 WHERE id = ?",
            [&row.id],
        )?;
        let state = read_dirty_window(&connection, table, "capture", CONSUMER)?;
        assert!(state.dirty);
        assert_eq!(state.full, full);
    }
    Ok(())
}

#[test]
fn generations_and_dirty_state_survive_reopen() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("mutation.db");
    {
        let connection = Connection::open(&path)?;
        let crud = system::Crud::new(&connection);
        crud.initialize()?;
        register(&connection)?;
        crud.person.insert(&Person {
            name: "Ada".into(),
            age: 10,
        })?;
    }
    let connection = Connection::open(&path)?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    assert_eq!(table_generations(&connection, &[TABLE])?[TABLE], 1);
    let state = read_dirty_window(&connection, TABLE, WINDOW, CONSUMER)?;
    assert!(state.dirty && !state.full);
    assert_eq!((state.lower, state.upper), (Some(10), Some(10)));
    Ok(())
}
