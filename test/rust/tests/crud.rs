use proprdb_runtime::{Change, Connection, Model, Result, Value};
use proprdb_rust_tests::{
    extra,
    generatedtest::example::{Choice, Note, Person, choice},
    rusttest::{Scalars, outer},
    system,
};

const PERSON_NAME: &str = "Ada";
const TEST_ID: &str = "01951d6e-a000-7000-8000-000000000001";
const NAME_AGE_PREDICATE: &str = "name = ? AND age = ?";
const TOMBSTONE_COUNT_SQL: &str = "SELECT COUNT(*) FROM _deleted WHERE table_name = ? AND id = ?";

fn person(name: &str, age: i64) -> Person {
    Person {
        name: name.into(),
        age,
    }
}

#[test]
fn crud_validation_indexes_listeners_and_statistics() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    crud.initialize()?;
    let changes = crud.person.listen()?;
    assert!(crud.person.insert(&Person::default()).is_err());
    assert!(changes.try_recv().is_err());
    let row = crud.person.insert(&person(PERSON_NAME, 35))?;
    assert_eq!(crud.person.select_by_id(&row.id)?, Some(row.clone()));
    assert_eq!(changes.recv().unwrap(), Change::Upsert(row.clone()));
    assert_eq!(
        crud.person.select(
            NAME_AGE_PREDICATE,
            &[Value::Text(PERSON_NAME.into()), Value::Integer(35)]
        )?,
        vec![row.clone()]
    );
    let statistics = crud.person.query_statistics(NAME_AGE_PREDICATE)?;
    assert_eq!(statistics.calls, 1);
    assert!(statistics.duration_sum_ns > 0);
    let updated = crud
        .person
        .update_by_id(&row.id, &person(PERSON_NAME, 36))?
        .unwrap();
    assert!(updated.at_ns > row.at_ns);
    assert_eq!(changes.recv().unwrap(), Change::Upsert(updated.clone()));
    assert!(
        crud.person
            .update_by_id("missing", &person(PERSON_NAME, 36))
            .is_err()
    );
    assert!(
        crud.person
            .update_by_id(&row.id, &Person::default())
            .is_err()
    );
    assert_eq!(crud.person.select_by_id(&row.id)?, Some(updated));
    let indexes: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND tbl_name = ? AND name LIKE ?",
        [system::PersonModel::TABLE_NAME, "idx_%"],
        |row| row.get(0),
    )?;
    assert_eq!(indexes, 2);
    assert!(crud.person.delete_by_id(&row.id)?);
    assert!(
        matches!(changes.recv().unwrap(), Change::Delete { id, at_ns } if id == row.id && at_ns > row.at_ns)
    );
    assert!(!crud.person.delete_by_id(&row.id)?);
    assert!(crud.person.select_by_id(&row.id)?.is_none());
    let tombstone: i64 = connection.query_row(
        "SELECT at_ns FROM _deleted WHERE table_name = ? AND id = ?",
        [system::PersonModel::TABLE_NAME, &row.id],
        |row| row.get(0),
    )?;
    assert!(tombstone > row.at_ns);
    assert!(crud.person.select("", &[]).is_err());
    assert!(
        crud.person
            .select("unknown = ?", &[Value::Integer(1)])
            .is_err()
    );
    Ok(())
}

#[test]
fn custom_ids_and_unsynced_deletes() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    for id in [
        "",
        "invalid",
        "01951D6E-A000-7000-8000-000000000001",
        "01951d6e-a000-4000-8000-000000000001",
    ] {
        assert!(
            crud.person
                .insert_with_id(id, &person(PERSON_NAME, 35))
                .is_err()
        );
    }
    let first = crud
        .person
        .insert_with_id(TEST_ID, &person(PERSON_NAME, 35))?;
    assert_eq!(first.id, TEST_ID);
    assert!(
        crud.person
            .insert_with_id(TEST_ID, &person(PERSON_NAME, 35))
            .is_err()
    );
    assert!(crud.person.delete_by_id(TEST_ID)?);
    let second = crud
        .person
        .insert_with_id(TEST_ID, &person(PERSON_NAME, 36))?;
    assert!(second.at_ns > first.at_ns);
    let count: i64 = connection.query_row(
        TOMBSTONE_COUNT_SQL,
        [system::PersonModel::TABLE_NAME, TEST_ID],
        |row| row.get(0),
    )?;
    assert_eq!(count, 0);
    let note = crud.note.insert(&Note {
        text: "local".into(),
    })?;
    assert!(crud.note.delete_by_id(&note.id)?);
    let count: i64 = connection.query_row(
        TOMBSTONE_COUNT_SQL,
        [system::NoteModel::TABLE_NAME, &note.id],
        |row| row.get(0),
    )?;
    assert_eq!(count, 0);
    Ok(())
}

#[test]
fn optional_oneof_nested_and_scalar_projections() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let row = crud.choice.insert(&Choice {
        selection: Some(choice::Selection::Label("selected".into())),
    })?;
    assert_eq!(
        crud.choice
            .select("label = ?", &[Value::Text("selected".into())])?,
        vec![row.clone()]
    );
    crud.choice.update_by_id(
        &row.id,
        &Choice {
            selection: Some(choice::Selection::Count(5)),
        },
    )?;
    assert_eq!(
        crud.choice
            .select("label IS NULL AND id = ?", &[Value::Text(row.id)])?
            .len(),
        1
    );
    let extra = extra::Crud::new(&connection);
    extra.initialize()?;
    for nick in [None, Some("nick".into())] {
        let data = Scalars {
            nick: nick.clone(),
            enabled: true,
            payload: vec![0, 1, 255],
            score: 1.5,
            amount: u32::MAX,
            state: 1,
            r#type: "test".into(),
        };
        let row = extra.scalars.insert(&data)?;
        assert_eq!(extra.scalars.select_by_id(&row.id)?.unwrap().data, data);
        let predicate = if nick.is_some() {
            "nick IS NOT NULL AND id = ?"
        } else {
            "nick IS NULL AND id = ?"
        };
        assert_eq!(
            extra
                .scalars
                .select(predicate, &[Value::Text(row.id)])?
                .len(),
            1
        );
    }
    let inner = outer::Inner {
        selection: Some(outer::inner::Selection::Blob(vec![1, 2, 3])),
    };
    let row = extra.outer_inner.insert(&inner)?;
    assert_eq!(
        extra
            .outer_inner
            .select("blob = ?", &[Value::Blob(vec![1, 2, 3])])?,
        vec![row.clone()]
    );
    extra.outer_inner.update_by_id(
        &row.id,
        &outer::Inner {
            selection: Some(outer::inner::Selection::Number(2.5)),
        },
    )?;
    assert_eq!(
        extra
            .outer_inner
            .select("number = ?", &[Value::Real(2.5)])?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn transaction_rollback_and_decode_errors() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    system::Crud::new(&connection).initialize()?;
    let id;
    {
        let transaction = connection.transaction()?;
        let crud = system::Crud::new(&transaction);
        id = crud.person.insert(&person(PERSON_NAME, 35))?.id;
        transaction.rollback()?;
    }
    let crud = system::Crud::new(&connection);
    assert!(crud.person.select_by_id(&id)?.is_none());
    let row = crud.person.insert(&person(PERSON_NAME, 35))?;
    connection.execute(
        "UPDATE generatedtest_example_person SET data = ? WHERE id = ?",
        (Value::Blob(vec![255]), row.id.as_str()),
    )?;
    assert!(crud.person.select_by_id(&row.id).is_err());
    Ok(())
}

#[test]
fn schema_reconciliation_backfills_and_preserves_rows() -> Result<()> {
    use prost::Message;
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("CREATE TABLE generatedtest_example_person (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, name TEXT NOT NULL DEFAULT '', obsolete TEXT)")?;
    let data = person(PERSON_NAME, 35);
    let mut data_bytes = data.encode_to_vec();
    data_bytes.extend([0xa0, 0x06, 0x01]);
    connection.execute(
        "INSERT INTO generatedtest_example_person (id, at_ns, data, name) VALUES (?, ?, ?, ?)",
        (TEST_ID, 42, data_bytes.clone(), PERSON_NAME),
    )?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let row = crud
        .person
        .select("age = ?", &[Value::Integer(35)])?
        .pop()
        .unwrap();
    assert_eq!(row.id, TEST_ID);
    assert_eq!(row.at_ns, 42);
    assert_eq!(row.data, data);
    let stored_bytes: Vec<u8> = connection.query_row(
        "SELECT data FROM generatedtest_example_person WHERE id = ?",
        [TEST_ID],
        |row| row.get(0),
    )?;
    assert_eq!(stored_bytes, data_bytes);
    assert!(
        connection
            .prepare("SELECT obsolete FROM generatedtest_example_person WHERE id = ?")
            .is_err()
    );
    Ok(())
}

#[test]
fn incompatible_schema_rolls_back_initialization() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("CREATE TABLE generatedtest_example_person (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, name INTEGER NOT NULL DEFAULT 0)")?;
    let crud = system::Crud::new(&connection);
    assert!(crud.initialize().is_err());
    assert!(
        connection
            .prepare("SELECT age FROM generatedtest_example_person WHERE id = ?")
            .is_err()
    );
    Ok(())
}

#[test]
fn legacy_oneof_presence_is_repaired() -> Result<()> {
    use prost::Message;
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("CREATE TABLE generatedtest_example_choice (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, label TEXT NOT NULL DEFAULT '')")?;
    let data = Choice {
        selection: Some(choice::Selection::Count(7)),
    };
    connection.execute(
        "INSERT INTO generatedtest_example_choice (id, at_ns, data) VALUES (?, ?, ?)",
        (TEST_ID, 42, data.encode_to_vec()),
    )?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    assert_eq!(
        crud.choice
            .select("label IS NULL AND id = ?", &[Value::Text(TEST_ID.into())])?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn protobuf_kind_changes_are_rejected() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    connection.execute(
        "UPDATE _proprdb_schema SET schema_hash = ? WHERE table_name = ?",
        ("name:string;age:int32", system::PersonModel::TABLE_NAME),
    )?;
    assert!(crud.initialize().is_err());
    let schema: String = connection.query_row(
        "SELECT schema_hash FROM _proprdb_schema WHERE table_name = ?",
        [system::PersonModel::TABLE_NAME],
        |row| row.get(0),
    )?;
    assert_eq!(schema, "name:string;age:int32");
    Ok(())
}
