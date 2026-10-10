use std::cell::RefCell;

use proprdb_runtime::{Connection, Model, Result};
use proprdb_rust_tests::{generatedtest::example::Person, system};

const LEGACY_ID: &str = "legacy-id";

thread_local! {
    static STATEMENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn take_statements() -> Vec<String> {
    STATEMENTS.with_borrow_mut(std::mem::take)
}

#[test]
fn initialization_and_reads_trust_stored_data() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    let row = crud.person.insert(&Person {
        name: "Ada".into(),
        age: 37,
    })?;
    connection.execute(
        "UPDATE generatedtest_example_person SET id = ?, data = X'' WHERE id = ?",
        [LEGACY_ID, &row.id],
    )?;
    connection.execute(
        "INSERT INTO _deleted (table_name, id, at_ns) VALUES (?, ?, 1)",
        [system::PersonModel::TABLE_NAME, LEGACY_ID],
    )?;
    crud.person.initialize()?;
    crud.initialize()?;
    let stored = crud.person.select_by_id(LEGACY_ID)?.unwrap();
    assert_eq!(stored.id, LEGACY_ID);
    assert_eq!(stored.data, Person::default());
    assert!(crud.person.insert_with_id(LEGACY_ID, &row.data).is_err());
    connection.execute(
        "UPDATE _proprdb_schema SET schema_hash = 'name:string' WHERE table_name = ?",
        [system::PersonModel::TABLE_NAME],
    )?;
    crud.person.initialize()?;
    connection.execute(
        "UPDATE generatedtest_example_person SET data = X'ff' WHERE id = ?",
        [LEGACY_ID],
    )?;
    crud.initialize()?;
    Ok(())
}

#[test]
fn unchanged_initialization_only_inspects_metadata() -> Result<()> {
    for full_init in [false, true] {
        let sqlite = rusqlite::Connection::open_in_memory()?;
        sqlite.trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
            Some(|event| {
                if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
                    STATEMENTS.with_borrow_mut(|statements| statements.push(sql.to_owned()));
                }
            }),
        );
        let connection = Connection::from(sqlite);
        let crud = system::Crud::new(&connection);
        crud.initialize()?;
        let before = connection.total_changes();
        take_statements();
        if full_init {
            crud.initialize()?;
        } else {
            crud.person.initialize()?;
        }
        let statements = take_statements();
        assert_eq!(connection.total_changes(), before);
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS _deleted "))
                .count(),
            1
        );
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.starts_with(
                    "SELECT id, at_ns, deleted, data_json FROM _unknown_types WHERE type_name = ?"
                ))
                .count(),
            if full_init { 3 } else { 1 }
        );
        for statement in statements {
            for prefix in ["INSERT ", "UPDATE ", "CREATE INDEX ", "DROP INDEX "] {
                assert!(!statement.starts_with(prefix), "{statement}");
            }
            if statement.starts_with("SELECT ") && !statement.contains("pragma_") {
                assert!(statement.contains(" WHERE "), "{statement}");
            }
        }
    }
    Ok(())
}
