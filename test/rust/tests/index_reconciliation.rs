use std::{cell::RefCell, collections::BTreeSet};

use proprdb_runtime::{Column, Connection, MessageDescriptor, Model, Result, Table, Value};
use proprdb_rust_tests::{
    generatedtest::example::{Choice, Person, choice},
    system::{self, ChoiceModel, PersonModel},
};
use prost::Message;

const PROJECTED_AGE: i64 = 37;
const TEST_ID: &str = "01951d6e-a000-7000-8000-000000000001";
const PERSON_NAME_INDEX: &str = "idx_generatedtest_example_person__name";
const PERSON_NAME_AGE_INDEX: &str = "idx_generatedtest_example_person__name_age";
const PERSON_TIME_INDEX: &str = "idx_generatedtest_example_person__at_ns";
const PERSON_NAME_TIME_INDEX: &str = "idx_generatedtest_example_person__name_at_ns";
const PERSON_TIME_ID_INDEX: &str = "idx_generatedtest_example_person__at_ns_id";
const STALE_PERSON_INDEX: &str = "idx_generatedtest_example_person__stale";
const APPLICATION_PERSON_INDEX: &str = "application_person_age";
const ADD_OBSOLETE_SQL: &str = "ALTER TABLE generatedtest_example_person ADD COLUMN obsolete TEXT";
const STALE_INDEX_SQL: &str = "CREATE INDEX idx_generatedtest_example_person__stale ON generatedtest_example_person (obsolete)";
const APPLICATION_INDEX_SQL: &str =
    "CREATE INDEX application_person_age ON generatedtest_example_person (age)";
const DROP_STALE_SQL: &str = "DROP INDEX \"idx_generatedtest_example_person__stale\"";
const COLUMN_COUNT_SQL: &str = "SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = 'obsolete'";
const SCHEMA_SQL: &str = "SELECT schema_hash FROM _proprdb_schema WHERE table_name = ?";
const CHOICE_LABEL_INDEX: &str = "idx_generatedtest_example_choice__label";
const CHOICE_TIME_INDEX: &str = "idx_generatedtest_example_choice__at_ns";
const CHOICE_LABEL_SQL: &str = "CREATE INDEX IF NOT EXISTS \"idx_generatedtest_example_choice__label\" ON \"generatedtest_example_choice\" (\"label\")";
const CHOICE_TIME_SQL: &str = "CREATE INDEX IF NOT EXISTS \"idx_generatedtest_example_choice__at_ns\" ON \"generatedtest_example_choice\" (\"at_ns\")";

thread_local! {
    static INDEX_DDL: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn record_statement(sql: &str) {
    if sql.starts_with("CREATE INDEX ") || sql.starts_with("DROP INDEX ") {
        INDEX_DDL.with_borrow_mut(|statements| statements.push(sql.to_owned()));
    }
}

fn take_index_ddl() -> Vec<String> {
    INDEX_DDL.with_borrow_mut(std::mem::take)
}

fn traced_connection() -> Result<Connection> {
    let sqlite = rusqlite::Connection::open_in_memory()?;
    sqlite.trace_v2(
        rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
        Some(|event| {
            if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
                record_statement(sql);
            }
        }),
    );
    take_index_ddl();
    Ok(Connection::from(sqlite))
}

fn index_names(connection: &Connection, table: &str) -> Result<BTreeSet<String>> {
    let mut statement = connection.prepare("SELECT name FROM pragma_index_list(?)")?;
    Ok(statement
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<_, _>>()?)
}

#[test]
fn generated_indexes_are_reconciled_selectively() -> Result<()> {
    let cases: &[(&str, &[&str], bool, &[&str])] = &[
        ("unchanged table", &[], false, &[]),
        ("unchanged CRUD", &[], true, &[]),
        (
            "missing timestamp index",
            &["DROP INDEX idx_generatedtest_example_person__at_ns"],
            false,
            &[PersonModel::INDEXES[2]],
        ),
        (
            "missing index",
            &["DROP INDEX idx_generatedtest_example_person__name"],
            false,
            &[PersonModel::INDEXES[0]],
        ),
        (
            "stale index and obsolete column",
            &[ADD_OBSOLETE_SQL, STALE_INDEX_SQL],
            false,
            &[DROP_STALE_SQL],
        ),
        (
            "stale index on current column",
            &[
                "CREATE INDEX idx_generatedtest_example_person__stale ON generatedtest_example_person (name)",
            ],
            false,
            &[DROP_STALE_SQL],
        ),
        (
            "reprojection",
            &[
                "UPDATE _proprdb_schema SET schema_hash = 'name:string' WHERE table_name = 'generatedtest_example_person'",
                "UPDATE generatedtest_example_person SET age = 0 WHERE name = 'Ada'",
            ],
            false,
            &[],
        ),
    ];
    for (name, setup, full_init, expected) in cases {
        let connection = traced_connection()?;
        let crud = system::Crud::new(&connection);
        crud.initialize()?;
        let initial = take_index_ddl();
        for sql in PersonModel::INDEXES {
            assert!(initial.iter().any(|actual| actual == sql), "{name}");
        }
        let row = crud.person.insert(&Person {
            name: "Ada".into(),
            age: PROJECTED_AGE,
        })?;
        connection.execute_batch(APPLICATION_INDEX_SQL)?;
        for sql in *setup {
            connection.execute_batch(sql)?;
        }
        take_index_ddl();
        if *full_init {
            crud.initialize()?;
        } else {
            crud.person.initialize()?;
        }
        assert_eq!(take_index_ddl(), *expected, "{name}");
        let indexes = index_names(&connection, PersonModel::TABLE_NAME)?;
        for index in [
            PERSON_NAME_INDEX,
            PERSON_NAME_AGE_INDEX,
            APPLICATION_PERSON_INDEX,
        ] {
            assert!(indexes.contains(index), "{name}: {index}");
        }
        for (index, expected) in [
            (PERSON_TIME_INDEX, "at_ns"),
            (PERSON_NAME_TIME_INDEX, "name,at_ns"),
            (PERSON_TIME_ID_INDEX, "at_ns,id"),
        ] {
            let columns: String = connection.query_row("SELECT group_concat(name) FROM (SELECT name FROM pragma_index_info(?) ORDER BY seqno)", [index], |row| row.get(0))?;
            assert_eq!(columns, expected, "{name}");
        }
        assert!(!indexes.contains(STALE_PERSON_INDEX), "{name}");
        let obsolete_count: i64 =
            connection.query_row(COLUMN_COUNT_SQL, [PersonModel::TABLE_NAME], |row| {
                row.get(0)
            })?;
        assert_eq!(obsolete_count, 0, "{name}");
        let age: i64 = connection.query_row(
            "SELECT age FROM generatedtest_example_person WHERE id = ?",
            [&row.id],
            |row| row.get(0),
        )?;
        assert_eq!(age, PROJECTED_AGE, "{name}");
        crud.person.initialize()?;
        assert!(take_index_ddl().is_empty(), "{name}");
    }
    Ok(())
}

struct IndexedChoice;

impl Model for IndexedChoice {
    type Data = Choice;
    const TABLE_NAME: &str = ChoiceModel::TABLE_NAME;
    const TYPE_NAME: &str = ChoiceModel::TYPE_NAME;
    const PROJECTION_SCHEMA: &str = ChoiceModel::PROJECTION_SCHEMA;
    const CREATE_TABLE_SQL: &str = ChoiceModel::CREATE_TABLE_SQL;
    const INSERT_SQL: &str = ChoiceModel::INSERT_SQL;
    const UPSERT_SQL: &str = ChoiceModel::UPSERT_SQL;
    const INDEX_PREFIX: &str = ChoiceModel::INDEX_PREFIX;
    const SYNC_ENABLED: bool = ChoiceModel::SYNC_ENABLED;
    const CHANGE_LISTENERS: bool = ChoiceModel::CHANGE_LISTENERS;
    const QUERY_STATISTICS: bool = ChoiceModel::QUERY_STATISTICS;
    const COLUMNS: &[Column] = ChoiceModel::COLUMNS;
    const INDEXES: &[&str] = &[CHOICE_LABEL_SQL, CHOICE_TIME_SQL];
    fn descriptor() -> Result<MessageDescriptor> {
        ChoiceModel::descriptor()
    }
    fn projected_values(data: &Choice) -> Vec<Value> {
        ChoiceModel::projected_values(data)
    }
}

#[test]
fn indexed_oneof_repair_preserves_unrelated_indexes() -> Result<()> {
    let connection = traced_connection()?;
    system::Crud::new(&connection).initialize()?;
    connection.execute_batch("DROP TABLE generatedtest_example_choice; CREATE TABLE generatedtest_example_choice (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, label TEXT NOT NULL DEFAULT ''); UPDATE _proprdb_schema SET schema_hash = 'label:string' WHERE table_name = 'generatedtest_example_choice'")?;
    let choice = Choice {
        selection: Some(choice::Selection::Count(7)),
    };
    connection.execute(
        "INSERT INTO generatedtest_example_choice (id, at_ns, data) VALUES (?, ?, ?)",
        rusqlite::params![TEST_ID, 1, choice.encode_to_vec()],
    )?;
    for sql in IndexedChoice::INDEXES {
        connection.execute_batch(sql)?;
    }
    take_index_ddl();
    let table = Table::<IndexedChoice>::new(&connection);
    table.initialize()?;
    assert_eq!(
        take_index_ddl(),
        [
            format!("DROP INDEX \"{CHOICE_LABEL_INDEX}\""),
            CHOICE_LABEL_SQL.to_owned()
        ]
    );
    let absent: bool = connection.query_row(
        "SELECT label IS NULL FROM generatedtest_example_choice WHERE id = ?",
        [TEST_ID],
        |row| row.get(0),
    )?;
    assert!(absent);
    let indexes = index_names(&connection, IndexedChoice::TABLE_NAME)?;
    assert!(indexes.contains(CHOICE_LABEL_INDEX));
    assert!(indexes.contains(CHOICE_TIME_INDEX));
    table.initialize()?;
    assert!(take_index_ddl().is_empty());
    Ok(())
}

#[test]
fn failed_reprojection_rolls_back_indexes_and_columns() -> Result<()> {
    let connection = traced_connection()?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    for sql in [ADD_OBSOLETE_SQL, STALE_INDEX_SQL, APPLICATION_INDEX_SQL] {
        connection.execute_batch(sql)?;
    }
    connection.execute(
        "INSERT INTO generatedtest_example_person (id, at_ns, data) VALUES (?, ?, ?)",
        rusqlite::params![TEST_ID, 1, vec![0xff_u8]],
    )?;
    let indexes_before = index_names(&connection, PersonModel::TABLE_NAME)?;
    let schema_before: String =
        connection.query_row(SCHEMA_SQL, [PersonModel::TABLE_NAME], |row| row.get(0))?;
    take_index_ddl();
    assert!(crud.person.initialize().is_err());
    assert_eq!(take_index_ddl(), [DROP_STALE_SQL]);
    assert_eq!(
        index_names(&connection, PersonModel::TABLE_NAME)?,
        indexes_before
    );
    let count: i64 = connection.query_row(COLUMN_COUNT_SQL, [PersonModel::TABLE_NAME], |row| {
        row.get(0)
    })?;
    assert_eq!(count, 1);
    let schema_after: String =
        connection.query_row(SCHEMA_SQL, [PersonModel::TABLE_NAME], |row| row.get(0))?;
    assert_eq!(schema_after, schema_before);
    Ok(())
}
