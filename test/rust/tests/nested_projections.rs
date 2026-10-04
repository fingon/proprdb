use proprdb_runtime::{Connection, Model, Result, Value};
use proprdb_rust_tests::{
    generatedtest::example::{Location, Photo, ZonedTimestamp, location, photo},
    system,
};
use prost::Message;

const TEST_ID: &str = "01951d6e-a000-7000-8000-000000000003";
const PROJECTION_SQL: &str = "SELECT location_lon, location_lat, exif_create_utc_time_seconds, exif_modify_utc_time_seconds, location_altitude, location_label, selected_location_lon, location_next_lon FROM generatedtest_example_photo WHERE id = ?";
const OLD_TABLE_SQL: &str = "CREATE TABLE generatedtest_example_photo (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL)";
const OLD_INSERT_SQL: &str =
    "INSERT INTO generatedtest_example_photo (id, at_ns, data) VALUES (?, ?, ?)";

fn projections(connection: &Connection, id: &str) -> Result<Vec<Value>> {
    Ok(connection.query_row(PROJECTION_SQL, [id], |row| {
        (0..8).map(|index| row.get(index)).collect()
    })?)
}

fn epoch_photo() -> Photo {
    Photo {
        location: Some(Location::default()),
        exif_create: Some(ZonedTimestamp {
            utc_time: Some(prost_types::Timestamp::default()),
        }),
        ..Photo::default()
    }
}

#[test]
fn presence_writes_and_rollback() -> Result<()> {
    let mut detailed = epoch_photo();
    let location = detailed.location.as_mut().unwrap();
    location.altitude = Some(0);
    location.description = Some(location::Description::Label(String::new()));
    location.next = Some(Box::new(Location::default()));
    detailed.selection = Some(photo::Selection::SelectedLocation(Location::default()));
    let cases = [
        (Photo::default(), vec![Value::Null; 8]),
        (
            epoch_photo(),
            vec![
                Value::Real(0.0),
                Value::Real(0.0),
                Value::Integer(0),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        ),
        (
            Photo {
                exif_create: Some(ZonedTimestamp::default()),
                ..Photo::default()
            },
            vec![Value::Null; 8],
        ),
        (
            detailed,
            vec![
                Value::Real(0.0),
                Value::Real(0.0),
                Value::Integer(0),
                Value::Null,
                Value::Integer(0),
                Value::Text(String::new()),
                Value::Real(0.0),
                Value::Real(0.0),
            ],
        ),
    ];
    for (data, expected) in cases {
        let connection = Connection::open_in_memory()?;
        let crud = system::Crud::new(&connection);
        crud.initialize()?;
        let row = crud.photo.insert(&data)?;
        assert_eq!(projections(&connection, &row.id)?, expected);
        let transaction = connection.transaction()?;
        crud.photo.update_by_id(&row.id, &Photo::default())?;
        transaction.rollback()?;
        assert_eq!(projections(&connection, &row.id)?, expected);
        crud.photo.update_by_id(&row.id, &Photo::default())?;
        assert_eq!(projections(&connection, &row.id)?, vec![Value::Null; 8]);
    }
    Ok(())
}

#[test]
fn backfill_preserves_bytes_and_timestamps() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(OLD_TABLE_SQL)?;
    let mut bytes = epoch_photo().encode_to_vec();
    bytes.extend([0xa0, 0x06, 0x01]);
    connection.execute(OLD_INSERT_SQL, (TEST_ID, 42, bytes.clone()))?;
    let crud = system::Crud::new(&connection);
    crud.initialize()?;
    crud.initialize()?;
    let stored: (i64, Vec<u8>) = connection.query_row(
        "SELECT at_ns, data FROM generatedtest_example_photo WHERE id = ?",
        [TEST_ID],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(stored, (42, bytes));
    assert_eq!(projections(&connection, TEST_ID)?[2], Value::Integer(0));
    assert_eq!(
        crud.photo
            .select(
                "location_lon = ? AND location_lat = ?",
                &[Value::Real(0.0), Value::Real(0.0)]
            )?
            .len(),
        1
    );
    let columns: String = connection.query_row(
        "SELECT group_concat(name) FROM pragma_index_info(?)",
        ["idx_generatedtest_example_photo__location_lon_location_lat"],
        |row| row.get(0),
    )?;
    assert_eq!(columns, "location_lon,location_lat");
    Ok(())
}

#[test]
fn corrupt_backfill_rolls_back_schema() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(OLD_TABLE_SQL)?;
    connection.execute(OLD_INSERT_SQL, (TEST_ID, 42, vec![0xff_u8]))?;
    let crud = system::Crud::new(&connection);
    assert!(crud.initialize().is_err());
    assert!(
        connection
            .prepare("SELECT location_lon FROM generatedtest_example_photo WHERE id = ?")
            .is_err()
    );
    Ok(())
}

#[test]
fn jsonl_import_projects_nested_values() -> Result<()> {
    let source_connection = Connection::open_in_memory()?;
    let target_connection = Connection::open_in_memory()?;
    let source = system::Crud::new(&source_connection);
    let target = system::Crud::new(&target_connection);
    source.initialize()?;
    target.initialize()?;
    let row = source.photo.insert(&epoch_photo())?;
    let mut bytes = Vec::new();
    source.write_jsonl("", &mut bytes)?;
    target.read_jsonl("source", std::io::Cursor::new(bytes))?;
    assert_eq!(
        projections(&target_connection, &row.id)?,
        projections(&source_connection, &row.id)?
    );
    assert_eq!(
        system::PhotoModel::TABLE_NAME,
        "generatedtest_example_photo"
    );
    Ok(())
}

#[test]
fn shared_jsonl_fixture_round_trip() -> Result<()> {
    let source_connection = Connection::open_in_memory()?;
    let target_connection = Connection::open_in_memory()?;
    let source = system::Crud::new(&source_connection);
    let target = system::Crud::new(&target_connection);
    source.initialize()?;
    target.initialize()?;
    source.read_jsonl(
        "source",
        std::io::Cursor::new(include_bytes!("../../testdata/nested-photo.jsonl")),
    )?;
    let expected = vec![
        Value::Real(0.0),
        Value::Real(0.0),
        Value::Integer(0),
        Value::Integer(42),
        Value::Integer(0),
        Value::Text(String::new()),
        Value::Real(0.0),
        Value::Real(0.0),
    ];
    assert_eq!(projections(&source_connection, TEST_ID)?, expected);
    let mut bytes = Vec::new();
    source.write_jsonl("", &mut bytes)?;
    target.read_jsonl("source", std::io::Cursor::new(bytes))?;
    assert_eq!(projections(&target_connection, TEST_ID)?, expected);
    Ok(())
}

#[test]
fn source_path_changes_are_rejected() -> Result<()> {
    for previous in [
        "location_lon:double:optional",
        "location_lon:double:optional:path=other.lon",
    ] {
        let connection = Connection::open_in_memory()?;
        let crud = system::Crud::new(&connection);
        crud.initialize()?;
        let schema = system::PhotoModel::PROJECTION_SCHEMA
            .replace("location_lon:double:optional:path=location.lon", previous);
        connection.execute(
            "UPDATE _proprdb_schema SET schema_hash = ? WHERE table_name = ?",
            [&schema, system::PhotoModel::TABLE_NAME],
        )?;
        assert!(crud.initialize().is_err());
        let stored: String = connection.query_row(
            "SELECT schema_hash FROM _proprdb_schema WHERE table_name = ?",
            [system::PhotoModel::TABLE_NAME],
            |row| row.get(0),
        )?;
        assert_eq!(stored, schema);
    }
    Ok(())
}
