package genexample

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

const (
	photoLocationIndex = "idx_generatedtest_example_photo__location_lon_location_lat"
	photoProjectionSQL = `SELECT location_lon, location_lat, exif_create_utc_time_seconds, exif_modify_utc_time_seconds, location_altitude, location_label, selected_location_lon, location_next_lon FROM "generatedtest_example_photo" WHERE id = ?`
	oldPhotoTableSQL   = `CREATE TABLE "generatedtest_example_photo" (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL)`
	insertOldPhotoSQL  = `INSERT INTO "generatedtest_example_photo" (id, at_ns, data) VALUES (?, ?, ?)`
)

func newProjectionDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	return db
}

func assertPhotoProjections(t *testing.T, db *sql.DB, id string, expected []any) {
	t.Helper()
	values := make([]any, len(expected))
	destinations := make([]any, len(expected))
	for index := range values {
		destinations[index] = &values[index]
	}
	assert.NilError(t, db.QueryRow(photoProjectionSQL, id).Scan(destinations...))
	assert.DeepEqual(t, values, expected)
}

func TestNestedProjectionPresenceAndWrites(t *testing.T) {
	cases := []struct {
		name     string
		data     *Photo
		expected []any
	}{
		{"missing", &Photo{}, []any{nil, nil, nil, nil, nil, nil, nil, nil}},
		{"zero coordinates", &Photo{Location: &Location{}}, []any{float64(0), float64(0), nil, nil, nil, nil, nil, nil}},
		{"missing inner timestamp", &Photo{ExifCreate: &ZonedTimestamp{}}, []any{nil, nil, nil, nil, nil, nil, nil, nil}},
		{"epoch", &Photo{ExifCreate: &ZonedTimestamp{UtcTime: &timestamppb.Timestamp{}}}, []any{nil, nil, int64(0), nil, nil, nil, nil, nil}},
		{"present optional and oneofs", &Photo{Location: &Location{Lon: 12, Lat: -5, Altitude: proto.Int64(0), Description: &Location_Label{Label: ""}, Next: &Location{}}, Selection: &Photo_SelectedLocation{SelectedLocation: &Location{}}, ExifModify: &ZonedTimestamp{UtcTime: &timestamppb.Timestamp{Seconds: 42}}}, []any{float64(12), float64(-5), nil, int64(42), int64(0), "", float64(0), float64(0)}},
		{"other oneof cases", &Photo{Location: &Location{Description: &Location_Code{Code: 4}}, Selection: &Photo_Other{Other: "other"}}, []any{float64(0), float64(0), nil, nil, nil, nil, nil, nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newProjectionDB(t)
			crud := NewCRUD(rt.WrapDB(db))
			assert.NilError(t, crud.Init())
			row, err := crud.Photo.Insert(tc.data)
			assert.NilError(t, err)
			assertPhotoProjections(t, db, row.ID, tc.expected)
			selected, err := crud.Photo.Select(selectByIDSQL, row.ID)
			assert.NilError(t, err)
			assert.Assert(t, is.Len(selected, 1))
			assert.Assert(t, proto.Equal(selected[0].Data, tc.data))
			updated, err := crud.Photo.UpdateByID(row.ID, &Photo{})
			assert.NilError(t, err)
			assert.Check(t, updated.AtNs > row.AtNs)
			assertPhotoProjections(t, db, row.ID, cases[0].expected)
		})
	}
}

func TestNestedProjectionBackfillPreservesPayloadAndSync(t *testing.T) {
	db := newProjectionDB(t)
	q := rt.WrapDB(db)
	assert.NilError(t, rt.EnsureCoreTables(q))
	_, err := db.Exec(oldPhotoTableSQL)
	assert.NilError(t, err)
	data := &Photo{Location: &Location{}, ExifCreate: &ZonedTimestamp{UtcTime: &timestamppb.Timestamp{Seconds: 42}}}
	payload, err := proto.Marshal(data)
	assert.NilError(t, err)
	payload = append(payload, 0xa0, 0x06, 0x01)
	_, err = db.Exec(insertOldPhotoSQL, validationUUIDv7, int64(7), payload)
	assert.NilError(t, err)
	_, err = db.Exec(`INSERT INTO _sync (remote, table_name, at_ns, object_id) VALUES (?, ?, ?, ?)`, testRemoteA, PhotoTableName, int64(7), validationUUIDv7)
	assert.NilError(t, err)
	table := NewPhotoTable(q)
	for range 2 {
		assert.NilError(t, table.Init())
	}
	assertPhotoProjections(t, db, validationUUIDv7, []any{float64(0), float64(0), int64(42), nil, nil, nil, nil, nil})
	var stored []byte
	var atNs int64
	assert.NilError(t, db.QueryRow(`SELECT data, at_ns FROM "generatedtest_example_photo" WHERE id = ?`, validationUUIDv7).Scan(&stored, &atNs))
	assert.DeepEqual(t, stored, payload)
	assert.Equal(t, atNs, int64(7))
	assert.NilError(t, db.QueryRow(`SELECT at_ns FROM _sync WHERE remote = ? AND table_name = ?`, testRemoteA, PhotoTableName).Scan(&atNs))
	assert.Equal(t, atNs, int64(7))
	indexes := tableIndexNamesByName(context.Background(), t, db, PhotoTableName)
	assert.Check(t, indexes[photoLocationIndex])
	selected, err := table.Select("location_lon = ? AND location_lat = ? AND exif_create_utc_time_seconds = ?", 0, 0, 42)
	assert.NilError(t, err)
	assert.Check(t, is.Len(selected, 1))
}

func TestNestedProjectionBackfillRollback(t *testing.T) {
	db := newProjectionDB(t)
	q := rt.WrapDB(db)
	assert.NilError(t, rt.EnsureCoreTables(q))
	_, err := db.Exec(oldPhotoTableSQL)
	assert.NilError(t, err)
	_, err = db.Exec(insertOldPhotoSQL, validationUUIDv7, int64(7), []byte{0xff})
	assert.NilError(t, err)
	assert.ErrorContains(t, NewPhotoTable(q).Init(), "unmarshal reprojection")
	var count int
	assert.NilError(t, db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, PhotoTableName, "location_lon").Scan(&count))
	assert.Equal(t, count, 0)
	assert.NilError(t, db.QueryRow(`SELECT COUNT(*) FROM _proprdb_schema WHERE table_name = ?`, PhotoTableName).Scan(&count))
	assert.Equal(t, count, 0)
	assert.NilError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND tbl_name = ? AND name LIKE 'idx_%'`, PhotoTableName).Scan(&count))
	assert.Equal(t, count, 0)
}

func TestNestedProjectionSyncAndTransactionRollback(t *testing.T) {
	sourceDB := newProjectionDB(t)
	targetDB := newProjectionDB(t)
	source := NewCRUD(rt.WrapDB(sourceDB))
	target := NewCRUD(rt.WrapDB(targetDB))
	assert.NilError(t, source.Init())
	assert.NilError(t, target.Init())
	row, err := source.Photo.Insert(&Photo{Location: &Location{}, ExifCreate: &ZonedTimestamp{UtcTime: &timestamppb.Timestamp{}}})
	assert.NilError(t, err)
	var output bytes.Buffer
	assert.NilError(t, source.WriteJSONL("", &output))
	assert.NilError(t, target.ReadJSONL(testRemoteA, &output))
	expected := []any{float64(0), float64(0), int64(0), nil, nil, nil, nil, nil}
	assertPhotoProjections(t, targetDB, row.ID, expected)
	tx, err := targetDB.Begin()
	assert.NilError(t, err)
	_, err = NewPhotoTable(rt.WrapTx(tx)).UpdateByID(row.ID, &Photo{})
	assert.NilError(t, err)
	assert.NilError(t, tx.Rollback())
	assertPhotoProjections(t, targetDB, row.ID, expected)
	_, err = source.Photo.UpdateByID(row.ID, &Photo{})
	assert.NilError(t, err)
	assert.NilError(t, source.WriteJSONL("", &output))
	assert.NilError(t, target.ReadJSONL(testRemoteA, &output))
	assertPhotoProjections(t, targetDB, row.ID, []any{nil, nil, nil, nil, nil, nil, nil, nil})
}

func TestNestedProjectionUnknownRowsDrain(t *testing.T) {
	db := newProjectionDB(t)
	q := rt.WrapDB(db)
	assert.NilError(t, rt.EnsureCoreTables(q))
	dataJSON, err := rt.MarshalAnyJSON(&Photo{Location: &Location{}})
	assert.NilError(t, err)
	_, err = db.Exec(insertUnknownRowSQL, PhotoTypeName, validationUUIDv7, int64(7), false, string(dataJSON))
	assert.NilError(t, err)
	assert.NilError(t, NewPhotoTable(q).Init())
	assertPhotoProjections(t, db, validationUUIDv7, []any{float64(0), float64(0), nil, nil, nil, nil, nil, nil})
	var count int
	assert.NilError(t, db.QueryRow(selectUnknownCountByIDSQL, PhotoTypeName, validationUUIDv7).Scan(&count))
	assert.Equal(t, count, 0)
}

func TestNestedProjectionSharedJSONLFixture(t *testing.T) {
	sourceDB := newProjectionDB(t)
	targetDB := newProjectionDB(t)
	source := NewCRUD(rt.WrapDB(sourceDB))
	target := NewCRUD(rt.WrapDB(targetDB))
	assert.NilError(t, source.Init())
	assert.NilError(t, target.Init())
	fixture, err := os.ReadFile("../testdata/nested-photo.jsonl")
	assert.NilError(t, err)
	assert.NilError(t, source.ReadJSONL(testRemoteA, bytes.NewReader(fixture)))
	const fixtureID = "01951d6e-a000-7000-8000-000000000003"
	expected := []any{float64(0), float64(0), int64(0), int64(42), int64(0), "", float64(0), float64(0)}
	assertPhotoProjections(t, sourceDB, fixtureID, expected)
	var output bytes.Buffer
	assert.NilError(t, source.WriteJSONL("", &output))
	assert.NilError(t, target.ReadJSONL(testRemoteA, &output))
	assertPhotoProjections(t, targetDB, fixtureID, expected)
}

func TestNestedProjectionRejectsSourcePathChanges(t *testing.T) {
	const desired = "location_lon:double:optional:path=location.lon"
	for _, previous := range []string{"location_lon:double:optional", "location_lon:double:optional:path=other.lon"} {
		t.Run(previous, func(t *testing.T) {
			db := newProjectionDB(t)
			q := rt.WrapDB(db)
			table := NewPhotoTable(q)
			assert.NilError(t, table.Init())
			previousSchema := strings.ReplaceAll(PhotoProjectionSchema, desired, previous)
			_, err := db.Exec(`UPDATE _proprdb_schema SET schema_hash = ? WHERE table_name = ?`, previousSchema, PhotoTableName)
			assert.NilError(t, err)
			assert.ErrorContains(t, table.Init(), "changed protobuf path")
			var stored string
			assert.NilError(t, db.QueryRow(`SELECT schema_hash FROM _proprdb_schema WHERE table_name = ?`, PhotoTableName).Scan(&stored))
			assert.Equal(t, stored, previousSchema)
			assert.Check(t, tableIndexNamesByName(context.Background(), t, db, PhotoTableName)[photoLocationIndex])
		})
	}
}
