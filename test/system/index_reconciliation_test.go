package genexample

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

const (
	testProjectedAge          = int64(37)
	projectionColumnCountSQL  = `SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`
	corruptPersonAgeSQL       = `UPDATE "generatedtest_example_person" SET age = 0 WHERE name = 'Ada'`
	applicationPersonIndex    = "application_person_age"
	obsoletePersonColumn      = "obsolete"
	obsoletePersonSQL         = `ALTER TABLE "generatedtest_example_person" ADD COLUMN "obsolete" TEXT`
	stalePersonIndexSQL       = `CREATE INDEX "idx_generatedtest_example_person__stale" ON "generatedtest_example_person" ("obsolete")`
	applicationPersonIndexSQL = `CREATE INDEX "application_person_age" ON "generatedtest_example_person" ("age")`
	choiceLabelIndex          = "idx_generatedtest_example_choice__label"
	choiceTimeIndex           = "idx_generatedtest_example_choice__at_ns"
	choiceLabelIndexSQL       = `CREATE INDEX "idx_generatedtest_example_choice__label" ON "generatedtest_example_choice" ("label")`
	choiceTimeIndexSQL        = `CREATE INDEX "idx_generatedtest_example_choice__at_ns" ON "generatedtest_example_choice" ("at_ns")`
	readPersonSchemaSQL       = `SELECT schema_hash FROM _proprdb_schema WHERE table_name = ?`
	stalePersonSchemaSQL      = `UPDATE _proprdb_schema SET schema_hash = 'stale' WHERE table_name = ?`
)

type recordingDBTX struct {
	rt.DBTX
	statements *[]string
}

func (q recordingDBTX) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	*q.statements = append(*q.statements, query)
	return q.DBTX.ExecContext(ctx, query, args...)
}

func (q recordingDBTX) WithTransaction(ctx context.Context, body func(rt.DBTX) error) error {
	return q.DBTX.WithTransaction(ctx, func(tx rt.DBTX) error {
		return body(recordingDBTX{DBTX: tx, statements: q.statements})
	})
}

func recordedIndexDDL(statements []string) []string {
	result := []string{}
	for _, statement := range statements {
		if strings.HasPrefix(statement, "CREATE INDEX ") || strings.HasPrefix(statement, "DROP INDEX ") {
			result = append(result, statement)
		}
	}
	return result
}

func TestGeneratedIndexReconciliation(t *testing.T) {
	cases := []struct {
		name        string
		setupSQL    []string
		fullInit    bool
		expectedDDL []string
	}{
		{name: "unchanged table"},
		{name: "unchanged CRUD", fullInit: true},
		{name: "missing index", setupSQL: []string{`DROP INDEX "` + personNameIndex + `"`}, expectedDDL: []string{PersonCreateIndexSQL1}},
		{name: "stale index and obsolete column", setupSQL: []string{obsoletePersonSQL, stalePersonIndexSQL}, expectedDDL: []string{`DROP INDEX "` + personStaleIndex + `"`}},
		{name: "stale index on current column", setupSQL: []string{`CREATE INDEX "` + personStaleIndex + `" ON "` + PersonTableName + `" ("name")`}, expectedDDL: []string{`DROP INDEX "` + personStaleIndex + `"`}},
		{name: "reprojection", setupSQL: []string{stalePersonSchemaSQL, corruptPersonAgeSQL}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := newProjectionDB(t)
			statements := []string{}
			q := recordingDBTX{DBTX: rt.WrapDB(db), statements: &statements}
			crud := NewCRUD(q)
			assert.NilError(t, crud.Init())
			initialDDL := recordedIndexDDL(statements)
			assert.Check(t, strings.Contains(strings.Join(initialDDL, "\n"), PersonCreateIndexSQL1))
			assert.Check(t, strings.Contains(strings.Join(initialDDL, "\n"), PersonCreateIndexSQL2))
			row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: testProjectedAge})
			assert.NilError(t, err)
			_, err = db.Exec(applicationPersonIndexSQL)
			assert.NilError(t, err)
			for _, statement := range tc.setupSQL {
				if statement == stalePersonSchemaSQL {
					_, err = db.Exec(statement, PersonTableName)
				} else {
					_, err = db.Exec(statement)
				}
				assert.NilError(t, err)
			}
			statements = nil
			if tc.fullInit {
				assert.NilError(t, crud.Init())
			} else {
				assert.NilError(t, crud.Person.Init())
			}
			expected := tc.expectedDDL
			if expected == nil {
				expected = []string{}
			}
			assert.DeepEqual(t, recordedIndexDDL(statements), expected)
			indexes := tableIndexNamesByName(ctx, t, db, PersonTableName)
			assert.Check(t, indexes[personNameIndex])
			assert.Check(t, indexes[personNameAgeIndex])
			assert.Check(t, indexes[applicationPersonIndex])
			assert.Check(t, !indexes[personStaleIndex])
			var obsoleteCount int
			assert.NilError(t, db.QueryRow(projectionColumnCountSQL, PersonTableName, obsoletePersonColumn).Scan(&obsoleteCount))
			assert.Equal(t, obsoleteCount, 0)
			var age int64
			assert.NilError(t, db.QueryRow(`SELECT age FROM "`+PersonTableName+`" WHERE id = ?`, row.ID).Scan(&age))
			assert.Equal(t, age, testProjectedAge)
			statements = nil
			assert.NilError(t, crud.Person.Init())
			assert.DeepEqual(t, recordedIndexDDL(statements), []string{})
		})
	}
}

func TestGeneratedIndexReconciliationRepairsIndexedOneof(t *testing.T) {
	ctx := context.Background()
	db := newProjectionDB(t)
	statements := []string{}
	q := recordingDBTX{DBTX: rt.WrapDB(db), statements: &statements}
	assert.NilError(t, rt.EnsureCoreTables(q))
	_, err := db.Exec(`CREATE TABLE "` + ChoiceTableName + `" (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, label TEXT NOT NULL DEFAULT '')`)
	assert.NilError(t, err)
	_, err = db.Exec(`INSERT INTO _proprdb_schema (table_name, schema_hash) VALUES (?, ?)`, ChoiceTableName, "label:string")
	assert.NilError(t, err)
	payload, err := proto.Marshal(&Choice{Selection: &Choice_Count{Count: 7}})
	assert.NilError(t, err)
	_, err = db.Exec(`INSERT INTO "`+ChoiceTableName+`" (id, at_ns, data, label) VALUES (?, ?, ?, '')`, validationUUIDv7, int64(1), payload)
	assert.NilError(t, err)
	for _, statement := range []string{choiceLabelIndexSQL, choiceTimeIndexSQL} {
		_, err = db.Exec(statement)
		assert.NilError(t, err)
	}
	binding := ChoiceGeneratedBinding
	binding.GeneratedIndexes = []rt.GeneratedIndexDescriptor{
		{Name: choiceLabelIndex, CreateSQL: choiceLabelIndexSQL},
		{Name: choiceTimeIndex, CreateSQL: choiceTimeIndexSQL},
	}
	statements = nil
	assert.NilError(t, rt.ReconcileGeneratedTableContext(ctx, q, binding))
	assert.DeepEqual(t, recordedIndexDDL(statements), []string{`DROP INDEX "` + choiceLabelIndex + `"`, choiceLabelIndexSQL})
	var label sql.NullString
	assert.NilError(t, db.QueryRow(`SELECT label FROM "`+ChoiceTableName+`" WHERE id = ?`, validationUUIDv7).Scan(&label))
	assert.Check(t, !label.Valid)
	indexes := tableIndexNamesByName(ctx, t, db, ChoiceTableName)
	assert.Check(t, indexes[choiceLabelIndex])
	assert.Check(t, indexes[choiceTimeIndex])
	statements = nil
	assert.NilError(t, rt.ReconcileGeneratedTableContext(ctx, q, binding))
	assert.DeepEqual(t, recordedIndexDDL(statements), []string{})
}

func TestGeneratedIndexReconciliationRollback(t *testing.T) {
	ctx := context.Background()
	db := newProjectionDB(t)
	statements := []string{}
	q := recordingDBTX{DBTX: rt.WrapDB(db), statements: &statements}
	crud := NewCRUD(q)
	assert.NilError(t, crud.Init())
	for _, statement := range []string{obsoletePersonSQL, stalePersonIndexSQL, applicationPersonIndexSQL} {
		_, err := db.Exec(statement)
		assert.NilError(t, err)
	}
	_, err := db.Exec(`INSERT INTO "`+PersonTableName+`" (id, at_ns, data) VALUES (?, ?, ?)`, validationUUIDv7, int64(1), []byte{0xff})
	assert.NilError(t, err)
	indexesBefore := tableIndexNamesByName(ctx, t, db, PersonTableName)
	var schemaBefore string
	assert.NilError(t, db.QueryRow(readPersonSchemaSQL, PersonTableName).Scan(&schemaBefore))
	statements = nil
	assert.ErrorContains(t, crud.Person.Init(), "unmarshal reprojection")
	assert.DeepEqual(t, recordedIndexDDL(statements), []string{`DROP INDEX "` + personStaleIndex + `"`})
	assert.DeepEqual(t, tableIndexNamesByName(ctx, t, db, PersonTableName), indexesBefore)
	var count int
	assert.NilError(t, db.QueryRow(projectionColumnCountSQL, PersonTableName, obsoletePersonColumn).Scan(&count))
	assert.Equal(t, count, 1)
	var schemaAfter string
	assert.NilError(t, db.QueryRow(readPersonSchemaSQL, PersonTableName).Scan(&schemaAfter))
	assert.Equal(t, schemaAfter, schemaBefore)
}
