package genexample

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	_ "github.com/mattn/go-sqlite3"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

func TestGeneratedQueryStatistics(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:query-statistics?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})
	adapter := rt.WrapDB(db)
	crud := NewCRUD(adapter)
	assert.NilError(t, crud.Init())

	_, err = crud.Person.Insert(&Person{Name: "Ada", Age: 37})
	assert.NilError(t, err)
	_, err = crud.Person.Insert(&Person{Name: "Grace", Age: 30})
	assert.NilError(t, err)
	_, err = crud.Note.Insert(&Note{Text: "not measured"})
	assert.NilError(t, err)
	assert.NilError(t, rt.ClearQueryStatistics(adapter))

	_, err = crud.Person.Select("name = ?", "Ada")
	assert.NilError(t, err)
	initialStatistics, err := rt.QueryStatistics(adapter)
	assert.NilError(t, err)
	assert.Check(t, is.Len(initialStatistics, 1))
	assert.Check(t, is.Equal(initialStatistics[0].Calls, int64(1)))
	initialDurationSumNs := initialStatistics[0].DurationSumNs
	_, err = crud.Person.Select("name = ?", "Grace")
	assert.NilError(t, err)
	_, err = crud.Person.Select("")
	assert.NilError(t, err)
	_, err = crud.Note.Select("text = ?", "not measured")
	assert.NilError(t, err)
	_, err = crud.Person.Select("missing = ?", "ignored")
	assert.Check(t, err != nil)

	statistics, err := rt.QueryStatistics(adapter)
	assert.NilError(t, err)
	assert.Check(t, is.Len(statistics, 2))
	assert.DeepEqual(t, statistics[0], rt.QueryStatistic{
		TableName:     PersonTableName,
		Query:         `SELECT id, at_ns, data FROM "` + PersonTableName + `"`,
		Calls:         1,
		DurationSumNs: statistics[0].DurationSumNs,
	})
	assert.Check(t, statistics[0].DurationSumNs >= 0)
	assert.DeepEqual(t, statistics[1], rt.QueryStatistic{
		TableName:     PersonTableName,
		Query:         `SELECT id, at_ns, data FROM "` + PersonTableName + `" WHERE name = ?`,
		Calls:         2,
		DurationSumNs: statistics[1].DurationSumNs,
	})
	assert.Check(t, statistics[1].DurationSumNs >= initialDurationSumNs)
	assert.Check(t, !strings.Contains(statistics[1].Query, "Ada"))
	assert.Check(t, !strings.Contains(statistics[1].Query, "Grace"))

	assert.NilError(t, rt.ClearQueryStatistics(adapter))
	statistics, err = rt.QueryStatistics(adapter)
	assert.NilError(t, err)
	assert.Check(t, is.Len(statistics, 0))

	tx, err := db.BeginTx(ctx, nil)
	assert.NilError(t, err)
	_, err = NewPersonTable(rt.WrapTx(tx)).Select("")
	assert.NilError(t, err)
	assert.NilError(t, tx.Rollback())
	statistics, err = rt.QueryStatistics(adapter)
	assert.NilError(t, err)
	assert.Check(t, is.Len(statistics, 0))

	_, err = db.ExecContext(ctx, `DROP TABLE `+rt.CoreTableQueryStatName)
	assert.NilError(t, err)
	_, err = crud.Person.Select("")
	assert.Check(t, err != nil)
	assert.Check(t, strings.Contains(err.Error(), "record query statistic"))
}
