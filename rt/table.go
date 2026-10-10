package proprdbrt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"google.golang.org/protobuf/proto"
)

const (
	errNilDBTX = "nil DBTX"
	errNilData = "nil data"
	errEmptyID = "empty id"
)

type MessagePointer[M any] interface {
	*M
	proto.Message
}

type Row[P proto.Message] struct {
	ID   string
	AtNs int64
	Data P
}

type Table struct {
	q       DBTX
	binding GeneratedTableBinding
}

func NewTable(q DBTX, binding GeneratedTableBinding) Table {
	if binding.Descriptor.ChangeListenersEnabled {
		q = WithChangeListeners(q)
	}
	return Table{q: q, binding: binding}
}

func (t Table) Init(ensureCore bool) (err error) {
	if t.q == nil {
		return errors.New(errNilDBTX)
	}
	slog.Debug("initialize generated table", "table", t.binding.Descriptor.TableName, "ensure_core", ensureCore)
	if ensureCore {
		if err := EnsureCoreTables(t.q); err != nil {
			return err
		}
	}
	ctx := context.Background()
	if err := ReconcileGeneratedTableContext(ctx, t.q, t.binding); err != nil {
		return err
	}
	if err := t.DrainUnknownRows(); err != nil {
		return fmt.Errorf("drain unknown rows for %s: %w", t.binding.Descriptor.TableName, err)
	}
	return nil
}

func (t Table) DrainUnknownRows() (err error) {
	if t.q == nil {
		return errors.New(errNilDBTX)
	}
	return DrainUnknownBindingsContext(context.Background(), t.q, []GeneratedTableBinding{t.binding})
}

func (t Table) Select[M any, P MessagePointer[M]](where string, args ...any) (result []Row[P], err error) {
	if t.q == nil {
		return nil, errors.New(errNilDBTX)
	}
	ctx := context.Background()
	tableName := t.binding.Descriptor.TableName
	query := `SELECT id, at_ns, data FROM ` + quoteSQLiteIdentifier(tableName)
	if strings.TrimSpace(where) != "" {
		query += " WHERE " + where
	}
	selectRows := func() (result []Row[P], err error) {
		rows, err := t.q.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("select from %s: %w", tableName, err)
		}
		defer func() {
			if closeErr := CloseRows(rows, "select"); closeErr != nil {
				if err == nil {
					err = closeErr
				} else {
					err = fmt.Errorf("%w (additionally, %v)", err, closeErr)
				}
				result = nil
			}
		}()
		result = make([]Row[P], 0)
		for rows.Next() {
			var row Row[P]
			var dataBytes []byte
			if err := rows.Scan(&row.ID, &row.AtNs, &dataBytes); err != nil {
				return nil, fmt.Errorf("scan row from %s: %w", tableName, err)
			}
			row.Data = P(new(M))
			if err := proto.Unmarshal(dataBytes, row.Data); err != nil {
				return nil, fmt.Errorf("unmarshal %s row: %w", t.binding.Descriptor.TypeName, err)
			}
			result = append(result, row)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate rows from %s: %w", tableName, err)
		}
		return result, nil
	}
	if t.binding.Descriptor.QueryStatisticsEnabled {
		return MeasureQueryContext(ctx, t.q, tableName, query, selectRows)
	}
	return selectRows()
}

func (t Table) Insert[M any, P MessagePointer[M]](data P) (row Row[P], err error) {
	if t.q == nil {
		return row, errors.New(errNilDBTX)
	}
	if data == nil {
		return row, errors.New(errNilData)
	}
	id, err := UUIDv7()
	if err != nil {
		return row, fmt.Errorf("generate uuidv7: %w", err)
	}
	return t.InsertWithID[M, P](id, data)
}

func (t Table) InsertWithID[M any, P MessagePointer[M]](id string, data P) (row Row[P], err error) {
	if t.q == nil {
		return row, errors.New(errNilDBTX)
	}
	if data == nil {
		return row, errors.New(errNilData)
	}
	return t.write[M, P](id, data, true)
}

func (t Table) UpdateByID[M any, P MessagePointer[M]](id string, data P) (row Row[P], err error) {
	if t.q == nil {
		return row, errors.New(errNilDBTX)
	}
	if err := validateObjectID(id); err != nil {
		return row, err
	}
	if data == nil {
		return row, errors.New(errNilData)
	}
	return t.write[M, P](id, data, false)
}

func (t Table) write[M any, P MessagePointer[M]](id string, data P, insert bool) (row Row[P], err error) {
	if err := validateObjectID(id); err != nil {
		return row, err
	}
	if t.binding.ValidateMessage != nil {
		if err := t.binding.ValidateMessage(data); err != nil {
			return row, fmt.Errorf("validate %s: %w", t.binding.Descriptor.TypeName, err)
		}
	}
	atNs, err := WriteLocalObjectContext(context.Background(), t.q, t.binding, id, data, insert)
	if err != nil {
		return row, err
	}
	return Row[P]{ID: id, AtNs: atNs, Data: data}, nil
}

func (t Table) UpdateRow[M any, P MessagePointer[M]](row Row[P]) (updated Row[P], err error) {
	if t.q == nil {
		return updated, errors.New(errNilDBTX)
	}
	if row.ID == "" {
		return updated, errors.New(errEmptyID)
	}
	if row.Data == nil {
		return updated, errors.New(errNilData)
	}
	return t.UpdateByID[M, P](row.ID, row.Data)
}

func validateObjectID(id string) (err error) {
	if id == "" {
		return errors.New(errEmptyID)
	}
	if err := ValidateUUID(id); err != nil {
		return fmt.Errorf("validate id %s: %w", id, err)
	}
	return nil
}

func (t Table) DeleteByID(id string) (err error) {
	if t.q == nil {
		return errors.New(errNilDBTX)
	}
	if id == "" {
		return errors.New(errEmptyID)
	}
	return DeleteLocalBoundObjectContext(context.Background(), t.q, t.binding, id)
}

func (t Table) DeleteRow[P proto.Message](row Row[P]) (err error) {
	return t.DeleteByID(row.ID)
}

func (t Table) Changes[P proto.Message](ctx context.Context) (changes <-chan TableChange[P], err error) {
	if t.q == nil {
		return nil, errors.New(errNilDBTX)
	}
	return TableChanges[P](ctx, t.q, t.binding.Descriptor.TableName)
}

func (t Table) UpsertWithAtNs[M any, P MessagePointer[M]](id string, atNs int64, data P) (err error) {
	if t.q == nil {
		return errors.New(errNilDBTX)
	}
	if id == "" {
		return errors.New(errEmptyID)
	}
	if data == nil {
		return errors.New(errNilData)
	}
	dataJSON, err := MarshalAnyJSON(data)
	if err != nil {
		return err
	}
	return t.applyIncoming(JSONLRecord{ID: id, AtNs: atNs, Data: dataJSON})
}

func (t Table) TombstoneWithAtNs(id string, atNs int64) (err error) {
	if t.q == nil {
		return errors.New(errNilDBTX)
	}
	if id == "" {
		return errors.New(errEmptyID)
	}
	dataJSON, err := MarshalTypeOnlyAnyJSON(t.binding.Descriptor.TypeName)
	if err != nil {
		return err
	}
	return t.applyIncoming(JSONLRecord{ID: id, Deleted: true, AtNs: atNs, Data: dataJSON})
}

func (t Table) applyIncoming(record JSONLRecord) (err error) {
	ctx := context.Background()
	return t.q.WithTransaction(ctx, func(tx DBTX) error {
		return ApplyIncomingObjectContext(ctx, tx, t.binding, record)
	})
}
