package proprdbrt

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	upsertQueryStatisticSQL = `INSERT INTO ` + CoreTableQueryStatName + ` (table_name, query, calls, duration_sum_ns)
VALUES (?, ?, 1, ?)
ON CONFLICT(table_name, query) DO UPDATE SET
calls = calls + excluded.calls,
duration_sum_ns = duration_sum_ns + excluded.duration_sum_ns`
	selectQueryStatisticsSQL = `SELECT table_name, query, calls, duration_sum_ns FROM ` + CoreTableQueryStatName + ` ORDER BY table_name, query`
	clearQueryStatisticsSQL  = `DELETE FROM ` + CoreTableQueryStatName
)

type QueryStatistic struct {
	TableName     string
	Query         string
	Calls         int64
	DurationSumNs int64
}

func MeasureQueryContext[T any](ctx context.Context, q DBTX, tableName, query string, body func() (T, error)) (result T, err error) {
	if q == nil {
		return result, errors.New("nil DBTX")
	}
	if tableName == "" {
		return result, errors.New("empty table name")
	}
	if query == "" {
		return result, errors.New("empty query")
	}
	if body == nil {
		return result, errors.New("nil query body")
	}
	startedAt := time.Now()
	result, err = body()
	if err != nil {
		return result, err
	}
	durationSumNs := time.Since(startedAt).Nanoseconds()
	if _, err := q.ExecContext(ctx, upsertQueryStatisticSQL, tableName, query, durationSumNs); err != nil {
		var zero T
		return zero, fmt.Errorf("record query statistic for table %s: %w", tableName, err)
	}
	return result, nil
}

func QueryStatistics(q DBTX) ([]QueryStatistic, error) {
	return QueryStatisticsContext(context.Background(), q)
}

func QueryStatisticsContext(ctx context.Context, q DBTX) (statistics []QueryStatistic, err error) {
	if q == nil {
		return nil, errors.New("nil DBTX")
	}
	rows, err := q.QueryContext(ctx, selectQueryStatisticsSQL)
	if err != nil {
		return nil, fmt.Errorf("query statistics: %w", err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil && err == nil {
			err = fmt.Errorf("close query statistics rows: %w", closeErr)
		}
	}()
	statistics = make([]QueryStatistic, 0)
	for rows.Next() {
		var statistic QueryStatistic
		if err := rows.Scan(&statistic.TableName, &statistic.Query, &statistic.Calls, &statistic.DurationSumNs); err != nil {
			return nil, fmt.Errorf("scan query statistic: %w", err)
		}
		statistics = append(statistics, statistic)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate query statistics: %w", err)
	}
	return statistics, nil
}

func ClearQueryStatistics(q DBTX) error {
	return ClearQueryStatisticsContext(context.Background(), q)
}

func ClearQueryStatisticsContext(ctx context.Context, q DBTX) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	if _, err := q.ExecContext(ctx, clearQueryStatisticsSQL); err != nil {
		return fmt.Errorf("clear query statistics: %w", err)
	}
	return nil
}
