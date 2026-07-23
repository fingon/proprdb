package proprdbrt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
)

type TableChange[T proto.Message] struct {
	ID      string
	AtNs    int64
	Deleted bool
	Data    T
}

type generatedTableChange struct {
	tableName string
	id        string
	atNs      int64
	deleted   bool
	data      proto.Message
}

type tableChangeSubscriber interface {
	enqueue(generatedTableChange)
	close()
}

type tableChangeBroker struct {
	mu          sync.Mutex
	nextID      uint64
	subscribers map[string]map[uint64]tableChangeSubscriber
}

type tableChangeCollector struct {
	changes []generatedTableChange
}

type changeTrackingDBTX struct {
	inner     DBTX
	broker    *tableChangeBroker
	collector *tableChangeCollector
}

type tableChangeSubscription[T proto.Message] struct {
	mu     sync.Mutex
	queue  []TableChange[T]
	wake   chan struct{}
	closed bool
	output chan TableChange[T]
}

func WithChangeListeners(q DBTX) DBTX {
	if q == nil {
		return nil
	}
	if _, ok := q.(*changeTrackingDBTX); ok {
		return q
	}
	return &changeTrackingDBTX{
		inner:  q,
		broker: &tableChangeBroker{subscribers: make(map[string]map[uint64]tableChangeSubscriber)},
	}
}

func (q *changeTrackingDBTX) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return q.inner.ExecContext(ctx, query, args...)
}

func (q *changeTrackingDBTX) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return q.inner.QueryContext(ctx, query, args...)
}

func (q *changeTrackingDBTX) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return q.inner.QueryRowContext(ctx, query, args...)
}

func (q *changeTrackingDBTX) WithTransaction(ctx context.Context, body func(DBTX) error) error {
	collector := &tableChangeCollector{}
	err := q.inner.WithTransaction(ctx, func(tx DBTX) error {
		return body(&changeTrackingDBTX{inner: tx, broker: q.broker, collector: collector})
	})
	if err != nil {
		return err
	}
	if q.collector != nil {
		q.collector.changes = append(q.collector.changes, collector.changes...)
		return nil
	}
	q.broker.publish(collector.changes)
	return nil
}

func TableChanges[T proto.Message](ctx context.Context, q DBTX, tableName string) (<-chan TableChange[T], error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	tracked, ok := q.(*changeTrackingDBTX)
	if !ok || tracked == nil || tracked.broker == nil {
		return nil, errors.New("DBTX does not support change listeners")
	}
	if tableName == "" {
		return nil, errors.New("empty table name")
	}
	subscription := &tableChangeSubscription[T]{
		queue:  make([]TableChange[T], 0),
		wake:   make(chan struct{}, 1),
		output: make(chan TableChange[T]),
	}
	id := tracked.broker.add(tableName, subscription)
	go subscription.run(ctx, func() {
		tracked.broker.remove(tableName, id)
	})
	return subscription.output, nil
}

func (b *tableChangeBroker) add(tableName string, subscriber tableChangeSubscriber) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	if b.subscribers[tableName] == nil {
		b.subscribers[tableName] = make(map[uint64]tableChangeSubscriber)
	}
	b.subscribers[tableName][b.nextID] = subscriber
	return b.nextID
}

func (b *tableChangeBroker) remove(tableName string, id uint64) {
	b.mu.Lock()
	subscriber := b.subscribers[tableName][id]
	delete(b.subscribers[tableName], id)
	if len(b.subscribers[tableName]) == 0 {
		delete(b.subscribers, tableName)
	}
	b.mu.Unlock()
	if subscriber != nil {
		subscriber.close()
	}
}

func (b *tableChangeBroker) hasSubscribers(tableName string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subscribers[tableName]) > 0
}

func (b *tableChangeBroker) publish(changes []generatedTableChange) {
	for _, change := range changes {
		b.mu.Lock()
		subscribers := make([]tableChangeSubscriber, 0, len(b.subscribers[change.tableName]))
		for _, subscriber := range b.subscribers[change.tableName] {
			subscribers = append(subscribers, subscriber)
		}
		b.mu.Unlock()
		for _, subscriber := range subscribers {
			subscriber.enqueue(change)
		}
	}
}

func (s *tableChangeSubscription[T]) enqueue(change generatedTableChange) {
	var data T
	if change.data != nil {
		typedData, ok := change.data.(T)
		if !ok {
			panic(fmt.Sprintf("change listener type mismatch for table %s: %T", change.tableName, change.data))
		}
		data = typedData
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.queue = append(s.queue, TableChange[T]{
		ID:      change.id,
		AtNs:    change.atNs,
		Deleted: change.deleted,
		Data:    data,
	})
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *tableChangeSubscription[T]) close() {
	s.mu.Lock()
	s.closed = true
	s.queue = nil
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *tableChangeSubscription[T]) run(ctx context.Context, unregister func()) {
	defer close(s.output)
	defer unregister()
	for {
		s.mu.Lock()
		if len(s.queue) > 0 {
			change := s.queue[0]
			s.queue = s.queue[1:]
			s.mu.Unlock()
			select {
			case s.output <- change:
			case <-ctx.Done():
				return
			}
			continue
		}
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return
		}
		select {
		case <-s.wake:
		case <-ctx.Done():
			return
		}
	}
}

func queueGeneratedTableChange(q DBTX, binding GeneratedTableBinding, id string, atNs int64, deleted bool, data proto.Message) {
	tracked, ok := q.(*changeTrackingDBTX)
	if !ok || !binding.Descriptor.ChangeListenersEnabled || !tracked.broker.hasSubscribers(binding.Descriptor.TableName) {
		return
	}
	var copiedData proto.Message
	if data != nil {
		copiedData = proto.Clone(data)
	}
	change := generatedTableChange{
		tableName: binding.Descriptor.TableName,
		id:        id,
		atNs:      atNs,
		deleted:   deleted,
		data:      copiedData,
	}
	if tracked.collector != nil {
		tracked.collector.changes = append(tracked.collector.changes, change)
		return
	}
	tracked.broker.publish([]generatedTableChange{change})
}
