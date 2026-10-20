package proprdbrt

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"gotest.tools/v3/assert"
)

func TestDecodePoolSharedLimit(t *testing.T) {
	const workerCount = 2
	pool := newDecodePool(workerCount)
	defer close(pool.jobs)
	release := make(chan struct{})
	started := make(chan struct{}, workerCount)
	var active, maximum atomic.Int32
	var callers sync.WaitGroup
	for range workerCount * 4 {
		callers.Go(func() {
			batch := newDecodeBatch(t.Context())
			err := batch.submit(pool, func() error {
				count := active.Add(1)
				for old := maximum.Load(); count > old; old = maximum.Load() {
					if maximum.CompareAndSwap(old, count) {
						break
					}
				}
				started <- struct{}{}
				<-release
				active.Add(-1)
				return nil
			})
			assert.Check(t, err == nil)
			assert.Check(t, batch.finish() == nil)
		})
	}
	for range workerCount {
		<-started
	}
	assert.Equal(t, active.Load(), int32(workerCount))
	close(release)
	done := make(chan struct{})
	go func() { callers.Wait(); close(done) }()
	for {
		select {
		case <-started:
		case <-done:
			assert.Equal(t, maximum.Load(), int32(workerCount))
			return
		}
	}
}

func TestDecodePoolCancellationAndRecovery(t *testing.T) {
	pool := newDecodePool(1)
	defer close(pool.jobs)
	started := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan struct{})
	assert.NilError(t, pool.submit(t.Context(), func() {
		close(started)
		<-release
		close(completed)
	}))
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	blocked := make(chan error, 1)
	go func() {
		blocked <- pool.submit(ctx, func() { t.Error("canceled job executed") })
	}()
	cancel()
	assert.Assert(t, errors.Is(<-blocked, context.Canceled))
	close(release)
	<-completed
	expected := errors.New("decode failure")
	batch := newDecodeBatch(t.Context())
	assert.NilError(t, batch.submit(pool, func() error { return expected }))
	assert.Assert(t, errors.Is(batch.finish(), expected))
	next := newDecodeBatch(t.Context())
	assert.NilError(t, next.submit(pool, func() error { return nil }))
	assert.NilError(t, next.finish())
}

func TestSharedDecodePoolIdentity(t *testing.T) {
	first := sharedDecodePool()
	assert.Assert(t, first == sharedDecodePool())
}

func TestDecodeBatchWaitsAfterCancellation(t *testing.T) {
	pool := newDecodePool(1)
	defer close(pool.jobs)
	ctx, cancel := context.WithCancel(t.Context())
	batch := newDecodeBatch(ctx)
	started := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan struct{})
	assert.NilError(t, batch.submit(pool, func() error {
		close(started)
		<-release
		close(completed)
		return nil
	}))
	<-started
	cancel()
	close(release)
	assert.Assert(t, errors.Is(batch.finish(), context.Canceled))
	select {
	case <-completed:
	default:
		t.Fatal("finish returned before accepted work completed")
	}
}

func BenchmarkBulkDecode(b *testing.B) {
	const rowCount = 256
	fields := make(map[string]any)
	for i := range 128 {
		fields[strconv.Itoa(i)] = strings.Repeat("payload", 32)
	}
	message, err := structpb.NewStruct(fields)
	assert.NilError(b, err)
	data, err := proto.Marshal(message)
	assert.NilError(b, err)
	for _, pooled := range []bool{false, true} {
		b.Run(fmt.Sprintf("pooled=%t", pooled), func(b *testing.B) {
			pool := sharedDecodePool()
			b.ReportAllocs()
			b.SetBytes(int64(len(data) * rowCount))
			for b.Loop() {
				messages := make([]*structpb.Struct, rowCount)
				batch := newDecodeBatch(b.Context())
				for i := range messages {
					message := new(structpb.Struct)
					messages[i] = message
					decode := func() error { return proto.Unmarshal(data, message) }
					if pooled {
						assert.NilError(b, batch.submit(pool, decode))
					} else {
						assert.NilError(b, decode())
					}
				}
				assert.NilError(b, batch.finish())
			}
		})
	}
}
