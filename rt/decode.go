package proprdbrt

import (
	"context"
	"runtime"
	"sync"
)

var sharedDecodePool = sync.OnceValue(func() *decodePool {
	return newDecodePool(runtime.GOMAXPROCS(0))
})

type decodePool struct {
	jobs chan func()
}

func newDecodePool(workerCount int) *decodePool {
	pool := &decodePool{jobs: make(chan func())}
	for range workerCount {
		go func() {
			for job := range pool.jobs {
				job()
			}
		}()
	}
	return pool
}

func (p *decodePool) submit(ctx context.Context, job func()) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.jobs <- job:
		return nil
	}
}

type decodeBatch struct {
	ctx     context.Context
	cancel  context.CancelFunc
	pending sync.WaitGroup
	failure sync.Once
	err     error
}

func newDecodeBatch(ctx context.Context) *decodeBatch {
	ctx, cancel := context.WithCancel(ctx)
	return &decodeBatch{ctx: ctx, cancel: cancel}
}

func (b *decodeBatch) fail(err error) {
	if err == nil {
		return
	}
	b.failure.Do(func() {
		b.err = err
		b.cancel()
	})
}

func (b *decodeBatch) submit(pool *decodePool, decode func() error) (err error) {
	b.pending.Add(1)
	err = pool.submit(b.ctx, func() {
		defer b.pending.Done()
		if b.ctx.Err() != nil {
			return
		}
		b.fail(decode())
	})
	if err != nil {
		b.pending.Done()
		b.fail(err)
	}
	return err
}

func (b *decodeBatch) finish() (err error) {
	b.pending.Wait()
	defer b.cancel()
	if b.err != nil {
		return b.err
	}
	return b.ctx.Err()
}
