package gopool

import (
	"context"
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type Option struct {
	MaxIdleWorkers int
	WorkerMaxAge   time.Duration
	TaskChanBuffer int
}

func DefaultOption() *Option {
	return &Option{
		MaxIdleWorkers: 1000,
		WorkerMaxAge:   time.Minute,
		TaskChanBuffer: 1000,
	}
}

var defaultGoPool = NewGoPool("__default__", nil)

func Go(f func()) {
	defaultGoPool.Go(f)
}

func CtxGo(ctx context.Context, f func()) {
	defaultGoPool.CtxGo(ctx, f)
}

func SetPanicHandler(f func(ctx context.Context, r any)) {
	defaultGoPool.SetPanicHandler(f)
}

type task struct {
	ctx context.Context
	f   func()
}

type GoPool struct {
	name string

	workers atomic.Int32
	maxIdle int32
	maxage  int64 // milliseconds

	panicHandler func(ctx context.Context, r any)

	tasks     chan task
	unixMilli atomic.Int64

	createWorker func()
}

func NewGoPool(name string, o *Option) *GoPool {
	if o == nil {
		o = DefaultOption()
	}
	p := &GoPool{
		name:    name,
		tasks:   make(chan task, o.TaskChanBuffer),
		maxage:  o.WorkerMaxAge.Milliseconds(),
		maxIdle: int32(o.MaxIdleWorkers),
	}

	p.createWorker = func() {
		p.runWorker()
	}
	return p
}

func (p *GoPool) Go(f func()) {
	p.CtxGo(context.Background(), f)
}

func (p *GoPool) CtxGo(ctx context.Context, f func()) {
	select {
	case p.tasks <- task{ctx: ctx, f: f}:
	default:
		// full? fall back to use go directly
		go p.runTask(ctx, f)
		return
	}
	// luckily ... it's true when there're many workers.
	if len(p.tasks) == 0 {
		return
	}
	// all worker is busy, create a new one
	go p.createWorker()
}

func (p *GoPool) SetPanicHandler(f func(ctx context.Context, r any)) {
	p.panicHandler = f
}

func (p *GoPool) runTask(ctx context.Context, f func()) {
	defer func(p *GoPool, ctx context.Context) {
		if r := recover(); r != nil {
			if p.panicHandler != nil {
				p.panicHandler(ctx, r)
			} else {
				log.Printf("GOPOOL: panic in pool: %s: %v: %s", p.name, r, debug.Stack())
			}
		}
	}(p, ctx)
	f()
}

func (p *GoPool) CurrentWorkers() int {
	return int(p.workers.Load())
}

func (p *GoPool) runWorker() {
	id := p.workers.Add(1)
	defer p.workers.Add(-1)

	if id > p.maxIdle {
		// —— 临时工：超编了，把积压任务清完就走 ——
		// drain task chan and exit without waiting
		for {
			select {
			case t := <-p.tasks:
				p.runTask(t.ctx, t.f)
			default:
				return
			}
		}
	}

	// —— 正式工：常驻，for range 一直等任务 ——
	createdAt := time.Now().UnixMilli() // for checking maxage
	for t := range p.tasks {
		p.runTask(t.ctx, t.f)

		now := p.unixMilli.Load()
		if now == 0 {
			// cas and create a new ticker
			now = time.Now().UnixMilli()
			if p.unixMilli.CompareAndSwap(0, now) {
				go p.runTicker()
			}
		}

		// 检查年龄，超龄退休
		if now-createdAt > p.maxage {
			return
		}
	}
}

var noopTask = task{f: func() {}}

func (p *GoPool) runTicker() {
	defer p.unixMilli.Store(0)

	d := max(time.Duration(p.maxage)*time.Millisecond/100, time.Millisecond) // 检查频率：年龄的 1/100
	t := time.NewTicker(d)
	defer t.Stop()

	for now := range t.C {
		if p.CurrentWorkers() == 0 {
			return // 没有工人了，ticker 自杀
		}
		p.unixMilli.Store(now.UnixMilli()) // 更新共享时钟
		p.tasks <- noopTask                // 塞个任务 唤醒一个工人
	}
}
