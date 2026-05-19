package burn

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

type StepConfig struct {
	Concurrency int
	Duration    time.Duration
	RateLimit   int // 单协程每秒最大请求数（0=不限速）
}

type Tester struct {
	errGroup         *errgroup.Group
	logger           *log.Logger
	concurrencyLimit int
	stats            *Stats
}

func NewTester(ctx context.Context, options ...OptionFunc) (*Tester, context.Context) {
	eg, ctx := errgroup.WithContext(ctx)
	t := &Tester{
		errGroup:         eg,
		logger:           log.New(os.Stdout, "[LOAD] ", log.LstdFlags),
		concurrencyLimit: 200,
		stats:            NewStats(),
	}
	for _, opt := range options {
		opt(t)
	}
	eg.SetLimit(t.concurrencyLimit) // 全局并发上限（保护压测机）
	return t, ctx
}

// BurnSteps 多阶段渐进式压测（核心方法）
func (t *Tester) BurnSteps(ctx context.Context, steps []StepConfig,
	fn func(ctx context.Context, reqID int) error) error {

	if len(steps) == 0 {
		return errors.New("未配置压测阶段")
	}

	var reqID int64
	t.stats.startTime = time.Now()

	// 启动实时监控协程（使用全局 ctx）
	monitorCtx, monitorCancel := context.WithCancel(ctx)
	defer monitorCancel()
	go t.monitorProgress(monitorCtx)

	t.logger.Printf("🔥 开始阶梯压测: %d 个阶段", len(steps))

	for idx, step := range steps {
		select {
		case <-ctx.Done():
			t.logger.Println("🛑 压测被取消")
			return nil
		default:
		}

		t.logger.Printf("\n📈 阶段 %d/%d: 并发=%d, 持续=%v, 单协程QPS限=%d",
			idx+1, len(steps), step.Concurrency, step.Duration, step.RateLimit)

		// 为当前阶段创建带超时的 context
		stageCtx, stageCancel := context.WithTimeout(ctx, step.Duration)
		stageStart := time.Now()

		// 使用 WaitGroup 跟踪本阶段所有 worker 是否完全退出
		var wg sync.WaitGroup
		wg.Add(step.Concurrency)

		// 启动本阶段并发任务（由全局 errgroup 管理，受 SetLimit 限制）
		for i := 0; i < step.Concurrency; i++ {
			id := atomic.AddInt64(&reqID, 1) - 1
			t.errGroup.Go(func() error {
				defer wg.Done()
				// 将 stageCtx 传入，使任务能感知阶段超时
				return t.workerLoop(stageCtx, ctx, int(id), step.RateLimit, fn)
			})
		}

		// 等待本阶段时间到（超时或全局取消）
		<-stageCtx.Done()
		stageCancel()

		// 等待本阶段所有 worker 真正退出，确保并发数不会叠加到下一阶段
		wg.Wait()

		stageElapsed := time.Since(stageStart)
		stats := t.Stats()
		total := atomic.LoadInt64(&stats.TotalRequests)
		success := atomic.LoadInt64(&stats.Success)

		t.logger.Printf("✅ 阶段 %d 完成: 耗时=%v, 累计请求=%d, 成功率=%.2f%%",
			idx+1, stageElapsed.Round(time.Second), total,
			float64(success)/float64(total)*100)

		// 阶段间隔
		select {
		case <-time.After(1 * time.Second):
		case <-ctx.Done():
			return nil
		}
	}

	t.logger.Println("\n🏁 所有阶段执行完成")
	return nil
}

// workerLoop 单协程工作循环（修复：将 stageCtx 传入 fn）
func (t *Tester) workerLoop(stageCtx, globalCtx context.Context, reqID int, rateLimit int,
	fn func(ctx context.Context, reqID int) error) error {

	var ticker *time.Ticker
	if rateLimit > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(rateLimit))
		defer ticker.Stop()
	}

	for {
		select {
		case <-stageCtx.Done():
			// 阶段结束，正常退出
			return nil
		case <-globalCtx.Done():
			return globalCtx.Err()
		default:
		}

		// 速率控制
		if ticker != nil {
			select {
			// 等待 ticker 触发（每 rateLimit 秒执行一次）
			case <-ticker.C:
			case <-stageCtx.Done():
				return nil
			case <-globalCtx.Done():
				return globalCtx.Err()
			}
		}

		// 执行请求时传入 stageCtx，这样超时/取消时请求能立刻感知
		start := time.Now()
		err := fn(globalCtx, reqID) // 这里将 globalCtx 传给任务
		duration := time.Since(start)

		success := err == nil
		t.stats.Record(duration, success)

		if !success && globalCtx.Err() == nil && stageCtx.Err() == nil {
			t.logger.Printf("❌ 请求 %d 失败: %v", reqID, err)
		}
	}
}

// monitorProgress 实时进度监控（原子读取字段）
func (t *Tester) monitorProgress(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastTotal int64
	var lastTime = time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := t.Stats()
			total := atomic.LoadInt64(&stats.TotalRequests)
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()

			if elapsed > 0 {
				currentQPS := float64(total-lastTotal) / elapsed
				totalDur := stats.TotalDuration.Load() // 原子读
				avgLatency := time.Duration(totalDur) / time.Duration(max(total, 1))
				successRate := float64(atomic.LoadInt64(&stats.Success)) / float64(max(total, 1)) * 100

				t.logger.Printf("📡 实时: QPS=%.1f | 平均延迟=%v | 成功率=%.1f%% | 总请求=%d",
					currentQPS, avgLatency.Round(time.Millisecond), successRate, total)
			}

			lastTotal = total
			lastTime = now
		}
	}
}

func (t *Tester) Stats() *Stats {
	return t.stats
}

func (t *Tester) Wait() error {
	return t.errGroup.Wait()
}
