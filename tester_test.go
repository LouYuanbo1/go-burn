package burn

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countCallsWorker 统计调用次数的测试任务函数
// helper_test.go 中替换 countCallsWorker 函数
func countCallsWorker(counter *atomic.Int64, errRate float64) func(ctx context.Context, reqID int) error {
	return func(ctx context.Context, reqID int) error {
		counter.Add(1)
		// ✅ 修复类型不匹配：先算出整数间隔，再用 int 取模
		if errRate > 0 && errRate <= 1.0 {
			interval := int(1.0 / errRate) // 例如 errRate=0.1 → interval=10
			if interval > 0 && reqID%interval == 0 {
				return fmt.Errorf("simulated error for reqID %d", reqID)
			}
		}
		// 响应上下文取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			return nil
		}
	}
}

// collectLatenciesWorker 收集延迟数据的测试任务
func collectLatenciesWorker(baseDelay time.Duration, jitter time.Duration) func(ctx context.Context, reqID int) error {
	return func(ctx context.Context, reqID int) error {
		// 模拟带抖动的延迟
		delay := baseDelay + time.Duration(reqID%10)*jitter
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			return nil
		}
	}
}

// TestOptionFunctions 验证选项函数正确设置字段
func TestOptionFunctions(t *testing.T) {
	ctx := context.Background()
	customLogger := log.New(os.Stderr, "[TEST] ", log.LstdFlags)
	customStats := NewStats()

	tester, _ := NewTester(ctx,
		WithLogger(customLogger),
		WithConcurrencyLimit(50),
		WithStats(customStats),
	)

	if tester.logger != customLogger {
		t.Error("WithLogger failed")
	}
	if tester.concurrencyLimit != 50 {
		t.Errorf("concurrencyLimit = %d, want 50", tester.concurrencyLimit)
	}
	if tester.stats != customStats {
		t.Error("WithStats failed")
	}
}

// TestBurnSteps_SingleStage 验证单阶段基本流程
func TestBurnSteps_SingleStage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tester, ctx := NewTester(ctx, WithConcurrencyLimit(20))
	var callCount atomic.Int64

	steps := []StepConfig{
		{Concurrency: 5, Duration: 1 * time.Second, RateLimit: 10},
	}

	tester.BurnSteps(ctx, 5*time.Second, steps, countCallsWorker(&callCount, 0))
	_ = tester.Wait()

	// 5 并发 × 1 秒 × 10 QPS 限 = 最多 50 次，但受 10ms 任务延迟限制
	// 实际：5 协程 × (1000ms/10ms) = ~500 次，但只运行 1 秒
	// 简化：验证至少执行了 5 次（每个协程至少 1 次）
	if count := callCount.Load(); count < 5 {
		t.Errorf("callCount = %d, want >= 5", count)
	}
}

// TestBurnSteps_StageIsolation 验证阶段间并发不叠加
func TestBurnSteps_StageIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tester, ctx := NewTester(ctx, WithConcurrencyLimit(100))
	var maxConcurrent atomic.Int64
	var currentConcurrent atomic.Int64

	// 任务函数：记录并发峰值
	worker := func(ctx context.Context, reqID int) error {
		cur := currentConcurrent.Add(1)
		if peak := maxConcurrent.Load(); cur > peak {
			maxConcurrent.CompareAndSwap(peak, cur)
		}
		defer currentConcurrent.Add(-1)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return nil
		}
	}

	steps := []StepConfig{
		{Concurrency: 10, Duration: 500 * time.Millisecond, RateLimit: 0},
		{Concurrency: 20, Duration: 500 * time.Millisecond, RateLimit: 0}, // 关键：验证不会 10+20=30
	}

	tester.BurnSteps(ctx, 5*time.Second, steps, worker)
	_ = tester.Wait()

	// 峰值并发应该 ≈ 20（第二阶段），而不是 30
	peak := maxConcurrent.Load()
	if peak > 25 { // 允许少量误差（协程调度延迟）
		t.Errorf("max concurrent = %d, want <= 25 (stage isolation failed)", peak)
	}
}

// TestWorkerLoop_RateLimit 验证速率控制生效
func TestWorkerLoop_RateLimit(t *testing.T) {
	ctx := context.Background()
	tester, _ := NewTester(ctx)

	stageCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	var callTimes []time.Time
	var mu sync.Mutex

	worker := func(ctx context.Context, reqID int) error {
		mu.Lock()
		callTimes = append(callTimes, time.Now())
		mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Millisecond): // 请求本身很快
			return nil
		}
	}

	// RateLimit = 5 → 每 200ms 最多 1 次
	_ = tester.workerLoop(stageCtx, ctx, 0, 5, worker)

	mu.Lock()
	calls := len(callTimes)
	mu.Unlock()

	// 300ms 内，RateLimit=5 → 最多 2 次（t=0, t=200ms）
	// 允许 1-3 次的误差（调度/计时精度）
	if calls < 1 || calls > 3 {
		t.Errorf("calls = %d in 300ms with RateLimit=5, want 1-3", calls)
	}

	// 验证调用间隔 >= 180ms（允许 20ms 误差）
	if len(callTimes) >= 2 {
		interval := callTimes[1].Sub(callTimes[0])
		if interval < 180*time.Millisecond {
			t.Errorf("interval = %v, want >= 180ms (RateLimit not working)", interval)
		}
	}
}

// TestTester_ConcurrencyLimit 验证全局并发限制保护压测机
func TestTester_ConcurrencyLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 设置全局限制 = 10
	tester, ctx := NewTester(ctx, WithConcurrencyLimit(10))

	var (
		peakConcurrent   atomic.Int64
		activeGoroutines atomic.Int64 // ✅ 统计活跃协程数（不是请求次数）
	)

	// ✅ 直接使用 tester.errGroup（已设置 SetLimit）
	for i := 0; i < 50; i++ {
		tester.errGroup.Go(func() error {
			// 🎯 协程启动时计数（每个协程只计一次）
			cur := activeGoroutines.Add(1)
			defer activeGoroutines.Add(-1)

			// 更新峰值
			for {
				peak := peakConcurrent.Load()
				if cur <= peak || peakConcurrent.CompareAndSwap(peak, cur) {
					break
				}
			}

			// ✅ 协程保持活跃，模拟真实工作
			// 使用 stageCtx 控制退出，避免无限循环
			<-ctx.Done()
			return ctx.Err()
		})
	}

	// 等待所有任务完成
	_ = tester.Wait()

	// ✅ 验证：峰值并发协程数不应超过限制 + 少量误差
	peak := peakConcurrent.Load()
	if peak > 12 { // 允许 2 个误差（调度/统计时机）
		t.Errorf("peak concurrent goroutines = %d, want <= 12 (SetLimit not working)", peak)
	}
}

// TestBurnSteps_GracefulCancel 验证全局取消时优雅退出
func TestBurnSteps_GracefulCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tester, ctx := NewTester(ctx, WithConcurrencyLimit(20))

	var stopped atomic.Bool
	worker := func(ctx context.Context, reqID int) error {
		select {
		case <-ctx.Done():
			stopped.Store(true)
			return ctx.Err()
		case <-time.After(10 * time.Second): // 长任务
			return nil
		}
	}

	steps := []StepConfig{
		{Concurrency: 10, Duration: 5 * time.Second, RateLimit: 0},
	}

	// 启动后 200ms 取消
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	tester.BurnSteps(ctx, 5*time.Second, steps, worker)
	err := tester.Wait()

	// 验证：应收到取消错误，且任务已停止
	if err != context.Canceled {
		t.Errorf("Wait() error = %v, want context.Canceled", err)
	}
	if !stopped.Load() {
		t.Error("worker did not respond to cancellation")
	}
}

// BenchmarkBurnSteps 基准测试：完整压测流程性能
func BenchmarkBurnSteps(b *testing.B) {
	for _, concurrency := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			tester, ctx := NewTester(ctx, WithConcurrencyLimit(concurrency))
			steps := []StepConfig{
				{Concurrency: concurrency, Duration: 1 * time.Second, RateLimit: 0},
			}

			b.ResetTimer()
			tester.BurnSteps(ctx, 5*time.Second, steps, collectLatenciesWorker(10*time.Millisecond, 1*time.Millisecond))
			_ = tester.Wait()
			b.StopTimer()
		})
	}
}
