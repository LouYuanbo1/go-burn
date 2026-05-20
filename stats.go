package burn

import (
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ==================== 统计模块 ====================

type Record struct {
	Timestamp time.Time
	Latency   time.Duration
	Success   bool
}

// Stats 压测统计指标（并发安全）
type Stats struct {
	TotalRequests int64
	Success       int64
	Failed        int64
	TotalDuration atomic.Int64 // 改用原子类型，避免 data race
	mu            sync.Mutex
	latencies     []time.Duration
	records       []Record
	startTime     time.Time
}

func NewStats() *Stats {
	return &Stats{
		startTime: time.Now(),
		latencies: make([]time.Duration, 0, 10000),
		records:   make([]Record, 0, 10000),
	}
}

func (s *Stats) Record(duration time.Duration, success bool) {
	atomic.AddInt64(&s.TotalRequests, 1)
	if success {
		atomic.AddInt64(&s.Success, 1)
	} else {
		atomic.AddInt64(&s.Failed, 1)
	}
	// 原子累加总延迟
	s.TotalDuration.Add(int64(duration))
	s.mu.Lock()
	s.latencies = append(s.latencies, duration)
	s.records = append(s.records, Record{
		Timestamp: time.Now(),
		Latency:   duration,
		Success:   success,
	})
	s.mu.Unlock()
}

func (s *Stats) Report() {
	s.mu.Lock()
	latencies := make([]time.Duration, len(s.latencies))
	copy(latencies, s.latencies)
	s.mu.Unlock()

	total := atomic.LoadInt64(&s.TotalRequests)
	if total == 0 {
		log.Println("⚠️  无请求记录")
		return
	}

	elapsed := time.Since(s.startTime)
	totalDur := time.Duration(s.TotalDuration.Load()) // 原子读取
	avgLatency := totalDur / time.Duration(total)
	qps := float64(total) / elapsed.Seconds()

	if len(latencies) > 0 {
		slices.Sort(latencies)
	}

	fmt.Printf("%s\n", strings.Repeat("=", 60))
	fmt.Printf("📊 压测报告 (耗时: %v)\n", elapsed.Round(time.Second))
	fmt.Printf("%s\n", strings.Repeat("-", 60))
	fmt.Printf("  总请求数:  %d\n", total)
	fmt.Printf("  成功:      %d (%.2f%%)\n", atomic.LoadInt64(&s.Success), float64(atomic.LoadInt64(&s.Success))/float64(total)*100)
	fmt.Printf("  失败:      %d (%.2f%%)\n", atomic.LoadInt64(&s.Failed), float64(atomic.LoadInt64(&s.Failed))/float64(total)*100)
	fmt.Printf("  平均 QPS:  %.2f\n", qps)
	fmt.Printf("  平均延迟:  %v\n", avgLatency.Round(time.Millisecond))
	if len(latencies) > 0 {
		fmt.Printf("  P50 延迟:  %v\n", latencies[len(latencies)*50/100])
		fmt.Printf("  P90 延迟:  %v\n", latencies[len(latencies)*90/100])
		fmt.Printf("  P99 延迟:  %v\n", latencies[min(len(latencies)*99/100, len(latencies)-1)])
	}
	fmt.Printf("%s\n", strings.Repeat("=", 60))
}

// ExportCSV 导出详细请求记录到 CSV 文件
func (s *Stats) ExportCSV(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	// 复制记录后释放锁，避免在 I/O 期间持有锁
	s.mu.Lock()
	recordsCopy := make([]Record, len(s.records))
	copy(recordsCopy, s.records)
	s.mu.Unlock()

	// 写入 CSV 头
	if _, err = fmt.Fprintf(file, "timestamp_ms,latency_ms,success\n"); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	for _, rec := range recordsCopy {
		successStr := "false"
		if rec.Success {
			successStr = "true"
		}
		if _, err = fmt.Fprintf(file, "%d,%.2f,%s\n",
			rec.Timestamp.UnixMilli(),
			rec.Latency.Seconds()*1000,
			successStr,
		); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
	}

	log.Printf("📁 结果已导出: %s (%d 条记录)", filename, len(recordsCopy))
	return nil
}
