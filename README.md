# go-burn 🔥

一个轻量级、渐进式的高性能压力测试工具库

[![Go Version](https://img.shields.io/badge/Go-%3E%3D1.25-blue)](https://golang.org/dl/) [![License](https://img.shields.io/github/license/LouYuanbo1/go-burn)](./LICENSE)

---

**go-burn** 是一款专为 Go 应用设计的压力测试工具库，支持多阶段渐进式压测、实时监控和详细的性能报告。其主要特点包括：

- 🚀 **渐进式压测** - 支持多阶段并发增长，模拟真实流量场景
- ⚡ **高并发支持** - 基于 goroutine 的高效并发处理
- 📊 **实时监控** - 实时输出 QPS、延迟、成功率等关键指标
- 🛡️ **安全限流** - 内置并发数限制和单协程速率限制
- 📈 **详细报告** - 提供完整的压测报告和统计信息
- 🔄 **优雅退出** - 支持中断信号处理，确保资源正确释放
- 📁 **结果导出** - 支持将详细数据导出为 CSV 格式

## 📦 安装

```bash
go get github.com/LouYuanbo1/go-burn
```

## 🚀 快速开始

以下是一个简单的 HTTP 接口压测示例：

```go
package main

import (
    "context"
    "fmt"
    "log"
    "net/http"
    "strings"
    "time"
    
    "github.com/LouYuanbo1/go-burn"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 创建压测器
    tester, ctx := burn.NewTester(ctx)

    // 定义多阶段压测配置
    steps := []burn.StepConfig{
        {Concurrency: 5, Duration: 10 * time.Second, RateLimit: 0},  // 阶段1: 5个并发，持续10秒，无速率限制
        {Concurrency: 20, Duration: 20 * time.Second, RateLimit: 2}, // 阶段2: 20个并发，持续20秒，单协程每秒最多2次请求
        {Concurrency: 50, Duration: 30 * time.Second, RateLimit: 5}, // 阶段3: 50个并发，持续30秒，单协程每秒最多5次请求
    }

    // 定义压测任务函数
    taskFn := func(ctx context.Context, reqID int) error {
        req, _ := http.NewRequestWithContext(ctx, "GET", "http://www.example.com", nil)
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return err
        }
        defer resp.Body.Close()
        
        if resp.StatusCode < 200 || resp.StatusCode >= 400 {
            return fmt.Errorf("unexpected status: %d", resp.StatusCode)
        }
        return nil
    }

    // 执行多阶段压测
    tester.BurnSteps(ctx, 5*time.Sencond, steps, taskFn)

    // 等待压测完成
    if err := tester.Wait(); err != nil && err != context.Canceled {
        log.Printf("压测异常退出: %v", err)
    }

    // 输出最终报告
    tester.Stats().Report()
}

## 🛠️ 高级用法

### 自定义配置选项

go-burn 提供了多种配置选项来满足不同需求：

```go
import (
    "log"
    "os"
)

// 自定义日志
logger := log.New(os.Stdout, "[CUSTOM] ", log.LstdFlags|log.Lmicroseconds)

// 创建带有自定义配置的压测器
tester, ctx := burn.NewTester(ctx,
    burn.WithLogger(logger),              // 自定义日志
    burn.WithConcurrencyLimit(300),       // 设置全局并发上限
)
```

### 阶段配置说明

每个压测阶段都包含以下参数：

- `Concurrency` - 当前阶段的并发数（goroutine 数量）
- `Duration` - 阶段持续时间
- `RateLimit` - 单个 goroutine 每秒的最大请求数（0 表示无限制）

### 实时监控

在压测过程中，go-burn 会实时输出以下指标：

- **QPS** - 每秒查询率
- **平均延迟** - 请求的平均响应时间
- **成功率** - 成功请求的百分比
- **总请求数** - 已发送的请求数量

示例输出：
```
[LOAD] 2024/05/19 10:30:15 📡 实时: QPS=150.2 | 平均延迟=23ms | 成功率=99.8% | 总请求=15020
```

### 结果导出

可以将详细的压测结果导出为 CSV 文件：

```go
// 导出详细统计数据到 CSV
err := tester.Stats().ExportCSV("load_test_results.csv")
if err != nil {
    log.Printf("导出失败: %v", err)
}
```

## 📊 报告示例

压测完成后，go-burn 会生成详细的统计报告：

```
============================================================
📊 压测报告 (耗时: 1m0s)
------------------------------------------------------------
  总请求数:  18000
  成功:      17950 (99.72%)
  失败:      50 (0.28%)
  平均 QPS:  300.00
  平均延迟:  15ms
  P50 延迟:  12ms
  P90 延迟:  28ms
  P99 延迟:  45ms
============================================================
```

## 📚 API 参考

### 核心类型

- `StepConfig` - 阶段配置结构体
  - `Concurrency int` - 并发数
  - `Duration time.Duration` - 持续时间
  - `RateLimit int` - 单协程速率限制（0为无限制）

- `Tester` - 主要的压测器类型
  - `BurnSteps(ctx context.Context,intervalMonitor time.Duration, steps []StepConfig, fn func(ctx context.Context, reqID int) error)` - 执行多阶段压测
  - `Stats() *Stats` - 获取统计信息
  - `Wait()` - 等待压测完成

- `Stats` - 统计信息类型
  - `Record(duration time.Duration, success bool)` - 记录请求统计
  - `Report()` - 输出统计报告
  - `ExportCSV(filename string)` - 导出 CSV 报告

### 配置选项

- `WithLogger(logger *log.Logger)` - 自定义日志记录器
- `WithConcurrencyLimit(limit int)` - 设置全局并发限制
- `WithStats(stats *Stats)` - 使用自定义统计对象

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](./LICENSE) 文件了解详情。

## 💡 贡献

欢迎提交 Issue 和 Pull Request 来改进此项目！
