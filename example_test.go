package burn

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
)

func createHTTPTester(url string, method string, headers map[string]string, body string) func(ctx context.Context, reqID int) error {
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	return func(ctx context.Context, reqID int) error {
		var req *http.Request
		var err error

		if body != "" {
			req, err = http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
		} else {
			req, err = http.NewRequestWithContext(ctx, method, url, nil)
		}
		if err != nil {
			return err
		}

		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
		return nil
	}
}

func TestTester(t *testing.T) {
	// 测试代码
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\n🛑 收到退出信号，正在优雅关闭...")
		cancel()
	}()

	logger := log.New(os.Stdout, "[LOAD] ", log.LstdFlags|log.Lmicroseconds)
	tester, ctx := NewTester(ctx,
		WithLogger(logger),
		WithConcurrencyLimit(300),
	)

	steps := []StepConfig{
		{Concurrency: 5, Duration: 10 * time.Second, RateLimit: 0},
		{Concurrency: 20, Duration: 20 * time.Second, RateLimit: 2},
		/*
			{Concurrency: 50, Duration: 30 * time.Second, RateLimit: 5},
			{Concurrency: 100, Duration: 20 * time.Second, RateLimit: 10},
			{Concurrency: 30, Duration: 10 * time.Second, RateLimit: 0},
		*/
	}

	worker := createHTTPTester(
		"http://www.baidu.com",
		"GET",
		map[string]string{
			"User-Agent": "LoadTester/1.0",
			"Accept":     "*/*",
		},
		"",
	)

	tester.BurnSteps(ctx, steps, worker)

	if err := tester.Wait(); err != nil && err != context.Canceled {
		logger.Printf("⚠️  压测异常退出: %v", err)
	}

	tester.Stats().Report()
}
