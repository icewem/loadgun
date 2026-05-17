package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// result — результат одного HTTP-запроса
type result struct {
	statusCode int
	duration   time.Duration
	err        error
}

// stats — итоговая статистика нагрузочного теста
type stats struct {
	total      int
	success    int
	failed     int
	status2xx  int
	status4xx  int
	status5xx  int
	latencies  []time.Duration
	minLatency time.Duration
	maxLatency time.Duration
	sumLatency time.Duration
}

func main() {
	url := flag.String("url", "http://localhost:8080/health", "target url")

	method := flag.String("method", "GET", "http method")

	body := flag.String("body", "", "request body")

	rps := flag.Int("rps", 10, "requests per second")

	duration := flag.Duration("duration", 10*time.Second, "test duration")

	workers := flag.Int("workers", 5, "number of workers")

	timeout := flag.Duration("timeout", 2*time.Second, "request timeout")

	flag.Parse()

	if *rps <= 0 {
		fmt.Println("rps must be > 0")
		return
	}

	if *workers <= 0 {
		fmt.Println("workers must be > 0")
		return
	}

	client := &http.Client{
		Timeout: *timeout,
	}

	// jobs — очередь задач
	jobs := make(chan struct{}, *rps)

	// results — очередь результатов
	results := make(chan result, *workers)

	var wg sync.WaitGroup

	// Запускаем worker pool
	for i := 0; i < *workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for range jobs {
				start := time.Now()

				req, err := http.NewRequest(*method, *url, bytes.NewBufferString(*body))
				if err != nil {
					results <- result{
						duration: time.Since(start),
						err:      err,
					}
					continue
				}

				if *body != "" {
					req.Header.Set("Content-Type", "application/json")
				}

				resp, err := client.Do(req)
				if err != nil {
					results <- result{
						duration: time.Since(start),
						err:      err,
					}
					continue
				}

				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				results <- result{
					statusCode: resp.StatusCode,
					duration:   time.Since(start),
					err:        nil,
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var st stats
	var statsWG sync.WaitGroup
	statsWG.Add(1)

	// Читаем результаты параллельно с выполнением теста
	go func() {
		defer statsWG.Done()

		for res := range results {
			st.total++

			st.latencies = append(st.latencies, res.duration)
			st.sumLatency += res.duration

			if st.minLatency == 0 || res.duration < st.minLatency {
				st.minLatency = res.duration
			}

			if res.duration > st.maxLatency {
				st.maxLatency = res.duration
			}

			if res.err != nil {
				st.failed++
				continue
			}

			// 2xx считаем успешным ответом
			if res.statusCode >= 200 && res.statusCode < 300 {
				st.success++
				st.status2xx++
				continue
			}

			// Всё, что не 2xx, считаем failed
			st.failed++

			// 400 + 500
			if res.statusCode >= 400 && res.statusCode < 500 {
				st.status4xx++
			}

			if res.statusCode >= 500 {
				st.status5xx++
			}
		}
	}()

	testDone := time.After(*duration)

	interval := time.Second / time.Duration(*rps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sent := 0

producerLoop:
	for {
		select {
		case <-ticker.C:
			jobs <- struct{}{}
			sent++

		case <-testDone:
			break producerLoop
		}
	}

	close(jobs)

	statsWG.Wait()

	if st.total == 0 {
		fmt.Println("no requests completed")
		return
	}

	sort.Slice(st.latencies, func(i, j int) bool {
		return st.latencies[i] < st.latencies[j]
	})

	avgLatency := st.sumLatency / time.Duration(st.total)

	fmt.Println("====== LOAD TEST RESULT ======")
	fmt.Printf("target:         %s %s\n", *method, *url)
	fmt.Printf("duration:       %s\n", *duration)
	fmt.Printf("configured rps: %d\n", *rps)
	fmt.Printf("workers:        %d\n", *workers)
	fmt.Printf("sent:           %d\n", sent)
	fmt.Printf("completed:      %d\n", st.total)
	fmt.Printf("success:        %d\n", st.success)
	fmt.Printf("failed:         %d\n", st.failed)
	fmt.Printf("2xx:            %d\n", st.status2xx)
	fmt.Printf("4xx:            %d\n", st.status4xx)
	fmt.Printf("5xx:            %d\n", st.status5xx)
	fmt.Printf("throughput:     %.2f req/s\n", float64(st.total)/duration.Seconds())
	fmt.Printf("avg latency:    %s\n", avgLatency)
	fmt.Printf("min latency:    %s\n", st.minLatency)
	fmt.Printf("max latency:    %s\n", st.maxLatency)
	fmt.Printf("p95 latency:    %s\n", percentile(st.latencies, 95))
	fmt.Printf("p99 latency:    %s\n", percentile(st.latencies, 99))
}

// percentile возвращает pXX latency.
func percentile(values []time.Duration, p int) time.Duration {
	if len(values) == 0 {
		return 0
	}

	index := (len(values)*p + 99) / 100
	index--

	if index < 0 {
		index = 0
	}

	if index >= len(values) {
		index = len(values) - 1
	}

	return values[index]
}
