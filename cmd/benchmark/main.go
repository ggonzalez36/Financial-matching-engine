package main

import (
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/fintech/matching-engine/internal/domain"
	"github.com/fintech/matching-engine/internal/engine"
)

func main() {
	fmt.Println("========================================================================")
	fmt.Println("  FINANCIAL MATCHING ENGINE: ULTRA-LOW-LATENCY STRESS BENCHMARK")
	fmt.Println("========================================================================")

	const totalOrders = 1_000_000

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	book := engine.NewOrderBook("BTC-USDT")

	// Pre-fill asks in the book to match against
	fmt.Printf("Populating 10,000 resting liquidity maker orders...\n")
	for i := 0; i < 10000; i++ {
		o := domain.AcquireOrder()
		o.ID = domain.OrderID(i + 1)
		o.Side = domain.SideSell
		o.Type = domain.OrderTypeLimit
		o.Price = domain.Price(50000 + (i % 50)) // tightly clustered around 50,000
		o.Quantity = 1000000
		o.LeavesQty = 1000000
		o.TIF = domain.TimeInForceGTC
		o.Timestamp = time.Now().UnixNano()
		book.Process(o)
	}

	fmt.Printf("Executing %d aggressive crossing orders through the hot path...\n", totalOrders)

	latencies := make([]int64, totalOrders)

	taker := domain.AcquireOrder()
	taker.Side = domain.SideBuy
	taker.Type = domain.OrderTypeLimit
	taker.Price = 50050
	taker.Quantity = 1
	taker.TIF = domain.TimeInForceGTC

	start := time.Now()

	for i := 0; i < totalOrders; i++ {
		t0 := time.Now().UnixNano()

		taker.ID = domain.OrderID(1000000 + i)
		taker.LeavesQty = 1
		taker.Status = domain.StatusNew

		rep := book.Process(taker)

		t1 := time.Now().UnixNano()
		latencies[i] = t1 - t0

		domain.ReleaseExecutionReport(rep)
	}

	totalDuration := time.Since(start)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Sort latencies to calculate accurate percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := time.Duration(latencies[len(latencies)*50/100])
	p90 := time.Duration(latencies[len(latencies)*90/100])
	p99 := time.Duration(latencies[len(latencies)*99/100])
	p999 := time.Duration(latencies[len(latencies)*999/1000])
	maxLat := time.Duration(latencies[len(latencies)-1])
	minLat := time.Duration(latencies[0])

	opsPerSec := float64(totalOrders) / totalDuration.Seconds()
	bytesAllocated := memAfter.TotalAlloc - memBefore.TotalAlloc
	gcCount := memAfter.NumGC - memBefore.NumGC

	fmt.Println("\n----------------------- BENCHMARK RESULTS -----------------------")
	fmt.Printf("Total Orders Processed : %d\n", totalOrders)
	fmt.Printf("Total Time Elapsed     : %v\n", totalDuration)
	fmt.Printf("Throughput (Ops/sec)   : %.2f orders/sec\n", opsPerSec)
	fmt.Printf("Mean Latency per Order : %v\n", totalDuration/totalOrders)
	fmt.Printf("Min Latency            : %v\n", minLat)
	fmt.Printf("P50 (Median) Latency   : %v\n", p50)
	fmt.Printf("P90 Latency            : %v\n", p90)
	fmt.Printf("P99 Latency            : %v\n", p99)
	fmt.Printf("P99.9 Latency          : %v\n", p999)
	fmt.Printf("Max (Worst-Case)       : %v\n", maxLat)
	fmt.Println("-------------------- RUNTIME & MEMORY STATS --------------------")
	fmt.Printf("Heap Allocated         : %d bytes (%.2f MB)\n", bytesAllocated, float64(bytesAllocated)/(1024*1024))
	fmt.Printf("GC Pause Cycles        : %d\n", gcCount)
	fmt.Println("=================================================================")
}
