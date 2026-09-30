package domain

import (
	"sync"
)

// In high-frequency matching engines, object allocations in the hot path are the primary cause
// of garbage collection latency spikes (stop-the-world pauses).
// Using sync.Pool allows recycling structs with zero heap allocation per order/trade.

var (
	orderPool = sync.Pool{
		New: func() any {
			return new(Order)
		},
	}

	tradePool = sync.Pool{
		New: func() any {
			return new(Trade)
		},
	}

	executionReportPool = sync.Pool{
		New: func() any {
			return &ExecutionReport{
				Trades: make([]Trade, 0, 8), // Pre-allocated trades slice to avoid slice growth
			}
		},
	}
)

// AcquireOrder pulls an Order from the pool and resets its fields.
func AcquireOrder() *Order {
	o := orderPool.Get().(*Order)
	o.Reset()
	return o
}

// ReleaseOrder returns an Order to the pool after clearing pointers.
func ReleaseOrder(o *Order) {
	if o == nil {
		return
	}
	o.Reset()
	orderPool.Put(o)
}

// AcquireTrade pulls a Trade struct from the pool.
func AcquireTrade() *Trade {
	t := tradePool.Get().(*Trade)
	t.Reset()
	return t
}

// ReleaseTrade returns a Trade struct to the pool.
func ReleaseTrade(t *Trade) {
	if t == nil {
		return
	}
	t.Reset()
	tradePool.Put(t)
}

// AcquireExecutionReport retrieves an ExecutionReport from the pool.
func AcquireExecutionReport() *ExecutionReport {
	er := executionReportPool.Get().(*ExecutionReport)
	er.Reset()
	return er
}

// ReleaseExecutionReport returns an ExecutionReport to the pool.
func ReleaseExecutionReport(er *ExecutionReport) {
	if er == nil {
		return
	}
	er.Reset()
	executionReportPool.Put(er)
}
