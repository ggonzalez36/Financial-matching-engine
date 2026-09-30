package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fintech/matching-engine/internal/domain"
	"github.com/fintech/matching-engine/internal/storage/wal"
)

var (
	ErrEngineStopped   = errors.New("matching engine is stopped")
	ErrSymbolNotFound  = errors.New("symbol worker not found")
	ErrQueueFull       = errors.New("inbound command queue is full (backpressure)")
)

// CommandType defines the operation type sent to a SymbolWorker.
type CommandType uint8

const (
	CmdPlaceOrder CommandType = iota + 1
	CmdCancelOrder
	CmdGetDepth
)

// Command encapsulates an inbound instruction destined for a symbol's matching loop.
type Command struct {
	Type     CommandType
	Order    *domain.Order
	CancelID domain.OrderID
	Depth    int
	// RespChan allows synchronous callers to await the result without global locks.
	RespChan chan any
}

// SymbolWorker manages a single instrument's Limit Order Book and its dedicated WAL.
// It executes in a single-threaded event loop (Actor model) to avoid any lock contention.
type SymbolWorker struct {
	symbol    string
	book      *OrderBook
	wal       *wal.WAL
	inbound   chan Command
	outbox    chan *domain.ExecutionReport
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	isRunning bool
}

// NewSymbolWorker constructs a worker for a specific symbol.
func NewSymbolWorker(symbol string, queueSize int, walInstance *wal.WAL) *SymbolWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &SymbolWorker{
		symbol:    symbol,
		book:      NewOrderBook(symbol),
		wal:       walInstance,
		inbound:   make(chan Command, queueSize),
		outbox:    make(chan *domain.ExecutionReport, queueSize),
		ctx:       ctx,
		cancel:    cancel,
		isRunning: true,
	}
}

// Start launches the dedicated worker event loop goroutine.
func (w *SymbolWorker) Start() {
	w.wg.Add(1)
	go w.eventLoop()
}

// Stop gracefully terminates the symbol worker after draining inbound commands.
func (w *SymbolWorker) Stop() {
	if !w.isRunning {
		return
	}
	w.isRunning = false
	w.cancel()
	w.wg.Wait()
	close(w.outbox)
}

// Outbox returns the channel of execution reports emitted by this worker.
func (w *SymbolWorker) Outbox() <-chan *domain.ExecutionReport {
	return w.outbox
}

// Submit enqueues a command for processing.
func (w *SymbolWorker) Submit(cmd Command) error {
	select {
	case <-w.ctx.Done():
		return ErrEngineStopped
	case w.inbound <- cmd:
		return nil
	default:
		return ErrQueueFull
	}
}

// eventLoop is the heart of the single-writer matching engine for this instrument.
// It processes orders, writes to the WAL, and emits execution events with zero locks.
func (w *SymbolWorker) eventLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			// Drain remaining messages before exiting
			w.drainInbound()
			return

		case cmd := <-w.inbound:
			w.handleCommand(cmd)
		}
	}
}

func (w *SymbolWorker) handleCommand(cmd Command) {
	switch cmd.Type {
	case CmdPlaceOrder:
		// 1. Write to Write-Ahead Log BEFORE matching (Strict Durability)
		if w.wal != nil {
			_ = w.wal.AppendOrderNew(cmd.Order)
		}

		// 2. Execute in-memory matching
		report := w.book.Process(cmd.Order)

		// 3. Log executed trades to WAL if any occurred
		if w.wal != nil && len(report.Trades) > 0 {
			for i := range report.Trades {
				_ = w.wal.AppendTrade(&report.Trades[i])
			}
		}

		// 4. Dispatch report to outbox for downstream consumers (gRPC/WebSocket feeds)
		select {
		case w.outbox <- report:
		default:
			// Non-blocking drop or overflow alert in extreme load
		}

		// 5. Notify synchronous caller if response channel was provided
		if cmd.RespChan != nil {
			cmd.RespChan <- report
		}

	case CmdCancelOrder:
		// 1. Log cancel request to WAL
		if w.wal != nil {
			_ = w.wal.AppendOrderCancel(cmd.CancelID, time.Now().UnixNano())
		}

		// 2. Perform O(1) in-memory cancellation
		canceledOrder, ok := w.book.Cancel(cmd.CancelID)

		report := domain.AcquireExecutionReport()
		report.Timestamp = time.Now().UnixNano()
		if ok {
			report.SubjectOrder = canceledOrder
			report.Status = domain.StatusCanceled
			report.EventType = domain.EventOrderCanceled
		} else {
			report.Status = domain.StatusRejected
			report.EventType = domain.EventOrderRejected
			report.RejectReason = fmt.Sprintf("order ID %d not found for cancellation", cmd.CancelID)
		}

		select {
		case w.outbox <- report:
		default:
		}

		if cmd.RespChan != nil {
			cmd.RespChan <- report
		}

	case CmdGetDepth:
		depth := w.book.GetDepth(cmd.Depth)
		if cmd.RespChan != nil {
			cmd.RespChan <- depth
		}
	}
}

func (w *SymbolWorker) drainInbound() {
	for {
		select {
		case cmd := <-w.inbound:
			w.handleCommand(cmd)
		default:
			return
		}
	}
}

// EngineDispatcher manages sharded SymbolWorkers across multiple trading instruments.
// It routes inbound traffic to the correct single-writer actor without global bottlenecks.
type EngineDispatcher struct {
	mu      sync.RWMutex
	workers map[string]*SymbolWorker
}

// NewEngineDispatcher creates a new multi-instrument dispatcher.
func NewEngineDispatcher() *EngineDispatcher {
	return &EngineDispatcher{
		workers: make(map[string]*SymbolWorker),
	}
}

// RegisterSymbol initializes and starts a worker for the given symbol.
func (d *EngineDispatcher) RegisterSymbol(symbol string, queueSize int, walInstance *wal.WAL) *SymbolWorker {
	d.mu.Lock()
	defer d.mu.Unlock()

	if worker, exists := d.workers[symbol]; exists {
		return worker
	}

	worker := NewSymbolWorker(symbol, queueSize, walInstance)
	worker.Start()
	d.workers[symbol] = worker
	return worker
}

// GetWorker retrieves the worker for a symbol.
func (d *EngineDispatcher) GetWorker(symbol string) (*SymbolWorker, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	worker, exists := d.workers[symbol];
	if !exists {
		return nil, ErrSymbolNotFound
	}
	return worker, nil
}

// StopAll stops all symbol workers gracefully.
func (d *EngineDispatcher) StopAll() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, worker := range d.workers {
		worker.Stop()
	}
}
