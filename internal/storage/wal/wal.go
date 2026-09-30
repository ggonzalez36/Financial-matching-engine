package wal

import (
	"bufio"
	"os"
	"sync"

	"github.com/fintech/matching-engine/internal/domain"
)

// WAL represents a high-throughput, crash-resilient Write-Ahead Log.
// It persists incoming orders and executed trades before or during in-memory matching.
type WAL struct {
	mu          sync.Mutex
	file        *os.File
	writer      *bufio.Writer
	syncOnWrite bool
	payloadBuf  [64]byte  // Reusable stack-allocated buffer for encoding payloads (zero alloc)
	frameBuf    [128]byte // Reusable stack-allocated buffer for framed records
}

// Open creates or appends to a WAL file at the specified path.
// bufferSize configures the userspace bufio.Writer capacity (e.g. 64KB - 256KB).
func Open(path string, bufferSize int, syncOnWrite bool) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	if bufferSize <= 0 {
		bufferSize = 64 * 1024 // 64 KB default buffer
	}

	return &WAL{
		file:        file,
		writer:      bufio.NewWriterSize(file, bufferSize),
		syncOnWrite: syncOnWrite,
	}, nil
}

// AppendOrderNew serializes and writes an OrderNew record to disk.
func (w *WAL) AppendOrderNew(o *domain.Order) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payload := EncodeOrderNew(o, w.payloadBuf[:])
	frame := BuildFrame(RecordOrderNew, payload, w.frameBuf[:])

	if _, err := w.writer.Write(frame); err != nil {
		return err
	}

	if w.syncOnWrite {
		if err := w.writer.Flush(); err != nil {
			return err
		}
		return w.file.Sync()
	}

	return nil
}

// AppendOrderCancel serializes and writes an OrderCancel record to disk.
func (w *WAL) AppendOrderCancel(orderID domain.OrderID, timestamp int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payload := EncodeOrderCancel(orderID, timestamp, w.payloadBuf[:])
	frame := BuildFrame(RecordOrderCancel, payload, w.frameBuf[:])

	if _, err := w.writer.Write(frame); err != nil {
		return err
	}

	if w.syncOnWrite {
		if err := w.writer.Flush(); err != nil {
			return err
		}
		return w.file.Sync()
	}

	return nil
}

// AppendTrade serializes and writes an executed trade to disk.
func (w *WAL) AppendTrade(t *domain.Trade) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payload := EncodeTrade(t, w.payloadBuf[:])
	frame := BuildFrame(RecordTrade, payload, w.frameBuf[:])

	if _, err := w.writer.Write(frame); err != nil {
		return err
	}

	if w.syncOnWrite {
		if err := w.writer.Flush(); err != nil {
			return err
		}
		return w.file.Sync()
	}

	return nil
}

// Flush flushes buffered data to the operating system's page cache.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Flush()
}

// Sync ensures all buffered writes are physically written to durable media (fsync).
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close flushes, syncs, and closes the underlying WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		_ = w.file.Close()
		return err
	}
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}
