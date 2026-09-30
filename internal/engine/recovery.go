package engine

import (
	"github.com/fintech/matching-engine/internal/domain"
	"github.com/fintech/matching-engine/internal/storage/wal"
)

// BookRecoveryHandler bridges the WAL reader with the OrderBook to reconstruct state.
type BookRecoveryHandler struct {
	book *OrderBook
}

// NewBookRecoveryHandler creates a new replay adapter.
func NewBookRecoveryHandler(book *OrderBook) *BookRecoveryHandler {
	return &BookRecoveryHandler{book: book}
}

// OnOrderNew re-executes the incoming order against the order book.
func (h *BookRecoveryHandler) OnOrderNew(order *domain.Order) error {
	_ = h.book.Process(order)
	return nil
}

// OnOrderCancel removes the canceled order from the order book.
func (h *BookRecoveryHandler) OnOrderCancel(orderID domain.OrderID, timestamp int64) error {
	_, _ = h.book.Cancel(orderID)
	return nil
}

// OnTrade is a no-op during state recovery because re-executing orders natively produces identical trades.
func (h *BookRecoveryHandler) OnTrade(trade *domain.Trade) error {
	domain.ReleaseTrade(trade)
	return nil
}

// RecoverOrderBook initializes an order book and reconstructs its exact state by replaying the WAL.
func RecoverOrderBook(symbol string, walPath string) (*OrderBook, int, error) {
	book := NewOrderBook(symbol)
	handler := NewBookRecoveryHandler(book)
	records, err := wal.Recover(walPath, handler)
	if err != nil {
		return nil, records, err
	}
	return book, records, nil
}
