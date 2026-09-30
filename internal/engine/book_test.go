package engine

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/fintech/matching-engine/internal/domain"
	"github.com/fintech/matching-engine/internal/storage/wal"
)

// Helper to create test orders quickly.
func makeOrder(id domain.OrderID, side domain.Side, oType domain.OrderType, px domain.Price, qty domain.Quantity, tif domain.TimeInForce) *domain.Order {
	o := domain.AcquireOrder()
	o.ID = id
	o.Side = side
	o.Type = oType
	o.Price = px
	o.Quantity = qty
	o.LeavesQty = qty
	o.TIF = tif
	o.Timestamp = time.Now().UnixNano()
	o.Status = domain.StatusNew
	return o
}

func TestPriceLevel_FIFO(t *testing.T) {
	book := NewOrderBook("BTC-USDT")

	// Place 3 sell orders at the exact same price: 100
	o1 := makeOrder(1, domain.SideSell, domain.OrderTypeLimit, 100, 10, domain.TimeInForceGTC)
	o2 := makeOrder(2, domain.SideSell, domain.OrderTypeLimit, 100, 20, domain.TimeInForceGTC)
	o3 := makeOrder(3, domain.SideSell, domain.OrderTypeLimit, 100, 30, domain.TimeInForceGTC)

	book.Process(o1)
	book.Process(o2)
	book.Process(o3)

	if book.OrderCount() != 3 {
		t.Fatalf("expected 3 resting orders, got %d", book.OrderCount())
	}

	// Incoming buy order for 25 units at price 100.
	// Should fill order 1 completely (10 units), and order 2 partially (15 units). Order 3 untouched.
	taker := makeOrder(4, domain.SideBuy, domain.OrderTypeLimit, 100, 25, domain.TimeInForceGTC)
	rep := book.Process(taker)

	if rep.Status != domain.StatusFilled {
		t.Fatalf("expected taker to be filled, got %s", rep.Status)
	}
	if len(rep.Trades) != 2 {
		t.Fatalf("expected 2 trades, got %d", len(rep.Trades))
	}

	// Verify FIFO order execution
	trade1 := rep.Trades[0]
	if trade1.MakerOrderID != 1 || trade1.Quantity != 10 {
		t.Errorf("trade 1 mismatch: %+v", trade1)
	}

	trade2 := rep.Trades[1]
	if trade2.MakerOrderID != 2 || trade2.Quantity != 15 {
		t.Errorf("trade 2 mismatch: %+v", trade2)
	}

	// Remaining in book: order 2 (leaves 5), order 3 (leaves 30)
	if book.OrderCount() != 2 {
		t.Errorf("expected 2 resting orders in book, got %d", book.OrderCount())
	}
	if book.BestAsk().TotalVolume != 35 {
		t.Errorf("expected remaining volume 35, got %d", book.BestAsk().TotalVolume)
	}
}

func TestLimitOrder_PricePriority(t *testing.T) {
	book := NewOrderBook("ETH-USDT")

	// Place asks at varying prices: 300, 310, 290
	a1 := makeOrder(1, domain.SideSell, domain.OrderTypeLimit, 300, 10, domain.TimeInForceGTC)
	a2 := makeOrder(2, domain.SideSell, domain.OrderTypeLimit, 310, 10, domain.TimeInForceGTC)
	a3 := makeOrder(3, domain.SideSell, domain.OrderTypeLimit, 290, 10, domain.TimeInForceGTC)

	book.Process(a1)
	book.Process(a2)
	book.Process(a3)

	// Best ask must be the lowest price: 290
	if book.BestAsk() == nil || book.BestAsk().Price != 290 {
		t.Fatalf("expected best ask 290, got %v", book.BestAsk())
	}

	// Aggressive buyer enters with price 305 and qty 15
	// Must match 10 @ 290, then 5 @ 300!
	buyer := makeOrder(4, domain.SideBuy, domain.OrderTypeLimit, 305, 15, domain.TimeInForceGTC)
	rep := book.Process(buyer)

	if rep.Status != domain.StatusFilled {
		t.Fatalf("expected buyer filled, got %s", rep.Status)
	}
	if len(rep.Trades) != 2 {
		t.Fatalf("expected 2 trades, got %d", len(rep.Trades))
	}
	if rep.Trades[0].Price != 290 || rep.Trades[0].Quantity != 10 {
		t.Errorf("trade 0 expected price 290, qty 10; got %+v", rep.Trades[0])
	}
	if rep.Trades[1].Price != 300 || rep.Trades[1].Quantity != 5 {
		t.Errorf("trade 1 expected price 300, qty 5; got %+v", rep.Trades[1])
	}

	// Best ask should now be 300 with leaves 5
	if book.BestAsk().Price != 300 || book.BestAsk().TotalVolume != 5 {
		t.Errorf("expected best ask 300 with 5 vol, got %d with %d vol",
			book.BestAsk().Price, book.BestAsk().TotalVolume)
	}
}

func TestOrderBook_CancelOrder(t *testing.T) {
	book := NewOrderBook("BTC-USDT")

	o1 := makeOrder(10, domain.SideBuy, domain.OrderTypeLimit, 50000, 100, domain.TimeInForceGTC)
	o2 := makeOrder(11, domain.SideBuy, domain.OrderTypeLimit, 50000, 200, domain.TimeInForceGTC)
	book.Process(o1)
	book.Process(o2)

	if book.BestBid().TotalVolume != 300 {
		t.Fatalf("expected volume 300, got %d", book.BestBid().TotalVolume)
	}

	// Cancel o1 in O(1)
	canceled, ok := book.Cancel(10)
	if !ok || canceled.Status != domain.StatusCanceled {
		t.Fatalf("failed to cancel order 10")
	}

	if book.OrderCount() != 1 {
		t.Errorf("expected 1 order left, got %d", book.OrderCount())
	}
	if book.BestBid().TotalVolume != 200 {
		t.Errorf("expected remaining volume 200, got %d", book.BestBid().TotalVolume)
	}

	// Cancel o2; price level should be deleted
	_, ok2 := book.Cancel(11)
	if !ok2 {
		t.Fatalf("failed to cancel order 11")
	}
	if book.BestBid() != nil {
		t.Errorf("expected nil best bid, got %v", book.BestBid())
	}
}

func TestTimeInForce_IOC(t *testing.T) {
	book := NewOrderBook("BTC-USDT")

	// Ask: 10 units @ 100
	ask := makeOrder(1, domain.SideSell, domain.OrderTypeLimit, 100, 10, domain.TimeInForceGTC)
	book.Process(ask)

	// IOC Buy: 30 units @ 100.
	// Matches 10 units, remaining 20 units must be CANCELED, not rested on the book.
	iocBuy := makeOrder(2, domain.SideBuy, domain.OrderTypeLimit, 100, 30, domain.TimeInForceIOC)
	rep := book.Process(iocBuy)

	if rep.Status != domain.StatusCanceled {
		t.Fatalf("expected IOC status Canceled, got %s", rep.Status)
	}
	if len(rep.Trades) != 1 || rep.Trades[0].Quantity != 10 {
		t.Fatalf("expected 1 trade with 10 qty, got %+v", rep.Trades)
	}
	if book.OrderCount() != 0 {
		t.Errorf("expected empty book, got %d orders", book.OrderCount())
	}
}

func TestTimeInForce_FOK(t *testing.T) {
	book := NewOrderBook("BTC-USDT")

	// Resting asks: 10 @ 100 and 10 @ 101 -> total 20 available
	book.Process(makeOrder(1, domain.SideSell, domain.OrderTypeLimit, 100, 10, domain.TimeInForceGTC))
	book.Process(makeOrder(2, domain.SideSell, domain.OrderTypeLimit, 101, 10, domain.TimeInForceGTC))

	// FOK Buy: 25 units @ 102. Insufficient liquidity (25 > 20) -> should be rejected with 0 trades!
	fokBuy := makeOrder(3, domain.SideBuy, domain.OrderTypeLimit, 102, 25, domain.TimeInForceFOK)
	rep := book.Process(fokBuy)

	if rep.Status != domain.StatusRejected {
		t.Fatalf("expected FOK rejected, got %s", rep.Status)
	}
	if len(rep.Trades) != 0 {
		t.Fatalf("expected 0 trades for failed FOK, got %d", len(rep.Trades))
	}
	// Original resting orders must remain intact
	if book.OrderCount() != 2 {
		t.Fatalf("expected resting orders intact, got %d", book.OrderCount())
	}

	// FOK Buy: 15 units @ 102 -> Sufficient liquidity (15 <= 20) -> fully fills!
	fokBuy2 := makeOrder(4, domain.SideBuy, domain.OrderTypeLimit, 102, 15, domain.TimeInForceFOK)
	rep2 := book.Process(fokBuy2)

	if rep2.Status != domain.StatusFilled {
		t.Fatalf("expected FOK filled, got %s", rep2.Status)
	}
	if len(rep2.Trades) != 2 {
		t.Fatalf("expected 2 trades, got %d", len(rep2.Trades))
	}
}

func TestWAL_WriteAndRecovery(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	// 1. Initialize WAL and write operations
	w, err := wal.Open(walPath, 64*1024, true)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	o1 := makeOrder(100, domain.SideBuy, domain.OrderTypeLimit, 45000, 50, domain.TimeInForceGTC)
	o2 := makeOrder(101, domain.SideSell, domain.OrderTypeLimit, 46000, 30, domain.TimeInForceGTC)
	o3 := makeOrder(102, domain.SideBuy, domain.OrderTypeLimit, 44500, 20, domain.TimeInForceGTC)

	_ = w.AppendOrderNew(o1)
	_ = w.AppendOrderNew(o2)
	_ = w.AppendOrderNew(o3)
	_ = w.AppendOrderCancel(102, time.Now().UnixNano()) // Cancel o3
	_ = w.Close()

	// 2. Replay and reconstruct order book
	recoveredBook, count, err := RecoverOrderBook("BTC-USDT", walPath)
	if err != nil {
		t.Fatalf("wal recovery failed: %v", err)
	}
	if count != 4 {
		t.Fatalf("expected 4 records recovered, got %d", count)
	}

	// Verify state
	if recoveredBook.OrderCount() != 2 {
		t.Fatalf("expected 2 active orders after cancel, got %d", recoveredBook.OrderCount())
	}
	if recoveredBook.BestBid() == nil || recoveredBook.BestBid().Price != 45000 {
		t.Errorf("expected best bid 45000, got %v", recoveredBook.BestBid())
	}
	if recoveredBook.BestAsk() == nil || recoveredBook.BestAsk().Price != 46000 {
		t.Errorf("expected best ask 46000, got %v", recoveredBook.BestAsk())
	}
}

// ---------------- BENCHMARKS ----------------

func BenchmarkOrderBook_Limit_NoMatch(b *testing.B) {
	book := NewOrderBook("BTC-USDT")
	orders := make([]*domain.Order, b.N)

	// Pre-create orders to benchmark pure engine logic without allocation interference
	for i := 0; i < b.N; i++ {
		orders[i] = &domain.Order{
			ID:        domain.OrderID(i + 1),
			Side:      domain.SideBuy,
			Type:      domain.OrderTypeLimit,
			Price:     domain.Price(10000 + (i % 500)),
			Quantity:  10,
			LeavesQty: 10,
			TIF:       domain.TimeInForceGTC,
			Status:    domain.StatusNew,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = book.Process(orders[i])
	}
}

func BenchmarkOrderBook_CrossingMatch(b *testing.B) {
	book := NewOrderBook("BTC-USDT")

	// Prepopulate sell asks
	for i := 0; i < 1000; i++ {
		o := &domain.Order{
			ID:        domain.OrderID(i + 1),
			Side:      domain.SideSell,
			Type:      domain.OrderTypeLimit,
			Price:     domain.Price(20000 + (i / 10)),
			Quantity:  1000000,
			LeavesQty: 1000000,
			TIF:       domain.TimeInForceGTC,
		}
		book.Process(o)
	}

	taker := &domain.Order{
		ID:        99999999,
		Side:      domain.SideBuy,
		Type:      domain.OrderTypeLimit,
		Price:     30000,
		Quantity:  1,
		LeavesQty: 1,
		TIF:       domain.TimeInForceGTC,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		taker.ID = domain.OrderID(10000000 + i)
		taker.LeavesQty = 1
		taker.Status = domain.StatusNew
		rep := book.Process(taker)
		domain.ReleaseExecutionReport(rep)
	}
}

func BenchmarkOrderBook_Cancel(b *testing.B) {
	book := NewOrderBook("BTC-USDT")
	orders := make([]*domain.Order, b.N)
	for i := 0; i < b.N; i++ {
		orders[i] = &domain.Order{
			ID:        domain.OrderID(i + 1),
			Side:      domain.SideBuy,
			Type:      domain.OrderTypeLimit,
			Price:     domain.Price(10000 + (i % 50)),
			Quantity:  10,
			LeavesQty: 10,
			TIF:       domain.TimeInForceGTC,
		}
		book.Process(orders[i])
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = book.Cancel(domain.OrderID(i + 1))
	}
}

