package engine

import (
	"time"

	"github.com/fintech/matching-engine/internal/domain"
)

// OrderBook represents an ultra-low-latency in-memory Limit Order Book (LOB) for a single trading pair.
//
// ARCHITECTURAL DESIGN (SINGLE-WRITER ACTOR MODEL):
// This OrderBook is strictly non-thread-safe by design!
// In high-frequency matching engines (LMAX Disruptor, Binance 2.0, CME Globex), synchronization locks
// (sync.RWMutex, sync.Mutex) introduce devastating CPU cache-line bouncing and thread contention under load.
//
// Instead, every OrderBook is owned by a single dedicated worker goroutine (actor) fed via an uncontented
// channel or ring buffer. This guarantees:
// 1. 100% deterministic sequential execution.
// 2. Zero lock overhead (0 CPU cycles spent on mutex lock/unlock).
// 3. Perfect L1/L2 data cache locality.
type OrderBook struct {
	symbol  string
	bids    *rbTree                       // Red-Black Tree for Bids (descending price priority)
	asks    *rbTree                       // Red-Black Tree for Asks (ascending price priority)
	orders  map[domain.OrderID]*domain.Order // O(1) order lookup by ID (for instant cancel/amend)
	levels  map[domain.Price]*PriceLevel     // O(1) price level lookup by Price
	bestBid *PriceLevel                   // Cached O(1) pointer to top of Bid book
	bestAsk *PriceLevel                   // Cached O(1) pointer to top of Ask book
}

// NewOrderBook initializes an empty order book for a given instrument symbol.
func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		symbol: symbol,
		bids:   newRBTree(),
		asks:   newRBTree(),
		orders: make(map[domain.OrderID]*domain.Order, 1024),
		levels: make(map[domain.Price]*PriceLevel, 256),
	}
}

// Symbol returns the trading symbol of the order book (e.g. "BTC-USDT").
func (ob *OrderBook) Symbol() string {
	return ob.symbol
}

// BestBid returns the current highest bid price level, or nil if no bids exist. O(1) cached.
func (ob *OrderBook) BestBid() *PriceLevel {
	return ob.bestBid
}

// BestAsk returns the current lowest ask price level, or nil if no asks exist. O(1) cached.
func (ob *OrderBook) BestAsk() *PriceLevel {
	return ob.bestAsk
}

// OrderCount returns total number of active resting orders in the book.
func (ob *OrderBook) OrderCount() int {
	return len(ob.orders)
}

// Process receives an incoming order and matches or rests it, returning an ExecutionReport.
// Memory allocation is minimized by using pooled ExecutionReports and trades slices.
func (ob *OrderBook) Process(order *domain.Order) *domain.ExecutionReport {
	report := domain.AcquireExecutionReport()
	report.SubjectOrder = order
	report.Timestamp = time.Now().UnixNano()

	// 1. Parameter Validation
	if order.Quantity == 0 || order.LeavesQty == 0 {
		report.Status = domain.StatusRejected
		report.EventType = domain.EventOrderRejected
		report.RejectReason = "order quantity must be greater than zero"
		return report
	}

	if order.Type == domain.OrderTypeLimit && order.Price == 0 {
		report.Status = domain.StatusRejected
		report.EventType = domain.EventOrderRejected
		report.RejectReason = "limit order price must be greater than zero"
		return report
	}

	if _, exists := ob.orders[order.ID]; exists {
		report.Status = domain.StatusRejected
		report.EventType = domain.EventOrderRejected
		report.RejectReason = "duplicate order ID"
		return report
	}

	// 2. Pre-check for FOK (Fill-Or-Kill): must guarantee complete execution immediately
	if order.TIF == domain.TimeInForceFOK {
		if !ob.canFullyFill(order) {
			report.Status = domain.StatusRejected
			report.EventType = domain.EventOrderRejected
			report.RejectReason = "FOK order cannot be fully filled against current liquidity"
			return report
		}
	}

	// 3. Execute Matching Logic
	switch order.Type {
	case domain.OrderTypeLimit:
		ob.matchLimit(order, report)
	case domain.OrderTypeMarket:
		ob.matchMarket(order, report)
	default:
		report.Status = domain.StatusRejected
		report.EventType = domain.EventOrderRejected
		report.RejectReason = "unsupported order type"
	}

	return report
}

// matchLimit matches an incoming aggressive limit order against opposite resting maker orders.
// Any unfilled remaining volume is either placed on the book (GTC) or canceled (IOC).
func (ob *OrderBook) matchLimit(taker *domain.Order, report *domain.ExecutionReport) {
	if taker.Side == domain.SideBuy {
		// Buyer wants lowest available ask price (Ask <= taker.Price)
		for taker.LeavesQty > 0 && ob.bestAsk != nil && ob.bestAsk.Price <= taker.Price {
			ob.matchAtLevel(ob.bestAsk, taker, report)
		}
	} else {
		// Seller wants highest available bid price (Bid >= taker.Price)
		for taker.LeavesQty > 0 && ob.bestBid != nil && ob.bestBid.Price >= taker.Price {
			ob.matchAtLevel(ob.bestBid, taker, report)
		}
	}

	// Handle Remaining Quantity
	if taker.LeavesQty == 0 {
		taker.Status = domain.StatusFilled
		report.Status = domain.StatusFilled
		report.EventType = domain.EventOrderFilled
	} else if taker.LeavesQty < taker.Quantity {
		taker.Status = domain.StatusPartiallyFilled
		report.Status = domain.StatusPartiallyFilled
		report.EventType = domain.EventOrderPartiallyFilled

		if taker.TIF == domain.TimeInForceIOC {
			// IOC cancels remaining unexecuted balance
			taker.Status = domain.StatusCanceled
			report.Status = domain.StatusCanceled
			report.EventType = domain.EventOrderCanceled
		} else {
			// GTC rests remaining balance in the book
			ob.restOrder(taker)
		}
	} else {
		// No match occurred (zero fill)
		if taker.TIF == domain.TimeInForceIOC {
			taker.Status = domain.StatusCanceled
			report.Status = domain.StatusCanceled
			report.EventType = domain.EventOrderCanceled
		} else {
			taker.Status = domain.StatusNew
			report.Status = domain.StatusNew
			report.EventType = domain.EventOrderPlaced
			ob.restOrder(taker)
		}
	}
}

// matchMarket sweeps available liquidity across price levels until filled or book is exhausted.
func (ob *OrderBook) matchMarket(taker *domain.Order, report *domain.ExecutionReport) {
	if taker.Side == domain.SideBuy {
		for taker.LeavesQty > 0 && ob.bestAsk != nil {
			ob.matchAtLevel(ob.bestAsk, taker, report)
		}
	} else {
		for taker.LeavesQty > 0 && ob.bestBid != nil {
			ob.matchAtLevel(ob.bestBid, taker, report)
		}
	}

	if taker.LeavesQty == 0 {
		taker.Status = domain.StatusFilled
		report.Status = domain.StatusFilled
		report.EventType = domain.EventOrderFilled
	} else if taker.LeavesQty < taker.Quantity {
		// Market order partially filled due to exhaustion of book; unexecuted balance is expired/canceled
		taker.Status = domain.StatusPartiallyFilled
		report.Status = domain.StatusPartiallyFilled
		report.EventType = domain.EventOrderPartiallyFilled
	} else {
		// Book was empty
		taker.Status = domain.StatusRejected
		report.Status = domain.StatusRejected
		report.EventType = domain.EventOrderRejected
		report.RejectReason = "insufficient book liquidity for market order"
	}
}

// matchAtLevel processes matches at a specific price level in FIFO (Price-Time) order.
func (ob *OrderBook) matchAtLevel(level *PriceLevel, taker *domain.Order, report *domain.ExecutionReport) {
	for taker.LeavesQty > 0 && !level.IsEmpty() {
		maker := level.Head()

		// Calculate execution volume
		matchQty := taker.LeavesQty
		if maker.LeavesQty < matchQty {
			matchQty = maker.LeavesQty
		}

		// Update order balances
		taker.LeavesQty -= matchQty
		maker.LeavesQty -= matchQty
		level.TotalVolume -= matchQty

		// Record Trade (Zero allocation: Trade struct appended into preallocated report.Trades)
		report.Trades = append(report.Trades, domain.Trade{
			MakerOrderID: maker.ID,
			TakerOrderID: taker.ID,
			Price:        maker.Price, // Trade execution price is always maker's resting limit price
			Quantity:     matchQty,
			Timestamp:    report.Timestamp,
			MakerSide:    maker.Side,
		})

		// Check if maker order is filled
		if maker.LeavesQty == 0 {
			maker.Status = domain.StatusFilled
			level.Dequeue()
			delete(ob.orders, maker.ID)
		} else {
			maker.Status = domain.StatusPartiallyFilled
			break // Taker is fully filled since maker still had leaves
		}
	}

	// If price level has no more orders, clean it up from book and tree
	if level.IsEmpty() {
		ob.removePriceLevel(level)
	}
}

// restOrder adds an unfilled or partially filled limit order to the resting order book.
func (ob *OrderBook) restOrder(order *domain.Order) {
	level, exists := ob.levels[order.Price]
	if !exists {
		level = acquirePriceLevel(order.Price)
		ob.levels[order.Price] = level

		if order.Side == domain.SideBuy {
			ob.bids.Insert(order.Price, level)
			// Update cached bestBid
			if ob.bestBid == nil || order.Price > ob.bestBid.Price {
				ob.bestBid = level
			}
		} else {
			ob.asks.Insert(order.Price, level)
			// Update cached bestAsk
			if ob.bestAsk == nil || order.Price < ob.bestAsk.Price {
				ob.bestAsk = level
			}
		}
	}

	level.Enqueue(order)
	ob.orders[order.ID] = order
}

// removePriceLevel removes an emptied price level from trees and maps.
func (ob *OrderBook) removePriceLevel(level *PriceLevel) {
	price := level.Price
	delete(ob.levels, price)

	if ob.bestBid != nil && ob.bestBid.Price == price {
		ob.bids.Delete(price)
		ob.bestBid = ob.bids.Max()
	} else if ob.bestAsk != nil && ob.bestAsk.Price == price {
		ob.asks.Delete(price)
		ob.bestAsk = ob.asks.Min()
	} else {
		// Not the current best; determine side by checking bid tree
		if ob.bids.Search(price) != nil {
			ob.bids.Delete(price)
		} else {
			ob.asks.Delete(price)
		}
	}

	releasePriceLevel(level)
}

// Cancel removes an order by ID from the book in O(1) time.
func (ob *OrderBook) Cancel(orderID domain.OrderID) (*domain.Order, bool) {
	order, exists := ob.orders[orderID]
	if !exists {
		return nil, false
	}

	level, exists := ob.levels[order.Price]
	if !exists {
		delete(ob.orders, orderID)
		return order, false
	}

	level.Remove(order)
	delete(ob.orders, orderID)
	order.Status = domain.StatusCanceled

	if level.IsEmpty() {
		ob.removePriceLevel(level)
	}

	return order, true
}

// canFullyFill checks whether opposite resting liquidity is sufficient to satisfy a FOK order.
func (ob *OrderBook) canFullyFill(order *domain.Order) bool {
	needed := order.LeavesQty
	var accumulated domain.Quantity

	if order.Side == domain.SideBuy {
		ob.asks.InOrder(func(level *PriceLevel) bool {
			if order.Type == domain.OrderTypeLimit && level.Price > order.Price {
				return false // Exceeded limit price
			}
			accumulated += level.TotalVolume
			return accumulated < needed
		})
	} else {
		ob.bids.ReverseInOrder(func(level *PriceLevel) bool {
			if order.Type == domain.OrderTypeLimit && level.Price < order.Price {
				return false // Below limit price
			}
			accumulated += level.TotalVolume
			return accumulated < needed
		})
	}

	return accumulated >= needed
}

// GetDepth generates a Level-2 (L2) market depth snapshot up to maxLevels.
func (ob *OrderBook) GetDepth(maxLevels int) domain.MarketDepth {
	depth := domain.MarketDepth{
		Symbol:    ob.symbol,
		Timestamp: time.Now().UnixNano(),
		Bids:      make([]domain.PriceLevelSnapshot, 0, maxLevels),
		Asks:      make([]domain.PriceLevelSnapshot, 0, maxLevels),
	}

	// Traverse Bids descending (highest first)
	ob.bids.ReverseInOrder(func(level *PriceLevel) bool {
		depth.Bids = append(depth.Bids, domain.PriceLevelSnapshot{
			Price:      level.Price,
			Quantity:   level.TotalVolume,
			OrderCount: level.OrderCount,
		})
		return len(depth.Bids) < maxLevels
	})

	// Traverse Asks ascending (lowest first)
	ob.asks.InOrder(func(level *PriceLevel) bool {
		depth.Asks = append(depth.Asks, domain.PriceLevelSnapshot{
			Price:      level.Price,
			Quantity:   level.TotalVolume,
			OrderCount: level.OrderCount,
		})
		return len(depth.Asks) < maxLevels
	})

	return depth
}
